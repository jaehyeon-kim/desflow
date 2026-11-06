"""The tasks of a model, and the start of their instances in a run.

`Tasks` holds what a model declares for its tasks: the `TaskSpec` of each task, the
gates of the lookups, the tasks whose rows a `row` generator reads and the tasks a
`previous` reads. Its methods make and start the instances of a task in a run. They
take the context, for its public reads, and the run's `Parts`, as their first two
arguments, so `Tasks` holds nothing of a run and no reference to the context.
"""

import contextlib
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    Mapping,
    Optional,
    Set,
    Tuple,
)

import simpy

from desflow.core.arrivals import whole
from desflow.core.expressions import Expression, Scope
from desflow.core.families import Reference
from desflow.core.scenario import apply_step
from desflow.core.task import TaskRun, TaskSpec
from desflow.models.flow import Entity, Upstream
from desflow.values.models import _Gen, has_generator

if TYPE_CHECKING:
    from desflow.core.context import SimulationContext
    from desflow.core.runs import Parts

logger = logging.getLogger("desflow.core.context")

# The default of `Tasks.new_run`'s `task_id`: the task's next instance id
_NEXT: Any = object()


def named(run: TaskRun) -> Any:
    """The generator of an instance, named after its task, not `run`: SimPy names a
    process after its generator's `__name__`, so `active_process.name` and the records
    read the task (P6).

    Args:
        run: The task instance.
    """
    generator = run.run()
    generator.__name__ = run.spec.name  # type: ignore[attr-defined]
    return generator


def _defuse(event: Any) -> None:
    """Marks a failed child flow as handled, so a failure nobody waits for ends that
    flow and not the run, as the builder and YAML treat every task failure."""
    if not event.ok:
        event.defused = True


class Tasks:
    """What a model declares for its tasks.

    Attributes:
        specs: The `TaskSpec` of each task, by name.
        gates: For each task, the tasks whose `lookup` gates its start.
        row_targets: The tasks whose rows a `row` generator reads.
        rows_deferred: Whether the `row` files are read later, while a blueprint
            builds the model.
        previous_users: The tasks a `previous` generator reads.
        takes_context: Whether a task's function takes the context after its id, as
            the function of a YAML task does.
    """

    def __init__(self) -> None:
        self.specs: Dict[str, TaskSpec] = {}
        self.gates: Dict[str, Set[str]] = {}
        self.row_targets: Set[str] = set()
        self.rows_deferred = False
        self.previous_users: Set[str] = set()
        self.takes_context = False

    def args_for(self, context: "SimulationContext") -> Tuple[Any, ...]:
        """The positional arguments a task's function takes after the task id: the
        context for a YAML task, and none for a builder task.

        Args:
            context: The context, which a YAML task's function takes.
        """
        return (context,) if self.takes_context else ()

    @contextlib.contextmanager
    def deferred_rows(self) -> Iterator[None]:
        """Leaves the `row` files unread while a blueprint builds the model."""
        self.rows_deferred = True
        try:
            yield
        finally:
            self.rows_deferred = False

    def new_run(
        self,
        context: "SimulationContext",
        parts: "Parts",
        task: str,
        entity: Entity,
        upstream: Optional[Upstream],
        *,
        child: bool = False,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        arg_values: Optional[Dict[str, Any]] = None,
        handles: Any = None,
        task_id: Any = _NEXT,
    ) -> TaskRun:
        """Makes one instance of `task`, with the task's spec and its next instance id.

        Args:
            context: The context, for its public reads.
            parts: The run's parts.
            task: The task's name.
            entity: The entity the instance runs for.
            upstream: What the previous task handed on.
            child: Whether the instance is the first of a child flow.
            args: The positional arguments after the task id; None gives the ones a
                task's function takes (`args_for`).
            kwargs: The keyword arguments of the task's function; None gives none.
            arg_values: The names the flow reads as `arg.<name>`.
            handles: The table of the flow that starts it, read as `handle.<name>`.
            task_id: The instance id; the task's next instance id when not given.
        """
        spec = self.specs[task]
        return TaskRun(
            context,
            parts,
            spec,
            context.next_instance(task) if task_id is _NEXT else task_id,
            entity,
            upstream,
            self.args_for(context) if args is None else args,
            {} if kwargs is None else kwargs,
            child=child,
            arg_values=arg_values,
            handles=handles,
        )

    def start(
        self,
        context: "SimulationContext",
        parts: "Parts",
        task: str,
        entity: Entity,
        upstream: Optional[Upstream],
        child: bool = False,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        arg_values: Optional[Dict[str, Any]] = None,
        handles: Any = None,
    ) -> simpy.Process:
        """Starts one instance of a task as a process, with its next instance id.

        `args` are the positional arguments after the task id; None gives the ones a
        YAML task takes, the context, and none for a builder task. `arg_values` are
        the names the flow reads as `arg.<name>`; `handles` is the table of the flow that
        starts it, which it reads as `handle.<name>`.

        Args:
            context: The context, for its public reads.
            parts: The run's parts.
            task: The task's name.
            entity: The entity the instance runs for.
            upstream: What the previous task handed on.
            child: Whether the instance is the first of a child flow.
            args: The positional arguments after the task id; None gives the ones a task's
                function takes (`args_for`).
            kwargs: The keyword arguments of the task's function; None gives none.
            arg_values: The names the flow reads as `arg.<name>`.
            handles: The table of the flow that starts it, read as `handle.<name>`.
        """
        run = self.new_run(
            context,
            parts,
            task,
            entity,
            upstream,
            child=child,
            args=args,
            kwargs=kwargs or {},
            arg_values=arg_values,
            handles=handles,
        )
        process = context.env.process(named(run))
        run.reach(process)
        if child and parts.run.on_unhandled_failure == "continue":
            # Under `continue`, a child flow that fails with no task waiting for it
            # ends that flow, not the run; under `stop` the failure stops the run.
            process.callbacks.append(_defuse)
        return process

    def spawn_children(
        self,
        context: "SimulationContext",
        parts: "Parts",
        spec: Mapping[str, Any],
        parent: Optional[Entity],
        upstream: Optional[Upstream],
        scope: Optional[Scope] = None,
    ) -> None:
        """Starts the entities of a `spawn:` key or `on:` action: `count` of them, read
        once before the first starts, each by `_spawn_entity`. The count and the `args`
        read `scope`, the spawning task's names when it is given, and the run and the
        parent's attributes otherwise; each child reads its `args` once.

        A count of 0 or less starts none, as `range` gives none. A function of the
        entity gives the count, and a result that is not a whole number (a boolean
        counts as 1) raises `TypeError`, as `range(2.0)` does.

        Args:
            context: The context, for its public reads.
            parts: The run's parts.
            spec: The `spawn:` key or `on:` action.
            parent: The spawning entity; None for none.
            upstream: What the spawning task hands on.
            scope: The spawning task's names; None reads the run and the parent's
                attributes.
        """
        count = spec["count"]
        if scope is None:
            scope = context.values.scope_of(parent)
        if callable(count):
            count = count(parent)
        elif isinstance(count, Expression):
            count = count.evaluate(scope)
        elif isinstance(count, _Gen):
            count = context.values.evaluate(count, scope)
        count = whole(count, f"the spawn count of task '{spec['task']}'")
        args = spec.get("args")
        for _ in range(count):
            self.spawn_entity(
                context,
                parts,
                spec["task"],
                parent,
                upstream,
                spec["after"],
                spec.get("copy_attributes", False),
                None if args is None else context.values.evaluate(dict(args), scope),
            )

    def spawn_entity(
        self,
        context: "SimulationContext",
        parts: "Parts",
        task: str,
        parent: Optional[Entity],
        upstream: Optional[Upstream],
        after: Optional[float],
        copy_attributes: bool = False,
        arg_values: Optional[Dict[str, Any]] = None,
    ) -> simpy.Process:
        """Starts `task` as the first task of a new entity, keyed `<task>-n`, with
        `parent` as its `spawned_by` and `attr.parent` the parent's key.

        Without `after` the entity is made and counted in now; with `after` a wrapper
        named after the task makes and counts it in when the task starts, built with
        SimPy's `start_delayed` so the events order as SimPy's.

        Args:
            context: The context, for its public reads.
            parts: The run's parts.
            task: The new entity's first task.
            parent: The spawning entity; None for none.
            upstream: What the spawning task hands on.
            after: The delay before the task starts; None for now.
            copy_attributes: Whether the new entity starts with the parent's attributes.
            arg_values: The names the new flow reads as `arg.<name>`.
        """
        parent_key = parent.key if parent is not None else None

        def make() -> Any:
            # A copy is one level deep: a nested mapping stays shared with the parent
            copied = dict(parent.attributes) if copy_attributes and parent else {}
            entity = parts.run.arrivals.new_entity(
                task, ({**copied, "parent": parent_key}, None)
            )
            entity.spawned_by = parent
            entity.entered = True
            parts.run.entity_entered(entity)
            run = self.new_run(
                context, parts, task, entity, upstream, kwargs={}, arg_values=arg_values
            )
            return run, named(run)

        if after is None:
            run, gen = make()
            process = context.env.process(gen)
            run.reach(process)
            return process

        def wrapper() -> Any:
            return (yield from make()[1])

        delayed = wrapper()
        delayed.__name__ = task  # type: ignore[attr-defined]
        import simpy.util

        return simpy.util.start_delayed(context.env, delayed, after)

    def run_on_action(
        self,
        context: "SimulationContext",
        parts: "Parts",
        action: Dict[str, Any],
        owner: str,
        entity: Optional[Entity],
        upstream: Optional[Upstream],
        names: Optional[Mapping[str, Any]] = None,
        scope: Optional[Scope] = None,
    ) -> None:
        """Runs one `on:` action: fire a signal, spawn a new entity, interrupt a
        task, or set a registry value. `owner` labels the action for a warning,
        `entity` is the entity it runs for, and `upstream` is what a spawn hands on.
        An entry with `when` runs only when its expression is true in the entity's
        scope, which is read before any value is evaluated.
        A value that holds a generator is evaluated first, over the entity's
        attributes and, for a task's action, the task's `scope`: its vars, `arg`, `item`,
        `upstream`, `process` and the rest.

        Args:
            context: The context, for its public reads.
            parts: The run's parts.
            action: The `on:` entry.
            owner: What the action belongs to, for a warning.
            entity: The entity it runs for; None for none.
            upstream: What a spawn hands on.
            names: The bare names its expressions read; None for none.
            scope: The task's scope, for a task's action; None otherwise.
        """
        needs_scope = action.get("when") is not None or (
            "spawn" not in action
            and (
                has_generator(action)
                or isinstance(action.get("fire", {}).get("signal"), Reference)
            )
        )
        if scope is None and needs_scope:
            assert parts.scope is not None
            scope = parts.scope.scope(
                upstream,
                {} if entity is None else entity.attributes,
                names=names,
                entity=None if entity is None else entity.key,
            )
        when = action.get("when")
        if when is not None:
            assert scope is not None
            if not when.evaluate(scope):
                return
        # A spawn reads its count and its args itself, once for each child
        if needs_scope and scope is not None and "spawn" not in action:
            action = context.values.evaluate(action, scope)
        if "fire" in action:
            spec = action["fire"]
            copied = self.copy_outcome(parts, spec, entity)
            if copied is None:
                logger.warning(
                    "the on: action of %s fires '%s' from '%s', which has nothing "
                    "to copy yet; it fires nothing",
                    owner,
                    spec["signal"],
                    spec["from"],
                )
                return
            ok, value = copied
            target = spec["signal"]
            if isinstance(target, Reference):
                assert scope is not None
                target = parts.run.families.open_reference(target, scope)
            parts.run.signals.fire(
                target,
                ok,
                value,
                entity,
                urgent=spec.get("urgent", False),
                after=spec.get("after"),
                priority=spec.get("priority"),
                defused=spec.get("defused", False),
            )
        elif "spawn" in action:
            self.spawn_children(
                context, parts, action["spawn"], entity, upstream, scope
            )
        elif "activate" in action:
            parts.run.arrivals.set_arrivals(action["activate"], True)
        elif "deactivate" in action:
            parts.run.arrivals.set_arrivals(action["deactivate"], False)
        elif "resume" in action:
            spec = action["resume"]
            parts.run.resume(
                spec["task"],
                spec.get("where"),
                spec.get("value"),
                spec.get("limit"),
                scope,
            )
        elif "interrupt" in action:
            spec = action["interrupt"]
            where = spec.get("where")
            context.interrupt(
                spec["task"],
                where=Expression(where) if isinstance(where, str) else where,
                cause=spec.get("cause"),
            )
        else:
            apply_step(context.env, action["path"], action["value"])

    def copy_outcome(
        self, parts: "Parts", fire: Dict[str, Any], entity: Optional[Entity]
    ) -> Optional[Tuple[bool, Any]]:
        """The outcome a `fire` action fires with: its literal `value`, as a success or,
        with `fail`, as the reason of a failure, or the outcome `from` copies from a
        signal or a task. None when there is nothing to copy (a signal not fired, a task
        with no ended instance, or no entity).

        Args:
            parts: The run's parts.
            fire: The `fire` action.
            entity: The entity the action runs for; None for none.
        """
        source = fire["from"]
        if source is None:
            return not fire.get("fail", False), fire["value"]
        if source in parts.run.signals.made:
            signal = parts.run.signals.made[source]
            if signal.ok is None:
                return None
            return signal.ok, signal.value
        # a task: copy its last-ended instance on this entity
        if (
            entity is None
            or entity.last_ended is None
            or source not in entity.last_ended
        ):
            return None
        event = entity.last_ended[source]
        if event.ok:
            return True, event.value
        return False, getattr(event.value, "reason", event.value)
