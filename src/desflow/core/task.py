"""The life of one task instance: what `@app.task` and a YAML task run.

`TaskSpec` holds what a task is, checked once when it is declared. `TaskRun` runs one
instance of it, in this order: the balk check; `queued`; the `wait`; the resource; the
`get`; `started`; the user function; the service time; the `put`; the `fail` check;
the finished payload, or `failed`; the `signal`; the `interrupt` action; `set`; the
`release`; then `next`, or `on_fail`, or the end of the flow. `open`, `cancel` and
`seize` come before the `wait`. `run` is a flow's SimPy process: it runs
the first instance, then each instance that one hands on to, in the same process. The
context keeps the decorator and the checks made before the clock starts.
"""

from __future__ import annotations

import functools
import numbers
from collections.abc import Mapping
from dataclasses import replace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from simpy import Interrupt
from simpy.events import Condition
from simpy.resources.resource import Preempted

from desflow.core.expressions import ANY_NAME, Expression, Scope, boolean_word
from desflow.core.flow import (
    Open,
    Release,
    RequestOptions,
    Router,
    Seize,
    Transfer,
    Wait,
    as_given,
    make_scope,
)
from desflow.resources.resource import QueueFull
from desflow.models.flow import Entity, Upstream, payload_of
from desflow.utils import suggestion
from desflow.values.check import names_read
from desflow.values.models import _Gen, has_generator
from desflow.values.runtime import RunStopped, stream_of
from desflow.values.template import BoundPayload, Template

if TYPE_CHECKING:
    from desflow.core.context import SimulationContext


class TaskFailed(Exception):
    """A task instance failed, and no `on_fail` handled it.

    A child flow, started by a `wait` branch or `context.call`, fails with it, so
    the task waiting for the flow fails too.

    Attributes:
        task (str): The task that failed.
        reason (Any): Why: its `fail` condition, its `fail_reason`, or the error its
            function raised.
    """

    def __init__(self, task: str, reason: Any):
        # SimPy copies a failure as type(error)(*error.args), so args are the
        # constructor's arguments
        super().__init__(task, reason)
        self.task = task
        self.reason = reason

    def __str__(self) -> str:
        return f"task '{self.task}' failed: {self.reason}"


# What a wait or a get returns when the task's patience ran out first
_RENEGED = object()
_UNSET = object()

# What `_seize` returns when the task left: it balked at a full line, or its patience
# ran out. The task has published that and ended its flow, or handed it on.
_LEFT = object()


class Handle:
    """An event a task opened under a name: a timeout, a request, a get, a put, a child
    flow, a signal's firing or a group of them, which later keys read as
    `handle.<name>`.

    Attributes:
        kind (str): `timeout`, `request`, `get`, `put`, `task`, `signal`, `any` or
            `all`.
        event: The SimPy event. A unit a preemption takes is asked for again under the
            same name, and the new request replaces the event here.
    """

    __slots__ = ("kind", "event", "value_of", "members")

    def __init__(self, kind: str, event: Any):
        self.kind = kind
        self.event = event
        # What reads the value of a get (a priority store wraps its items), and, for a
        # group, its leaves' events by name
        self.value_of: Optional[Callable[[Any], Any]] = None
        self.members: Optional[Dict[str, Any]] = None

    def read(self, name: str, part: str) -> Any:
        """The part of the event `handle.<name>.<part>` reads.

        Args:
            name: The name, for the message.
            part: `triggered`, `processed`, `ok`, `value` or `alive`.

        Raises:
            KeyError: If `alive` is read on an event that is not a child flow, or `ok`
                or `value` before the event happened, as SimPy's `Event.value` raises.
        """
        event = self.event
        if part == "triggered":
            return event.triggered
        if part == "processed":
            return event.processed
        if part == "alive":
            if self.kind != "task":
                raise KeyError(
                    f"handle.{name} is a {self.kind}, and only a child flow is alive "
                    f"or not"
                )
            return event.is_alive
        if not event.triggered:
            raise KeyError(f"handle.{name}.{part} is read before the event happened")
        if part == "ok":
            return event.ok
        if self.members is not None:
            return {
                name: _branch_value(member)
                for name, member in self.members.items()
                if member.processed
            }
        if self.value_of is not None and event.ok:
            return self.value_of(event)
        return _branch_value(event)


class HandleTable(dict):
    """The handles of one flow: the names its tasks opened, with a link to the table
    of the flow that started it, so a child flow reads its parent's names and its own
    name hides a parent's.

    Attributes:
        parent (Optional[HandleTable]): The table of the flow that started this one.
    """

    def __init__(self, parent: Optional["HandleTable"] = None):
        super().__init__()
        self.parent = parent

    def find(self, name: str) -> Optional[Handle]:
        """The handle under `name` in this table, or in its parents'.

        Args:
            name: The opened name.

        Returns:
            Optional[Handle]: The handle, or None if no table up the chain has it.
        """
        table: Optional[HandleTable] = self
        while table is not None:
            if name in table:
                return table[name]
            table = table.parent
        return None


class _NoRecord:
    """What the body of a task with `payload: null` returns: the task finishes and
    publishes no record."""

    def __repr__(self) -> str:
        return "NO_RECORD"


NO_RECORD = _NoRecord()

# The service still owed, in seconds, at or below which a resumed task has none left
_NO_TIME_LEFT = 1e-9


def _won(event: Any) -> bool:
    """Whether a request, get or put counts as won against a patience or a wait that
    ended at the same instant: only when the event was processed before the wait's
    condition was, as SimPy's `req in results` is true only for such an event. One
    that is granted but not yet processed has lost, and its unit or item is not kept.
    """
    return event.processed


class TaskSpec:
    """What a task is: its keys, checked when it is declared, and what `run()`
    checks before the clock starts."""

    def __init__(
        self,
        name: str,
        service_id: Optional[str],
        resource_id: Optional[str],
        priority: Any,
        get: Optional[Transfer],
        put: Optional[Transfer],
        router: Router,
        patience: Any = None,
        balk: Optional[str] = None,
        wait: Optional[Wait] = None,
        signal: Optional[str] = None,
        fail: Any = None,
        fail_reason: Optional[str] = None,
        on_fail: Optional[str] = None,
        on_renege: Optional[str] = None,
        on_balk: Optional[str] = None,
        service_time: Any = None,
        key_field: Optional[str] = None,
        key: Any = None,
        attribute_changes: Optional[Dict[str, Any]] = None,
        resources: Optional[List[str]] = None,
        keep: Optional[List[str]] = None,
        preempt: Union[bool, str] = True,
        on_interrupt: str = "resume",
        on_interrupt_next: Optional[str] = None,
        interrupt: Optional[Dict[str, Any]] = None,
        spawn: Optional[Dict[str, Any]] = None,
        subscribe: Optional[Wait] = None,
        on: Optional[List[Dict[str, Any]]] = None,
        vars: Optional[Dict[str, Any]] = None,
        vars_once: Optional[Dict[str, Any]] = None,
        on_start: Optional[Dict[str, Any]] = None,
        finish_vars: Optional[Dict[str, Any]] = None,
        set_args: Optional[Dict[str, Any]] = None,
        activate: Optional[List[str]] = None,
        deactivate: Optional[List[str]] = None,
        history: Optional[int] = None,
        seize: Optional[Seize] = None,
        release: Optional[Release] = None,
        open: Optional[Open] = None,
        cancel: Optional[str] = None,
    ):
        self.name = name
        # The units the flow keeps from this task until a `release`, the events it
        # opens under names, and the opened request or get it cancels
        self.seize = seize
        self.release = release
        self.open = open
        self.cancel = cancel
        # How many finished payloads the task keeps for `lookup` and `aggregate`; None
        # leaves it to whether any lookup names the task
        self.history = history
        # Names written in this process's own locals, after `set`, read as `arg.<name>`
        self.set_args: Dict[str, Any] = dict(set_args or {})
        # The arrivals whose source the task turns on and off when it finishes
        self.activate: List[str] = list(activate or [])
        self.deactivate: List[str] = list(deactivate or [])
        # The named values and the started record, each a mapping of generators; the
        # values may be validated `Value`s or plain data
        self.vars: Dict[str, Any] = dict(vars or {})
        self.vars_once: Dict[str, Any] = dict(vars_once or {})
        self.on_start: Dict[str, Any] = dict(on_start or {})
        self.finish_vars: Dict[str, Any] = dict(finish_vars or {})
        self.service_id = service_id
        self.key_field = key_field
        # One record key for every record of an instance, drawn at its first record
        self.key = key
        # Every resource it holds, acquired in this order; resource_id is the first
        self.resources: List[str] = list(
            resources or ([resource_id] if resource_id else [])
        )
        self.resource_id = self.resources[0] if self.resources else None
        self.keep: List[str] = list(keep or [])
        preempt = boolean_word(preempt)
        self.preempt = preempt
        self.on_interrupt = on_interrupt
        self.on_interrupt_next = on_interrupt_next
        self.interrupt = interrupt
        # Start a new entity on success (`spawn:` key); subscribe to an event; run
        # `on:` actions on every end. `on` is a list of normalized action mappings.
        self.spawn = spawn
        self.subscribe = subscribe
        self.on: List[Dict[str, Any]] = list(on or [])
        self.interrupt_where = (
            Expression(interrupt["where"])
            if interrupt is not None and interrupt.get("where") is not None
            else None
        )
        self.priority = priority
        self.get = get
        self.put = put
        self.router = router
        self.patience = patience
        self.balk = balk
        self.wait = wait
        self.signal = signal
        self.fail = fail
        self.fail_reason = fail_reason
        self.on_fail = on_fail
        self.on_renege = on_renege
        self.on_balk = on_balk
        self.service_time = service_time
        self.attribute_changes = dict(attribute_changes or {})
        # Whether `set` and the cause of `interrupt` hold a generator, which is
        # evaluated when it is used; a plain one is used as it is
        self.set_generated = has_generator(self.attribute_changes)
        self.cause_generated = interrupt is not None and has_generator(
            interrupt.get("cause")
        )
        self.rank_of = Expression(priority) if isinstance(priority, str) else None
        # A `preempt` written as text is an expression, read as each request is made
        self.preempt_of = Expression(preempt) if isinstance(preempt, str) else None
        self.patience_of = Expression(patience) if isinstance(patience, str) else None
        self.balk_of = Expression(balk) if balk is not None else None
        self.fail_of = (
            Expression(fail, names=ANY_NAME) if isinstance(fail, str) else None
        )
        self.service_time_of = (
            Expression(service_time, names=ANY_NAME)
            if isinstance(service_time, str)
            else None
        )
        # Waits for a unit or an item, so it queues
        self.queues = (
            bool(self.resources)
            or (get is not None and get.wait)
            or wait is not None
            or seize is not None
        )
        # Publishes started before its work
        self.timed = service_id is not None or service_time is not None or self.queues
        # Set when the function is decorated
        self.user_func: Callable[..., Any] = lambda *args, **kwargs: None
        self.declared: Any = {}

    @property
    def service_reads(self) -> List[str]:
        """The vars the service time reads, which are given a value when it is."""
        return sorted(
            names_read(self.service_time_of or self.service_time) & set(self.var_names)
        )

    @property
    def var_names(self) -> List[str]:
        """Every var the task names, in the order they get a value."""
        return [*self.vars_once, *self.vars, *self.finish_vars]

    @property
    def has_service(self) -> bool:
        """Whether the task works for a time: a `service_id` or a `service_time`."""
        return self.service_id is not None or self.service_time is not None

    @property
    def expressions(self) -> List[Tuple[str, Expression]]:
        """Every expression the task holds, with the key it is written under."""
        found: List[Tuple[str, Expression]] = []
        for key, expression in (
            ("priority", self.rank_of),
            ("preempt", self.preempt_of),
            ("patience", self.patience_of),
            ("service_time", self.service_time_of),
            ("balk", self.balk_of),
            ("fail", self.fail_of),
        ):
            if expression is not None:
                found.append((key, expression))
        for key, transfer in (("get", self.get), ("put", self.put)):
            if transfer is not None:
                found.extend((key, e) for e in transfer.expressions)
        found.extend(("next", e) for e in self.router.conditions)
        if self.seize is not None:
            found.extend(("seize", e) for e in self.seize.expressions)
        if self.open is not None:
            found.extend(("open", e) for e in self.open.expressions)
        if self.release is not None:
            found.extend(("release", e) for e in self.release.expressions)
        if self.interrupt_where is not None:
            found.append(("interrupt", self.interrupt_where))
        if self.wait is not None:
            found.extend(("wait", e) for e in self.wait.conditions)
            found.extend(("wait", e) for e in self.wait.time_expressions)
            found.extend(("wait", e) for e in self.wait.request_expressions)
            found.extend(("wait_when", e) for e in self.wait.predicates)
        return found


class TaskRun:
    """One instance of a task, run by `run()`, the SimPy process of its flow."""

    def __init__(
        self,
        context: SimulationContext,
        spec: TaskSpec,
        task_id: Any,
        entity: Entity,
        upstream: Optional[Upstream],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        child: bool = False,
        arg_values: Optional[Dict[str, Any]] = None,
        handles: Optional[HandleTable] = None,
    ):
        self.context = context
        self.spec = spec
        self.task_id = task_id
        self.entity = entity
        self.upstream = upstream
        self.args = args
        self.kwargs = kwargs
        # A child flow returns its last payload, or fails, to the task waiting for it
        self.child = child
        self.env = context.env
        self.item: Any = None
        # The priority of the item a priority store gave, which lasts with the item
        self.item_priority: Any = None
        self._taken_priority: Any = None
        # Whether a `get` branch of the wait took an item out of its store
        self._wait_took_item = False
        # The process's local names, read as `arg.<name>`: what a parent passed and
        # what `set_args` wrote. They are one dictionary for the whole flow, so the
        # next task of the flow shares it; None until one is passed or written
        self.arg_values: Optional[Dict[str, Any]] = arg_values
        # Why the task was reached by an `on_interrupt_next` edge, read as `cause.`
        self.cause_values: Optional[Dict[str, Any]] = None
        # The first task of the flow this process runs, read as `process.flow_name`
        self.flow_name: str = spec.name
        self.waits: Dict[str, Dict[str, Any]] = {}
        self.process: Any = None
        # The events this flow opened under names, made on the first `open`; and the
        # table of the flow that started this one (a child flow), which it reads
        self._handles: Optional[HandleTable] = None
        self._parent_handles: Optional[HandleTable] = handles
        # The requests this flow holds or waits for from a `seize` or an `open`, shared
        # by the instances of one `next` chain, as a process keeps its own requests,
        # and the `seize` requests this instance still waits to be granted
        self._seized: List[Any] = []
        self._awaiting: List[Any] = []
        # Whether the context lists this instance as running, which `reach` does from
        # the step the instance is started in and `_run_one` does otherwise
        self._listed = False
        # Whether the lookup gate dropped this start, so its `on:` actions do not run
        self._dropped = False
        # Whether the failure the task ends in is a run failure: a wait that failed,
        # or a task function that raised, as an uncaught exception ends a SimPy process
        self._raised = False
        # The instance this one handed on to, which `run` runs next; None if none
        self._following: Optional["TaskRun"] = None
        # The put or get the task is waiting for, cancelled by an interrupt
        self._pending: Any = None
        # The child flows a `{task}` wait started, by branch, kept for a resume
        self._children: Dict[str, Any] = {}
        # The exception a task function raised, to chain the TaskFailed it causes
        self._fail_error: Optional[BaseException] = None
        # Units won by a `request` wait branch, held through the service and given
        # back when the task ends, as a `resource:` unit is
        self._wait_held: List[Any] = []
        # The requests a wait branch lost by being granted after the wait ended,
        # whose units go back when the task ends
        self._lost_requests: List[Any] = []
        # The live `request`, `get` and `put` branch events of the current wait, kept
        # so a resume reuses them and a renege or an abandon cancels them
        self._wait_events: Dict[str, Any] = {}
        # The patience still to use, and when the current wait began
        self._patience_left: Optional[float] = None
        # When a single `timeout` wait began and its length, for `cause.left`
        self._single_timeout: Optional[Tuple[float, float]] = None
        self._patience_since: Optional[float] = None
        # What the instance waits on now, read as `process.target`; None once ended
        self._target: Optional[str] = None
        # The instance's end event (for `on:`, `running`, `subscribe`), and the
        # Upstream it hands on for that end; both None when nothing reads the end
        self.ended: Any = None
        self.outcome: Optional[Upstream] = None
        # The running instances a `running` branch selected, by branch name, kept
        # so the branch's value reads their end events after the wait
        self._running_runs: Dict[str, List["TaskRun"]] = {}
        # The values of `vars_once`, `vars` and `finish_vars`, once evaluated
        self._once: Dict[str, Any] = {}
        self._vars: Dict[str, Any] = {}
        self._finish: Dict[str, Any] = {}
        self._once_made = self._vars_made = False
        self._instance_key: Optional[str] = None
        # An instance whose task needs an end event (for `running`, `subscribe`, a
        # `fire from`, or its own `on:` actions) gets one, defused so its failure
        # stops the run only through the task process.
        if spec.name in context._ended:
            make_event = getattr(self.env, "internal_event", self.env.event)
            self.ended = make_event()  # type: ignore[misc]
            self.ended.defused = True
            if not child:
                # Count one pending end against the entity, taken back when the end
                # event is processed, so the entity leaves and a drain ends when the
                # end event is processed, after the `on:` actions have run.
                entity.ends_pending += 1
                self.ended.callbacks.append(
                    functools.partial(context._end_processed, entity)
                )
        if spec.name in context._tracked:
            self._list_instance()
        if spec.name in context._whole_flow:
            context._flow_runs.setdefault(spec.name, []).append(self)

    # ------------------------------------------------------------------ records

    def publish(self, value: Any, key: Any = None, lifecycle: bool = False) -> None:
        """Publishes an event of this instance, keyed by its entity.

        Args:
            value: The event's value: a status such as `status("queued")`, or the
                task's payload.
            key: The record's key when it is not the entity's: the payload field
                `key_field` names.
            lifecycle: True for `queued` and `started`, which a plain-output sink
                does not receive.
        """
        if key is None and self.spec.key is not None:
            key = self._record_key()
        self.env.publish_event(
            self.entity.key,
            value,
            self.spec.name,
            self.task_id,
            key=key,
            lifecycle=lifecycle,
        )

    def _record_key(self) -> str:
        """The record key of this instance: the task's `key`, drawn at its first
        record and kept, as text.

        Raises:
            ValueError: If the key is not an integer or text.
        """
        if self._instance_key is None:
            self._ensure_once()
            made = self.context.values.evaluate(self.spec.key, self.scope())
            if isinstance(made, bool) or not isinstance(made, (int, str)):
                raise ValueError(
                    f"task '{self.spec.name}' key gave {made!r}, which is not an "
                    f"integer or text"
                )
            self._instance_key = str(made)
        return self._instance_key

    def _payload_key(self, payload: Any) -> Any:
        """Returns the record key `key_field` takes from the payload, or None.

        Raises:
            ValueError: If the payload is not a mapping with that field.
        """
        field = self.spec.key_field
        if field is None:
            return None
        if not isinstance(payload, Mapping) or payload.get(field) is None:
            raise ValueError(
                f"task '{self.spec.name}' has key_field '{field}', and its payload "
                f"has no value for it"
            )
        return str(payload[field])

    def status(self, state: str, **extra: Any) -> Dict[str, Any]:
        """Returns the value of a status event of this task.

        Args:
            state: The status, such as `queued`, `started` or `failed`.
            **extra: Fields added after `status`, such as `reason` or `cause`.

        Returns:
            Dict[str, Any]: `path_id` when the task has a service, then `status` and
                the extra fields.
        """
        value: Dict[str, Any] = {}
        if self.spec.service_id is not None:
            value["path_id"] = f"{self.context.sim_id}.service.{self.spec.service_id}"
        value["status"] = state
        value.update(extra)
        return value

    def scope(self, payload_upstream: Optional[Upstream] = None) -> Scope:
        """Returns what the task's expressions read now.

        Args:
            payload_upstream: The finished payload, for a `next` condition read after
                the task ends; None before it.

        Returns:
            Scope: The names an expression reads: the entity's attributes, the levels,
                the waits, the signals and the item.
        """
        return make_scope(
            payload_upstream if payload_upstream is not None else self.upstream,
            self.entity.attributes,
            self.env,
            self.context._levels(),
            item=self.item,
            waits=self.waits,
            signals=self.context._signals,
            process={
                "name": self.spec.name,
                "instance": self.task_id,
                "target": self._target,
                "key": self.entity.key,
                "flow_name": self.flow_name,
            },
            resources=self.context._resources_map,
            names=self._names(),
            entity=self.entity.key,
            args=self.arg_values,
            cause=self.cause_values,
            upstream_payload=payload_of(self.upstream),
            item_priority=self.item_priority,
            peek=self.env.peek,
            instances=self.context._instance_scopes,
            stream=stream_of(self.entity),
            handles=self._handle_of,
        )

    # ----------------------------------------------------------- generated values

    def _names(self) -> Dict[str, Any]:
        """The bare names an expression reads now: the vars that have a value."""
        if not (self._once or self._vars or self._finish):
            return {}
        return {**self._once, **self._vars, **self._finish}

    def _ensure_once(self) -> None:
        """Gives the entity its `vars_once`, at the first event of the entity that
        needs them; later instances and tasks of the entity read the same values."""
        spec = self.spec
        if self._once_made:
            return
        self._once_made = True
        if not spec.vars_once:
            return
        values = self.context.values
        self._once = dict(
            values.once(
                spec.name,
                self.entity.key,
                lambda: values.evaluate_vars(spec.vars_once, self.scope()),
            )
        )

    def _ensure_vars(self) -> None:
        """Evaluates `vars`, each seeing the `vars_once` and the vars before it."""
        if self._vars_made:
            return
        self._ensure_once()
        self._vars_made = True
        if self.spec.vars:
            self._vars = self.context.values.evaluate_vars(
                self.spec.vars, self.scope(), known=self._once
            )

    def _generated(self, value: Any) -> Any:
        """A value of `set`, an `interrupt` cause or similar, evaluated now."""
        return self.context.values.evaluate(value, self.scope())

    def _start_values(self) -> None:
        """With `on_start`: evaluates the vars and publishes the started record."""
        spec = self.spec
        if not spec.on_start:
            return
        self._ensure_vars()
        self.publish(self.context.values.evaluate(spec.on_start, self.scope()))

    def _finish_values(self, payload: Any) -> Any:
        """At step 14, after the service: the vars not yet evaluated, the
        `finish_vars`, then the payload if it is a template.

        Returns:
            Any: The payload; a `Template` or `BoundPayload` is rendered to a mapping.
        """
        spec = self.spec
        self._ensure_vars()
        if spec.finish_vars:
            self._finish = self.context.values.evaluate_vars(
                spec.finish_vars, self.scope(), known={**self._once, **self._vars}
            )
        values = self.context.values
        if isinstance(payload, BoundPayload):
            if payload.template.uses_previous:
                values.keep_previous(spec.name)
            return values.render(
                payload.template,
                payload.entity or self.entity,
                self.scope(),
                payload.fields,
                payload.attributes,
            )
        if isinstance(payload, Template):
            if payload.uses_previous:
                values.keep_previous(spec.name)
            return values.render(payload, self.entity, self.scope())
        return payload

    @property
    def _has_values(self) -> bool:
        spec = self.spec
        return bool(spec.vars or spec.vars_once or spec.finish_vars or spec.on_start)

    # ----------------------------------------------------------- end event

    def _list_instance(self) -> None:
        """Lists this instance under its task on its entity, and on the entity that
        spawned it while that entity's flow has not ended, for a `running` wait."""
        if self.entity.left:
            return
        if self.entity.instances is None:
            self.entity.instances = {}
        self.entity.instances.setdefault(self.spec.name, []).append(self)
        parent = self.entity.spawned_by
        if parent is not None and not parent.ending:
            if parent.instances is None:
                parent.instances = {}
            parent.instances.setdefault(self.spec.name, []).append(self)

    def _set_ended(
        self,
        outcome: Optional[Upstream],
        ok: bool,
        result: Any,
        actions: bool = True,
    ) -> None:
        """Sets `outcome` and triggers the end event, if this instance has one and it
        has not been triggered. A success carries the finished payload or None; a
        failure carries the exception a waiter receives. The `on:` actions run after
        it unless `actions` is False."""
        if self.ended is None:
            return
        self.outcome = outcome
        if self.ended.triggered:
            return
        self._record_last_ended()
        if ok:
            self.ended.succeed(result)
        else:
            self.ended.fail(result)
        if actions:
            self._run_end_actions()

    def _record_last_ended(self) -> None:
        """Records this instance as the last-ended of its task on its entity, for a
        `fire from` that copies it, unless the entity has left."""
        if self.spec.name in self.context._copied and not self.entity.left:
            if self.entity.last_ended is None:
                self.entity.last_ended = {}
            self.entity.last_ended[self.spec.name] = self.ended

    def _end_default(self, ok: bool, error: Any = None, actions: bool = True) -> None:
        """Triggers the end event for a balk, a renege or an error, when no other end
        triggered it: a success with None, or a failure with the error. The `on:`
        actions run after it unless `actions` is False."""
        if self.ended is None or self.ended.triggered:
            return
        self._record_last_ended()
        if ok:
            self.ended.succeed(None)
        else:
            self.ended.fail(error)
        if actions:
            self._run_end_actions()

    def _run_end_actions(self) -> None:
        """Runs the task's `on:` actions in the step the instance ends, right after
        the end event is triggered, as a statement at the end of a SimPy process's
        step runs. A signal's `on:` stays a callback of the firing."""
        try:
            for action in self.spec.on:
                self._run_end_action(action)
        except Exception as error:
            if not self.child:
                raise
            # A child flow's error would reach only the flow that waits for it, and
            # none if that flow lost its wait. An action that cannot run stops the
            # run, so it fails an event nobody handles, as a failing callback did.
            make_event = getattr(self.env, "internal_event", self.env.event)
            stop = make_event()  # type: ignore[misc]
            stop.fail(error)

    def _run_end_action(self, action: Dict[str, Any]) -> None:
        """Runs one of the task's `on:` actions."""
        self.context._run_on_action(
            action,
            f"task '{self.spec.name}'",
            self.entity,
            self.outcome,
            names=self._names(),
            scope=self.scope(),
        )

    def _running_event(self, node: Any) -> Any:
        """The `AllOf` of the end events of a `running` branch's selected instances:
        the task's instances on this entity and the entities it spawned, ended ones
        included, skipping this instance, each narrowed by `where` in its scope."""
        target = node.target
        where = target["where"]
        if target.get("whole_flow"):
            # Every flow of the model that has an instance of the task, ended ones too,
            # waited for as a SimPy process is: the wait ends in the step after the
            # flow's process ends
            flows: List[Any] = []
            chosen: List["TaskRun"] = []
            for run in self.context._flow_runs.get(target["task"], []):
                if (
                    run is not self
                    and run.process is not None
                    and run.process not in flows
                    and (where is None or where.evaluate(run.scope()))
                ):
                    flows.append(run.process)
                    chosen.append(run)
            self._running_runs[node.name] = chosen
            return self.env.all_of(flows)
        selected = [
            run
            for run in (self.entity.instances or {}).get(target["task"], [])
            if run is not self
            and run.ended is not None
            and (where is None or where.evaluate(run.scope()))
        ]
        self._running_runs[node.name] = selected
        return self.env.all_of([run.ended for run in selected])

    def _running_value(self, node: Any, done: bool) -> Any:
        """A `running` branch's value: the one instance's payload, a mapping of id to
        payload for several in start order, or an empty mapping when none matched."""
        if not done:
            return None
        selected = self._running_runs.get(node.name, [])
        if not selected:
            return {}
        if node.target.get("whole_flow"):
            # The last payload of each flow, which its process records as it ends
            payloads = {
                run.task_id: getattr(run.process, "flow_payload", None)
                for run in selected
            }
            return next(iter(payloads.values())) if len(payloads) == 1 else payloads
        if len(selected) == 1:
            return selected[0].ended.value
        return {run.task_id: run.ended.value for run in selected}

    def _subscribe_event(self, wait: Wait) -> Any:
        """The event a `subscribe:` key subscribes the instance to: a child flow for
        a task branch, a signal's firing, or the running instances of a task."""
        node = wait.root
        if node.kind == "task":
            return self.context._start(
                node.target,
                self.entity,
                self.upstream,
                child=True,
                args=self.args,
                kwargs=self.kwargs,
            )
        if node.kind == "signal":
            return self.context._signals[node.target].wait()
        if node.kind == "running":
            return self._running_event(node)
        raise ValueError(f"subscribe does not take a {node.kind} branch")

    # --------------------------------------------------------------------- run

    def run(self):
        """The process of a flow: this instance, then each instance it hands on to.

        One loop runs the instances, so no generator nests at any length of flow, and
        the next instance asks for its unit in the step the last one ends, as the
        next statement of a SimPy process does. A root flow's process ends with the
        first payload, a child flow's with the last. The process is named after the
        task it runs now (P6).
        """
        run, first, kept = self, True, None
        while True:
            value = yield from run._run_one()
            if first:
                first, kept = False, value
            if run._following is None:
                # What a `running` wait for the whole flow takes as its value
                self.env.active_process.flow_payload = value  # type: ignore[union-attr]
                return value if self.child else kept
            # The instance that ended keeps no link, so a long flow frees each one.
            following, run._following = run._following, None
            run = following
            self.env.active_process._generator.__name__ = run.spec.name  # type: ignore[attr-defined]

    def reach(self, process: Any) -> None:
        """Lists this instance as running, under its task, with the process that runs
        it. A task that starts an instance calls this in the step it starts it in, so
        an `interrupt` in that step finds the instance, as it finds a SimPy process
        made one statement earlier. The interrupt then reaches the instance at its
        first `yield`.

        Args:
            process: The SimPy process that runs this instance.
        """
        self.process = process
        self.context._running.setdefault(self.spec.name, []).append(self)
        self._listed = True

    def _run_one(self):
        """One instance: everything from the balk check to the next task.

        While it runs, the instance is listed with its task, so `interrupt` can
        reach it.
        """
        self.process = self.env.active_process
        if not self._listed:
            self.reach(self.process)
        if not self.child and not self.entity.entered:
            self.entity.entered = True
            # A spawn without `after` has already counted the entity in; the first
            # instance of any other entity counts it in here.
            self.context._entity_entered(self.entity)
        try:
            result = yield from self._run()
        except RunStopped:
            # A `row` with on_end: stop ended the run; this instance ends quietly
            return None
        except BaseException as error:
            # An error ends the instance: the end event fails with it, so a waiter
            # receives it. The run still raises. No `on:` action runs after it,
            # as no statement runs after an uncaught exception in SimPy.
            self._end_default(ok=False, error=error, actions=False)
            raise
        finally:
            self._target = None
            self._stop_running()
        # A balk or a renege returns with the end event not yet triggered: it
        # succeeds with None, so a `running` waiter resumes with no value.
        self._end_default(ok=True, actions=not self._dropped)
        return result

    @staticmethod
    def _target_label(node: Any) -> Optional[str]:
        """What `process.target` reads for a wait node: None for a group."""
        if node.is_group:
            return None
        if node.kind in ("signal", "task", "handle"):
            return f"{node.kind}.{node.target}"
        if node.kind == "request":
            return f"resource.{node.target}"
        if node.kind in ("get", "put"):
            return f"{node.kind}.{node.target.target}"
        if node.kind == "running":
            return f"task.{node.target['task']}"
        return node.kind  # timeout or condition

    def _stop_running(self) -> None:
        running = self.context._running.get(self.spec.name, [])
        if self in running:
            running.remove(self)
            # A wait on `running(task)` reads the count again now it has changed
            notify = getattr(self.env, "notify_change", None)
            if notify is not None:
                notify()

    def _gate_closed(self) -> bool:
        """Whether a start waits for a row to look up: the task looks up another task
        that has no finished row yet. The start is dropped, and a child flow is not
        held back (its lookup reports the missing row)."""
        gate = self.context._gates.get(self.spec.name)
        if not gate or self.child:
            return False
        values = self.context.values
        if values.ready(gate):
            values.note_started(self.spec.name, gate)
            return False
        values.refuse_start(self.spec.name, gate)
        return True

    def _template_waits(self, payload: Any) -> bool:
        """Whether the Template a task function returned looks up a task that has no
        row yet, so the start is dropped. A function returns its Template only when
        it is called, so the gate cannot see it when the model is built.

        Args:
            payload: What the task function returned.

        Returns:
            bool: True when the start is dropped; a child flow is never dropped.
        """
        template = (
            payload.template
            if isinstance(payload, BoundPayload)
            else payload
            if isinstance(payload, Template)
            else None
        )
        if template is None or not template.looks_up or self.child:
            return False
        values = self.context.values
        if values.ready(template.looks_up):
            values.note_started(self.spec.name, template.looks_up)
            return False
        values.refuse_start(
            self.spec.name,
            template.looks_up,
            consumed_item=self.spec.get is not None or self._wait_took_item,
        )
        return True

    def _run(self):
        spec, env, stats = self.spec, self.env, self.env.stats
        if self._gate_closed():
            self._dropped = True
            self._release_kept(own=self.child)
            self._end_flow("ended")
            return None
        if spec.balk_of is not None and spec.balk_of.evaluate(self.scope()):
            self._release_kept(own=self.child)
            self.publish(self.status("balked"))
            stats.task_balked(spec.name, self.task_id)
            self.outcome = Upstream(
                self.task_id,
                self.entity.key,
                self.entity.attributes,
                self.status("balked"),
            )
            self._leave(spec.on_balk)
            return None
        if spec.subscribe is not None:
            # Be told when the event happens: subscribe this instance, then go on.
            self.context.subscribe(self._subscribe_event(spec.subscribe))
        if (
            spec.resources
            and spec.wait is None
            and spec.seize is None
            and spec.open is None
            and spec.cancel is None
        ):
            # Nothing yields or makes room between here and the first request, so a
            # full line is known now and the task balks before it queues, as in SimPy
            # it would when the request raised. A `seize` waits first, an `open` adds
            # requests to the line, and a `cancel` takes one out.
            first = self.context._resources_map[spec.resources[0]]
            limit = first._line_limit()
            if limit is not None and len(first.queue) >= limit:
                self._release_kept(own=self.child)
                self._balk_full(first)
                return None
        # The patience counts waiting time only: it runs while the task waits for
        # its wait, its resources or its get, and pauses while it is served, so a
        # preempted task that queues again goes on from the time it had used, as
        # SimPy's reneging pattern `req | env.timeout(patience_left)` does.
        # The entity's `vars_once` are made at its first event, before anything waits
        failure: Any = None
        try:
            self._ensure_once()
        except Exception as error:  # noqa: BLE001: a task's failure
            failure = f"{type(error).__name__}: {error}"
            self._fail_error = error
            self._raised = True
            self.publish(self.status("failed", reason=failure))
            stats.task_failed(spec.name, self.task_id)
        if spec.queues and failure is None:
            self.publish(self.status("queued"), lifecycle=True)
            stats.task_queued(spec.name, self.task_id)
            if spec.patience is not None:
                self._patience_left = self._patience()

        if failure is None and (
            spec.open is not None or spec.cancel is not None or spec.seize is not None
        ):
            try:
                holding = yield from self._holding_steps()
            except Exception as error:  # noqa: BLE001: a task's failure
                failure = self._fail_with(error)
            else:
                if holding is _LEFT:
                    return None
                if holding is not None:
                    return self._abandon(holding)

        while spec.wait is not None and failure is None:
            try:
                if self._seized or self._awaiting:
                    # A unit a preemption took is asked for again before the wait
                    holding = yield from self._acquire()
                    if holding is _LEFT:
                        return None
                    if holding is not None:
                        return self._abandon(holding)
                outcome = yield from self._wait(self._deadline())
                self._waited()
            except Interrupt as interrupt:
                self._waited()
                timeout_left = (
                    None
                    if self._single_timeout is None
                    else max(
                        self._single_timeout[1] - (env.now - self._single_timeout[0]),
                        0.0,
                    )
                )
                if self._interrupted(interrupt):
                    if spec.on_interrupt == "restart":
                        self._children = {}  # a restart starts its child flows again
                        self._cancel_wait_events()  # and re-makes its wait branches
                    continue  # resume or restart: wait again
                return self._abandon(interrupt, timeout_left)
            if outcome is _RENEGED:
                self._renege()
                return None
            failure = outcome
            if failure is not None:
                self._raised = True
                self.publish(self.status("failed", reason=failure))
                stats.task_failed(spec.name, self.task_id)
            break

        payload: Any = None
        silent = False  # `payload: null`: the task publishes no finished record
        rank = self._rank() if self.spec.resources else None
        may_preempt = self._preempt() if self.spec.resources else False
        remaining: Optional[float] = None  # service still owed, when interrupted
        started = got_item = put_done = served = no_time_left = False
        values_done = False
        # The requests the task holds, and the position in `spec.resources` each one
        # asked for. Kept across a resume, as a SimPy process keeps its request
        # across `except Interrupt`.
        held: List[Any] = []
        slots: Dict[int, Any] = {}
        while failure is None:
            service_from: Optional[float] = None
            try:
                if self._seized or self._awaiting:
                    # A `seize` unit a preemption took is asked for again under the
                    # same name, and the task goes on once it is granted
                    holding = yield from self._acquire()
                    if holding is _LEFT:
                        self._release(held)
                        return None
                    if holding is not None:
                        self._release(held)
                        return self._abandon(holding)
                # A unit a request wait branch won and a preemption then took is
                # requested again on resume (C8). A breakdown leaves the unit with
                # its holder, and a unit a `release` of another flow freed is not
                # asked for.
                for index in range(len(self._wait_held)):
                    won = self._wait_held[index]
                    if (
                        won not in won.resource.users
                        and not won.freed
                        and not no_time_left
                    ):
                        self._target = f"resource.{won.resource.obj_id}"
                        try:
                            # Asked again with the rank and flag it was won with
                            again = getattr(won, "asked", None) or (
                                self._rank_on(won.resource),
                                self._preempt(),
                            )
                            fresh = won.resource.request(again[0], preempt=again[1])
                            fresh.asked = again
                            self._stamp(fresh)
                            self._track(fresh)
                        except QueueFull:
                            self._release(held)
                            self._release_kept(own=self.child)
                            self._balk_full(won.resource)
                            return None
                        self._wait_held[index] = fresh
                        deadline = self._deadline()
                        if deadline is None:
                            yield fresh
                        else:
                            yield fresh | deadline
                            self._waited()
                            # The patience ran out before the unit was free, as
                            # for a `resource:` unit.
                            if not _won(fresh):
                                fresh.cancel()
                                self._release(held)
                                self._renege()
                                return None
                for at, resource in enumerate(spec.resources):
                    if at in slots or no_time_left:
                        continue
                    self._target = f"resource.{resource}"
                    try:
                        here = self.context._resources_map[resource]
                        request = here.request(
                            self._rank_on(here, None, rank), preempt=may_preempt
                        )
                        self._stamp(request)
                        self._track(request)
                    except QueueFull:
                        self._release(held)
                        self._release_kept(own=self.child)
                        self._balk_full(self.context._resources_map[resource])
                        return None
                    held.append(request)
                    slots[at] = request
                    deadline = self._deadline()
                    if deadline is None:
                        yield request
                    else:
                        yield request | deadline
                        self._waited()
                        # The patience ran out before a unit was free: leave the
                        # queue, give back the units held so far, and renege.
                        if not _won(request):
                            self._release(held)
                            self._renege()
                            return None
                if spec.get is not None and not got_item and not spec.get.wait:
                    # Made and not waited for: it takes an item later and discards it
                    self._transfer_event(spec.get, "get", None)
                    got_item = True
                if spec.get is not None and not got_item:
                    self._target = f"get.{spec.get.target}"
                    got = yield from self._transfer(
                        spec.get, "get", None, self._deadline()
                    )
                    self._waited()
                    if got is _RENEGED:
                        self._release(held)
                        self._renege()
                        return None
                    self.item, got_item = got, True
                    self.item_priority = self._taken_priority
                if not started:
                    started = True
                    if spec.timed:
                        self.publish(self.status("started"), lifecycle=True)
                        stats.task_started(spec.name, self.task_id)
                    # Units the task before kept for this one go back once it starts.
                    # A child flow gives back only its own: the units its parent
                    # kept stay until the parent's next task starts.
                    self._release_kept(own=self.child)
                    try:
                        payload = spec.user_func(
                            self.task_id, *self.args, **self._extra()
                        )
                        if payload is NO_RECORD:
                            payload, silent = None, True
                    except Exception as error:  # noqa: BLE001: a task's failure
                        failure = f"{type(error).__name__}: {error}"
                        self._fail_error = error
                        self._raised = True
                    if failure is None and self._template_waits(payload):
                        # The Template looks up a task with no row yet: the start is
                        # dropped as the gate drops one, giving back the units and
                        # the wait branches' requests. The function has run, `queued`
                        # and `started` are published as far as the task has them, and
                        # the item a `get` or a `wait` get branch took is consumed.
                        self._release(held)
                        self._release_kept(own=self.child)
                        self._end_wait()
                        stats.task_dropped(spec.name, self.task_id)
                        self._dropped = True
                        self._end_flow("ended")
                        return None
                    if failure is None and spec.on_start:
                        try:
                            self._start_values()
                        except Exception as error:  # noqa: BLE001: as above
                            failure = f"{type(error).__name__}: {error}"
                            self._fail_error = error
                            self._raised = True
                elif not no_time_left:
                    self.publish(self.status("resumed"))
                    stats.task_resumed(spec.name, self.task_id)
                if failure is None:
                    if spec.has_service and not served:
                        if remaining is None:
                            remaining = self._service()
                        service_from = env.now
                        self._target = "service"
                        yield env.timeout(remaining)
                        remaining, service_from, served = 0.0, None, True
                    if not values_done and (
                        self._has_values
                        or isinstance(payload, (Template, BoundPayload))
                    ):
                        try:
                            payload = self._finish_values(payload)
                        except Exception as error:  # noqa: BLE001: as above
                            failure = f"{type(error).__name__}: {error}"
                            self._fail_error = error
                            self._raised = True
                    values_done = True
                if failure is None:
                    if spec.put is not None and not put_done:
                        if spec.put.wait:
                            self._target = f"put.{spec.put.target}"
                            yield from self._transfer(spec.put, "put", payload, None)
                        else:
                            # Made and not waited for, and never set as pending, so
                            # an interrupt, an abandon or a patience cannot cancel it
                            self._transfer_event(spec.put, "put", payload)
                        put_done = True
                    failure = self._failure(payload)
                if failure is None:
                    if not silent:
                        self.publish(payload, key=self._payload_key(payload))
                        self.context.values.record(spec.name, payload, self.entity)
                    stats.task_finished(spec.name, self.task_id)
                    # A stop condition, such as `t >= 300`, is checked after every
                    # finished task as well as after every change of state
                    env.notify_change()
                else:
                    self.publish(self.status("failed", reason=failure))
                    stats.task_failed(spec.name, self.task_id)
            except Interrupt as interrupt:
                self._waited()
                all_granted = bool(held) and all(r.triggered for r in held)
                if spec.on_interrupt == "abandon":
                    self._release(held)
                else:
                    # A plain interrupt leaves a granted unit with the task; a
                    # preemption or a breakdown has already taken it
                    self._keep_granted(held, slots)
                # A unit asked for again and not yet granted leaves the line, so
                # the resume asks once, not twice
                for request in self._wait_held:
                    if not request.triggered:
                        request.cancel()
                # The service time that was left when the interrupt came, for
                # `cause.left`; None when nothing was being served
                left = (
                    remaining - (env.now - service_from)
                    if service_from is not None and remaining is not None
                    else None
                )
                if (
                    left is None
                    and spec.has_service
                    and not served
                    and remaining is None
                    and spec.on_interrupt == "abandon"
                    and spec.on_interrupt_next is not None
                    and all_granted
                ):
                    # Taken off the unit at the instant it was granted, before the
                    # service began: the whole time is left
                    left = remaining = self._service()
                if service_from is not None and remaining is not None:
                    # resume serves what is left; restart draws a new service time
                    if spec.on_interrupt == "resume":
                        remaining -= env.now - service_from
                        if remaining <= _NO_TIME_LEFT:
                            # The work ended at this instant: the task ends here and
                            # asks for no unit again, as SimPy's `while left > 0:`
                            # ends. `_NO_TIME_LEFT` allows for float error.
                            remaining, served, no_time_left = 0.0, True, True
                    else:
                        remaining = None
                if self._interrupted(interrupt):
                    continue
                return self._abandon(interrupt, left)
            except Exception:
                # Any other error gives the units back before it goes on, as a
                # `with` block does, so a child flow whose failure is handled
                # elsewhere does not keep them for the rest of the run
                self._release(held)
                self._release_kept(own=True)
                self._end_wait()
                raise
            self._release(held)
            break

        self._stop_running()
        self._end_wait()
        if failure is not None:
            if spec.signal is not None:
                self.context._fire_signal(spec.signal, False, failure, self.entity)
            failed = Upstream(
                self.task_id,
                self.entity.key,
                self.entity.attributes,
                self.status("failed", reason=failure),
            )
            # The end event fails whether or not on_fail handles the task, so a
            # waiter sees the failure. The `on:` actions see it too, unless a wait
            # failed or the function raised and no `on_fail` handles that: then the
            # task ends in a run failure, and no action runs (D3). The task's own
            # `fail` is not a run failure.
            self._set_ended(
                failed,
                ok=False,
                result=TaskFailed(spec.name, failure),
                actions=not self._raised or spec.on_fail is not None,
            )
            if spec.on_fail is not None:
                return self._follow(spec.on_fail, failed)
            # No task follows, so the units this flow keeps for it go back
            self._release_kept(own=True)
            if self.child:
                raise TaskFailed(spec.name, failure) from self._fail_error
            # Under `continue` a root task's failure ends the flow and the run goes
            # on; under `stop` it raises, so the exception reaches the caller of
            # `run()`, as SimPy's unhandled failure does.
            if getattr(self.env, "on_unhandled_failure", "stop") == "stop":
                raise TaskFailed(spec.name, failure) from self._fail_error
            self._end_flow("failed")
            return None

        if spec.signal is not None:
            self.context._fire_signal(spec.signal, True, payload, self.entity)
        if spec.interrupt is not None:
            cause = spec.interrupt.get("cause")
            cause = self._generated(cause) if spec.cause_generated else cause
            if spec.interrupt.get("handle") is not None:
                self._interrupt_child(spec.interrupt["handle"], cause)
            else:
                self.context.interrupt(
                    spec.interrupt["task"],
                    where=spec.interrupt_where,
                    cause=cause,
                )
        if spec.spawn is not None:
            # Start a new entity on success, after the signal and interrupt actions
            # and before `set`, so its upstream shares the spawning entity's
            # attributes dictionary as later `set` calls change it.
            self.context._spawn_children(
                spec.spawn,
                self.entity,
                Upstream(
                    self.task_id, self.entity.key, self.entity.attributes, payload
                ),
                self.scope(),
            )
        self.entity.attributes.update(
            self._generated(spec.attribute_changes)
            if spec.set_generated
            else spec.attribute_changes
        )
        if spec.set_args:
            # Written in this process's own names, after `set`
            if self.arg_values is None:
                self.arg_values = {}
            self.arg_values.update(
                self.context.values.evaluate(spec.set_args, self.scope())
            )
        if spec.release is not None:
            self._release_step()
        if spec.activate:
            self.context._set_arrivals(spec.activate, True)
        if spec.deactivate:
            self.context._set_arrivals(spec.deactivate, False)
        handed = Upstream(
            self.task_id, self.entity.key, self.entity.attributes, payload
        )
        following = spec.router.choose(
            handed,
            env,
            self.context._levels(),
            item=self.item,
            scope=self.scope(handed) if spec.router.conditions else None,
        )
        # The end event succeeds once the next task is chosen, before the hand-on.
        self._set_ended(handed, ok=True, result=payload)
        if following is None:
            self._release_kept(own=True)
            if self.child:
                return payload
            self._end_flow("finished")
            return payload
        self._follow(following, handed)
        return payload

    # ------------------------------------------------- holding: seize, open, release

    def _handle_of(self, name: str) -> Optional[Handle]:
        """The handle under `name` in this flow's table or its parents', or None."""
        table = self._handles if self._handles is not None else self._parent_handles
        return None if table is None else table.find(name)

    def _handle(self, name: str, key: str) -> Handle:
        """The handle under `name`.

        Raises:
            KeyError: If no task of this flow, or of the flow that started it, opened
                that name before this step.
        """
        found = self._handle_of(name)
        if found is None:
            raise KeyError(
                f"task '{self.spec.name}' {key}: no event is open under the name "
                f"'{name}' at this step"
            )
        return found

    def _table(self) -> HandleTable:
        """This flow's table of handles, made when the first name is opened."""
        if self._handles is None:
            self._handles = HandleTable(self._parent_handles)
        return self._handles

    def _visible_handles(self) -> Optional[HandleTable]:
        """The table a child flow started now reads: this flow's, or its parent's."""
        return self._handles if self._handles is not None else self._parent_handles

    def _fail_with(self, error: BaseException) -> str:
        """Publishes the failure of a step that raised, as the task's own failure, and
        returns its reason."""
        reason = f"{type(error).__name__}: {error}"
        self._fail_error = error
        self._raised = True
        self.publish(self.status("failed", reason=reason))
        self.env.stats.task_failed(self.spec.name, self.task_id)
        return reason

    def _rank_on(
        self,
        resource: Any,
        options: Optional[RequestOptions] = None,
        task_rank: Any = _UNSET,
    ) -> Any:
        """The rank a request for `resource` asks with. A resource with a `rank` rule
        ranks every request by it, in this flow's scope. Otherwise the request's own
        `priority` ranks it (a `seize` or a `request` mapping). A request mapping with
        no `priority` ranks 0, as SimPy does, and anything else the task's.

        Args:
            resource: The resource asked.
            options: The `seize` or `request` mapping the request is made by, if any.
            task_rank: The task's rank when it was read already, so it is read once.
        """
        rule = getattr(resource, "rank_of", None)
        if rule is not None:
            try:
                return rule.evaluate(self.scope())
            except ValueError as error:
                raise ValueError(
                    f"the queue rank of resource '{resource.obj_id}': {error}"
                ) from None
        priority = None if options is None else options.priority
        if isinstance(priority, Expression):
            return priority.evaluate(self.scope())
        if callable(priority):
            return priority(self.task_id, self.entity.attributes)
        if priority is not None:
            return priority
        if options is not None and options.mapping:
            return 0
        return self._rank() if task_rank is _UNSET else task_rank

    def _preempt(self, options: Optional[RequestOptions] = None) -> bool:
        """Whether a request may preempt: its own flag (a `seize` or a `request`
        mapping), or else true for a request mapping, as SimPy's default, and the
        task's for anything else. An expression is read now, as the request is made."""
        preempt = None if options is None else options.preempt
        if preempt is None:
            if options is not None and options.mapping:
                return True
            preempt = self.spec.preempt_of or self.spec.preempt
        if isinstance(preempt, Expression):
            return bool(preempt.evaluate(self.scope()))
        return bool(preempt)

    def _stamp(self, request: Any) -> None:
        """Marks a request with the entity that made it, for the records and for the
        `holder.attr` of a preemption rule."""
        request.entity_key = self.entity.key
        request.attributes = self.entity.attributes

    def _track(self, request: Any) -> None:
        """Lists a request a `resource:` or a wait `request` made, as `_ask` lists a
        `seize`, so a `release: {resource, task}` of another flow can free its unit.
        The flow's own `seize` list does not hold it, so a preemption does not ask for
        it again here."""
        request.run = self
        request.flow = []
        self._note_held(request)

    def _note_held(self, request: Any) -> None:
        """Lists a request under its task when a `release: {resource, task}` names that
        task, so another flow can free the unit. The list drops requests that no longer
        hold or wait, each time it has doubled."""
        listed = self.context._held_by_task.get(self.spec.name)
        if listed is None:
            return
        if len(listed) >= self.context._held_prune:
            listed[:] = [
                r
                for r in listed
                if r in r.resource.users
                or (not r.triggered and r in r.resource.put_queue)
            ]
            self.context._held_prune = max(64, 2 * len(listed))
        listed.append(request)

    def _ask(
        self,
        resource: Any,
        rank: Any,
        preempt: bool,
        handle: Optional[Handle] = None,
        run: Optional["TaskRun"] = None,
    ) -> Any:
        """Requests a unit for the flow, and lists it with the flow's requests.

        Args:
            resource: The resource.
            rank: The request's priority.
            preempt: Whether it may preempt.
            handle: The handle the request is opened under, or None for a `seize`.
            run: The instance the request counts as made by; this one by default.

        Raises:
            QueueFull: If the waiting line is full.
        """
        request = resource.request(rank, preempt=preempt)
        self._stamp(request)
        request.asked = (rank, preempt)
        request.handle = handle
        request.flow = self._seized
        request.run = run or self
        self._seized.append(request)
        self._note_held(request)
        return request

    def _forget(self, request: Any) -> None:
        """Takes a request out of the lists of its flow and of its task."""
        if request in request.flow:
            request.flow.remove(request)
        listed = self.context._held_by_task.get(request.run.spec.name)
        if listed is not None and request in listed:
            listed.remove(request)

    def _give_back(self, request: Any) -> None:
        """Gives back a unit the request holds; a request that holds none is left as it
        is, so it is granted later, and one given back already has no effect."""
        if request in request.resource.users:
            request.freed = True
            request.resource.release(request)
            self._forget(request)

    def _reask(self, old: Any) -> Any:
        """Asks again, under the same name, for a unit a preemption took, or a request
        that left its line: the new request takes the old one's place in the flow's
        list, and a handle that named the old one names the new one."""
        rank, preempt = old.asked
        old.resource._remove(old)
        fresh = self._ask(old.resource, rank, preempt, old.handle, old.run)
        self._forget(old)
        if old.handle is not None:
            old.handle.event = fresh
        return fresh

    def _ask_lost(self) -> Any:
        """Asks again for each unit of the flow a preemption took. A `seize` unit is
        waited for, and a unit opened under a name only replaces the request that name
        holds. Returns the resource whose full line stopped it, or None."""
        for old in [r for r in self._seized if r.preempted]:
            try:
                fresh = self._reask(old)
            except QueueFull:
                self._forget(old)
                return old.resource
            if fresh.handle is None:
                self._awaiting.insert(0, fresh)
        return None

    def _cancel_awaiting(self) -> None:
        """Takes the `seize` requests this instance still waits for out of their
        lines. A unit granted already stays held, as the Unit states table says."""
        for request in self._awaiting:
            if not request.triggered:
                request.cancel()
                self._forget(request)
        self._awaiting = []

    def _holding_steps(self):
        """Steps 5, 7 and 8 of the task's keys: `open`, `cancel` and `seize`.

        Returns:
            None when they are done, `_LEFT` when the task balked or reneged, or the
            `Interrupt` that ends the task under `abandon`.
        """
        spec = self.spec
        if spec.open is not None:
            full = self._open()
            if full is not None:
                self._release_kept(own=self.child)
                self._balk_full(full)
                return _LEFT
        if spec.cancel is not None:
            self._cancel_step()
        if spec.seize is not None:
            seize = spec.seize
            resource = self.context._resources_map[seize.resource]
            try:
                request = self._ask(
                    resource, self._rank_on(resource, seize), self._preempt(seize)
                )
            except QueueFull:
                self._release_kept(own=self.child)
                self._balk_full(resource)
                return _LEFT
            self._awaiting.append(request)
            return (yield from self._acquire())
        return None

    def _acquire(self):
        """Waits until every `seize` request of this instance is granted, after asking
        again for each unit of the flow a preemption took. An interrupt follows the
        task's policy while it waits: `resume` keeps the place, `restart` leaves the line
        and asks again at the back, and `abandon` leaves it and ends the task. The
        patience runs while it waits.

        Returns:
            None when every request is granted, `_LEFT` when the task balked or
            reneged, or the `Interrupt` that ends the task under `abandon`.
        """
        spec = self.spec
        while True:
            full = self._ask_lost()
            if full is not None:
                self._cancel_awaiting()
                self._release_kept(own=self.child)
                self._balk_full(full)
                return _LEFT
            if not self._awaiting:
                return None
            request = self._awaiting[0]
            self._target = f"resource.{request.resource.obj_id}"
            deadline = self._deadline()
            try:
                if deadline is None:
                    yield request
                else:
                    yield request | deadline
                    self._waited()
                    if not _won(request):
                        # The patience ran out first. A unit granted at this instant
                        # but not yet taken goes back
                        self._cancel_awaiting()
                        self._release_unit(request)
                        self._renege()
                        return _LEFT
            except Interrupt as interrupt:
                self._waited()
                if not self._interrupted(interrupt):
                    self._cancel_awaiting()
                    return interrupt
                if spec.on_interrupt == "restart":
                    try:
                        self._awaiting = [
                            r if r.triggered else self._reask(r) for r in self._awaiting
                        ]
                    except QueueFull:
                        self._cancel_awaiting()
                        self._release_kept(own=self.child)
                        self._balk_full(request.resource)
                        return _LEFT
                continue
            self._awaiting.remove(request)

    def _release_unit(self, request: Any) -> None:
        """Gives back a unit a `seize` was granted but did not take, or takes the
        request out of its line."""
        request.cancel()
        self._give_back(request)
        self._forget(request)

    def _open(self) -> Any:
        """Makes the events of `open`, in the order written, at step 5. Nothing waits
        for them. A request is queued at once and publishes `queued`, and `started`
        when it is granted, whatever the flow is doing then.

        Returns:
            The resource whose full line made the task balk, or None. A request opened
            earlier stays queued.
        """
        table = self._table()  # a child flow started below reads the names that follow
        for name, node in self.spec.open.nodes.items():  # type: ignore[union-attr]
            if node.kind == "request":
                resource = self.context._resources_map[node.target]
                handle = Handle("request", None)
                try:
                    request = self._ask(
                        resource,
                        self._rank_on(resource, node.options),
                        self._preempt(node.options),
                        handle,
                    )
                except QueueFull:
                    return resource
                handle.event = request
                self.publish(self.status("queued"), lifecycle=True)
                request.callbacks.append(self._started_later)
            elif node.kind in ("get", "put"):
                payload = (
                    self.upstream.payload
                    if node.kind == "put" and self.upstream is not None
                    else None
                )
                event = self._transfer_event(node.target, node.kind, payload)
                handle = Handle(node.kind, event)
                if node.kind == "get":
                    handle.value_of = functools.partial(
                        self._transfer_value, node.target, "get"
                    )
                    self.publish(self.status("queued"), lifecycle=True)
                    event.callbacks.append(self._started_later)
            else:
                members: Dict[str, Any] = {}
                handle = Handle(node.kind, self._open_event(node, members))
                if node.is_group:
                    handle.members = members
            table[name] = handle
        return None

    def _started_later(self, event: Any) -> None:
        """Publishes `started` for an opened request or get, as it is granted."""
        self.publish(self.status("started"), lifecycle=True)

    def _open_event(self, node: Any, members: Dict[str, Any]) -> Any:
        """The event of an opened leaf or group of timeouts, child flows, signals and
        handles. Each leaf's event is also put in `members` under its name, which a
        group's value reads."""
        if node.is_group:
            events = [self._open_event(child, members) for child in node.children]
            return (
                self.env.any_of(events)
                if node.kind == "any"
                else self.env.all_of(events)
            )
        if node.kind == "timeout":
            event = self._timeout_event(node)
        elif node.kind == "signal":
            event = self.context._signals[node.target].wait()
        elif node.kind == "handle":
            event = self._handle(node.target, "open").event
        else:
            event = self.context._start(
                node.target,
                self.entity,
                self.upstream,
                child=True,
                args=self.args,
                kwargs=self.kwargs,
                arg_values=(
                    None if node.args is None else self._generated(dict(node.args))
                ),
                handles=self._visible_handles(),
            )
        members[str(node.name)] = event
        return event

    def _cancel_step(self) -> None:
        """Step 7: removes the waiting request, get or put the name holds from its line.
        A granted one is left as it is, so its unit or item stays taken, and one
        cancelled before is a run failure, as SimPy's second `cancel` raises."""
        name = self.spec.cancel
        handle = self._handle(str(name), "cancel")
        if handle.kind not in ("request", "get", "put"):
            raise ValueError(
                f"task '{self.spec.name}' cancel: '{name}' was opened as a "
                f"{handle.kind}, and only a request, get or put waits in a line"
            )
        event = handle.event
        if getattr(event, "left_line", False):
            raise ValueError(
                f"task '{self.spec.name}' cancel: '{name}' was cancelled already"
            )
        if not event.triggered:
            event.cancel()
            event.left_line = True
            if handle.kind == "request":
                self._forget(event)

    def _release_step(self) -> None:
        """Step 21: gives back the units the task's `release` names."""
        release = self.spec.release
        assert release is not None
        if release.handle is not None:
            handle = self._handle(release.handle, "release")
            if handle.kind != "request":
                raise ValueError(
                    f"task '{self.spec.name}' release: '{release.handle}' was opened "
                    f"as a {handle.kind}, which holds no unit"
                )
            self._give_back(handle.event)
            return
        name = str(release.resource)
        if release.task is None:
            # The oldest unit of that resource this flow holds
            held = [
                r
                for r in self._seized
                if r.resource.obj_id == name and r in r.resource.users
            ]
            if held:
                users = held[0].resource.users
                self._give_back(min(held, key=users.index))
            return
        listed = self.context._held_by_task.get(release.task, [])
        chosen = [
            r
            for r in listed
            if r.resource.obj_id == name
            and r in r.resource.users
            and (release.where is None or release.where.evaluate(r.run.scope()))
        ]
        for request in sorted(chosen, key=lambda r: r.resource.users.index(r)):
            self._give_back(request)

    def _deadline(self) -> Any:
        """A timer for the patience left, started now, or None without patience."""
        if self._patience_left is None:
            return None
        self._patience_since = self.env.now
        return self.env.timeout(self._patience_left)

    def _waited(self) -> None:
        """Takes the time waited since `_deadline` from the patience left."""
        if self._patience_since is not None and self._patience_left is not None:
            self._patience_left -= self.env.now - self._patience_since
            self._patience_since = None

    def _release(self, held: List[Any]) -> None:
        """Cancels requests still waiting and gives back granted units, last first,
        as nested `with` blocks do; units the task keeps for the next one stay."""
        for request in reversed(held):
            request.cancel()
            name = request.resource.obj_id
            if (
                name in self.spec.keep
                and request.triggered
                and request in request.resource.users
            ):
                self.entity.held[name] = request
                continue
            request.resource.release(request)
        held.clear()
        if self._pending is not None:
            self._pending.cancel()
            self._pending = None

    def _keep_granted(self, held: List[Any], slots: Dict[int, Any]) -> None:
        """Keeps the granted units a resumed task goes on with: those still held on a
        resource that is up, and one that another flow released whatever the state
        of the resource (the holder is not told); cancels and gives back the others,
        so the resume asks for only the units a preemption or a breakdown took."""
        for at, request in list(slots.items()):
            resource = request.resource
            # A unit another flow released is not asked for again: only a preemption
            # or a breakdown takes a unit from its holder. A resource class may
            # preempt by its own rule, so a unit missing from `users` that no
            # `release` freed counts as taken.
            if request.triggered and (
                request.freed
                or (request in resource.users and not getattr(resource, "down", False))
            ):
                continue
            request.cancel()
            resource.release(request)
            held.remove(request)
            del slots[at]
        if self._pending is not None:
            self._pending.cancel()
            self._pending = None

    def _end_wait(self) -> None:
        """At the task's true end, not a resume, gives back the units a `request` wait
        branch won and cancels any wait-branch event still live, so a renege, a
        failing wait or an abandon leaves nothing reserved."""
        for request in self._wait_held:
            if request in request.resource.users:
                request.resource.release(request)
        self._wait_held = []
        for request in self._lost_requests:
            if request in request.resource.users:
                request.resource.release(request)
        self._lost_requests = []
        self._cancel_wait_events()

    def _cancel_wait_events(self) -> None:
        """Cancels every live request, get or put wait-branch event, giving back a
        unit a request already won. A store or container get or put has no `users`,
        so it is only cancelled, as SimPy leaves a won get."""
        branches = self.spec.wait.branches if self.spec.wait is not None else {}
        for name, event in self._wait_events.items():
            event.cancel()
            node = branches.get(name)
            if (
                node is not None
                and node.kind == "request"
                and event.triggered
                and event in event.resource.users
            ):
                event.resource.release(event)
        self._wait_events = {}

    def _release_kept(self, lost_only: bool = False, own: bool = False) -> None:
        """Gives back the units the task before kept for this one; with `lost_only`,
        only those on a resource that is down or that a preemption already took, and
        with `own`, only those this flow's process asked for, because the child flows
        of one entity share its kept units."""
        for name in list(self.entity.held):
            request = self.entity.held[name]
            resource = request.resource
            if own and request.proc is not self.env.active_process:
                continue
            if (
                lost_only
                and request in resource.users
                and not getattr(resource, "down", False)
            ):
                continue
            del self.entity.held[name]
            resource.release(request)

    def _interrupted(self, interrupt: Interrupt) -> bool:
        """Publishes an interrupt or a preemption; True when the task goes on.

        The task has already cancelled its waits. Under `abandon` it has given back
        its units, and so are the units the task before kept; otherwise a plain
        interrupt leaves a granted unit, and only a unit a breakdown or a preemption
        took goes back.
        """
        spec, stats = self.spec, self.env.stats
        abandon = spec.on_interrupt == "abandon"
        # A child flow that abandons gives back only what its own tasks asked for:
        # the units its parent kept stay until the parent's next task starts
        self._release_kept(lost_only=not abandon, own=self.child and abandon)
        cause = interrupt.cause
        preempted = isinstance(cause, Preempted)
        if preempted:
            value = self.status(
                "preempted",
                resource=cause.resource.obj_id,
                preempted_by=self.context._entity_key_of(cause.by),
                usage_since=cause.usage_since,
            )
        else:
            value = self.status("interrupted", cause=cause)
        self.publish(value)
        stats.task_interrupted(spec.name, self.task_id, preempted=preempted)
        return spec.on_interrupt != "abandon"

    def _abandon(self, interrupt: Interrupt, left: Optional[float] = None) -> None:
        """Ends the task after an interrupt with `on_interrupt: abandon`. `left` is
        the service time that was left, for `cause.left`."""
        spec, stats = self.spec, self.env.stats
        self._end_wait()
        # A `seize` unit a preemption took is ended: a later `release` does nothing
        self._cancel_awaiting()
        for request in [r for r in self._seized if r.preempted]:
            self._forget(request)
        stats.task_dropped(spec.name, self.task_id)
        self._stop_running()
        handed = Upstream(
            self.task_id,
            self.entity.key,
            self.entity.attributes,
            self.status("interrupted", cause=_cause(interrupt)),
        )
        if spec.on_interrupt_next is not None:
            self._set_ended(handed, ok=True, result=None)
            self._follow(
                spec.on_interrupt_next, handed, self._cause_values(interrupt, left)
            )
            return
        if self.child:
            # In a child flow with no on_interrupt_next, the end event fails with
            # the TaskFailed the abandon raises, so a waiter fails with it.
            self._set_ended(
                handed,
                ok=False,
                result=TaskFailed(spec.name, f"interrupted: {_cause(interrupt)}"),
            )
            raise TaskFailed(spec.name, f"interrupted: {_cause(interrupt)}")
        self._set_ended(handed, ok=True, result=None)
        self._end_flow("ended")

    def _cause_values(
        self, interrupt: Interrupt, left: Optional[float]
    ) -> Dict[str, Any]:
        """What `cause.<name>` reads in the task an `on_interrupt_next` edge reaches:
        for a preemption `by`, `usage_since` and `resource`, for an interrupt its
        `value`, and `left` where the task was serving."""
        cause = interrupt.cause
        values: Dict[str, Any] = {}
        if isinstance(cause, Preempted):
            values["by"] = self.context._entity_key_of(cause.by)
            values["usage_since"] = cause.usage_since
            values["resource"] = cause.resource.obj_id
        else:
            values["value"] = cause
        if left is not None:
            values["left"] = left
        return values

    # ----------------------------------------------------------------- helpers

    def _extra(self) -> Dict[str, Any]:
        """The keyword arguments the user function declares."""
        extra = dict(self.kwargs)
        declared = self.spec.declared
        if "entity" in declared:
            extra["entity"] = self.entity
        if "upstream" in declared:
            extra["upstream"] = self.upstream
        if "item" in declared and self.spec.get is not None:
            extra["item"] = self.item
        if "wait" in declared and self.spec.wait is not None:
            extra["wait"] = self.waits
        return extra

    def _service(self) -> Union[int, float]:
        """The time this instance works: a draw from the live `service_id`
        distribution, or its `service_time`."""
        spec = self.spec
        if spec.service_id is not None:
            config = self.env.registry.get_config(
                f"{self.context.sim_id}.service.{spec.service_id}"
            )
            sampler = self.context.sampler
            assert sampler is not None  # a run is under way
            return sampler.sample(config)
        if spec.service_reads:
            # A var the service time reads is given its value now, and the payload
            # reads the same value
            self._ensure_vars()
        return self._place(spec.service_time_of or spec.service_time, "service_time")

    def _place(self, value: Any, key: str) -> Union[int, float]:
        """Evaluates a time place: a number as it is, text read as an expression over
        the entity's names, a generator, or a function of the entity.

        Args:
            value: A number, the `Expression` of a text, a generator, or a function
                of the entity.
            key: The task key the value was written under, for the message.

        Returns:
            Union[int, float]: The seconds.

        Raises:
            TypeError: If the value is not a number of seconds, or a boolean.
            ValueError: If the seconds are negative.
        """
        if isinstance(value, Expression):
            value = value.evaluate(self.scope())
        elif isinstance(value, _Gen):
            value = self.context.values.evaluate(value, self.scope())
        elif callable(value):
            value = value(self.entity)
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            raise TypeError(
                f"task '{self.spec.name}' {key} gave {value!r}, which is not a number "
                f"of seconds"
            )
        if value < 0:
            raise ValueError(
                f"task '{self.spec.name}' {key} gave {value!r}, which is below 0"
            )
        return value

    def _patience(self) -> Union[int, float]:
        spec = self.spec
        if isinstance(spec.patience, _Gen):
            return self._place(spec.patience, "patience")
        if spec.patience_of is not None:
            given = as_given(spec.patience_of.evaluate(self.scope()))
        elif callable(spec.patience):
            given = as_given(spec.patience(self.task_id, self.entity.attributes))
        else:
            return as_given(spec.patience)
        if given < 0:
            raise ValueError(
                f"task '{spec.name}' patience gave {given!r}, which is below 0"
            )
        return given

    def _rank(self) -> Any:
        spec = self.spec
        if spec.rank_of is not None:
            return spec.rank_of.evaluate(self.scope())
        if callable(spec.priority):
            return spec.priority(self.task_id, self.entity.attributes)
        return 0 if spec.priority is None else spec.priority

    def _balk_full(self, resource: Any) -> None:
        """Ends the flow of a task whose resource's waiting line is full: it balks."""
        self._end_wait()
        record = self.status("balked", resource=resource.obj_id)
        self.publish(record)
        self.env.stats.task_balked(self.spec.name, self.task_id)
        self.outcome = Upstream(
            self.task_id, self.entity.key, self.entity.attributes, dict(record)
        )
        self._leave(self.spec.on_balk)

    def _renege(self) -> None:
        self._release_kept(own=True)
        self._end_wait()
        self.publish(self.status("reneged"))
        self.env.stats.task_reneged(self.spec.name, self.task_id)
        self.outcome = Upstream(
            self.task_id,
            self.entity.key,
            self.entity.attributes,
            self.status("reneged"),
        )
        self._leave(self.spec.on_renege)

    def _leave(self, target: Optional[str]) -> None:
        """Ends the flow of an instance that balked or reneged, or hands it on.

        Args:
            target: The task named by `on_balk` or `on_renege`, or None to end the
                flow. The task gets the balk or renege record as its upstream.
        """
        if target is None:
            self._end_flow("ended")
        else:
            assert self.outcome is not None  # a balk or renege sets it
            self._follow(target, self.outcome)

    def _end_flow(self, how: str) -> None:
        """Records the end of the entity's flow: `finished`, `failed`, or `ended`
        for a balk, a renege or an abandon. A child flow's end is its parent's.

        The entity leaves once its own flow has ended and no end event or counted
        signal firing is still pending, so a drain waits for the end event to be
        processed.
        """
        if self.child:
            return
        stats, arrival = self.env.stats, self.entity.arrival
        if how == "finished":
            stats.flow_finished(arrival)
        elif how == "failed":
            stats.flow_failed(arrival)
        else:
            stats.flow_ended(arrival)
        self.entity.ending = True
        if self.entity.ends_pending == 0:
            self.context._entity_left(self.entity)

    def _failure(self, payload: Any) -> Any:
        """The reason the task fails after its service, or None when it does not."""
        spec = self.spec
        if spec.fail is None:
            return None
        if spec.fail_of is not None:
            handed = Upstream(
                self.task_id, self.entity.key, self.entity.attributes, payload
            )
            failed = spec.fail_of.evaluate(self.scope(handed))
            text = spec.fail_of.text
        else:
            failed = spec.fail(self.task_id, payload)
            text = getattr(spec.fail, "__name__", "fail")
        if not failed:
            return None
        return spec.fail_reason if spec.fail_reason is not None else text

    def _follow(
        self, task: str, handed: Upstream, cause: Optional[Dict[str, Any]] = None
    ) -> None:
        """Makes the next instance, which `run` runs in the same process. `cause` is
        what an interrupt that led here gives `cause.<name>`."""
        context = self.context
        if task not in context._tasks:
            raise ValueError(
                f"task '{self.spec.name}' routes to '{task}', which is not a task"
                + suggestion(task, context._tasks)
            )
        following = TaskRun(
            context,
            context._task_specs[task],
            context.next_instance(task),
            self.entity,
            handed,
            self.args,
            self.kwargs,
            child=self.child,
            arg_values=self.arg_values,
        )
        # The flow is one process: its first task's name, the item it took and the
        # local names it wrote go on with it, as Python locals do
        following.flow_name = self.flow_name
        following.item, following.item_priority = self.item, self.item_priority
        # So are the names it opened and the units it holds, which stay with the flow
        following._handles, following._parent_handles = (
            self._handles,
            self._parent_handles,
        )
        following._seized = self._seized
        following.process = self.process
        following.cause_values = cause
        self._following = following

    def _transfer(
        self, transfer: Transfer, direction: str, payload: Any, deadline: Any
    ):
        """Gets from or puts into a store or container, waiting as long as it takes.

        A get with a `deadline` waits for whichever comes first; when the deadline
        does, the get is cancelled, so it takes nothing later, and `_RENEGED` is
        returned.
        """
        event = self._transfer_event(transfer, direction, payload)
        self._pending = event
        if deadline is None:
            yield event
        else:
            yield event | deadline
            if not _won(event):
                event.cancel()
                self._pending = None
                return _RENEGED
        self._pending = None
        return self._transfer_value(transfer, direction, event)

    def _transfer_event(self, transfer: Transfer, direction: str, payload: Any) -> Any:
        """Makes the SimPy get or put event, without waiting for it. A wait branch
        makes it this way to put it in the wait's condition; `_transfer` then waits."""
        context, scope = self.context, self.scope()
        if transfer.store is not None:
            store = context._stores_map[transfer.store]
            ranked = self._queue_rank(store, direction, scope)
            if direction == "get":
                event = store.get(transfer.filter_for(scope, self.entity), **ranked)
            else:
                if transfer.item is not None:
                    # The item of a `put`, read as the put is made
                    payload = self._generated(transfer.item)
                if context._store_priority.get(transfer.store):
                    rank = transfer.priority_for(self.task_id, self.upstream, scope)
                    event = store.put(store.wrap(payload, rank), **ranked)
                else:
                    event = store.put(payload, **ranked)
        else:
            container = context._containers_map[transfer.target]
            amount = transfer.amount_for(self.task_id, self.upstream, scope)
            ranked = self._queue_rank(container, direction, scope)
            event = (
                container.get(amount, **ranked)
                if direction == "get"
                else container.put(amount, **ranked)
            )
        # Who is waiting, for `putting()` and `getting()`
        event.entity_key = self.entity.key
        return event

    def _queue_rank(self, holder: Any, direction: str, scope: Scope) -> Dict[str, Any]:
        """The `rank` keyword of a get or put on a store or container whose waiting
        gets or puts are ranked, read once as the event is made; nothing for one that
        is not."""
        rule = getattr(holder, f"{direction}_rank_of", None)
        if rule is None:
            return {}
        try:
            return {"rank": rule.evaluate(scope)}
        except ValueError as error:
            raise ValueError(
                f"the {direction}_queue rank of '{holder.obj_id}': {error}"
            ) from None

    def _transfer_value(self, transfer: Transfer, direction: str, event: Any) -> Any:
        """The value a finished get or put event gives the task: the item a get took,
        unwrapped from a priority store's wrapper, or None for a put."""
        if direction == "get" and transfer.store is not None:
            if self.context._store_priority.get(transfer.store):
                self._taken_priority = event.value.priority
                return event.value.item
            return event.value
        return None

    def _wait(self, deadline: Any):
        """Waits for the task's `wait`, then records each branch for `wait.<branch>`.

        Returns None when the wait succeeded, `_RENEGED` when the patience ran out
        first, or the failure reason when a branch failed.
        """
        env, wait = self.env, self.spec.wait
        assert wait is not None
        self._target = self._target_label(wait.root)
        events: Dict[str, Any] = {}
        built: Dict[int, Any] = {}
        stop_watching: List[Callable[[], None]] = []
        self._single_timeout = None

        def build(node: Any) -> Any:
            """Builds the SimPy event for a node, nesting groups as SimPy conditions."""
            if node.is_group:
                members = [build(child) for child in node.children]
                if node.kind == "any":
                    event: Any = env.any_of(members)
                elif node.kind == "all":
                    event = env.all_of(members)
                elif node.kind == "when":
                    event = Condition(
                        env,
                        lambda evs, c, n=node: self._holds(n, c, built),
                        members,
                    )
                else:
                    count = node.count
                    event = Condition(env, lambda evs, c, n=count: c >= n, members)
            elif node.kind == "signal":
                event = self.context._signals[node.target].wait()
            elif node.kind == "task":
                # A wait resumed after an interrupt waits again for the child flow it
                # started, as a SimPy process yields the same process again
                child = self._children.get(node.name)
                if child is None:
                    child = self.context._start(
                        node.target,
                        self.entity,
                        self.upstream,
                        child=True,
                        args=self.args,
                        kwargs=self.kwargs,
                        arg_values=(
                            None
                            if node.args is None
                            else self._generated(dict(node.args))
                        ),
                        handles=self._visible_handles(),
                    )
                    self._children[node.name] = child
                event = child
            elif node.kind == "timeout":
                event = self._timeout_event(node)
                if node is wait.root:
                    self._single_timeout = (env.now, event._delay)
            elif node.kind == "handle":
                # The event `open` made, whatever has happened to it since: the same
                # event again after an interrupt, with its first end
                event = self._handle(node.target, "wait").event
            elif node.kind == "request":
                # Kept across a resume, as a `task` branch's child flow is, so the
                # task does not queue a second request each time it waits again.
                event = self._wait_events.get(node.name)
                if event is None:
                    resource = self.context._resources_map[node.target]
                    rank = self._rank_on(resource, node.options)
                    may_preempt = self._preempt(node.options)
                    event = resource.request(rank, preempt=may_preempt)
                    event.asked = (rank, may_preempt)
                    self._stamp(event)
                    self._track(event)
                    self._wait_events[node.name] = event
            elif node.kind in ("get", "put"):
                event = self._wait_events.get(node.name)
                if event is None:
                    payload = (
                        self.upstream.payload
                        if node.kind == "put" and self.upstream is not None
                        else None
                    )
                    event = self._transfer_event(node.target, node.kind, payload)
                    self._wait_events[node.name] = event
            elif node.kind == "running":
                event = self._running_event(node)
            else:
                event, stop = self._condition(node.target)
                stop_watching.append(stop)
            built[id(node)] = event
            if node.name is not None:
                events[node.name] = event
            return event

        def done_at_tie(node: Any) -> bool:
            """Whether a node is done when the patience ends at the instant the wait
            would: a leaf processed by the time the task resumes is in the value of
            SimPy's condition, which is built when the condition is processed. One
            granted or triggered but not yet processed has lost."""
            event = built[id(node)]
            if not node.is_group:
                return _won(event) and event.ok
            won = sum(done_at_tie(child) for child in node.children)
            if node.kind == "any":
                return won > 0 or not node.children
            if node.kind == "all":
                return won == len(node.children)
            if node.kind == "when":
                return self._holds(node, won, built, tie=done_at_tie)
            return won >= node.count

        try:
            root_event = build(wait.root)
        except QueueFull as error:
            # A request branch met a full line: the branches already built are
            # cancelled, and the wait fails with SimPy's message.
            for stop in stop_watching:
                stop()
            self._cancel_wait_events()
            return str(error)
        # A single-branch wait yields its event, as `yield event` does in SimPy, so
        # the task goes on in the step the event is processed; a group yields its
        # own condition.
        joined = root_event
        failure: Any = None
        try:
            yield joined if deadline is None else joined | deadline
        except Interrupt:
            raise
        except Exception as error:  # noqa: BLE001: a branch failed
            failure = getattr(error, "reason", None) or str(error)
        finally:
            # Losing condition branches stop checking; losing child flows keep
            # running, as SimPy leaves processes running.
            for stop in stop_watching:
                stop()
        # When the patience ends first and the wait's event or condition is not yet
        # processed, the wait has lost the tie. It still succeeds when its leaves
        # processed by then satisfy it.
        tie = failure is None and not joined.processed
        if tie and not done_at_tie(wait.root):
            return _RENEGED
        # Named nodes are recorded children before parents (the parse registers a
        # group after its children), so a group reads its children's recorded values.
        for name, node in wait.branches.items():
            event = events[name]
            if tie:
                done = done_at_tie(node)
                if node.is_group:
                    value = self._group_value(node) if done else None
                elif node.kind in ("request", "get", "put"):
                    value = self._resolve_branch(node, event, done)
                elif node.kind == "running":
                    value = self._running_value(node, done)
                else:
                    value = _branch_value(event) if done else None
            elif node.is_group:
                done = event.processed
                value = self._group_value(node) if done else None
            elif node.kind in ("request", "get", "put"):
                # A branch granted at the instant the wait ended, after the wait's
                # condition was processed, is not in its value: the wait ignores it,
                # so a get's item is lost and a request's unit is given back.
                done = _won(event) and event.ok if joined.ok else event.processed
                value = self._resolve_branch(node, event, done)
            elif node.kind == "running":
                # The branch event is an `AllOf` condition, so the wait's value holds
                # its leaves, not the condition itself; `processed` is the done flag,
                # as for a group node.
                done = event.processed
                value = self._running_value(node, done)
            else:
                done = (
                    (event.processed if event is joined else event in joined.value)
                    if joined.ok
                    else event.processed
                )
                value = _branch_value(event) if done else None
            self.waits[name] = {
                "done": done,
                # A group done at a patience tie is not triggered yet, and has no `ok`
                "ok": (event.ok if event.triggered else True) if done else None,
                "value": value,
            }
        # The wait resolved: a won request is now tracked in `_wait_held`, and losing
        # branches are cancelled, so the per-branch events need no more tracking.
        self._wait_events = {}
        return failure

    def _interrupt_child(self, name: str, cause: Any) -> None:
        """Interrupts the child flow a name was opened as, as `process.interrupt(cause)`
        does; a flow that has ended is a run failure, as it is in SimPy."""
        handle = self._handle(name, "interrupt")
        if handle.kind != "task":
            raise ValueError(
                f"task '{self.spec.name}' interrupt: '{name}' was opened as a "
                f"{handle.kind}, and only a child flow can be interrupted"
            )
        handle.event.interrupt(cause)

    def _timeout_event(self, node: Any) -> Any:
        """The SimPy timeout of a `timeout` branch of a wait or an `open`: the seconds
        and the value are read as the event is made, as the arguments of
        `env.timeout(delay, value)` are."""
        seconds, timeout_value = node.target
        if not isinstance(seconds, numbers.Real):
            # Only the vars_once exist before the task starts
            self._ensure_once()
            seconds = self._place(seconds, "timeout")
        if has_generator(timeout_value):
            self._ensure_once()
            timeout_value = self._generated(timeout_value)
        return self.env.timeout(seconds, timeout_value)

    def _holds(
        self,
        node: Any,
        done_count: int,
        built: Dict[int, Any],
        tie: Optional[Callable[[Any], bool]] = None,
    ) -> bool:
        """Whether a `when` group's condition holds now. It reads each named branch
        under the group as `wait.<branch>.done`, `.ok` and `.value`, and `done_count`.
        A branch is done when its event is processed, or with `tie` when `tie(branch)`
        says so."""
        waits: Dict[str, Dict[str, Any]] = {}

        def walk(group: Any) -> None:
            for child in group.children:
                event = built.get(id(child))
                if event is None:
                    continue
                done = tie(child) if tie is not None else event.processed
                if child.name is not None:
                    waits[child.name] = {
                        "done": done,
                        "ok": (event.ok if event.triggered else True) if done else None,
                        "value": _branch_value(event) if done else None,
                    }
                if child.is_group:
                    walk(child)

        walk(node)
        scope = replace(self.scope(), waits=waits, done_count=done_count)
        result = node.predicate.evaluate(scope)
        return bool(result)

    def _group_value(self, node: Any) -> dict:
        """What `wait.<group>.value` reads: a flat map of every done leaf under the
        group, nested groups included whether or not they are done themselves, as
        SimPy's `ConditionValue` is."""
        value: dict = {}
        for child in node.children:
            if child.is_group:
                value.update(self._group_value(child))
            elif child.name is not None and self.waits[child.name]["done"]:
                value[child.name] = self.waits[child.name]["value"]
        return value

    def _resolve_branch(self, node: Any, event: Any, done: bool) -> Any:
        """Settles a won or losing request, get or put wait branch, and returns the
        value `wait.<branch>` reads: a won request keeps its unit for the task to
        release at its end; a won get gives the item; a losing branch is cancelled,
        and a losing granted request gives its unit back."""
        if node.kind == "get" and (done or event.triggered):
            self._wait_took_item = True
        if done:
            if node.kind == "request":
                self._wait_held.append(event)
                return event
            if node.kind == "get":
                return self._transfer_value(node.target, "get", event)
            return None
        # A losing branch leaves the queue or reservation behind nothing. A request
        # granted after the wait's condition was processed keeps its unit until the
        # task ends, as a `with` block holds a granted request it did not use.
        event.cancel()
        if node.kind == "request" and event.triggered and event in event.resource.users:
            self._lost_requests.append(event)
        return None

    def _condition(self, condition: Expression):
        """An event that succeeds once the condition holds, and how to stop it."""
        env = self.env
        event = env.event()

        def check() -> None:
            if not event.triggered and condition.evaluate(self.scope()):
                event.succeed(True)
                stop()

        def stop() -> None:
            env.unwatch_changes(check)

        check()
        if not event.triggered:
            env.watch_changes(check, queues=condition.reads_queues)
        return event, stop


def flow_sets(
    routes: Mapping[str, Any],
    getters: Any,
    interrupt_targets: Any,
) -> Tuple[set, set]:
    """The tasks that read what an earlier task of the flow left.

    Args:
        routes: The tasks each task can lead to by `next`, `on_fail`, `on_renege`,
            `on_balk` and `on_interrupt_next`, by task name.
        getters: The tasks that take an item with a waiting store `get`.
        interrupt_targets: The tasks an `on_interrupt_next` edge reaches.

    Returns:
        Tuple[set, set]: The tasks reached after a waiting store get, which read
            `item` and `item_priority`, and the tasks an interrupt edge reaches,
            which read `cause.<name>`.
    """
    after_get: set = set()
    pending = list(getters)
    while pending:
        for target in routes.get(pending.pop(), ()):
            if target not in after_get:
                after_get.add(target)
                pending.append(target)
    return after_get, set(interrupt_targets)


def whole_flow_problems(
    routes: Mapping[str, Any], waits: Mapping[str, List[str]]
) -> List[str]:
    """The `whole_flow` waits that can wait for their own flow.

    A flow is one process, so the flow of task X ends only after every task that X
    leads to has ended, and after every child flow that a task of it waits for. A task
    that X leads to, or that such a child flow leads to, and that waits for the whole
    flow of X, waits for a flow that cannot end first.

    Args:
        routes: The tasks each task can lead to, by task name, as for `flow_sets`, and
            the child tasks its `wait` branches start.
        waits: For each task, the tasks its `running` branches with `whole_flow` name.

    Returns:
        List[str]: One message for each such task and target, naming both.
    """
    found: List[str] = []
    for task, targets in waits.items():
        for target in targets:
            reached: set = set()
            pending = list(routes.get(target, ()))
            while pending:
                name = pending.pop()
                if name not in reached:
                    reached.add(name)
                    pending.extend(routes.get(name, ()))
            message = (
                f"task '{task}' waits for the whole flow of task '{target}', and "
                f"'{target}' leads to '{task}', so the flow it waits for is the one "
                f"it runs in, which ends only after the wait does; drop whole_flow "
                f"to wait for the instance of '{target}' alone"
            )
            if task in reached and message not in found:
                found.append(message)
    return found


def scope_problem(
    expression: Expression,
    own_item: bool,
    later_item: bool,
    cause_ok: bool,
    in_when: bool,
) -> Optional[str]:
    """What an expression reads that no earlier step of its task gives, or None.

    Args:
        expression: The expression.
        own_item: Whether the place is evaluated after the task's own waiting store
            get, or is that get's filter.
        later_item: Whether the task is reached after a waiting store get by a route.
        cause_ok: Whether an `on_interrupt_next` edge reaches the task.
        in_when: Whether the place is the `when` of a wait group.

    Returns:
        Optional[str]: The reason the place cannot read the name, or None when it can.
    """
    flags = expression.references["flag"]
    if (flags & {"item", "item_priority"}) and not (own_item or later_item):
        return (
            "item and item_priority read the item a store get took, so they are read "
            "in the get's filter, in the places evaluated after a task's own get, "
            "and in the tasks that follow one"
        )
    if "done_count" in flags and not in_when:
        return "done_count is read in the when of a wait group"
    if expression.references["cause"] and not cause_ok:
        return "cause.<name> is read in a task that an on_interrupt_next edge reaches"
    return None


def _cause(interrupt: Interrupt) -> Any:
    """An interrupt's cause as a record holds it: a preemption by its resource."""
    cause = interrupt.cause
    if isinstance(cause, Preempted):
        return f"preempted on {cause.resource.obj_id}"
    return cause


def _branch_value(event: Any) -> Any:
    """A finished branch's value; a failed one's is its reason."""
    if event.ok:
        return event.value
    return getattr(event.value, "reason", str(event.value))


def check_acquisition_order(
    resources: Dict[str, List[str]],
    handoffs: Dict[str, Tuple[List[str], Any]],
    waits: Optional[Dict[str, Optional[Wait]]] = None,
    holds: Optional[Dict[str, "Holding"]] = None,
) -> List[str]:
    """
    Finds tasks that acquire shared resources in opposite orders, which can each wait
    for ever, and returns a warning for each pair.

    Two tasks that each hold one resource while they wait for the other can each
    wait for ever. A task that lists `[picker, bench]` acquires the picker first; a
    task's `seize` comes first, then its wait requests, then its resources; a task
    that keeps the picker for its next task acquires it before every resource that
    task requests or lists; and a task that follows a `seize` acquires what it
    wants after the resource the flow still holds from it, until a `release` of that
    resource. Two requests under an `all` (or an `at_least` that needs two) are made
    together, so either can be held while the other is waited for: no other place
    may acquire the two one after the other, in either order. If one resource comes
    before another in one place and after it in another, the model gets a warning, as
    SimPy runs such a model and its run ends with no error when the tasks wait.

    A task that keeps a resource for a task that requests it in its wait is refused,
    because the wait would queue behind the unit it keeps: the same model written
    another way runs.

    Args:
        resources: Each task's `resource` list, in the order it acquires them.
        handoffs: Each task's `keep` list and the tasks its `next` can lead to.
        waits: Each task's wait, if it has one.
        holds: Each task's `seize`, `release` and the tasks that can follow it.

    Returns:
        List[str]: One warning for each conflicting pair, naming both places.

    Raises:
        ValueError: If a task keeps a resource for a task that requests it in its wait.
    """
    waits = waits or {}
    holds = holds or {}
    before: Dict[Tuple[str, str], str] = {}
    together: Dict[Tuple[str, str], str] = {}
    warnings: List[str] = []
    warned: set = set()

    def warn(where: str, held: str, wanted: str, other: str) -> None:
        pair = frozenset((held, wanted))
        if pair in warned:
            return
        warned.add(pair)
        warnings.append(
            f"{other}, but {where} acquires '{held}' before '{wanted}'; the tasks can "
            f"each hold one and wait for the other for ever, and the run then ends "
            f"with no error. Acquire shared resources in one order everywhere to "
            f"avoid it"
        )

    def note(first: str, second: str, where: str) -> None:
        if (second, first) in before:
            warn(
                where,
                first,
                second,
                f"{before[(second, first)]} acquires '{second}' before '{first}'",
            )
        for pair in ((first, second), (second, first)):
            if pair in together:
                warn(where, first, second, together[pair])
        before.setdefault((first, second), where)

    def note_together(first: str, second: str, task: str) -> None:
        said = f"task '{task}' requests '{first}' and '{second}' together in its wait"
        for pair in ((first, second), (second, first)):
            if pair in before:
                warn(before[pair], pair[0], pair[1], said)
        together.setdefault((first, second), said)

    def requested(task: str) -> List[str]:
        wait = waits.get(task)
        return wait.requests() if wait is not None else []

    def seized(task: str) -> List[str]:
        holding = holds.get(task)
        return [] if holding is None or holding.seize is None else [holding.seize]

    for task, wait in waits.items():
        if wait is not None:
            for first, second in wait.requests_together():
                note_together(first, second, task)
    for task, listed in resources.items():
        own = seized(task)
        acquired = [*own, *requested(task), *listed]
        for i, first in enumerate(acquired):
            for second in acquired[i + 1 :]:
                if first in requested(task) and second in requested(task):
                    continue  # two wait requests: made together, or one at most
                note(first, second, f"task '{task}'")
    # The resources a flow holds when it reaches a task, from the `seize` of an earlier
    # task and until its `release`. A walk counts each (task, held) once, so a task that
    # loops on itself and seizes again ends it
    for start in [name for name, held in holds.items() if held.seize is not None]:
        walked: set = set()
        stack: List[Tuple[str, frozenset]] = [(start, frozenset())]
        while stack:
            task, held = stack.pop()
            if (task, held) in walked:
                continue
            walked.add((task, held))
            holding = holds.get(task)
            if holding is None:
                continue
            for first in sorted(held):
                for second in [
                    *seized(task),
                    *requested(task),
                    *resources.get(task, []),
                ]:
                    if second != first:
                        note(
                            first,
                            second,
                            f"task '{task}' holding '{first}' from a seize",
                        )
            now = set(held)
            if holding.seize is not None:
                now.add(holding.seize)
            if holding.release is not None:
                now.discard(holding.release)
            stack.extend((target, frozenset(now)) for target in holding.following)
    for task, (keep, following) in handoffs.items():
        for kept in keep:
            for after in following:
                if kept in requested(after):
                    raise ValueError(
                        f"task '{task}' keeps resource '{kept}' for '{after}', which "
                        f"requests '{kept}' in its wait, so the request would wait "
                        f"for the unit kept for it; list '{kept}' in the resource of "
                        f"'{after}' instead"
                    )
                for second in [*requested(after), *resources.get(after, [])]:
                    if second != kept:
                        note(kept, second, f"task '{task}' keeping it for '{after}'")
    return warnings


class Holding:
    """What a task does with a flow's held units, for `check_acquisition_order`.

    Attributes:
        seize (Optional[str]): The resource its `seize` asks for.
        release (Optional[str]): The resource its `release` gives back, by name.
        following (Set[str]): The tasks that can run next in the same flow.
    """

    def __init__(
        self, seize: Optional[str], release: Optional[str], following: Any
    ) -> None:
        self.seize = seize
        self.release = release
        self.following = set(following)


# What each use of an opened name needs the name to have been opened as
_HANDLE_NEEDS = {
    "line": (
        ("request", "get", "put"),
        "a request, get or put, the only events that wait in a line",
    ),
    "unit": (("request",), "a request, the only event that holds a unit"),
    "child": (("task",), "a child flow, the only event that can be interrupted"),
}


def handle_problem(
    opens: Mapping,
    uses: List[Tuple[str, str, str, str]],
) -> Optional[Tuple[str, str, str]]:
    """The first use of an opened name that no task makes sense of, or None.

    Args:
        opens: For each name, the task that opens it and what it opens as (`timeout`,
            `request`, `get`, `put`, `task`, `signal`, `any` or `all`), once for each
            task that opens it.
        uses: Each use as (task, key, name, need). `need` is `any` for a read or a
            wait, `line` for a `cancel`, `unit` for a `release` and `child` for an
            `interrupt`.

    Returns:
        Optional[Tuple[str, str, str]]: The task, the key and a message that names the
        name and why it fails, or None.
    """
    for task, key, name, need in uses:
        if name not in opens:
            return (
                task,
                key,
                f"names '{name}', which no task opens"
                + (suggestion(name, opens) or "; open it with open"),
            )
        if need in _HANDLE_NEEDS:
            kinds, words = _HANDLE_NEEDS[need]
            for opener, kind in opens[name]:
                if kind not in kinds:
                    return (
                        task,
                        key,
                        f"'{name}' is opened as a {kind} in task '{opener}', and "
                        f"{key} needs {words}",
                    )
    return None
