"""The state of one run of a `SimulationContext`, and the sets the load checks give.

`start()` makes a new `RunState` for each run, so no run starts from the state an
earlier run left: the task instances running, suspended or holding, the open batches,
the instance counters, the entities in the model, the end of the run and the drain.
`EndSets` holds what the load checks work out from the declarations about which task
instances need an end event; it is the same for every run of one model.
"""

import itertools
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import simpy

from desflow.core.arrivals import ArrivalRun
from desflow.core.expressions import Expression, Scope
from desflow.core.families import FamilyRun, whole
from desflow.core.holders import HolderRun
from desflow.core.signals import SignalRun
from desflow.models.flow import Entity

if TYPE_CHECKING:
    from desflow.core.arrivals import Arrivals
    from desflow.core.context import RunDefaults
    from desflow.core.environment import DynamicRealtimeEnvironment
    from desflow.core.families import Families
    from desflow.core.flow import LiveScope
    from desflow.core.holders import Holders
    from desflow.core.signals import Signals
    from desflow.core.sinks import Sinks
    from desflow.core.task import TaskRun
    from desflow.core.tasks import Tasks
    from desflow.values.runtime import ValueRuntime

# The length at which a task's list of held requests is first pruned
HELD_PRUNE = 64


@dataclass(frozen=True)
class EndSets:
    """Which tasks' instances need an end event, worked out by the load checks.

    Attributes:
        tracked: The tasks a `running` branch or `subscribe` names.
        copied: The tasks a `fire from` copies.
        ended: Every task whose instances get an end event: the two above and every
            task with `on:` actions.
        whole_flow: The tasks a `running` branch with `whole_flow` names.
        held: The tasks a `release: {resource, task}` names, whose requests a run
            lists.
    """

    tracked: FrozenSet[str] = frozenset()
    copied: FrozenSet[str] = frozenset()
    ended: FrozenSet[str] = frozenset()
    whole_flow: FrozenSet[str] = frozenset()
    held: FrozenSet[str] = frozenset()


@dataclass
class RunState:
    """The state of one run, made new by each `start()`.

    The run settings (`segments`, `steps`, `sync`, `on_unhandled_failure`) are set by
    `start()` before its checks; the build after them sets the run's `env`,
    `values`, `arrival_run`, `holder_run`, `signal_run` and `family_run` as it makes
    them, and the end (`until`, `end`, `end_done`, `limit_event`).

    Attributes:
        env: The run's environment, None until the build makes it.
        values: The run's values, None until the build makes them.
        held: The tasks a `release: {resource, task}` names, whose requests the run
            lists.
        running: The task instances running now, by task, which `interrupt()`
            reaches.
        suspended: The events of the instances suspended in a
            `wait: {suspend: true}`, by task, in the order they suspended.
        open_batches: The batch open under each batch name.
        flow_runs: The instances of each task a `running` branch with `whole_flow`
            names, ended ones too.
        held_by_task: The requests the tasks named by a `release: {resource, task}`
            hold or wait for, by task.
        held_prune: The length at which each of those lists is next pruned.
        routes: The generator each weighted `next` draws from, by task.
        instance_counters: The next instance id of each task.
        in_system: The entities in the model now.
        draining: Whether the drain has begun.
        until: The run's `until`.
        end: The event that ends the run when a `stop`, `drain` or sample limit is
            given.
        end_done: Whether `end` has been processed.
        stopped: Whether `stop_run` or the last segment ended the run.
        limit_event: The internal event at `until` that `advance()` stops at, made
            on first use.
        finish_run: Ends the run at the current time, set with `end`.
        moved: Whether `advance()` or `step()` has moved the run, so `finish()`
            leaves the segments and steps to the caller who is driving it.
        segments: The run's `segments`, or None.
        steps: Whether the run's `steps` is on.
        sync: Whether the run's `sync` is on.
        sample_seen: How many events a `desflow run --sample` preview has printed.
        on_unhandled_failure: What an unhandled task failure does: `stop` or
            `continue`.
        arrival_run: What the arrivals keep while the run goes on, None until the
            build makes it.
        holder_run: The run's resources, stores and containers, None until the build
            makes it.
        signal_run: The run's signals, None until the build makes them.
        family_run: What makes and picks the members of the families, None until
            the build makes it.
    """

    env: Optional["DynamicRealtimeEnvironment"] = None
    values: Optional["ValueRuntime"] = None
    held: FrozenSet[str] = frozenset()
    running: Dict[str, List["TaskRun"]] = field(default_factory=dict)
    suspended: Dict[str, List[Any]] = field(default_factory=dict)
    open_batches: Dict[str, Any] = field(default_factory=dict)
    flow_runs: Dict[str, List["TaskRun"]] = field(default_factory=dict)
    held_by_task: Dict[str, List[Any]] = field(default_factory=dict)
    held_prune: Dict[str, int] = field(default_factory=dict)
    routes: Dict[str, np.random.Generator] = field(default_factory=dict)
    instance_counters: Dict[str, "itertools.count[int]"] = field(default_factory=dict)
    in_system: int = 0
    draining: bool = False
    until: Any = None
    end: Optional[simpy.Event] = None
    end_done: bool = False
    stopped: bool = False
    limit_event: Optional[simpy.Event] = None
    finish_run: Optional[Callable[..., None]] = None
    moved: bool = False
    segments: Any = None
    steps: bool = False
    sync: bool = False
    sample_seen: int = 0
    on_unhandled_failure: str = "stop"
    arrival_run: Optional[ArrivalRun] = None
    holder_run: Optional[HolderRun] = None
    signal_run: Optional[SignalRun] = None
    family_run: Optional[FamilyRun] = None

    def __post_init__(self) -> None:
        self.held_by_task = {task: [] for task in sorted(self.held)}
        self.held_prune = {task: HELD_PRUNE for task in self.held_by_task}

    @property
    def arrivals(self) -> ArrivalRun:
        """What the arrivals keep while the run goes on, once the build has made it."""
        assert self.arrival_run is not None  # the run is built
        return self.arrival_run

    @property
    def holders(self) -> HolderRun:
        """The run's resources, stores and containers, once the build has made them."""
        assert self.holder_run is not None  # the run is built
        return self.holder_run

    @property
    def signals(self) -> SignalRun:
        """The run's signals, once the build has made them."""
        assert self.signal_run is not None  # the run is built
        return self.signal_run

    @property
    def families(self) -> FamilyRun:
        """What makes and picks the members of the families, once the build has
        made it."""
        assert self.family_run is not None  # the run is built
        return self.family_run

    def next_instance(self, task: str) -> int:
        """The next instance id of a task, counting from 0.

        Args:
            task: The task's name.
        """
        counter = self.instance_counters.setdefault(task, itertools.count())
        return next(counter)

    def note_held(self, task: str, request: Any) -> None:
        """Lists a request under its task when a `release: {resource, task}` names that
        task, so another flow can free the unit. The list drops requests that no longer
        hold or wait, each time it has doubled.

        Args:
            task: The task's name.
            request: The request.
        """
        listed = self.held_by_task.get(task)
        if listed is None:
            return
        if len(listed) >= self.held_prune[task]:
            listed[:] = [
                r
                for r in listed
                if r in r.resource.users
                or (not r.triggered and r in r.resource.put_queue)
            ]
            self.held_prune[task] = max(HELD_PRUNE, 2 * len(listed))
        listed.append(request)

    def entity_entered(self, entity: Entity) -> None:
        """Counts an entity whose first task instance has begun as in the model.

        Args:
            entity: The entity.
        """
        assert self.env is not None  # the run is built
        self.in_system += 1
        self.env.stats.flow_started(entity.arrival)

    def entity_left(self, entity: Entity) -> None:
        """Counts an entity out; a drain ends when the last one leaves.

        It drops the entity's instance lists, so a live run keeps lists only for
        entities still in the model and a `running` branch reads None as no instance.

        Args:
            entity: The entity.
        """
        assert self.values is not None  # the run is built
        self.in_system -= 1
        entity.instances = None
        entity.spawned_by = None
        entity.last_ended = None
        if entity.loop is None:
            self.values.forget(entity.key)
        else:
            entity.loop.active -= 1
            entity.loop.release(self.values, entity.key)
        if self.draining and self.in_system == 0 and self.finish_run is not None:
            self.finish_run()

    def end_processed(self, entity: Entity, _event: Any = None) -> None:
        """Takes 1 from `entity.ends_pending`; when it reaches 0 and the flow has
        ended, counts the entity out.

        Args:
            entity: The entity.
            _event: The event whose callback this is; not read.
        """
        entity.ends_pending -= 1
        if entity.ends_pending == 0 and entity.ending:
            self.entity_left(entity)

    def instance_scopes(self, task: str) -> List[Any]:
        """The scopes of the running instances of a task, for `running(task, ...)`.

        Args:
            task: The task's name.
        """
        return [run.scope() for run in self.running.get(task, [])]

    def entity_key_of(self, process: Any) -> Optional[str]:
        """The key of the entity whose task runs in `process`, as text; None for a
        process that is not a task, or None itself.

        Args:
            process: The process, or None.
        """
        if process is not None:
            for runs in self.running.values():
                for run in runs:
                    if run.flow.process is process:
                        return str(run.entity.key)
        return None

    def resume(
        self,
        task: str,
        where: Union[str, Expression, None],
        value: Any,
        limit: Any,
        scope: Optional[Scope] = None,
    ) -> int:
        """Wakes the instances of `task` suspended in a `wait: {suspend: true}`, in
        the order they suspended, as `event.succeed(value)` wakes a process.

        Args:
            task: The task whose suspended instances to wake.
            where: A condition read in each instance's scope, or None for all.
            value: What each woken instance reads as `wait.suspend.value`, already
                evaluated.
            limit: How many to wake at most: a whole number, a boolean counting as 1,
                an `Expression` read in `scope`, or None for all. 0 wakes none; below 0 is refused.
            scope: What a `limit` expression reads.

        Returns:
            int: How many instances were woken.

        Raises:
            TypeError: If `limit` is not a whole number.
        """
        if isinstance(limit, Expression):
            if scope is None:
                assert self.values is not None  # the run is built
                scope = self.values.scope_of(None)
            limit = limit.evaluate(scope)
        if limit is not None:
            limit = whole(limit, f"the resume limit of task '{task}'")
            if limit < 0:
                raise ValueError(
                    f"a resume limit must be 0 or more, got {limit!r} "
                    f"(the resume limit of task '{task}')"
                )
        condition = Expression(where) if isinstance(where, str) else where
        woken = 0
        for event in list(self.suspended.get(task, [])):
            if limit is not None and woken >= limit:
                break
            if condition is not None and not condition.evaluate(event.run.scope()):
                continue
            event.cancel()  # leaves the list
            event.succeed(value)
            woken += 1
        return woken


@dataclass(frozen=True)
class Parts:
    """The pieces of a model and the state of its open run, which the package's own
    code is given in place of reading the context's private names.

    The context's build makes one for each run and gives it to every task instance;
    `parts_of` in `core/context.py` makes one from a context's current attributes.

    Attributes:
        tasks: The tasks (`Tasks`).
        families: The families (`Families`).
        arrivals: The arrivals (`Arrivals`).
        holders: The resources, stores, containers and breakdowns (`Holders`).
        signals: The signals (`Signals`).
        sinks: The sinks (`Sinks`).
        defaults: What a run takes when its caller does not say (`RunDefaults`).
        scenario_steps: The scenario's steps.
        startup: The loops and processes a run starts, in the order they were added.
        ends: Which tasks' instances need an end event (`EndSets`).
        run: The state of the open run (`RunState`).
        scope: What every scope of the open run reads (`LiveScope`), None before the
            first build.
        services: The services' distributions, by name.
        processes: The functions `add_process` started, which `explain` shows as
            opaque nodes.
        run_scenario: The process that runs the scenario, which `explain` tells
            apart from the processes a model adds. It is the context's bound method
            `_run_scenario`, because `explain/graph.py` finds the scenario process
            by comparing each started function with it.
        readings: The parsed readings of each `add_readings` call, which the family
            check reads.
    """

    tasks: "Tasks"
    families: "Families"
    arrivals: "Arrivals"
    holders: "Holders"
    signals: "Signals"
    sinks: "Sinks"
    defaults: "RunDefaults"
    scenario_steps: List[Dict[str, Any]]
    startup: List[Tuple[Callable, Optional[str]]]
    ends: EndSets
    run: RunState
    scope: Optional["LiveScope"]
    services: Dict[str, Any]
    processes: List[Callable]
    run_scenario: Callable
    readings: List[Dict[str, Expression]]
