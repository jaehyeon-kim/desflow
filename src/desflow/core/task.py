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
import math
import numbers
from collections.abc import Mapping
import dataclasses
from dataclasses import dataclass, replace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)

from simpy import Interrupt
from simpy.events import Condition, Event
from simpy.resources.resource import Preempted

from desflow.core.expressions import ANY_NAME, Expression, Scope, boolean_word
from desflow.core.flow import (
    Listen,
    Open,
    Release,
    RequestOptions,
    Router,
    Seize,
    Transfer,
    Wait,
    WaitNode,
    as_given,
    next_task,
    transfer_expressions,
)
from desflow.resources.resource import QueueFull
from desflow.models.flow import Entity, Upstream, payload_of
from desflow.utils import suggestion
from desflow.values.check import names_read
from desflow.core.arrivals import whole
from desflow.core.families import Reference, name_of, references
from desflow.values.models import _Gen, has_generator
from desflow.values.runtime import RunStopped, stream_of
from desflow.values.template import BoundPayload, Template

if TYPE_CHECKING:
    from desflow.core.context import SimulationContext
    from desflow.core.runs import Parts


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


@dataclass(frozen=True)
class _Left:
    """What `_wait_phase` returns when the task ended before its service: `value` is
    what `_run` returns."""

    value: Any


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

    __slots__ = ("kind", "event", "value_of", "members", "items")

    def __init__(self, kind: str, event: Any):
        self.kind = kind
        self.event = event
        # What reads the value of a get (a priority store wraps its items), and, for a
        # group, its leaves' events by name
        self.value_of: Optional[Callable[[Any], Any]] = None
        self.members: Optional[Dict[str, Any]] = None
        # For an `each` group, the handle of each element, in order
        self.items: Optional[List["Handle"]] = None

    def read(self, name: str, part: str) -> Any:
        """The part of the event `handle.<name>.<part>` reads.

        Args:
            name: The name, for the message.
            part: `triggered`, `processed`, `ok`, `value` or `alive`, and `values` or
                `flags` for an `each` group.

        Raises:
            KeyError: If `alive` is read on an event that is not a child flow, or `ok`
                or `value` before the event happened, as SimPy's `Event.value` raises.
        """
        event = self.event
        if part in ("values", "flags"):
            if self.items is None:
                raise KeyError(
                    f"handle.{name}.{part} is read on a {self.kind}, and only an "
                    f"each group has {part}"
                )
            if part == "flags":
                return [item.event.processed for item in self.items]
            return [
                item.read(name, "value") if item.event.processed else None
                for item in self.items
            ]
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
    """What a task function returns to finish with no record: the Builder form of a
    blueprint's `payload: null`."""

    def __repr__(self) -> str:
        return "NO_RECORD"


NO_RECORD = _NoRecord()
"""What a task function returns to finish with no record: the Builder form of a
blueprint's `payload: null`.

Return `desflow.NO_RECORD` from the function a task wraps. The task still runs its
service, `set`, signal and `next`, and publishes no finished event.

Example:
    ```python
    from desflow import NO_RECORD


    @app.task(name="quiet", service_time=1)
    def quiet(task_id):
        return NO_RECORD
    ```
"""

# The service still owed, in seconds, at or below which a resumed task has none left
_NO_TIME_LEFT = 1e-9


class _HandlerFailed(Exception):
    """The flow `on_interrupt_run` names failed. The task has published `failed`
    already."""

    def __init__(self, reason: Any):
        super().__init__(reason)
        self.reason = reason


class _Suspension(Event):
    """The event of a `suspend` wait branch. It is listed under its task, in the order
    the instances suspended, until a `resume` wakes it or the wait leaves it."""

    def __init__(self, run: "TaskRun"):
        super().__init__(run.env)
        self.run = run
        run.parts.run.suspended.setdefault(run.spec.name, []).append(self)

    def cancel(self) -> None:
        """Takes the event out of the list of the suspended, so no `resume` reaches it.
        An event already woken is not listed any more."""
        listed = self.run.parts.run.suspended.get(self.run.spec.name, [])
        if self in listed:
            listed.remove(self)


def _won(event: Any) -> bool:
    """Whether a request, get or put counts as won against a patience or a wait that
    ended at the same instant: only when the event was processed before the wait's
    condition was, as SimPy's `req in results` is true only for such an event. One
    that is granted but not yet processed has lost, and its unit or item is not kept.
    """
    return event.processed


class Batch:
    """A task's `batch` key: members wait together and one resource and service run
    covers all of them.

    Attributes:
        name: The name of the batch. Tasks with the same name share one open batch.
            It defaults to the task's name.
        size: How many members close the batch: a number, or an expression read when
            the first member joins.
        timeout: How long after its first member the batch closes, or None.
        close_when: An expression tested as each member joins, which closes the batch
            when it holds; it reads `batch.size` and `batch.age`.
    """

    def __init__(
        self,
        size: Any,
        name: Optional[str] = None,
        timeout: Any = None,
        close_when: Optional[str] = None,
    ):
        """
        Args:
            size: A number, or text read as an expression.
            name: The batch's name, which defaults to the task's name.
            timeout: Seconds, an expression, a generator or a function of the entity.
            close_when: An expression over `batch.size`, `batch.age` and the run.

        Raises:
            ValueError: If a value is not of the kind the key takes.
        """
        if isinstance(size, bool) or not (
            isinstance(size, (numbers.Real, str, _Gen)) or callable(size)
        ):
            raise ValueError(f"batch size is a number or an expression, got {size!r}")
        if isinstance(timeout, bool):
            raise ValueError(f"batch timeout is a time, got {timeout!r}")
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError(f"batch name is text, got {name!r}")
        self.name = name
        self.size = size
        self.timeout = timeout
        self.close_when = close_when
        self.size_of = (
            Expression(size, names=ANY_NAME) if isinstance(size, str) else None
        )
        self.timeout_of = (
            Expression(timeout, names=ANY_NAME) if isinstance(timeout, str) else None
        )
        self.close_when_of = (
            None
            if close_when is None
            else Expression(close_when, names=ANY_NAME, batch=True)
        )

    @property
    def expressions(self) -> List[Expression]:
        """Every expression the batch holds."""
        return [e for e in (self.size_of, self.timeout_of, self.close_when_of) if e]


class _OpenBatch:
    """The members that wait to be served together, and what they wait for."""

    def __init__(self, key: str, size: Union[int, float], since: float):
        self.key = key
        self.size = size
        self.since = since
        self.members: List["TaskRun"] = []
        self.closed = False
        self.finished = False
        self.dead = False
        self.leader: Optional["TaskRun"] = None


class TaskExpression(NamedTuple):
    """One expression of a task.

    Attributes:
        key: The key it is written under, or for a wait the part it is read in
            (`wait_when`, `wait_group`), which the checks read.
        expression: The parsed expression.
        place: Where it is written under the task, such as `("get", "amount")` or
            `("next", "when", 2, "condition")`, for the six keys that hold one
            expression, a get's or put's `amount`, `priority` and `filter`, and the
            condition of each `when` rule of `next`; None for an expression inside a
            reference, a wait, a batch or another structure.
    """

    key: str
    expression: Expression
    place: Optional[Tuple[Union[str, int], ...]]


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
        fail_reason: Any = None,
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
        cancel_index: Optional[Expression] = None,
        on_interrupt_run: Optional[str] = None,
        resume: Optional[Dict[str, Any]] = None,
        batch: Optional[Batch] = None,
        listen: Optional[Listen] = None,
    ):
        self.name = name
        # Members wait together and are served by one run of the resource and service
        self.batch = batch
        # The task run as a child flow after an interrupt, and the suspended
        # instances this task wakes when it finishes
        self.on_interrupt_run = on_interrupt_run
        self.resume = resume
        # The units the flow keeps from this task until a `release`, the events it
        # opens under names, and the opened request or get it cancels
        self.seize = seize
        self.release = release
        self.open = open
        # The callbacks added to a signal or an opened name when the task starts
        self.listen = listen
        self.cancel = cancel
        # The expression that picks one element when `cancel` names an `each` group
        self.cancel_index = cancel_index
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
        self.resources: List[Union[str, Reference]] = [
            name_of(r, "resource")
            for r in (resources or ([resource_id] if resource_id else []))
        ]
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
        self.signal: Union[str, Reference, None] = (
            None if signal is None else name_of(signal, "signal")
        )
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
            or batch is not None
        )
        # Publishes started before its work
        self.timed = service_id is not None or service_time is not None or self.queues
        # Set when the function is decorated
        self.user_func: Callable[..., Any] = lambda *args, **kwargs: None
        self.declared: Any = {}

    @property
    def has_each(self) -> bool:
        """bool: Whether the task has a `wait` or an `open` with an `each` node."""
        for wait in (self.wait, self.subscribe, getattr(self.open, "wait", None)):
            if wait is None:
                continue
            for node in wait.all_nodes:
                if node.is_each:
                    return True
        return False

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
    def expressions(self) -> List[TaskExpression]:
        """Every expression the task holds, with the key it is written under and,
        where the spec knows it, its place under the task."""
        found: List[TaskExpression] = []
        for key, expression in (
            ("priority", self.rank_of),
            ("preempt", self.preempt_of),
            ("patience", self.patience_of),
            ("service_time", self.service_time_of),
            ("balk", self.balk_of),
            ("fail", self.fail_of),
        ):
            if expression is not None:
                found.append(TaskExpression(key, expression, (key,)))
        for key, transfer in (("get", self.get), ("put", self.put)):
            if transfer is not None:
                parts = {id(e): part for part, e in transfer_expressions(transfer)}
                found.extend(
                    TaskExpression(
                        key, e, (key, parts[id(e)]) if id(e) in parts else None
                    )
                    for e in transfer.expressions
                )
        # Weights and when rules are never both given, so unweighted conditions are
        # the conditions of the when rules, in rule order
        found.extend(
            TaskExpression(
                "next",
                e,
                None if self.router.weighted else ("next", "when", i, "condition"),
            )
            for i, e in enumerate(self.router.conditions)
        )
        if self.batch is not None:
            found.extend(
                TaskExpression("batch", e, None) for e in self.batch.expressions
            )
        for reference in references(self.resources, self.signal):
            found.extend(
                TaskExpression(
                    "signal" if reference is self.signal else "resource", e, None
                )
                for e in reference.expressions
            )
        if self.subscribe is not None:
            found.extend(
                TaskExpression("subscribe", e, None)
                for e in self.subscribe.reference_expressions
            )
        if self.seize is not None:
            found.extend(
                TaskExpression("seize", e, None) for e in self.seize.expressions
            )
        if self.open is not None:
            found.extend(TaskExpression("open", e, None) for e in self.open.expressions)
        if self.listen is not None:
            found.extend(
                TaskExpression("listen", e, None) for e in self.listen.expressions
            )
        found.extend(
            TaskExpression("on", a["when"], None)
            for a in self.on
            if a.get("when") is not None
        )
        if self.release is not None:
            found.extend(
                TaskExpression("release", e, None) for e in self.release.expressions
            )
        if self.interrupt_where is not None:
            found.append(TaskExpression("interrupt", self.interrupt_where, None))
        if self.interrupt is not None and self.interrupt.get("index") is not None:
            found.append(TaskExpression("interrupt", self.interrupt["index"], None))
        if self.cancel_index is not None:
            found.append(TaskExpression("cancel", self.cancel_index, None))
        if self.resume is not None:
            for part in ("where", "limit"):
                if isinstance(self.resume.get(part), Expression):
                    found.append(TaskExpression("resume", self.resume[part], None))
        if self.wait is not None:
            found.extend(TaskExpression("wait", e, None) for e in self.wait.conditions)
            found.extend(
                TaskExpression("wait", e, None) for e in self.wait.time_expressions
            )
            found.extend(
                TaskExpression("wait", e, None) for e in self.wait.request_expressions
            )
            found.extend(
                TaskExpression("wait", e, None) for e in self.wait.reference_expressions
            )
            found.extend(
                TaskExpression("wait_when", e, None) for e in self.wait.predicates
            )
            found.extend(
                TaskExpression("wait_group", e, None)
                for e in self.wait.group_expressions
            )
        return found


@dataclass
class _Flow:
    """What the instances of one flow hand on, one to the next, as one process keeps
    its locals: a `next` instance takes a copy of its predecessor's.

    Attributes:
        name: The first task of the flow, read as `process.flow_name`.
        parent_handles: The table of the flow that started this one (a child flow),
            which it reads.
        item: The item the flow took.
        item_priority: The priority of the item a priority store gave, which lasts
            with the item.
        process: The SimPy process the flow runs in.
        handles: The events the flow opened under names, made on the first `open`.
        seized: The requests the flow holds or waits for from a `seize` or an
            `open`, shared by the instances of one `next` chain, as a process keeps
            its own requests.
    """

    name: str
    parent_handles: Optional[HandleTable]
    item: Any = None
    item_priority: Any = None
    process: Any = None
    handles: Optional[HandleTable] = None
    seized: List[Any] = dataclasses.field(default_factory=list)


class TaskRun:
    """One instance of a task, run by `run()`, the SimPy process of its flow."""

    def __init__(
        self,
        context: SimulationContext,
        parts: "Parts",
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
        # The run's parts: its state, its scope and the model's pieces
        self.parts = parts
        self.spec = spec
        self.task_id = task_id
        self.entity = entity
        self.upstream = upstream
        self.args = args
        self.kwargs = kwargs
        # A child flow returns its last payload, or fails, to the task waiting for it
        self.child = child
        self.env = context.env
        # What the flow hands on to its next instance
        self.flow = _Flow(spec.name, handles)
        self._taken_priority: Any = None
        # Whether a `get` branch of the wait took an item out of its store
        self._wait_took_item = False
        # The process's local names, read as `arg.<name>`: what a parent passed and
        # what `set_args` wrote. They are one dictionary for the whole flow, so the
        # next task of the flow shares it; None until one is passed or written
        self.arg_values: Optional[Dict[str, Any]] = arg_values
        # Why the task was reached by an `on_interrupt_next` edge, read as `cause.`
        self.cause_values: Optional[Dict[str, Any]] = None
        self.waits: Dict[str, Dict[str, Any]] = {}
        # The `seize` requests this instance still waits to be granted
        self._awaiting: List[Any] = []
        # Whether the context lists this instance as running, which `reach` does from
        # the step the instance is started in and `_run_one` does otherwise
        self._listed = False
        # Whether the lookup gate dropped this start, so its `on:` actions do not run
        self._dropped = False
        # Whether the failure the task ends in is a run failure: a wait that failed,
        # or a task function that raised, as an uncaught exception ends a SimPy process
        self._raised = False
        self._done_counted = False  # `task_finished` is counted
        self._signalled = False  # the `signal` fired as succeeded
        # The instance this one handed on to, which `run` runs next; None if none
        self._following: Optional["TaskRun"] = None
        # The put or get the task is waiting for, cancelled by an interrupt
        self._pending: Any = None
        # The child flows a `{task}` wait started, by branch, kept for a resume
        self._children: Dict[str, Any] = {}
        # The exception a task function raised, to chain the TaskFailed it causes
        self._fail_error: Optional[BaseException] = None
        self._first_resource: Optional[str] = None  # the first `resource`, read once
        # Units won by a `request` wait branch, held through the service and given
        # back when the task ends, as a `resource:` unit is
        self._wait_held: List[Any] = []
        # The requests a wait branch lost by being granted after the wait ended,
        # whose units go back when the task ends
        self._lost_requests: List[Any] = []
        # The live `request`, `get` and `put` branch events of the current wait, kept
        # so a resume reuses them and a renege or an abandon cancels them
        self._wait_events: Dict[str, Any] = {}
        # The names of the requests a restart kept, whose leaf is compared once with
        # the resource it now asks for; and the element a `handle` leaf with an
        # `index` resolved on its first build, kept across a resume and a restart
        self._restart_kept: Set[str] = set()
        self._leaf_elements: Dict[str, Handle] = {}
        # The wait's tree with each `each` node made into a group of one leaf for each
        # element, and its leaves by name, kept for a resume; the element an `each`
        # leaf is being made for, which `each.index` and `each.value` read
        self._expansion: Optional[Tuple[Any, Dict[str, Any]]] = None
        self._each_scope: Optional[Dict[str, Any]] = None
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
        # The batch this instance joined, the event it waits on there, whether the
        # batch has already published its `started`, and whether another member's
        # run of the resource and service served it
        self._batch: Optional[_OpenBatch] = None
        self._batch_event: Any = None
        self._batch_started = False
        # Interrupted after `started` was published at a batch's close, so the next
        # grant is a resume
        self._owes_resume = False
        self._batch_served = False
        # An instance whose task needs an end event (for `running`, `subscribe`, a
        # `fire from`, or its own `on:` actions) gets one, defused so its failure
        # stops the run only through the task process.
        if spec.name in self.parts.ends.ended:
            make_event = getattr(self.env, "internal_event", self.env.event)
            self.ended = make_event()  # type: ignore[misc]
            self.ended.defused = True
            if not child:
                # Count one pending end against the entity, taken back when the end
                # event is processed, so the entity leaves and a drain ends when the
                # end event is processed, after the `on:` actions have run.
                entity.ends_pending += 1
                self.ended.callbacks.append(
                    functools.partial(self.parts.run.end_processed, entity)
                )
        if spec.name in self.parts.ends.tracked:
            self._list_instance()
        if spec.name in self.parts.ends.whole_flow:
            self.parts.run.flow_runs.setdefault(spec.name, []).append(self)

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
        live = self.parts.scope
        assert live is not None
        return live.scope(
            payload_upstream if payload_upstream is not None else self.upstream,
            self.entity.attributes,
            item=self.flow.item,
            waits=self.waits,
            process={
                "name": self.spec.name,
                "instance": self.task_id,
                "target": self._target,
                "key": self.entity.key,
                "flow_name": self.flow.name,
            },
            each=self._each_scope,
            names=self._names(),
            entity=self.entity.key,
            args=self.arg_values,
            cause=self.cause_values,
            upstream_payload=payload_of(self.upstream),
            item_priority=self.flow.item_priority,
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
        if self.spec.name in self.parts.ends.copied and not self.entity.left:
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
        self.parts.tasks.run_on_action(
            self.context,
            self.parts,
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
            for run in self.parts.run.flow_runs.get(target["task"], []):
                if (
                    run is not self
                    and run.flow.process is not None
                    and run.flow.process not in flows
                    and (where is None or where.evaluate(run.scope()))
                ):
                    flows.append(run.flow.process)
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
                run.task_id: getattr(run.flow.process, "flow_payload", None)
                for run in selected
            }
            return next(iter(payloads.values())) if len(payloads) == 1 else payloads
        if len(selected) == 1:
            return selected[0].ended.value
        return {run.task_id: run.ended.value for run in selected}

    def _named(self, name: Union[str, Reference]) -> str:
        """The name a resource, store, container or signal place gives now: the name
        itself, or the member a reference picks, read in the step of the place."""
        if isinstance(name, Reference):
            return self.parts.run.families.open_reference(name, self.scope())
        return name

    def _subscribe_event(self, wait: Wait) -> Any:
        """The event a `subscribe:` key subscribes the instance to: a child flow for
        a task branch, a signal's firing, or the running instances of a task."""
        node = wait.root
        if node.kind == "task":
            return self.parts.tasks.start(
                self.context,
                self.parts,
                node.target,
                self.entity,
                self.upstream,
                child=True,
                args=self.args,
                kwargs=self.kwargs,
            )
        if node.kind == "signal":
            return self.parts.run.signals.made[self._named(node.target)].wait()
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
        self.flow.process = process
        self.parts.run.running.setdefault(self.spec.name, []).append(self)
        self._listed = True

    def _run_one(self):
        """One instance: everything from the balk check to the next task.

        While it runs, the instance is listed with its task, so `interrupt` can
        reach it.
        """
        self.flow.process = self.env.active_process
        if not self._listed:
            self.reach(self.flow.process)
        if not self.child and not self.entity.entered:
            self.entity.entered = True
            # A spawn without `after` has already counted the entity in; the first
            # instance of any other entity counts it in here.
            self.parts.run.entity_entered(self.entity)
        try:
            result = yield from self._run()
        except RunStopped:
            # A `row` with on_end: stop ended the run; this instance ends quietly
            return None
        except BaseException as error:
            if not self._send_to_on_fail(error):
                # An error ends the instance: the end event fails with it, so a waiter
                # receives it. The run still raises. No `on:` action runs after it,
                # as no statement runs after an uncaught exception in SimPy.
                self._end_default(ok=False, error=error, actions=False)
                raise
            result = None
        finally:
            self._target = None
            self._stop_running()
            self._leave_batch()
        # A balk or a renege returns with the end event not yet triggered: it
        # succeeds with None, so a `running` waiter resumes with no value.
        self._end_default(ok=True, actions=not self._dropped)
        return result

    def _send_to_on_fail(self, error: BaseException) -> bool:
        """Hands an error that came out of the task to its `on_fail` task, as an
        `except Exception:` block handles it: a step that raised, or an expression
        that cannot be evaluated. An interrupt is not a failure, and the failure of
        a task that has handed on already is not this task's.

        Args:
            error: What the task raised.

        Returns:
            bool: True when `on_fail` takes the instance, which then leads to it.
        """
        spec = self.spec
        if (
            spec.on_fail is None
            or not isinstance(error, Exception)
            or isinstance(error, (Interrupt, TaskFailed))
            or self._following is not None
        ):
            return False
        reason = self._fail_with(error)
        self._end_wait()
        self._cancel_awaiting()
        self._release_kept(own=True)
        self._fail(reason)
        return True

    def _fail(self, failure: str) -> None:
        """Ends the task as failed: fires its `signal` as failed, makes the end event
        fail, and goes to `on_fail` when the task has one. Every failure of a task
        goes through here: a `fail` condition, a wait or a child flow that failed, and
        an error a step raised in a task with `on_fail`. A signal the task fired as
        succeeded before the error is not fired again.

        Args:
            failure: The reason.

        Raises:
            TaskFailed: If no `on_fail` handles the failure and the run stops on it.
        """
        spec = self.spec
        if spec.signal is not None and not self._signalled:
            self.parts.run.signals.fire(
                self._named(spec.signal), False, failure, self.entity
            )
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
        running = self.parts.run.running.get(self.spec.name, [])
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
        gate = self.parts.tasks.gates.get(self.spec.name)
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
        if not self._admit():
            return None
        # The patience counts waiting time only: it runs while the task waits for
        # its wait, its resources or its get, and pauses while it is served, so a
        # preempted task that queues again goes on from the time it had used, as
        # SimPy's reneging pattern `req | env.timeout(patience_left)` does.
        # The entity's `vars_once` are made at its first event, before anything waits
        failure: Any = None
        try:
            self._ensure_once()
        except Exception as error:  # a task's failure
            # Nothing is counted done yet, so the whole of _fail_with applies
            failure = self._fail_with(error)
        if spec.queues and failure is None:
            self.publish(self.status("queued"), lifecycle=True)
            stats.task_queued(spec.name, self.task_id)
            if spec.patience is not None:
                self._patience_left = self._patience()

        outcome = yield from self._wait_phase(failure)
        if isinstance(outcome, _Left):
            return outcome.value
        failure = outcome

        payload: Any = None
        silent = False  # `payload: null`: the task publishes no finished record
        rank = self._rank() if self.spec.resources else None
        may_preempt = self._preempt() if self.spec.resources else False
        remaining: Optional[float] = None  # service still owed, when interrupted
        started = got_item = put_done = no_time_left = False
        served = self._batch_served  # a member another member's service served
        values_done = False
        # The requests the task holds, and the position in `spec.resources` each one
        # asked for. Kept across a resume, as a SimPy process keeps its request
        # across `except Interrupt`.
        held: List[Any] = []
        slots: Dict[int, Any] = {}
        while failure is None:
            service_from: Optional[float] = None
            try:
                if self.flow.seized or self._awaiting:
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
                            self._balk_at(won.resource, held)
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
                    if at in slots or no_time_left or self._batch_served:
                        continue
                    if at == 0 and self._first_resource is not None:
                        resource = self._first_resource
                    else:
                        resource = self._named(resource)
                    self._target = f"resource.{resource}"
                    here = self.parts.run.holders.resources[resource]
                    try:
                        request = here.request(
                            self._rank_on(here, None, rank), preempt=may_preempt
                        )
                        self._stamp(request)
                        self._track(request)
                    except QueueFull:
                        self._balk_at(here, held)
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
                    self.flow.item, got_item = got, True
                    self.flow.item_priority = self._taken_priority
                if not started:
                    started = True
                    if spec.timed and not self._batch_started:
                        self.publish(self.status("started"), lifecycle=True)
                        stats.task_started(spec.name, self.task_id)
                    elif self._owes_resume:
                        # `started` came at the batch's close and the task was
                        # interrupted since: this grant is a resume
                        self._owes_resume = False
                        self.publish(self.status("resumed"))
                        stats.task_resumed(spec.name, self.task_id)
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
                    except Exception as error:  # a task's failure
                        failure = self._record_error(error)
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
                        except Exception as error:  # as above
                            failure = self._record_error(error)
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
                    if self._batch is not None and not self._batch_served:
                        # The members wait for the one run that serves them all
                        yield from self._batch_finish()
                    if not values_done and (
                        self._has_values
                        or isinstance(payload, (Template, BoundPayload))
                    ):
                        try:
                            payload = self._finish_values(payload)
                        except Exception as error:  # as above
                            failure = self._record_error(error)
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
                    self._done_counted = True
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
                    try:
                        yield from self._interrupt_flow(interrupt, left)
                    except _HandlerFailed as stopped:
                        failure = stopped.reason
                        break
                    continue
                return self._abandon(interrupt, left)
            except _HandlerFailed as stopped:
                # Raised while the task asked again for a unit it lost
                self._release(held)
                failure = stopped.reason
                break
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
            return self._fail(failure)

        return self._after_success(payload)

    def _admit(self) -> bool:
        """The checks before the task queues: a closed gate drops it, and its `balk`
        or a full line makes it balk. Subscribes it when it has a `subscribe`.

        Returns:
            bool: False when the task left, True when it goes on."""
        spec, stats = self.spec, self.env.stats
        if self._gate_closed():
            self._dropped = True
            self._release_kept(own=self.child)
            self._end_flow("ended")
            return False
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
            return False
        if spec.subscribe is not None:
            # Be told when the event happens: subscribe this instance, then go on.
            self.context.subscribe(self._subscribe_event(spec.subscribe))
        if (
            spec.resources
            and spec.wait is None
            and spec.seize is None
            and spec.open is None
            and spec.cancel is None
            and spec.batch is None
        ):
            # Nothing yields or makes room between here and the first request, so a
            # full line is known now and the task balks before it queues, as in SimPy
            # it would when the request raised. A `seize` waits first, an `open` adds
            # requests to the line, and a `cancel` takes one out.
            # A reference is read once here, and the request below uses that member
            self._first_resource = self._named(spec.resources[0])
            first = self.parts.run.holders.resources[self._first_resource]
            limit = first._line_limit()
            if limit is not None and len(first.queue) >= limit:
                self._balk_at(first)
                return False
        return True

    def _wait_phase(self, failure: Any):
        """The holding steps, the `wait` and the `batch`, before the service.

        Returns:
            Any: The failure, or None; or a `_Left` with what `_run` returns when
            the task ended here."""
        spec, env, stats = self.spec, self.env, self.env.stats
        if failure is None and (
            spec.open is not None
            or spec.listen is not None
            or spec.cancel is not None
            or spec.seize is not None
        ):
            try:
                holding = yield from self._holding_steps()
            except _HandlerFailed as stopped:
                failure = stopped.reason
            except Exception as error:  # a task's failure
                failure = self._fail_with(error)
            else:
                if holding is _LEFT:
                    return _Left(None)
                if holding is not None:
                    self._abandon(holding)
                    return _Left(None)

        while spec.wait is not None and failure is None:
            try:
                if self.flow.seized or self._awaiting:
                    # A unit a preemption took is asked for again before the wait
                    holding = yield from self._acquire()
                    if holding is _LEFT:
                        return _Left(None)
                    if holding is not None:
                        self._abandon(holding)
                        return _Left(None)
                outcome = yield from self._wait(self._deadline())
                self._waited()
            except _HandlerFailed as stopped:
                failure = stopped.reason
                break
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
                        # a granted request is kept by the names of the expansion, so
                        # the events are cancelled before it is made again
                        self._cancel_wait_events(keep_granted=True)
                        self._restart_kept = set(self._wait_events)
                        self._expansion = None  # and re-makes its wait branches
                    try:
                        yield from self._interrupt_flow(interrupt, timeout_left)
                    except _HandlerFailed as stopped:
                        failure = stopped.reason
                        break
                    continue  # resume or restart: wait again
                self._abandon(interrupt, timeout_left)
                return _Left(None)
            if outcome is _RENEGED:
                self._renege()
                return _Left(None)
            failure = outcome
            if failure is not None:
                self._raised = True
                self.publish(self.status("failed", reason=failure))
                stats.task_failed(spec.name, self.task_id)
            break

        while spec.batch is not None and failure is None and not self._batch_served:
            try:
                outcome = yield from self._batch_phase()
            except _HandlerFailed as stopped:
                failure = stopped.reason
                break
            except Exception as error:  # a size, timeout or close_when that failed
                failure = self._fail_with(error)
                break
            if outcome is _RENEGED:
                self._renege()
                return _Left(None)
            if outcome is not None:
                self._abandon(outcome, None)
                return _Left(None)
            break
        return failure

    def _after_success(self, payload: Any) -> Any:
        """The steps after a served task succeeds, in order: its signal, interrupt,
        spawn, `set`, `set_args`, release, resume, activate and deactivate, then the
        hand-on to the next task. Returns the payload."""
        spec, env = self.spec, self.env
        if spec.signal is not None:
            self.parts.run.signals.fire(
                self._named(spec.signal), True, payload, self.entity
            )
            self._signalled = True
        if spec.interrupt is not None:
            cause = spec.interrupt.get("cause")
            cause = self._generated(cause) if spec.cause_generated else cause
            if spec.interrupt.get("handle") is not None:
                self._interrupt_child(
                    spec.interrupt["handle"], cause, spec.interrupt.get("index")
                )
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
            self.parts.tasks.spawn_children(
                self.context,
                self.parts,
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
        if spec.resume is not None:
            self._resume_step()
        if spec.activate:
            self.parts.run.arrivals.set_arrivals(spec.activate, True)
        if spec.deactivate:
            self.parts.run.arrivals.set_arrivals(spec.deactivate, False)
        handed = Upstream(
            self.task_id, self.entity.key, self.entity.attributes, payload
        )
        following = next_task(
            spec.router,
            handed,
            self.parts.run.routes.get(spec.name),
            env,
            self.parts.run.holders.levels(),
            item=self.flow.item,
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

    # ------------------------------------------------------------------- batch

    def _batch_size(self) -> Union[int, float]:
        """Evaluates the batch `size` in this member's scope, once, when it is the first
        member.

        Returns:
            Union[int, float]: The size.

        Raises:
            TypeError: If the value is not a finite number.
        """
        batch = self.spec.batch
        assert batch is not None
        value: Any = batch.size
        if batch.size_of is not None:
            value = batch.size_of.evaluate(self.scope())
        elif isinstance(value, _Gen):
            value = self.context.values.evaluate(value, self.scope())
        elif callable(value):
            value = value(self.entity)
        if (
            isinstance(value, bool)
            or not isinstance(value, numbers.Real)
            or not math.isfinite(value)
        ):
            raise TypeError(
                f"task '{self.spec.name}' batch size gave {value!r}, which is not a "
                f"finite number"
            )
        return value

    def _batch_key(self) -> str:
        """The name of the batch this task joins: its `name`, or the task's name. Tasks
        with the same name share one open batch."""
        assert self.spec.batch is not None
        return self.spec.batch.name or self.spec.name

    def _batch_join(self) -> None:
        """Adds this instance to the open batch of its task, opening one when there is
        none, and closes the batch when it is now full or its `close_when` holds."""
        spec, env, context = self.spec, self.env, self.context
        assert spec.batch is not None
        key = self._batch_key()
        batch = self.parts.run.open_batches.get(key)
        if batch is None:
            size = self._batch_size()
            limit = None
            if spec.batch.timeout is not None:
                limit = self._place(
                    spec.batch.timeout_of or spec.batch.timeout, "batch timeout"
                )
            batch = _OpenBatch(key, size, env.now)
            self.parts.run.open_batches[key] = batch
            if limit is not None:
                timer = env.timeout(limit)
                timer.callbacks.append(
                    functools.partial(self._batch_timer, batch, context)
                )
        batch.members.append(self)
        self._batch = batch
        if len(batch.members) >= batch.size:
            self._batch_close(batch)
            return
        when = spec.batch.close_when_of
        if when is not None:
            scope = replace(
                self.scope(),
                batch={"size": len(batch.members), "age": env.now - batch.since},
            )
            if when.evaluate(scope):
                self._batch_close(batch)

    def _batch_timer(self, batch: _OpenBatch, context: Any, _event: Any) -> None:
        """Closes the batch the timeout belongs to, if it is still open; a batch with
        no member left runs nothing."""
        if batch.closed or batch.dead:
            return
        if not batch.members:
            batch.dead = True
            if self.parts.run.open_batches.get(batch.key) is batch:
                del self.parts.run.open_batches[batch.key]
            return
        batch.members[0]._batch_close(batch)

    def _batch_close(self, batch: _OpenBatch) -> None:
        """Closes the batch: publishes `started` for every member in join order, and
        wakes the first member, which runs the resource and service."""
        batch.closed = True
        batch.leader = batch.members[0]
        if self.parts.run.open_batches.get(batch.key) is batch:
            del self.parts.run.open_batches[batch.key]
        for member in batch.members:
            member._batch_started = True
            member.publish(member.status("started"), lifecycle=True)
            member.env.stats.task_started(member.spec.name, member.task_id)
        # Only the first member runs now; the others wait until it has served them
        batch.leader._batch_notify("closed")

    def _batch_notify(self, value: str) -> None:
        """Wakes this member if it waits for the batch."""
        event, self._batch_event = self._batch_event, None
        if event is not None and not event.triggered:
            event.succeed(value)

    def _batch_phase(self):
        """Joins the batch and waits until this member serves it (the first member of
        the closed batch) or has been served.

        Returns:
            Any: None when the member goes on, `_RENEGED` when its patience ran out
            before the batch closed, or the `Interrupt` that ended its task under
            `abandon`.

        Raises:
            _HandlerFailed: If an `on_interrupt_run` task failed.
        """
        env, spec = self.env, self.spec
        if self._batch is None:
            self._target = "batch"
            self._batch_join()
        event: Any = None
        while True:
            batch = self._batch
            assert batch is not None
            if batch.finished:
                self._batch_served = True
                return None
            if batch.closed and batch.leader is self:
                return None
            if event is None or event.triggered:
                event = self._batch_event = env.event()
            deadline = None if batch.closed else self._deadline()
            try:
                if deadline is None:
                    yield event
                else:
                    yield event | deadline
                    self._waited()
                    if not _won(event):
                        if batch.closed:
                            # The patience counts until the batch closes: a member
                            # served later waits on without it
                            self._patience_left = None
                            continue
                        self._batch_event = None
                        self._leave_batch()
                        return _RENEGED
            except Interrupt as interrupt:
                self._waited()
                self._batch_event = event = None
                closed = batch.closed
                if not closed:
                    # An interrupted member leaves the batch, and `size` counts the
                    # members left
                    self._leave_batch()
                if not self._interrupted(interrupt):
                    return interrupt
                yield from self._interrupt_flow(interrupt, None)
                if not closed:
                    # `resume` and `restart` join again, at the back of the members.
                    # The member has not started, so it publishes no `resumed`, as a
                    # task interrupted in a queue publishes none
                    self._batch_join()
                else:
                    # A member of a closed batch has started and is served by the
                    # leader's service, so there is nothing to restart: it resumes
                    # and goes on waiting to be served
                    self._owes_resume = False
                    self.publish(self.status("resumed"))
                    self.env.stats.task_resumed(spec.name, self.task_id)

    def _batch_finish(self):
        """Wakes every member of the batch in join order once its resource and service
        are done, this one included."""
        batch = self._batch
        assert batch is not None
        if batch.finished:
            return
        batch.finished = True
        mine = self._batch_event = self.env.event()
        for member in list(batch.members):
            if member is self:
                mine.succeed("done")
            else:
                member._batch_notify("done")
        yield mine
        self._batch_event = None

    def _leave_batch(self) -> None:
        """Takes this instance out of its batch. When it was the member serving a
        closed batch, the next member serves it."""
        batch, self._batch = self._batch, None
        self._batch_event = None
        if batch is None:
            return
        if self in batch.members:
            batch.members.remove(self)
        if batch.finished or batch.dead:
            return
        if batch.closed:
            if batch.leader is self and batch.members:
                batch.leader = batch.members[0]
                batch.leader._batch_notify("lead")
        elif not batch.members:
            batch.dead = True
            if self.parts.run.open_batches.get(batch.key) is batch:
                del self.parts.run.open_batches[batch.key]

    # ------------------------------------------------- holding: seize, open, release

    def _resume_step(self) -> None:
        """Step 21: wakes the instances suspended in `wait: {suspend: true}`. The value
        and the limit are read once, as the arguments of one call are; the instances
        that wake run in a later step of this instant, as `event.succeed(value)`
        schedules them."""
        spec = self.spec.resume
        assert spec is not None
        scope = self.scope()
        value = spec.get("value")
        if has_generator(value):
            value = self.context.values.evaluate(value, scope)
        limit = spec.get("limit")
        if isinstance(limit, _Gen):
            limit = self.context.values.evaluate(limit, scope)
        self.parts.run.resume(spec["task"], spec.get("where"), value, limit, scope)

    def _handle_of(self, name: str) -> Optional[Handle]:
        """The handle under `name` in this flow's table or its parents', or None."""
        table = (
            self.flow.handles
            if self.flow.handles is not None
            else self.flow.parent_handles
        )
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
        if self.flow.handles is None:
            self.flow.handles = HandleTable(self.flow.parent_handles)
        return self.flow.handles

    def _visible_handles(self) -> Optional[HandleTable]:
        """The table a child flow started now reads: this flow's, or its parent's."""
        return (
            self.flow.handles
            if self.flow.handles is not None
            else self.flow.parent_handles
        )

    def _record_error(self, error: BaseException) -> str:
        """Records the error a step raised as the task's own failure, so the task's
        `on:` actions do not run, and returns its reason."""
        reason = f"{type(error).__name__}: {error}"
        self._fail_error = error
        self._raised = True
        return reason

    def _fail_with(self, error: BaseException) -> str:
        """Publishes the failure of a step that raised, as the task's own failure, and
        returns its reason."""
        reason = self._record_error(error)
        self.publish(self.status("failed", reason=reason))
        if not self._done_counted:
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
        self.parts.run.note_held(self.spec.name, request)

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
        request.flow = self.flow.seized
        request.run = run or self
        self.flow.seized.append(request)
        self._note_held(request)
        return request

    def _forget(self, request: Any) -> None:
        """Takes a request out of the lists of its flow and of its task."""
        if request in request.flow:
            request.flow.remove(request)
        listed = self.parts.run.held_by_task.get(request.run.spec.name)
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
        for old in [r for r in self.flow.seized if r.preempted]:
            try:
                fresh = self._reask(old)
            except QueueFull:
                self._forget(old)
                return old.resource
            if fresh.handle is None:
                self._awaiting.insert(0, fresh)
        return None

    def _forget_preempted(self) -> None:
        """Ends the `seize` requests a preemption took, so a later `release` does
        nothing and a task that follows asks for no unit it lost."""
        for request in [r for r in self.flow.seized if r.preempted]:
            self._forget(request)

    def _cancel_awaiting(self) -> None:
        """Takes the `seize` requests this instance still waits for out of their
        lines. A unit granted already stays held, as the Unit states table says."""
        for request in self._awaiting:
            if not request.triggered:
                request.cancel()
                self._forget(request)
        self._awaiting = []

    def _holding_steps(self):
        """Steps 5 to 8 of the task's keys: `open`, `listen`, `cancel` and `seize`.

        Returns:
            None when they are done, `_LEFT` when the task balked or reneged, or the
            `Interrupt` that ends the task under `abandon`.
        """
        spec = self.spec
        if spec.open is not None:
            full = self._open()
            if full is not None:
                self._balk_at(full)
                return _LEFT
        if spec.listen is not None:
            self._listen_step()
        if spec.cancel is not None:
            self._cancel_step()
        if spec.seize is not None:
            seize = spec.seize
            resource = self.parts.run.holders.resources[self._named(seize.resource)]
            try:
                request = self._ask(
                    resource, self._rank_on(resource, seize), self._preempt(seize)
                )
            except QueueFull:
                self._balk_at(resource)
                return _LEFT
            self._awaiting.append(request)
            return (yield from self._acquire())
        return None

    def _listen_step(self) -> None:
        """Step 6: adds the callback of `listen` to the event it names, behind the
        callbacks and waiters that are already on it, as `ev.callbacks.append(f)` does.

        Raises:
            RuntimeError: If the event was processed already, which takes no callback.
            KeyError: If no event is open under the name of a `handle`.
            ValueError: If an `index` is used on a name that is no `each` group.
            IndexError: If the `index` is outside the elements.
        """
        listen = self.spec.listen
        assert listen is not None
        if listen.signal is not None:
            name = self._named(listen.signal)
            event = self.parts.run.signals.made[name].wait()
            what = f"signal '{name}'"
        else:
            handle = self._handle(str(listen.handle), "listen")
            if listen.index is not None:
                handle = self._element(
                    handle, str(listen.handle), listen.index, "listen"
                )
            event = handle.event
            what = f"the event '{listen.handle}'"
        if event.callbacks is None:
            raise RuntimeError(
                f"task '{self.spec.name}' listen: {what} was processed already, so "
                f"it takes no callback"
            )
        event.callbacks.append(self._listened)

    def _listened(self, _event: Event) -> None:
        """The callback of `listen`: runs its `on` entries for the entity that
        listens, in that entity's scope as it is now."""
        assert self.spec.listen is not None
        for action in self.spec.listen.on:
            self.parts.tasks.run_on_action(
                self.context,
                self.parts,
                action,
                f"the listen of task '{self.spec.name}'",
                self.entity,
                self.upstream,
                names=self._names(),
                scope=self.scope(),
            )

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
                self._balk_at(full)
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
                        self._balk_at(request.resource)
                        return _LEFT
                yield from self._interrupt_flow(interrupt, None)
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
            if node.is_each:
                handle, full = self._open_each(node)
            else:
                handle, full = self._open_one(node)
            if full is not None:
                return full
            table[name] = handle
        return None

    def _open_one(self, node: Any) -> Tuple[Optional[Handle], Any]:
        """Makes the event of one opened leaf or group of leaves.

        Returns:
            The handle and None, or None and the resource whose full line made the
            task balk.
        """
        if node.kind == "request":
            resource = self.parts.run.holders.resources[self._named(node.target)]
            handle = Handle("request", None)
            try:
                request = self._ask(
                    resource,
                    self._rank_on(resource, node.options),
                    self._preempt(node.options),
                    handle,
                )
            except QueueFull:
                return None, resource
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
        return handle, None

    def _open_each(self, node: Any) -> Tuple[Optional[Handle], Any]:
        """Makes the events of an opened `each`, one for each element in order, and the
        group's event under its `group` rule.

        Returns:
            The group's handle, which holds the handle of each element, and None; or
            None and the resource whose full line made the task balk.
        """
        group = self._each_group(node, {})
        items: List[Handle] = []
        built: Dict[int, Any] = {}
        for leaf in group.children:
            previous, self._each_scope = self._each_scope, leaf.element
            try:
                handle, full = self._open_one(leaf)
            finally:
                self._each_scope = previous
            if full is not None:
                return None, full
            assert handle is not None
            items.append(handle)
            built[id(leaf)] = handle.event
        handle = Handle(
            "each", self._group_event(group, [h.event for h in items], built)
        )
        handle.items = items
        return handle, None

    def _each_values(self, node: Any) -> List[Any]:
        """The elements of an `each`, read once as the wait or the `open` starts: a
        number n gives 0 to n - 1, and a list or an expression's list gives its
        values."""
        scope = self.scope()
        place = node.each
        if isinstance(place, Expression):
            place = place.evaluate(scope)
        elif isinstance(place, _Gen) or callable(place):
            self._ensure_once()
            place = self._generated(place)
        if isinstance(place, (list, tuple, range)):
            return list(place)
        count = whole(place, f"each of '{node.name}'")
        return list(range(max(count, 0)))

    def _each_group(self, node: Any, nodes: Dict[str, Any]) -> Any:
        """The group of leaves an `each` node makes now: one leaf for each element,
        named `<name>[<index>]`, that reads its `each.index` and `each.value`.

        Args:
            node: The `each` node.
            nodes: Receives each leaf by name.

        Returns:
            WaitNode: A group of the `each`'s `group` kind, named as the node.
        """
        template = node.template
        values = self._each_values(node)
        leaves = []
        for index, value in enumerate(values):
            leaf = WaitNode(
                f"{node.name}[{index}]",
                template.kind,
                target=template.target,
                args=template.args,
            )
            leaf.options = template.options
            leaf.index = template.index
            leaf.element = {"index": index, "value": value}
            leaves.append(leaf)
            nodes[str(leaf.name)] = leaf
        kind, argument = node.group
        count = 0
        predicate = None
        if kind == "at_least":
            wanted = whole(
                argument.evaluate(self.scope())
                if isinstance(argument, Expression)
                else argument,
                f"at_least of each '{node.name}'",
            )
            if not leaves:
                wanted = 0  # an empty each succeeds at once under every group
            elif wanted < 1:
                raise ValueError(f"at_least of each '{node.name}' gave {wanted}")
            if wanted > len(leaves):
                raise ValueError(
                    f"at_least of each '{node.name}' gave {wanted}, which is above "
                    f"the {len(leaves)} events it waits for"
                )
            count = wanted
        elif kind == "when":
            predicate = argument
        return WaitNode(
            node.name, kind, children=leaves, count=count, predicate=predicate
        )

    def _group_event(self, node: Any, members: List[Any], built: Dict[int, Any]) -> Any:
        """The SimPy condition of a group node over the events of its children."""
        env = self.env
        if node.kind == "any":
            return env.any_of(members)
        if node.kind == "all":
            return env.all_of(members)
        if node.kind == "when":
            return Condition(
                env, lambda evs, c, n=node: self._holds(n, c, built), members
            )
        return Condition(env, lambda evs, c, n=node.count: c >= n, members)

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
            event = self.parts.run.signals.made[self._named(node.target)].wait()
        elif node.kind == "handle":
            event = self._handle_event(node, "open")
        else:
            event = self.parts.tasks.start(
                self.context,
                self.parts,
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
        if self.spec.cancel_index is not None:
            handle = self._element(handle, str(name), self.spec.cancel_index, "cancel")
        elif handle.items is not None:
            for element in list(handle.items):
                self._cancel_handle(str(name), element)
            return
        self._cancel_handle(str(name), handle)

    def _cancel_handle(self, name: str, handle: Handle) -> None:
        """Removes one opened request, get or put from its line; the name is for the
        messages."""
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
            found = [handle]
            if release.index is not None:
                found = [
                    self._element(handle, release.handle, release.index, "release")
                ]
            elif handle.items is not None:
                found = list(handle.items)
            for one in found:
                if one.kind != "request":
                    raise ValueError(
                        f"task '{self.spec.name}' release: '{release.handle}' was "
                        f"opened as a {one.kind}, which holds no unit"
                    )
                self._give_back(one.event)
            return
        assert release.resource is not None
        name = self._named(release.resource)
        if release.task is None:
            # The oldest unit of that resource this flow holds
            held = [
                r
                for r in self.flow.seized
                if r.resource.obj_id == name and r in r.resource.users
            ]
            if held:
                users = held[0].resource.users
                self._give_back(min(held, key=users.index))
            return
        listed = self.parts.run.held_by_task.get(release.task, [])
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
        self._leaf_elements = {}
        self._expansion = None

    def _cancel_wait_events(self, keep_granted: bool = False) -> None:
        """Cancels every live request, get or put wait-branch event, giving back a
        unit a request already won. A store or container get or put has no `users`,
        so it is only cancelled, as SimPy leaves a won get.

        Args:
            keep_granted: Leaves a request that is granted, whether the task has taken
                the grant or not, with its branch, as a restart does: the unit is
                held, and the wait that is made again reuses the request.
        """
        branches = self.spec.wait.branches if self.spec.wait is not None else {}
        if self._expansion is not None:
            branches = self._expansion[1]
        kept: Dict[str, Any] = {}
        for name, event in self._wait_events.items():
            node = branches.get(name)
            granted = (
                node is not None
                and node.kind == "request"
                and event.triggered
                and event in event.resource.users
            )
            if granted and keep_granted:
                kept[name] = event
                continue
            event.cancel()
            if granted:
                event.resource.release(event)
        self._wait_events = kept

    def _drop_wait_event(self, event: Any) -> None:
        """Cancels a wait-branch event, giving back the unit a request already won."""
        granted = (
            hasattr(event, "resource")
            and event.triggered
            and event in event.resource.users
        )
        event.cancel()
        if granted:
            event.resource.release(event)

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
                preempted_by=self.parts.run.entity_key_of(cause.by),
                usage_since=cause.usage_since,
            )
        else:
            value = self.status("interrupted", cause=cause)
        self.publish(value)
        stats.task_interrupted(spec.name, self.task_id, preempted=preempted)
        self._owes_resume = self._batch_started
        return spec.on_interrupt != "abandon"

    def _abandon(self, interrupt: Interrupt, left: Optional[float] = None) -> None:
        """Ends the task after an interrupt with `on_interrupt: abandon`. `left` is
        the service time that was left, for `cause.left`."""
        spec, stats = self.spec, self.env.stats
        self._end_wait()
        # A `seize` unit a preemption took is ended: a later `release` does nothing
        self._cancel_awaiting()
        self._forget_preempted()
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

    def _interrupt_flow(self, interrupt: Interrupt, left: Optional[float]):
        """Runs the task `on_interrupt_run` names for this entity, inside this flow's
        process, as the body of an `except Interrupt:` block runs; without that key it
        does nothing. The task then goes on with the time left, or a new time under
        `restart`. The child reads `cause.<name>`. It runs in the same process, so an
        interrupt that arrives while it runs reaches the child at its own wait, and
        one that arrives while it takes no time is delivered at the task's next wait.

        Args:
            interrupt: The interrupt the task caught.
            left: The service time that was left, for `cause.left`, or None.

        Raises:
            _HandlerFailed: If the child fails. The task has published `failed`.
        """
        name = self.spec.on_interrupt_run
        if name is None:
            return
        run = self.parts.tasks.new_run(
            self.context,
            self.parts,
            name,
            self.entity,
            self.upstream,
            child=True,
            args=self.args,
            kwargs=self.kwargs,
            handles=self._visible_handles(),
        )
        run.cause_values = self._cause_values(interrupt, left)
        run.flow.name = name
        self._target = f"task.{name}"
        try:
            while True:
                yield from run._run_one()
                if run._following is None:
                    break
                run, run._following = run._following, None  # type: ignore[assignment]
        except Exception as error:  # the child's failure
            reason = _reason_of(error)
            self._fail_error = error
            self._raised = True
            self.publish(self.status("failed", reason=reason))
            self.env.stats.task_failed(self.spec.name, self.task_id)
            # The task leaves with the failure, so a `seize` request still in a line
            # leaves it, as it does when the task fails any other way
            self._cancel_awaiting()
            self._forget_preempted()
            raise _HandlerFailed(reason) from None

    def _cause_values(
        self, interrupt: Interrupt, left: Optional[float]
    ) -> Dict[str, Any]:
        """What `cause.<name>` reads in the task an `on_interrupt_next` edge reaches:
        for a preemption `by`, `usage_since` and `resource`, for an interrupt its
        `value`, and `left` where the task was serving."""
        cause = interrupt.cause
        values: Dict[str, Any] = {}
        if isinstance(cause, Preempted):
            values["by"] = self.parts.run.entity_key_of(cause.by)
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
            extra["item"] = self.flow.item
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

    def _balk_at(self, resource: Any, held: Optional[List[Any]] = None) -> None:
        """Balks at a resource whose waiting line is full: gives back the requests
        the flow holds, then the units it kept for its child task, then publishes the
        balk."""
        if held is not None:
            self._release(held)
        self._release_kept(own=self.child)
        self._balk_full(resource)

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
            self.parts.run.entity_left(self.entity)

    def _failure(self, payload: Any) -> Any:
        """The reason the task fails after its service, or None when it does not: its
        `fail_reason` (text, or a generator read now), or the text of its `fail`. A
        `fail` expression or a `fail_reason` that cannot be evaluated fails the task
        with the error as its reason."""
        spec = self.spec
        if spec.fail is None:
            return None
        handed = Upstream(
            self.task_id, self.entity.key, self.entity.attributes, payload
        )
        if spec.fail_of is not None:
            try:
                failed = spec.fail_of.evaluate(self.scope(handed))
            except ValueError as error:  # as a reason that cannot be made, below
                self._fail_error = error
                self._raised = True
                return f"{type(error).__name__}: {error}"
            text = spec.fail_of.text
        else:
            failed = spec.fail(self.task_id, payload)
            text = getattr(spec.fail, "__name__", "fail")
        if not failed:
            return None
        if spec.fail_reason is None:
            return text
        if not has_generator(spec.fail_reason):
            return spec.fail_reason
        try:
            reason = self.context.values.evaluate(spec.fail_reason, self.scope(handed))
        except Exception as error:  # a reason that cannot be made fails the task
            self._fail_error = error
            self._raised = True
            return f"{type(error).__name__}: {error}"
        return text if reason is None else reason

    def _follow(
        self, task: str, handed: Upstream, cause: Optional[Dict[str, Any]] = None
    ) -> None:
        """Makes the next instance, which `run` runs in the same process. `cause` is
        what an interrupt that led here gives `cause.<name>`."""
        tasks = self.parts.tasks
        if task not in tasks.specs:
            raise ValueError(
                f"task '{self.spec.name}' routes to '{task}', which is not a task"
                + suggestion(task, tasks.specs)
            )
        following = tasks.new_run(
            self.context,
            self.parts,
            task,
            self.entity,
            handed,
            child=self.child,
            args=self.args,
            kwargs=self.kwargs,
            arg_values=self.arg_values,
        )
        # The flow is one process: what it holds goes on with it, as Python locals do
        following.flow = replace(self.flow)
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
        scope = self.scope()
        if transfer.store is not None:
            held_name = self._named(transfer.store)
            store = self.parts.run.holders.stores[held_name]
            ranked = self._queue_rank(store, direction, scope)
            if direction == "get":
                event = store.get(transfer.filter_for(scope, self.entity), **ranked)
            else:
                if transfer.item is not None:
                    # The item of a `put`, read as the put is made
                    payload = self._generated(transfer.item)
                if self.parts.holders.store_priority.get(
                    self.parts.families.origin(held_name)
                ):
                    rank = transfer.priority_for(self.task_id, self.upstream, scope)
                    event = store.put(store.wrap(payload, rank), **ranked)
                else:
                    event = store.put(payload, **ranked)
        else:
            held_name = self._named(transfer.target)
            container = self.parts.run.holders.containers[held_name]
            amount = transfer.amount_for(self.task_id, self.upstream, scope)
            ranked = self._queue_rank(container, direction, scope)
            event = (
                container.get(amount, **ranked)
                if direction == "get"
                else container.put(amount, **ranked)
            )
        # Who is waiting, for `putting()` and `getting()`, and which holder it is for
        event.entity_key = self.entity.key
        event.holder_name = held_name
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
            name = getattr(event, "holder_name", transfer.store)
            if self.parts.holders.store_priority.get(self.parts.families.origin(name)):
                self._taken_priority = event.value.priority
                return event.value.item
            return event.value
        return None

    def _wait(self, deadline: Any):
        """Waits for the task's `wait`, then records each branch for `wait.<branch>`.

        Returns None when the wait succeeded, `_RENEGED` when the patience ran out
        first, or the failure reason when a branch failed.
        """
        wait = self.spec.wait
        assert wait is not None
        try:
            root, branches = self._expanded(wait)
        except QueueFull as error:
            return str(error)
        self._target = self._target_label(root)
        events: Dict[str, Any] = {}
        built: Dict[int, Any] = {}
        stop_watching: List[Callable[[], None]] = []
        self._single_timeout = None

        try:
            root_event = self._build_wait_event(
                root, root, built, events, stop_watching
            )
            for name in [n for n in self._wait_events if n not in events]:
                # A request kept across a restart whose leaf `each` no longer makes
                self._drop_wait_event(self._wait_events.pop(name))
            self._restart_kept = set()
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
        except Exception as error:  # a branch failed
            failure = _reason_of(error)
        finally:
            # Losing condition branches stop checking; losing child flows keep
            # running, as SimPy leaves processes running.
            for stop in stop_watching:
                stop()
        # The wait is over, by a branch, the patience or a failure: a branch that
        # suspended and was not woken leaves the list of the suspended
        for event in self._wait_events.values():
            if isinstance(event, _Suspension):
                event.cancel()
        # When the patience ends first and the wait's event or condition is not yet
        # processed, the wait has lost the tie. It still succeeds when its leaves
        # processed by then satisfy it.
        tie = failure is None and not joined.processed
        if tie and not self._done_at_tie(root, built):
            return _RENEGED
        self._record_branches(branches, events, joined, tie, built)
        # The wait resolved: a won request is now tracked in `_wait_held`, and losing
        # branches are cancelled, so the per-branch events need no more tracking.
        self._wait_events = {}
        self._leaf_elements = {}
        self._expansion = None
        return failure

    def _build_wait_event(
        self,
        node: Any,
        root: Any,
        built: Dict[int, Any],
        events: Dict[str, Any],
        stop_watching: List[Callable[[], None]],
    ) -> Any:
        """Builds the SimPy event for a node, nesting groups as SimPy conditions,
        with the element an `each` leaf is made for in `each.index`."""
        previous, self._each_scope = (
            self._each_scope,
            (node.element if node.element is not None else self._each_scope),
        )
        try:
            return self._build_wait_node(node, root, built, events, stop_watching)
        finally:
            self._each_scope = previous

    def _build_wait_node(
        self,
        node: Any,
        root: Any,
        built: Dict[int, Any],
        events: Dict[str, Any],
        stop_watching: List[Callable[[], None]],
    ) -> Any:
        """The SimPy event of one node of the wait, recorded in `built` and, for a
        named node, in `events`."""
        if node.is_group:
            members = [
                self._build_wait_event(child, root, built, events, stop_watching)
                for child in node.children
            ]
            event: Any = self._group_event(node, members, built)
        elif node.kind == "signal":
            event = self.parts.run.signals.made[self._named(node.target)].wait()
        elif node.kind == "task":
            # A wait resumed after an interrupt waits again for the child flow it
            # started, as a SimPy process yields the same process again
            child = self._children.get(node.name)
            if child is None:
                child = self.parts.tasks.start(
                    self.context,
                    self.parts,
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
                self._children[node.name] = child
            event = child
        elif node.kind == "timeout":
            event = self._timeout_event(node)
            if node is root:
                self._single_timeout = (self.env.now, event._delay)
        elif node.kind == "handle":
            # The event `open` made, whatever has happened to it since: the same
            # event again after an interrupt, with its first end
            event = self._handle_event(node, "wait")
        elif node.kind == "request":
            # Kept across a resume, as a `task` branch's child flow is, so the
            # task does not queue a second request each time it waits again.
            event = self._wait_events.get(node.name)
            if event is not None and node.name in self._restart_kept:
                # A restart reads the leaf again: a unit kept for a leaf that now
                # asks for another resource is given back. A resume reads nothing
                # again, so the request keeps its place.
                if (
                    event.resource
                    is not self.parts.run.holders.resources[self._named(node.target)]
                ):
                    self._drop_wait_event(event)
                    event = None
            if event is None:
                resource = self.parts.run.holders.resources[self._named(node.target)]
                rank = self._rank_on(resource, node.options)
                may_preempt = self._preempt(node.options)
                event = resource.request(rank, preempt=may_preempt)
                event.asked = (rank, may_preempt)
                self._stamp(event)
                self._track(event)
                self._wait_events[node.name] = event
        elif node.kind == "suspend":
            # Kept across a resume, so the instance stays in its place among the
            # suspended; a restart makes it again, behind the others
            event = self._wait_events.get(node.name)
            if event is None:
                event = _Suspension(self)
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

    def _done_at_tie(self, node: Any, built: Dict[int, Any]) -> bool:
        """Whether a node is done when the patience ends at the instant the wait
        would: a leaf processed by the time the task resumes is in the value of
        SimPy's condition, which is built when the condition is processed. One
        granted or triggered but not yet processed has lost."""
        event = built[id(node)]
        if not node.is_group:
            return _won(event) and event.ok
        won = sum(self._done_at_tie(child, built) for child in node.children)
        if node.kind == "any":
            return won > 0 or not node.children
        if node.kind == "all":
            return won == len(node.children)
        if node.kind == "when":
            return self._holds(
                node, won, built, tie=lambda child: self._done_at_tie(child, built)
            )
        return won >= node.count

    def _record_branches(
        self,
        branches: Dict[str, Any],
        events: Dict[str, Any],
        joined: Any,
        tie: bool,
        built: Dict[int, Any],
    ) -> None:
        """Records each named branch's done flag, ok flag and value in `waits`."""
        # Named nodes are recorded children before parents (the parse registers a
        # group after its children), so a group reads its children's recorded values.
        for name, node in branches.items():
            event = events[name]
            if tie:
                done = self._done_at_tie(node, built)
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
            if node.template is not None:
                # An `each` group: its elements' values and flags in order
                leaves = [self.waits[str(child.name)] for child in node.children]
                self.waits[name]["values"] = [
                    found["value"] if found["done"] else None for found in leaves
                ]
                self.waits[name]["flags"] = [bool(found["done"]) for found in leaves]

    def _expanded(self, wait: Wait) -> Tuple[Any, Dict[str, Any]]:
        """The wait's tree with each `each` node made into a group of one leaf for each
        element, and every named node of it, children before their parents. It is made
        when the wait starts and kept for a resume, so the elements are read once.

        Args:
            wait: The task's wait.

        Returns:
            Tuple[WaitNode, Dict[str, WaitNode]]: The root and the named nodes.
        """
        if self._expansion is not None:
            return self._expansion
        named: Dict[str, Any] = {}

        def walk(node: Any) -> Any:
            if node.is_each:
                found = self._each_group(node, named)
                found.template = node.template
            elif node.is_group:
                found = WaitNode(
                    node.name,
                    node.kind,
                    children=[walk(child) for child in node.children],
                    count=node.count,
                    predicate=node.predicate,
                )
            else:
                found = node
            if found.name is not None:
                named[found.name] = found
            return found

        expanded = (walk(wait.root), named)
        self._expansion = expanded
        return expanded

    def _handle_event(self, node: Any, key: str) -> Any:
        """The event of the handle a `handle` leaf names: the group's, or the one of
        the element its `index` gives, read now."""
        handle = self._handle(node.target, key)
        if node.index is None:
            return handle.event
        if node.name is None or key != "wait":
            return self._element(handle, node.target, node.index, key).event
        # The element is resolved on the first build and kept by the leaf's name, so a
        # resume or a restart waits for the same event (Y4)
        if node.name not in self._leaf_elements:
            self._leaf_elements[node.name] = self._element(
                handle, node.target, node.index, key
            )
        return self._leaf_elements[node.name].event

    def _element(
        self, handle: Handle, name: str, index: Expression, key: str
    ) -> Handle:
        """The handle of one element of an `each` group, by an index read now.

        Raises:
            ValueError: If the name is not an `each` group.
            IndexError: If the index is outside its elements.
        """
        if handle.items is None:
            raise ValueError(
                f"task '{self.spec.name}' {key}: '{name}' was opened as a "
                f"{handle.kind}, and only an each group has elements"
            )
        found = whole(index.evaluate(self.scope()), f"the index of '{name}' in {key}")
        if not -len(handle.items) <= found < len(handle.items):
            raise IndexError(
                f"task '{self.spec.name}' {key}: index {found} is outside the "
                f"{len(handle.items)} elements of '{name}'"
            )
        return handle.items[found]

    def _interrupt_child(
        self, name: str, cause: Any, index: Optional[Expression] = None
    ) -> None:
        """Interrupts the child flow a name was opened as, as `process.interrupt(cause)`
        does; a flow that has ended is a run failure, as it is in SimPy. For an `each`
        group of child flows, `index` picks one, and none interrupts each in order."""
        handle = self._handle(name, "interrupt")
        if index is not None:
            handle = self._element(handle, name, index, "interrupt")
        elif handle.items is not None:
            for element in list(handle.items):
                self._interrupt_handle(name, element, cause)
            return
        self._interrupt_handle(name, handle, cause)

    def _interrupt_handle(self, name: str, handle: Handle, cause: Any) -> None:
        """Interrupts one child flow."""
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


def _cause(interrupt: Interrupt) -> Any:
    """An interrupt's cause as a record holds it: a preemption by its resource."""
    cause = interrupt.cause
    if isinstance(cause, Preempted):
        return f"preempted on {cause.resource.obj_id}"
    return cause


def _reason_of(error: BaseException) -> Any:
    """The reason a failure carries: its `reason` as it was given, which may be empty
    or 0, or the error's text when it has none."""
    reason = getattr(error, "reason", None)
    return str(error) if reason is None else reason


def _branch_value(event: Any) -> Any:
    """A finished branch's value; a failed one's is its reason."""
    if event.ok:
        return event.value
    return getattr(event.value, "reason", str(event.value))
