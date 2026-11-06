"""The checks of a model's tasks, families and rules, made before a run.

Each function reads the declared tasks and returns the problems it finds, or raises,
before the clock starts; `core/task.py` holds the life of one task instance.
"""

from __future__ import annotations
import ast
import itertools
from collections.abc import Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
)
from pydantic import BaseModel
from desflow.core.expressions import ANY_NAME, Expression
from desflow.core.flow import (
    Wait,
    article,
)
from desflow.utils import suggestion
from desflow.core.families import SECTIONS, Reference
from desflow.core.task import TaskSpec
from desflow.values.check import generators_in, task_value_places
from desflow.values.models import Expr, When, _Gen

if TYPE_CHECKING:
    from desflow.core.holders import Holders
    from desflow.core.signals import Signals


# The keys of a shared batch that are compared by `_same_form`
_COMPARED_BY_FORM = ("size", "timeout", "close_when", "service_time")


def _same_form(value: Any) -> Any:
    """A form of a batch or service value that two equal values share, whatever their
    spelling or where they were written: a pydantic value by its fields (not its private
    place in the file), an expression by its parsed tree, text that is a number by its
    number, and a mapping or list by its parts.

    Args:
        value: A `size`, `timeout`, `close_when` or `service_time`.

    Returns:
        Any: A value to compare with `==`.
    """
    if isinstance(value, BaseModel):
        return (type(value).__name__, _same_form(value.model_dump()))
    if isinstance(value, Mapping):
        return {key: _same_form(part) for key, part in value.items()}
    if isinstance(value, (list, tuple)):
        return [_same_form(part) for part in value]
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass
        try:
            return ast.dump(ast.parse(value.strip(), mode="eval"))
        except SyntaxError:
            return value
    return value


class BatchMismatch(NamedTuple):
    """Two tasks that share a batch name and differ in one key.

    Attributes:
        batch: The batch name.
        first: The task that opened the name.
        other: The task that differs, whose `batch` line a blueprint names.
        key: The key that differs.
    """

    batch: str
    first: str
    other: str
    key: str


def batch_mismatch(
    specs: Mapping[str, "TaskSpec"],
) -> Optional[BatchMismatch]:
    """Finds two tasks that share a batch name and differ in what the batch needs one
    answer for: its `size`, `timeout` and `close_when`, its `resource` and its
    service (`service` or `service_time`). `on_interrupt`, `patience`, `payload` and
    `next` may differ, because each member keeps its own.

    Args:
        specs: The tasks by name.

    Returns:
        Optional[BatchMismatch]: The batch name, the first task, the other task and
        the key that differs, or None when every shared batch agrees.
    """
    first: Dict[str, "TaskSpec"] = {}
    for name, spec in specs.items():
        if spec.batch is None:
            continue
        key = spec.batch.name or name
        opener = first.setdefault(key, spec)
        if opener is spec:
            continue
        for label, value, other in (
            ("size", opener.batch.size, spec.batch.size),  # type: ignore[union-attr]
            ("timeout", opener.batch.timeout, spec.batch.timeout),  # type: ignore[union-attr]
            (
                "close_when",
                opener.batch.close_when,  # type: ignore[union-attr]
                spec.batch.close_when,
            ),
            ("resource", opener.resources, spec.resources),
            ("service", opener.service_id, spec.service_id),
            ("service_time", opener.service_time, spec.service_time),
        ):
            if label in _COMPARED_BY_FORM:
                value, other = _same_form(value), _same_form(other)
            if value != other:
                return BatchMismatch(key, opener.name, name, label)
    return None


def batch_mismatch_text(key: str, first: str, other: str, label: str) -> str:
    """The refusal for two tasks that share a batch and differ in one key.

    Args:
        key: The batch name.
        first: The task that opened the name.
        other: The task that differs.
        label: The key that differs.

    Returns:
        str: The message, which names both tasks.
    """
    return (
        f"tasks '{first}' and '{other}' share the batch '{key}' but differ in "
        f"{label}; the members of one batch are served by one run of the resource and "
        f"service, so give both tasks the same size, timeout, close_when, resource "
        f"and service, or different batch names"
    )


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
        interrupt_targets: The tasks an `on_interrupt_next` or `on_interrupt_run` edge
            reaches.

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
    has_each: bool = True,
    each_here: bool = True,
) -> Optional[str]:
    """What an expression reads that no earlier step of its task gives, or None.

    Args:
        expression: The expression.
        own_item: Whether the place is evaluated after the task's own waiting store
            get, or is that get's filter.
        later_item: Whether the task is reached after a waiting store get by a route.
        cause_ok: Whether an `on_interrupt_next` or `on_interrupt_run` edge reaches the task.
        in_when: Whether the place is the `when` of a wait group.
        has_each: Whether the task has a `wait` or an `open` with `each`, which
            `each.index` and `each.value` need.
        each_here: Whether the place is in the branch of an `each` (a leaf, its
            `args`), the only place that reads `each.index` and `each.value`.

    Returns:
        Optional[str]: The reason the place cannot read the name, or None when it can.
    """
    if expression.references["each"] and not has_each:
        return (
            "each.index and each.value are read in the branch of a wait or an open "
            "with each"
        )
    if expression.references["each"] and not each_here:
        return "each.index is read only in the branch of an each"
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
        return (
            "cause.<name> is read in a task that an on_interrupt_next edge or an "
            "on_interrupt_run edge reaches"
        )
    return None


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

    A computed member (`{of, key}` or `{of, lowest}`) is left out, because the member
    is not known before the run.

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
    notes = _OrderNotes(waits or {}, holds or {})
    resources = {
        task: [r for r in listed if not isinstance(r, Reference)]
        for task, listed in resources.items()
    }
    _note_requests_together(notes)
    _note_each_task(notes, resources)
    _note_seized_flows(notes, resources)
    # Last, as it refuses a model rather than warning of it
    _note_handoffs(notes, resources, handoffs)
    return notes.warnings


class _OrderNotes:
    """The orders `check_acquisition_order` has seen, and its warnings.

    Attributes:
        waits: Each task's wait, if it has one.
        holds: Each task's `seize`, `release` and the tasks that can follow it.
        before: For each pair of resources, the first place that acquires the first
            before the second.
        together: For each pair, the first wait that requests both together.
        warnings: The warnings, one for each conflicting pair.
        warned: The pairs already warned of.
    """

    def __init__(
        self, waits: Dict[str, Optional[Wait]], holds: Dict[str, "Holding"]
    ) -> None:
        self.waits = waits
        self.holds = holds
        self.before: Dict[Tuple[str, str], str] = {}
        self.together: Dict[Tuple[str, str], str] = {}
        self.warnings: List[str] = []
        self.warned: set = set()

    def warn(self, where: str, held: str, wanted: str, other: str) -> None:
        pair = frozenset((held, wanted))
        if pair in self.warned:
            return
        self.warned.add(pair)
        self.warnings.append(
            f"{other}, but {where} acquires '{held}' before '{wanted}'; the tasks can "
            f"each hold one and wait for the other for ever, and the run then ends "
            f"with no error. Acquire shared resources in one order everywhere to "
            f"avoid it"
        )

    def note(self, first: str, second: str, where: str) -> None:
        if (second, first) in self.before:
            self.warn(
                where,
                first,
                second,
                f"{self.before[(second, first)]} acquires '{second}' before '{first}'",
            )
        for pair in ((first, second), (second, first)):
            if pair in self.together:
                self.warn(where, first, second, self.together[pair])
        self.before.setdefault((first, second), where)

    def note_together(self, first: str, second: str, task: str) -> None:
        said = f"task '{task}' requests '{first}' and '{second}' together in its wait"
        for pair in ((first, second), (second, first)):
            if pair in self.before:
                self.warn(self.before[pair], pair[0], pair[1], said)
        self.together.setdefault((first, second), said)

    def requested(self, task: str) -> List[str]:
        wait = self.waits.get(task)
        return (
            [r for r in wait.requests() if not isinstance(r, Reference)]
            if wait is not None
            else []
        )

    def seized(self, task: str) -> List[str]:
        holding = self.holds.get(task)
        if holding is None or holding.seize is None:
            return []
        return [] if isinstance(holding.seize, Reference) else [holding.seize]


def _note_requests_together(notes: _OrderNotes) -> None:
    """Notes each pair of requests a wait makes together."""
    for task, wait in notes.waits.items():
        if wait is not None:
            for first, second in wait.requests_together():
                if not isinstance(first, Reference) and not isinstance(
                    second, Reference
                ):
                    notes.note_together(first, second, task)


def _note_each_task(notes: _OrderNotes, resources: Dict[str, List[str]]) -> None:
    """Notes the order each task acquires its own resources in: its `seize`, then
    its wait requests, then its resources."""
    for task, listed in resources.items():
        own = notes.seized(task)
        acquired = [*own, *notes.requested(task), *listed]
        for i, first in enumerate(acquired):
            for second in acquired[i + 1 :]:
                if first in notes.requested(task) and second in notes.requested(task):
                    continue  # two wait requests: made together, or one at most
                notes.note(first, second, f"task '{task}'")


def _note_seized_flows(notes: _OrderNotes, resources: Dict[str, List[str]]) -> None:
    """Notes what each task acquires after the resources its flow holds when it
    reaches the task, from the `seize` of an earlier task and until its `release`.
    A walk counts each (task, held) once, so a task that loops on itself and seizes
    again ends it."""
    holds = notes.holds
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
                    *notes.seized(task),
                    *notes.requested(task),
                    *resources.get(task, []),
                ]:
                    if second != first:
                        notes.note(
                            first,
                            second,
                            f"task '{task}' holding '{first}' from a seize",
                        )
            now = set(held)
            if holding.seize is not None and not isinstance(holding.seize, Reference):
                now.add(holding.seize)
            if holding.release is not None and not isinstance(
                holding.release, Reference
            ):
                now.discard(holding.release)
            stack.extend((target, frozenset(now)) for target in holding.following)


def _note_handoffs(
    notes: _OrderNotes,
    resources: Dict[str, List[str]],
    handoffs: Dict[str, Tuple[List[str], Any]],
) -> None:
    """Notes that a task keeping a resource for the next acquires it before what
    the next task wants, and refuses a kept resource the next task's wait requests.

    Raises:
        ValueError: If a task keeps a resource for a task that requests it in its wait.
    """
    for task, (keep, following) in handoffs.items():
        for kept in [k for k in keep if not isinstance(k, Reference)]:
            for after in following:
                if kept in notes.requested(after):
                    raise ValueError(
                        f"task '{task}' keeps resource '{kept}' for '{after}', which "
                        f"requests '{kept}' in its wait, so the request would wait "
                        f"for the unit kept for it; list '{kept}' in the resource of "
                        f"'{after}' instead"
                    )
                for second in [*notes.requested(after), *resources.get(after, [])]:
                    if second != kept:
                        notes.note(
                            kept, second, f"task '{task}' keeping it for '{after}'"
                        )


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


def _name_places(spec: "TaskSpec") -> List[Tuple[str, str, Any]]:
    """Every place of a task that names a resource, store, container or signal, as
    (key, kind, name), where the name may be a `Reference`."""
    found: List[Tuple[str, str, Any]] = [
        ("resource", "resource", name) for name in spec.resources
    ]
    found += [("keep", "resource", name) for name in spec.keep]
    if spec.seize is not None:
        found.append(("seize", "resource", spec.seize.resource))
    if (
        spec.release is not None
        and spec.release.resource is not None
        and spec.release.handle is None
    ):
        found.append(("release", "resource", spec.release.resource))
    for key, transfer in (("get", spec.get), ("put", spec.put)):
        if transfer is not None:
            found.append(
                (key, "store", transfer.store)
                if transfer.store is not None
                else (key, "container", transfer.container)
            )
    if spec.signal is not None:
        found.append(("signal", "signal", spec.signal))
    waits = [("wait", spec.wait), ("subscribe", spec.subscribe)]
    if spec.open is not None:
        waits.append(("open", spec.open.wait))
    for key, wait in waits:
        if wait is None:
            continue
        for node in wait.all_nodes:
            if node.kind in ("request", "signal"):
                found.append(
                    (key, node.kind.replace("request", "resource"), node.target)
                )
            elif node.kind in ("get", "put"):
                transfer = node.target
                found.append(
                    (key, "store", transfer.store)
                    if transfer.store is not None
                    else (key, "container", transfer.container)
                )
    for action in spec.on:
        if "fire" in action:
            found.append(("on", "signal", action["fire"]["signal"]))
    if spec.listen is not None:
        if spec.listen.signal is not None:
            found.append(("listen", "signal", spec.listen.signal))
        for action in spec.listen.on:
            if "fire" in action:
                found.append(("listen", "signal", action["fire"]["signal"]))
    return found


def family_problem(
    specs: Mapping[str, "TaskSpec"],
    families: Mapping[str, Any],
    declared: Mapping[str, Mapping[str, Any]],
    signal_actions: Optional[Mapping[str, List[Dict[str, Any]]]] = None,
) -> Optional[Tuple[Tuple[str, str], str, str]]:
    """The first place that names a family wrongly, or None.

    A family is named by a reference `{of: <family>, key: ...}`, never by its bare name;
    a reference's `of` is a family of the kind the place takes, or a list of declared
    names that are no family.

    Args:
        specs: The tasks, by name.
        families: The declared families, by name, each with its `kind`.
        declared: For each kind (`resource`, `store`, `container`, `signal`), the
            declared names.
        signal_actions: The `on:` actions of each signal, by signal name.

    Returns:
        Optional[Tuple[Tuple[str, str], str, str]]: The holder, `("tasks", <task>)` or
        `("signals", <signal>)`, the key and the message, or None.
    """
    # Keyed by the section and the name, so a task named `signals.x` stays a task
    places: Dict[Tuple[str, str], List[Tuple[str, str, Any]]] = {
        ("tasks", name): _name_places(spec) for name, spec in specs.items()
    }
    for signal, actions in (signal_actions or {}).items():
        places[("signals", signal)] = [
            ("on", "signal", a["fire"]["signal"]) for a in actions if "fire" in a
        ]
    for task, found in places.items():
        for key, kind, name in found:
            if isinstance(name, Reference):
                problem = _reference_problem(name, kind, families, declared)
            elif name in families and families[name].kind == kind:
                problem = (
                    f"names the family '{name}', which is named by a reference: "
                    f"{{of: {name}, key: <expression>}}"
                )
            else:
                problem = None
            if problem is not None:
                return task, key, problem
    return None


def _reference_problem(
    name: Reference, kind: str, families: Mapping[str, Any], declared: Mapping[str, Any]
) -> Optional[str]:
    """What is wrong with a reference in a place that takes `kind`, or None."""
    if name.listed:
        for each in name.names:
            if each in families:
                return (
                    f"lists '{each}', which is a family; a list names declared "
                    f"{kind}s that are not families"
                )
            if each not in declared.get(kind, ()):
                return f"lists '{each}', which is not a declared {kind}"
        if name.key is None:
            return "lists names, so it takes key and not lowest"
        return None
    family = families.get(str(name.of))
    if family is None or family.kind != kind:
        return (
            f"names '{name.of}', which is not a family of {kind}s; add family: true "
            f"to its entry"
        )
    return None


class FamilyProblem(NamedTuple):
    """The first place where a model names or reads a family wrongly; each caller
    words its own refusal from it.

    Attributes:
        kind: `name` for a place that names a family wrongly, `read` for an
            expression that reads one wrongly, `entry` for a declared entry named like
            a member of a family.
        holder: `("tasks", <task>)` or `("signals", <signal>)` for a name,
            `("tasks", <task>)` for a read of a task, `("rule", <place>)` for a read
            in a rule, with the place
            `rule_expressions` gives, and `(<section>, <name>)` for an entry.
        key: The key of the task or signal it is written under; "" for a rule or an
            entry.
        message: What is wrong, worded to follow the holder and the key.
        where: Where it is written in a blueprint file: the holder and the key, the
            place a caller gave with its own reads, a rule's location (`()` for a rule
            that has none) or the entry.
    """

    kind: str
    holder: Tuple[str, str]
    key: str
    message: str
    where: Tuple[Any, ...]


# The sources an expression reads a member of a family from, which take a key
_FAMILY_READ_SOURCES = (
    "users",
    "queue",
    "held",
    "waiting",
    "level",
    "putters",
    "getters",
    "putting",
    "getting",
    "count",
    "items",
    "signal",
)


def family_read_problem(
    expression: Expression, families: Mapping[str, Any]
) -> Optional[str]:
    """What is wrong with the way an expression reads families, or None: a family read
    without an index, or an index on a name that is no family.

    Args:
        expression: The parsed expression.
        families: The declared families, by name.

    Returns:
        Optional[str]: The reason, naming the read.
    """
    for source, name in sorted(expression.plain):
        if name in families and source in _FAMILY_READ_SOURCES:
            return (
                f"reads {source}.{name} of a family; read one member with "
                f"{source}.{name}[<key>], or list the keys with members({name})"
            )
    for source, name in sorted(expression.indexed):
        if name not in families:
            return (
                f"reads {source}.{name}[...], but '{name}' is not a family; add "
                f"family: true to its entry"
            )
    return None


def declared_names(holders: "Holders", signals: "Signals") -> Dict[str, Dict[str, Any]]:
    """The declared names of each kind, a family's entry included.

    Args:
        holders: The declared resources, stores and containers.
        signals: The declared signals.

    Returns:
        Dict[str, Dict[str, Any]]: For `resource`, `store`, `container` and `signal`,
        the declared entries by name.
    """
    return {
        "resource": holders.resources_config,
        "store": holders.stores_config,
        "container": holders.containers_config,
        "signal": {name: None for name in signals.entries},
    }


def first_family_problem(
    specs: Mapping[str, TaskSpec],
    families: Mapping[str, Any],
    holders: "Holders",
    signals: "Signals",
    readings: Iterable[Mapping[str, Expression]],
    *,
    task_reads: Iterable[Tuple[str, str, Tuple[Any, ...], Expression]] = (),
    entries: Optional[Iterable[Tuple[str, str]]] = None,
) -> Optional[FamilyProblem]:
    """The first place where a model names or reads a family wrongly, or None.

    The checks run in this order for every caller: the places that name a family
    (`family_problem`), the expressions of each task and of the generators of its
    value places, the expressions of the rules, the caller's `task_reads`, then the
    declared entries named like a member, such as `desk[1]` beside the family `desk`.

    Args:
        specs: The tasks, by name.
        families: The declared families, by name, each with its `kind`.
        holders: The declared resources, stores and containers.
        signals: The declared signals.
        readings: The parsed readings of each `add_readings` call.
        task_reads: More expressions of tasks, each as (task, key, where it is written
            in a blueprint file, expression), checked after the rules.
        entries: The declared entries to check, each as (section, name), such as
            `("resources", "desk[1]")`; by default every declared resource, store,
            container and signal that is no family.

    Returns:
        Optional[FamilyProblem]: The first problem, or None.
    """
    declared = declared_names(holders, signals)
    found = family_problem(specs, families, declared, signals.on)
    if found is not None:
        holder, key, message = found
        return FamilyProblem("name", holder, key, message, (*holder, key))
    reads = itertools.chain(
        (
            (("tasks", task), key, ("tasks", task, key), expression)
            for task, spec in specs.items()
            for key, expression in _task_expressions(spec)
        ),
        (
            (("rule", place), "", location, expression)
            for place, expression, location in rule_expressions(
                holders, readings, signals.on
            )
        ),
        (
            (("tasks", task), key, where, expression)
            for task, key, where, expression in task_reads
        ),
    )
    for holder, key, where, expression in reads:
        reason = family_read_problem(expression, families)
        if reason is not None:
            return FamilyProblem("read", holder, key, reason, where)
    if entries is None:
        entries = [
            (SECTIONS[kind], name)
            for kind, names in declared.items()
            for name in names
            if name not in families
        ]
    for section, name in entries:
        for family in families:
            if name != family and name.startswith(f"{family}["):
                return FamilyProblem(
                    "entry",
                    (section, name),
                    "",
                    f"starts with the name of the family '{family}' and a bracket; "
                    f"the members of a family are named that way, so no declared "
                    f"entry may be",
                    (section, name),
                )
    return None


def _task_expressions(spec: TaskSpec) -> Iterator[Tuple[str, Expression]]:
    """Each expression of a task, then each expression of the generators of its value
    places, with the key it is written under."""
    for key, expression, _place in spec.expressions:
        yield key, expression
    for key, _where, _after, place in task_value_places(
        lambda key: builder_value(spec, key)
    ):
        for _where, generator in generators_in(place, (_Gen,)):
            for text in generator_expressions(generator):
                try:
                    read = Expression(text, names=ANY_NAME)
                except ValueError:
                    continue
                yield key, read


def rule_expressions(
    holders: "Holders",
    readings: Iterable[Mapping[str, Expression]],
    signal_on: Mapping[str, List[Dict[str, Any]]],
) -> List[Tuple[str, Expression, Tuple[Any, ...]]]:
    """The `queue` ranks, `preempt` rules, store and container ranks, telemetry
    readings and the `when` of a signal's `on` entries, each with the place it is
    written for a message and, for a signal's `when`, its location in a blueprint
    (`()` for the others).

    Args:
        holders: The declared resources, stores and containers.
        readings: The parsed readings of each `add_readings` call.
        signal_on: The `on:` actions of each signal, by signal name.

    Returns:
        List[Tuple[str, Expression, Tuple[Any, ...]]]: The place, the expression and
        the location of each.
    """
    found: List[Tuple[str, Expression, Tuple[Any, ...]]] = []
    for name, (rank, _ties) in holders.resource_rank.items():
        found.append((f"resource '{name}' queue rank", rank, ()))
    for name, rule in holders.resource_rule.items():
        for key, expression in zip(("when", "victim"), rule):
            if expression is not None:
                found.append((f"resource '{name}' preempt {key}", expression, ()))
    for (name, direction), (rank, _ties) in holders.holder_ranks.items():
        found.append((f"'{name}' {direction}_queue rank", rank, ()))
    for parsed in readings:
        for name, expression in parsed.items():
            found.append((f"telemetry reading '{name}'", expression, ()))
    for name, actions in signal_on.items():
        for index, action in enumerate(actions):
            if action.get("when") is not None:
                found.append(
                    (
                        f"signal '{name}' on entry {index} when",
                        action["when"],
                        ("signals", name, "on", index, "when"),
                    )
                )
            for _where, generator in generators_in(action, (Expr,)):
                try:
                    read = Expression(generator.expr, names=ANY_NAME)
                except ValueError:
                    continue
                found.append((f"signal '{name}' on entry {index} value", read, ()))
    return found


def builder_value(spec: Any, key: str) -> Any:
    """The value of one place of `TASK_VALUE_PLACES` in a task made by the Builder.
    A Builder task has no payload generator: its body is a function.

    Args:
        spec: The task's spec.
        key: The place, such as `set` or `batch.size`.

    Returns:
        Any: The value written there, or None.
    """
    if key == "payload":
        return None
    if key == "set":
        return spec.attribute_changes
    if key == "put.item":
        return spec.put.item if spec.put is not None else None
    if key == "listen":
        return None if spec.listen is None else spec.listen.on
    if key.startswith("batch."):
        return None if spec.batch is None else getattr(spec.batch, key[6:])
    if key in ("wait", "open"):
        node = getattr(spec, key)
        if node is None:
            return None
        root = node.root if key == "wait" else node.wait.root
        return [leaf.args for leaf in _walk_nodes(root) if leaf.args]
    return getattr(spec, key)


def _walk_nodes(node: Any) -> Iterator[Any]:
    """A wait node and every node below it."""
    yield node
    for child in node.children:
        yield from _walk_nodes(child)


def generator_expressions(generator: Any) -> List[str]:
    """The texts of the expressions one generator holds: its `omit_when`, the `expr` of
    an `expr` generator and the condition of a `when`.

    Args:
        generator: The generator.

    Returns:
        List[str]: The texts, in that order.
    """
    texts: List[str] = []
    if generator.omit_when is not None:
        texts.append(generator.omit_when)
    if isinstance(generator, Expr):
        texts.append(generator.expr)
    elif isinstance(generator, When):
        texts.append(generator.condition)
    return texts


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
            `request`, `get`, `put`, `task`, `signal`, `any` or `all`, or `each` and the
            kind of an `each` group), once for each task that opens it.
        uses: Each use as (task, key, name, need). `need` is `any` for a read or a
            wait, `line` for a `cancel`, `unit` for a `release`, `child` for an
            `interrupt` and `element` for a use with an `index`.

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
        if need == "element":
            for opener, kind in opens[name]:
                if not kind.startswith("each "):
                    return (
                        task,
                        key,
                        f"'{name}' is opened as {article(kind)} {kind} in task '{opener}', and an "
                        f"index needs an each group, the only name that has elements",
                    )
        elif need in _HANDLE_NEEDS:
            kinds, words = _HANDLE_NEEDS[need]
            for opener, kind in opens[name]:
                if kind.removeprefix("each ") not in kinds:
                    return (
                        task,
                        key,
                        f"'{name}' is opened as {article(kind)} {kind} in task '{opener}', and "
                        f"{key} needs {words}",
                    )
    return None
