"""Turns a blueprint file into a `SimulationContext`.

Everything is built through the public builder methods, so a blueprint and a Python
script that make the same calls produce the same simulation.
"""

import importlib
import importlib.util
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)

from pydantic import BaseModel, ValidationError

from desflow.blueprint.loader import BlueprintError, SourceMap, read_document
from desflow.blueprint.models import (
    Blueprint,
    BlueprintTest,
    QueueRule,
    ReleaseSpec,
    Resource,
    Run,
    SeizeSpec,
    SegmentEvery,
    TestsFile,
    WhenExpr,
    unknown_key,
)
from desflow.config import LABELS
from desflow.connectors.egress.local import ConsoleEgress
from desflow.core.expressions import ANY_NAME, Expression, Scope
from desflow.core.arrivals import ArrivalPlan
from desflow.core.flow import Open, Wait
from desflow.core.registry import ParamRejected, is_queue_limit, queue_limit_value
from desflow.core.task import (
    NO_RECORD,
    Holding,
    check_acquisition_order,
    flow_sets,
    handle_problem,
    scope_problem,
)
from desflow.core.context import SimulationContext
from desflow.core.environment import in_zone, resolve_zone
from desflow.utils import suggestion
from desflow.values.check import (
    task_value_places,
    check_value,
    children,
    generators_in,
    load_rows,
    lookup_cycle,
    task_value_problems,
)
from desflow.values.models import (
    Aggregate,
    Expr,
    Lookup,
    Previous,
    Row,
    Upstream,
    When,
    _Gen,
    has_generator,
)
from desflow.values.template import BoundPayload, Template, bind as bind_values

logger = logging.getLogger(__name__)

# Short connector names, mapped to the module, the class and the extra that installs
# what the module imports. Modules are imported only when a blueprint names them, so
# a blueprint that uses Kafka does not need the Postgres driver.
EGRESS_TYPES: Dict[str, Tuple[str, str, str]] = {
    "Console": ("desflow.connectors.egress.local", "ConsoleEgress", ""),
    "Kafka": ("desflow.connectors.egress.kafka", "KafkaEgress", "kafka"),
    "Parquet": (
        "desflow.connectors.egress.storage",
        "ParquetStorageEgress",
        "parquet",
    ),
    "Jsonl": ("desflow.connectors.egress.storage", "JsonlStorageEgress", "parquet"),
    "Iceberg": (
        "desflow.connectors.egress.iceberg",
        "IcebergStorageEgress",
        "iceberg",
    ),
    "Postgres": (
        "desflow.connectors.egress.postgres",
        "PostgresEgress",
        "postgres",
    ),
    "Redis": ("desflow.connectors.egress.redis", "RedisEgress", "redis"),
}
INGRESS_TYPES: Dict[str, Tuple[str, str, str]] = {
    "Local": ("desflow.connectors.ingress.local", "LocalIngress", ""),
    "File": ("desflow.connectors.ingress.file", "FileIngress", ""),
    "Kafka": ("desflow.connectors.ingress.kafka", "KafkaIngress", "kafka"),
    "Postgres": (
        "desflow.connectors.ingress.postgres",
        "PostgresIngress",
        "postgres",
    ),
    "Redis": ("desflow.connectors.ingress.redis", "RedisIngress", "redis"),
}


@dataclass(frozen=True)
class Preview:
    """A preview run of a blueprint, as `desflow run --sample` and `--stdout` make.

    Every egress entry is replaced by a JSON console on stdout, and a second one for
    labels when the file has a `when: labels` entry, so no sink module the file
    names by a short `type` is imported, and no sink is written to.

    Attributes:
        sample: Print this many events, unpaced and with no ingress, then stop.
            None, for `--stdout`, prints every record of the run as it is paced,
            except the `system.simulation.lag_seconds` readings.
    """

    sample: Optional[int] = None


def build(
    path: Union[str, Path],
    preview: Optional[Preview] = None,
    variables: Optional[Mapping[str, str]] = None,
) -> Tuple[SimulationContext, Run]:
    """Reads, validates and builds a blueprint file.

    The structure, keys and names of the whole file are validated with or without
    a preview. A preview builds none of the connectors it replaces or skips, so it
    checks only their type; their config is checked only by a run that builds
    them, such as `desflow run FILE` without `--sample` or `--stdout`.

    Args:
        path: The YAML file.
        preview: Replace the file's egress with a JSON console, and for a sample
            leave out its ingress; None builds the file as written.
        variables: Values for `${NAME}` that are used before the environment, as
            `desflow run --var NAME=VALUE` gives them.

    Returns:
        The built context, and the `run` section for whoever starts it.

    Raises:
        BlueprintError: If the file is invalid. The message names the file and line.
    """
    built = load_blueprint(path, preview, variables)
    return built.context, built.run


def check(
    path: Union[str, Path], variables: Optional[Mapping[str, str]] = None
) -> SimulationContext:
    """Builds a blueprint as `desflow run` would, without running it, and checks the
    extras that a run imports only when it starts writing.

    It is `build` and one more check: a `Parquet` or `Jsonl` entry needs PyArrow,
    which those connectors import when they write, so a missing `parquet` extra
    would otherwise show only once the run has started.

    Args:
        path: The YAML file.
        variables: Values for `${NAME}`, as for `build`.

    Returns:
        SimulationContext: The built context, which `desflow check` only reads.

    Raises:
        BlueprintError: If the file is invalid or an entry needs an extra that is
            not installed. The message names the file and line.
    """
    built = load_blueprint(path, None, variables)
    if importlib.util.find_spec("pyarrow") is None:
        for index, entry in enumerate(built.blueprint.egress):
            if entry.type in ("Parquet", "Jsonl"):
                raise built.source.error(
                    ("egress", index, "type"),
                    f"egress type {entry.type} needs PyArrow, which is not installed. "
                    f'Install the extra with pip install "desflow[parquet]"',
                )
    return built.context


class Built(NamedTuple):
    """A blueprint file after it is validated and built.

    Attributes:
        context: The built context, not yet run.
        run: The `run` section.
        blueprint: The validated file.
        source: The lines of the file's keys, for messages that name a line.
    """

    context: SimulationContext
    run: Run
    blueprint: Blueprint
    source: SourceMap


def load_blueprint(
    path: Union[str, Path],
    preview: Optional[Preview] = None,
    variables: Optional[Mapping[str, str]] = None,
) -> Built:
    """Reads, validates and builds a blueprint file, and keeps the validated file.

    `build` and `check` call it, and `desflow test` reads the file's `tests` from it.

    Args:
        path: The YAML file.
        preview: As for `build`.
        variables: As for `build`.

    Returns:
        Built: The context, the `run` section, the blueprint and its source map.

    Raises:
        BlueprintError: If the file is invalid. The message names the file and line.
    """
    data, source = read_document(path, variables=variables)
    try:
        # One `now` for the whole file, so relative times agree with each other;
        # an instant, read in the run's zone
        now = datetime.now(timezone.utc)
        blueprint = Blueprint.model_validate(data, context={"now": now})
    except ValidationError as exc:
        raise _validation_error(exc, source) from None

    _check_references(blueprint, source)
    _check_arrival_places(blueprint, source)
    _bind_values(blueprint, source)
    context = _build_context(blueprint, source, preview)
    _check_values(blueprint, context, source)
    _check_scenario(blueprint, context, source)
    _check_registry_reads(blueprint, context, source)
    _warn_on_paths(blueprint, context, source)
    return Built(context, blueprint.run, blueprint, source)


def read_tests_file(
    path: Union[str, Path], variables: Optional[Mapping[str, str]] = None
) -> Tuple[List[BlueprintTest], SourceMap]:
    """Reads the `tests.yaml` beside a builder file: a mapping with a `tests` list.

    Args:
        path: The file.
        variables: Values for `${NAME}`, as for `build`.

    Returns:
        The tests, and the source map of the file's lines.

    Raises:
        BlueprintError: If the file is not valid. The message names the file and line.
    """
    data, source = read_document(path, variables=variables)
    try:
        return TestsFile.model_validate(data).tests, source
    except ValidationError as exc:
        raise _validation_error(exc, source, TestsFile) from None


def _file_location(loc: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """The keys of a Pydantic error location that the file has: the union tags
    pydantic adds (`<gen>`, `<map>`, `name`, `rules`, and `function-after[...]` for a
    model that has an `after` validator) and the kind after `<gen>` are left out."""
    kept = []
    for index, part in enumerate(loc):
        if isinstance(part, str) and part.startswith("<") and part.endswith(">"):
            continue
        if isinstance(part, str) and part.startswith("function-after["):
            continue
        if index and loc[index - 1] == "<gen>":
            continue
        if index and loc[index - 1] == "next" and part in ("name", "rules"):
            continue
        kept.append(part)
    return tuple(kept)


def _validation_error(
    exc: ValidationError, source: SourceMap, model: Any = Blueprint
) -> BlueprintError:
    """Lists every validation error, each with its line."""
    messages = []
    for error in exc.errors():
        # A task's `next` is a name or rules, and pydantic adds which one to the
        # location; the file has no such key, so it is left out.
        location = _file_location(error["loc"])
        where = ".".join(str(part) for part in location) or "top level"
        text = error["msg"]
        if error["type"] == "extra_forbidden":
            text = unknown_key(model, tuple(error["loc"]))
        messages.append(
            str(source.error(_without_classes(location), f"{where}: {text}"))
        )
    return BlueprintError("\n".join(messages))


def _without_classes(location: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """`location` without the class names a wait's union adds, which the file does not
    have, so that the line found is the line of the key inside the wait."""
    from typing import get_args

    from desflow.blueprint.models import TimeoutAfter, WaitBranch

    classes = {member.__name__ for member in get_args(WaitBranch)}
    classes.add(TimeoutAfter.__name__)
    return tuple(part for part in location if part not in classes)


def _check_generator_places(
    blueprint: Blueprint,
    name: str,
    task: Any,
    source: SourceMap,
    reached_after_get: bool,
    reached_by_interrupt: bool,
) -> None:
    """Checks the `expr` and `when` text of every generator a task holds, in each place
    of `TASK_VALUE_PLACES`: the rules of `scope_problem` (`cause`, `item`, `item_priority` and
    `done_count`), that every signal, store, container, resource and task name an
    expression reads exists, and that a `wait.<branch>` is a branch of the task."""
    takes_item = task.get is not None and task.get.store is not None and task.get.wait
    var_names = {*task.vars_once, *task.vars, *task.finish_vars}
    branches = set(Wait(task.wait).branches) if task.wait else set()

    def value_of(key: str) -> Any:
        if key == "payload" and callable(task.payload):
            return {}
        return _task_value(task, key)

    for _key, where, after, value in task_value_places(value_of):
        for text, path in _generator_texts(value, ("tasks", name) + where):
            try:
                read = Expression(text, names=ANY_NAME)
            except ValueError:
                continue
            for loop in sorted(read.references["loop"] & var_names):
                raise source.error(
                    path,
                    f"'{text}' in task '{name}': the loop name '{loop}' is a var of "
                    "the task",
                )
            problem = scope_problem(
                read,
                own_item=bool(takes_item and after),
                later_item=reached_after_get,
                cause_ok=reached_by_interrupt,
                in_when=False,
            )
            if problem is None and read.references["item"]:
                if not (takes_item and after) and not reached_after_get:
                    problem = (
                        "item.<field> reads the item a store get took, so it is read "
                        "in the get's filter, the places evaluated after a task's own "
                        "get, and the tasks that follow one"
                    )
            if problem:
                raise source.error(path, f"'{text}' in task '{name}': {problem}")
            _check_run_names(read, blueprint, source, path)
            for branch in sorted(read.references["wait"] - branches):
                raise source.error(
                    path,
                    f"'{text}' in task '{name}': wait.{branch} is not a branch of "
                    f"this task's wait" + suggestion(branch, branches),
                )


def _check_handles(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that every handle a task reads, waits for, cancels, releases or
    interrupts is opened by some task, and opened as something that use makes sense
    of: a cancel needs a request, get or put, a release a request, an interrupt a
    task."""
    opens: Dict[str, List[Tuple[str, str]]] = {}
    for name, task in blueprint.tasks.items():
        if task.open is not None:
            for handle, node in Open(task.open).nodes.items():
                opens.setdefault(handle, []).append((name, node.kind))
    uses: List[Tuple[str, str, str, str]] = []
    for name, task in blueprint.tasks.items():
        if task.wait is not None:
            uses += [
                (name, "wait", h, "any")
                for h in sorted(Wait(task.wait).targets("handle"))
            ]
        if task.open is not None:
            uses += [
                (name, "open", h, "any")
                for h in sorted(Open(task.open).targets("handle"))
            ]
        if task.cancel is not None:
            handle = task.cancel if isinstance(task.cancel, str) else task.cancel.handle
            uses.append((name, "cancel", handle, "line"))
        if isinstance(task.release, ReleaseSpec) and task.release.handle is not None:
            uses.append((name, "release", task.release.handle, "unit"))
        if task.interrupt is not None and task.interrupt.handle is not None:
            uses.append((name, "interrupt", task.interrupt.handle, "child"))
        for expression, location in _expressions(name, task):
            uses += [
                (name, str(location[2]), h, "any")
                for h in sorted(expression.references["handle"])
            ]
    problem = handle_problem(opens, uses)
    if problem is not None:
        holder, key, message = problem
        raise source.error(("tasks", holder, key), f"task '{holder}': {key} {message}")


def _check_switches(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that every name in an `activate` or `deactivate`, of a task, a task's
    `on:` entry or a signal's `on:` entry, is an arrival that has a `spawn`."""

    def check(names: Any, owner: str, location: Tuple[Union[str, int], ...]) -> None:
        for arrival in [names] if isinstance(names, str) else list(names or []):
            if arrival not in blueprint.arrivals:
                raise source.error(
                    location,
                    f"{owner} switches '{arrival}', "
                    + _undefined(arrival, blueprint.arrivals, "arrivals"),
                )
            if blueprint.arrivals[arrival].spawn is None:
                raise source.error(
                    location,
                    f"{owner} switches arrival '{arrival}', which has no spawn, so "
                    "it starts nothing to turn on or off",
                )

    for name, task in blueprint.tasks.items():
        for key in ("activate", "deactivate"):
            check(getattr(task, key), f"task '{name}'", ("tasks", name, key))
        for index, entry in enumerate(task.on):
            for key in ("activate", "deactivate"):
                check(
                    getattr(entry, key),
                    f"the on: entry of task '{name}'",
                    ("tasks", name, "on", index, key),
                )
    for name, signal in blueprint.signals.items():
        for index, entry in enumerate(signal.on):
            for key in ("activate", "deactivate"):
                check(
                    getattr(entry, key),
                    f"the on: entry of signal '{name}'",
                    ("signals", name, "on", index, key),
                )


def _no_priority(name: str, resource: Resource) -> str:
    """Why a request for `resource` takes no `priority`, or an empty string when it
    does: the resource does not queue by priority, or it ranks its queue by an
    expression, which is the request's priority."""
    if isinstance(resource.queue, QueueRule):
        return (
            f"resource '{name}' ranks its queue by queue.rank, which is every "
            "request's priority; remove the priority"
        )
    if resource.queue != "priority":
        return (
            f"resource '{name}' does not set queue: priority; set it, or remove the "
            "priority"
        )
    return ""


def _check_references(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that every name one section uses is defined in another."""
    after_get_tasks, cause_targets = flow_sets(
        {
            name: {
                *(target for target, _ in _routes(name, task)),
                *(
                    target
                    for target in (
                        task.on_interrupt_next,
                        task.on_fail,
                    )
                    if target is not None
                ),
            }
            for name, task in blueprint.tasks.items()
        },
        [
            name
            for name, task in blueprint.tasks.items()
            if task.get is not None and task.get.store is not None and task.get.wait
        ],
        [t.on_interrupt_next for t in blueprint.tasks.values() if t.on_interrupt_next],
    )
    _check_switches(blueprint, source)
    for name in set(blueprint.stores) & set(blueprint.containers):
        raise source.error(
            ("stores", name),
            f"'{name}' is both a store and a container; level.{name} needs one",
        )

    for name, task in blueprint.tasks.items():
        if task.service is not None and task.service not in blueprint.services:
            raise source.error(
                ("tasks", name, "service"),
                f"task '{name}' uses service '{task.service}', "
                + _undefined(task.service, blueprint.services, "services"),
            )
        for resource in task.resources():
            if resource not in blueprint.resources:
                raise source.error(
                    ("tasks", name, "resource"),
                    f"task '{name}' uses resource '{resource}', "
                    + _undefined(resource, blueprint.resources, "resources"),
                )
            if task.priority is not None and _no_priority(
                resource, blueprint.resources[resource]
            ):
                raise source.error(
                    ("tasks", name, "priority"),
                    f"task '{name}' has a priority, but "
                    + _no_priority(resource, blueprint.resources[resource]),
                )
        for key, resource in (
            *(("seize", r) for r in task.seized()),
            *(("open", r) for r in task.open_requests()),
        ):
            if resource not in blueprint.resources:
                raise source.error(
                    ("tasks", name, key),
                    f"task '{name}' uses resource '{resource}', "
                    + _undefined(resource, blueprint.resources, "resources"),
                )
        if (
            task.release is not None
            and not isinstance(task.release, str)
            and task.release.resource is not None
            and task.release.resource not in blueprint.resources
        ):
            raise source.error(
                ("tasks", name, "release"),
                f"task '{name}' uses resource '{task.release.resource}', "
                + _undefined(task.release.resource, blueprint.resources, "resources"),
            )
        if isinstance(task.release, str) and task.release not in blueprint.resources:
            raise source.error(
                ("tasks", name, "release"),
                f"task '{name}' releases '{task.release}', "
                + _undefined(task.release, blueprint.resources, "resources"),
            )
        if (
            isinstance(task.seize, SeizeSpec)
            and task.seize.priority is not None
            and task.seize.resource in blueprint.resources
            and _no_priority(
                task.seize.resource, blueprint.resources[task.seize.resource]
            )
        ):
            raise source.error(
                ("tasks", name, "seize", "priority"),
                f"task '{name}' has a priority on its seize, but "
                + _no_priority(
                    task.seize.resource, blueprint.resources[task.seize.resource]
                ),
            )
        for place, group in (
            ("wait", None if task.wait is None else Wait(task.wait)),
            ("open", None if task.open is None else Open(task.open).wait),
        ):
            for node in group.branches.values() if group else ():
                if (
                    node.options is not None
                    and node.options.priority is not None
                    and node.target in blueprint.resources
                    and _no_priority(node.target, blueprint.resources[node.target])
                ):
                    raise source.error(
                        ("tasks", name, place),
                        f"task '{name}' has a priority on its request in its "
                        f"{place}, but "
                        + _no_priority(node.target, blueprint.resources[node.target]),
                    )
        for resource in [*task.wait_requests(), *task.seized(), *task.open_requests()]:
            if (
                task.priority is not None
                and resource in blueprint.resources
                and _no_priority(resource, blueprint.resources[resource])
            ):
                raise source.error(
                    ("tasks", name, "priority"),
                    f"task '{name}' has a priority, but "
                    + _no_priority(resource, blueprint.resources[resource]),
                )
        for key, target in (
            ("on_interrupt_next", task.on_interrupt_next),
            ("interrupt", task.interrupt.task if task.interrupt else None),
            ("release", getattr(task.release, "task", None)),
        ):
            if target is not None and target not in blueprint.tasks:
                raise source.error(
                    ("tasks", name, key),
                    f"task '{name}' names task '{target}', "
                    + _undefined(target, blueprint.tasks, "tasks"),
                )
        for field in ("get", "put"):
            step = getattr(task, field)
            if step is None:
                continue
            kind, target = (
                ("store", step.store)
                if step.store is not None
                else ("container", step.container)
            )
            section = blueprint.stores if kind == "store" else blueprint.containers
            if target not in section:
                raise source.error(
                    ("tasks", name, field),
                    f"task '{name}' uses {kind} '{target}', "
                    + _undefined(target, section, f"{kind}s"),
                )
            if step.filter is not None and blueprint.stores[target].priority:
                raise source.error(
                    ("tasks", name, field, "filter"),
                    f"store '{target}' serves by priority, and SimPy has no store "
                    f"that both ranks and filters its items; remove the filter, or "
                    f"the store's priority",
                )
        for target, location in _routes(name, task):
            if target not in blueprint.tasks:
                raise source.error(
                    location,
                    f"task '{name}' routes to '{target}', "
                    + _undefined(target, blueprint.tasks, "tasks"),
                )
        wait = None if task.wait is None else Wait(task.wait)
        opened = None if task.open is None else Open(task.open)
        named = [
            ("task", "on_fail", task.on_fail),
            ("signal", "signal", task.signal),
            *(
                ("task", "wait", target)
                for target in sorted(wait.targets("task") if wait else [])
            ),
            *(
                ("signal", "wait", target)
                for target in sorted(wait.targets("signal") if wait else [])
            ),
            *(
                ("task", "open", target)
                for target in sorted(opened.targets("task") if opened else [])
            ),
            *(
                ("signal", "open", target)
                for target in sorted(opened.targets("signal") if opened else [])
            ),
        ]
        for kind, key, target in named:
            defined = blueprint.tasks if kind == "task" else blueprint.signals
            if target is not None and target not in defined:
                raise source.error(
                    ("tasks", name, key),
                    f"task '{name}' names {kind} '{target}', "
                    + _undefined(target, defined, f"{kind}s"),
                )
        for node in opened.wait.branches.values() if opened else []:
            if node.kind in ("get", "put"):
                kind = "store" if node.target.store is not None else "container"
                section = blueprint.stores if kind == "store" else blueprint.containers
                if node.target.target not in section:
                    raise source.error(
                        ("tasks", name, "open"),
                        f"task '{name}' opens a {node.kind} of {kind} "
                        f"'{node.target.target}', "
                        + _undefined(node.target.target, section, f"{kind}s"),
                    )
        branches = set(wait.branches) if wait else set()
        takes_item = task.get is not None and task.get.store is not None
        reached_after_get = name in after_get_tasks
        reached_by_interrupt = name in cause_targets
        var_names = {*task.vars_once, *task.vars, *task.finish_vars}
        _check_generator_places(
            blueprint,
            name,
            task,
            source,
            reached_after_get=name in after_get_tasks,
            reached_by_interrupt=name in cause_targets,
        )
        for expression, location in _expressions(name, task):
            # A var gets its value after the service, so only put, fail and next
            # (evaluated after it) read one as a bare name
            after = location[2] in ("put", "fail", "next")
            early_vars = {*task.vars_once, *task.vars}
            for loop in sorted(expression.references["loop"] & var_names):
                raise source.error(
                    location,
                    f"'{expression.text}': the loop name '{loop}' is a var of the task",
                )
            for bare in sorted(expression.references["name"]):
                if bare in var_names and after:
                    continue
                if bare in early_vars and location[2] == "service_time":
                    continue
                raise source.error(
                    location,
                    f"'{expression.text}' uses the name '{bare}', "
                    + (
                        "which is not one of the task's vars"
                        + (suggestion(bare, var_names) or "")
                        if after or bare not in var_names
                        else f"a var, which {location[2]} cannot read: vars get a "
                        "value after the service"
                        if location[2] != "service_time"
                        else "a finish_vars name, which a service_time cannot read: "
                        "it gets its value after the service"
                    )
                    + "; write payload.<field>, attr.<name>, level.<store> or "
                    "another source for anything else",
                )
            if task.payload is None and expression.references["payload"]:
                raise source.error(
                    location,
                    f"'{expression.text}' reads payload, but payload: null gives "
                    f"the task no payload",
                )
            for signal in expression.references["signal"]:
                if signal not in blueprint.signals:
                    raise source.error(
                        location,
                        f"signal.{signal} names no signal under signals"
                        + (
                            suggestion(signal, blueprint.signals)
                            or "; add it under signals"
                        ),
                    )
            in_when = location[2:4] == ("wait", "when")
            for branch in expression.references["wait"]:
                # balk and patience are read before the wait starts
                if branch not in branches or (
                    location[2] in ("wait", "balk", "patience", "open", "seize")
                    and not in_when
                ):
                    raise source.error(
                        location,
                        f"wait.{branch} is not a branch of this task's wait, read "
                        f"after it; read it in the task's payload, put, next or "
                        f"fail, after the wait" + suggestion(branch, branches),
                    )
            # item.<field> reads what a store get took: a get's own filter, then
            # the put and next after it
            # A get with wait: false gives the task no item, so only its own filter
            # reads one
            reads_item = (
                location[2] in ("put", "next", "fail", "service_time")
                and takes_item
                and task.get is not None
                and task.get.wait
            ) or location[2:4] == ("get", "filter")
            problem = scope_problem(
                expression,
                own_item=bool(takes_item and reads_item),
                later_item=reached_after_get,
                cause_ok=reached_by_interrupt,
                in_when=in_when,
            )
            if problem:
                raise source.error(location, f"'{expression.text}': {problem}")
            if (
                expression.references["item"]
                and not (takes_item and reads_item)
                and not reached_after_get
            ):
                raise source.error(
                    location,
                    "item.<field> reads the item a store get took, so it is "
                    "read in the get's filter, the put and next of a task with one",
                )
            for level in expression.references["level"]:
                if level not in blueprint.stores and level not in blueprint.containers:
                    raise source.error(
                        location,
                        f"level.{level} names no store or container"
                        + (
                            suggestion(
                                level, [*blueprint.stores, *blueprint.containers]
                            )
                            or "; add it under stores or containers"
                        ),
                    )
            _check_resource_names(expression, blueprint, source, location)
            _check_store_names(expression, blueprint, source, location)

    _check_handles(blueprint, source)

    _check_events(blueprint, source)

    _check_run(blueprint, source)

    try:
        check_acquisition_order(
            {name: task.resources() for name, task in blueprint.tasks.items()},
            {
                name: (task.keep, {target for target, _ in _routes(name, task)})
                for name, task in blueprint.tasks.items()
            },
            {
                name: Wait(task.wait) if task.wait is not None else None
                for name, task in blueprint.tasks.items()
            },
            {
                name: Holding(
                    task.seized()[0] if task.seized() else None,
                    task.release
                    if isinstance(task.release, str)
                    else (
                        task.release.resource
                        if task.release is not None and task.release.task is None
                        else None
                    ),
                    {
                        *(target for target, _ in _routes(name, task)),
                        *(
                            t
                            for t in (
                                task.on_fail,
                                task.on_interrupt_next,
                            )
                            if t is not None
                        ),
                    },
                )
                for name, task in blueprint.tasks.items()
            },
        )
    except ValueError as exc:
        raise source.error(("tasks",), str(exc)) from None

    for kind in ("resources", "stores"):
        for name, capacity in getattr(blueprint, kind).items():
            for field in ("current_cap", "max_cap"):
                value = getattr(capacity, field)
                if value is None or (
                    math.isinf(value) and (value > 0 or kind == "resources")
                ):
                    continue
                if not float(value).is_integer():
                    raise source.error(
                        (kind, name, field),
                        f"{kind[:-1]} '{name}' needs a whole number for {field}",
                    )

    for name, arrival in blueprint.arrivals.items():
        if arrival.spawn is not None and arrival.spawn not in blueprint.tasks:
            raise source.error(
                ("arrivals", name, "spawn"),
                f"arrival '{name}' spawns task '{arrival.spawn}', "
                + _undefined(arrival.spawn, blueprint.tasks, "tasks"),
            )

    stop = blueprint.run.stop or {}
    if "signal" in stop and stop["signal"] not in blueprint.signals:
        raise source.error(
            ("run", "stop"),
            f"run.stop names signal '{stop['signal']}', "
            + _undefined(stop["signal"], blueprint.signals, "signals"),
        )

    for index, step in enumerate(blueprint.scenario):
        if (
            step.interrupt is not None
            and step.interrupt.task is not None
            and step.interrupt.task not in blueprint.tasks
        ):
            raise source.error(
                ("scenario", index, "interrupt"),
                f"the step interrupts task '{step.interrupt.task}', "
                + _undefined(step.interrupt.task, blueprint.tasks, "tasks"),
            )

    for index, egress in enumerate(blueprint.egress):
        if (
            egress.when in ("history", "live")
            and blueprint.simulation.go_live_at is None
        ):
            raise source.error(
                ("egress", index, "when"),
                f"when: {egress.when} needs simulation.go_live_at",
            )


def _starts(blueprint: Blueprint) -> Set[str]:
    """The tasks an entity can start a flow from: an arrival's task, or one a spawn
    starts. Each spawn begins a new entity, so a spawn target is a start of its own."""
    starts = {a.spawn for a in blueprint.arrivals.values() if a.spawn is not None}
    for task in blueprint.tasks.values():
        if task.spawn is not None:
            starts.add(task.spawn.task)
        starts.update(e.spawn.task for e in task.on if e.spawn is not None)
    for signal in blueprint.signals.values():
        starts.update(e.spawn.task for e in signal.on if e.spawn is not None)
    return starts


def _reach_from(blueprint: Blueprint, start: str) -> Tuple[Set[str], bool]:
    """The tasks reachable on one entity from `start`, and whether a reached task has a
    `!python` payload, which can start any task with `context.call`."""
    tasks = blueprint.tasks
    seen: Set[str] = set()
    unknown = False
    frontier = [start]
    while frontier:
        name = frontier.pop()
        if name in seen or name not in tasks:
            continue
        seen.add(name)
        task = tasks[name]
        if callable(task.payload):
            unknown = True
        frontier.extend(target for target, _ in _routes(name, task))
        frontier.extend(n for n in (task.on_fail, task.on_interrupt_next) if n)
        if task.wait is not None:
            frontier.extend(Wait(task.wait).targets("task"))
        if task.open is not None:
            frontier.extend(Open(task.open).targets("task"))
        if task.subscribe is not None:
            frontier.extend(Wait(task.subscribe).targets("task"))
    return seen, unknown


def _check_events(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks the `spawn`, `subscribe` and `on:` names a task or signal uses, at the
    line of each action.

    A spawned task must exist and must not share an arrival's name; a `subscribe`
    target, an `on:` fire's signal and an `on:` interrupt's task must exist; and an
    `on:` fire's `from` must be a signal or a task, and, for a task, one that runs on
    the same entity as the task or firing the `on:` list hangs on.
    """
    tasks, signals = blueprint.tasks, blueprint.signals
    arrivals = set(blueprint.arrivals)

    def spawn_clash(spawned: str, owner: str, location: tuple) -> None:
        if spawned not in tasks:
            raise source.error(
                location,
                f"{owner} spawns task '{spawned}', "
                + _undefined(spawned, tasks, "tasks"),
            )
        if spawned in arrivals:
            raise source.error(
                location,
                f"task '{spawned}', which {owner} spawns, has the name of arrival "
                f"'{spawned}'; rename the task or the arrival, because their flow "
                f"statistics and entity numbers would merge",
            )

    starts = _starts(blueprint)
    reach = {start: _reach_from(blueprint, start) for start in starts}

    def firing_tasks(signal_name: str) -> set:
        """The tasks whose end fires `signal_name`, directly or through a chain of
        signals' `on:` fire actions."""
        direct: Dict[str, set] = {}
        chain: Dict[str, set] = {}
        for name, task in tasks.items():
            if task.signal is not None:
                direct.setdefault(task.signal, set()).add(name)
            for entry in task.on:
                if entry.fire is not None:
                    direct.setdefault(entry.fire.signal, set()).add(name)
        for name, signal in signals.items():
            for entry in signal.on:
                if entry.fire is not None:
                    chain.setdefault(entry.fire.signal, set()).add(name)
        found: set = set()
        queue, seen = [signal_name], set()
        while queue:
            current = queue.pop()
            if current in seen:
                continue
            seen.add(current)
            found |= direct.get(current, set())
            queue.extend(chain.get(current, set()))
        return found

    def check_fire(fire: Any, owner: str, location: tuple, anchors: set) -> None:
        """Checks a fire action's signal exists, and its `from` names a signal or a
        task that runs on the same entity as the `anchors` the `on:` list hangs on."""
        if fire.signal not in signals:
            raise source.error(
                location,
                f"the on: action of {owner} fires signal '{fire.signal}', "
                + _undefined(fire.signal, signals, "signals"),
            )
        if (fire.urgent or fire.after is not None) and not signals[fire.signal].once:
            raise source.error(
                location,
                "`urgent` and `after` need a one-shot signal (`once: true`); "
                "a re-arming signal fires now",
            )
        source_name = fire.from_
        if source_name is None:
            return
        is_signal, is_task = source_name in signals, source_name in tasks
        if is_signal and is_task:
            raise source.error(
                location,
                f"the on: action of {owner} fires from '{source_name}', which is both "
                f"a signal and a task; rename one",
            )
        if not is_signal and not is_task:
            raise source.error(
                location,
                f"the on: action of {owner} fires from '{source_name}', which is "
                f"neither a signal nor a task",
            )
        if not is_task:
            return
        anchored = [s for s in starts if reach[s][0] & anchors]
        if not anchored:
            return  # the task or firing never starts statically; nothing to refuse
        if any(source_name in reach[s][0] for s in anchored):
            return  # some start reaches the from task together with the anchor
        if any(reach[s][1] for s in anchored):
            return  # a reached !python payload could start the from task
        if not any(source_name in reach[s][0] for s in starts):
            return  # no start reaches the from task, so there is nothing to copy
        raise source.error(
            location,
            f"the on: action of {owner} fires '{fire.signal}' from task "
            f"'{source_name}', which never runs on the same entity; name a task of "
            f"the same flow",
        )

    for name, task in tasks.items():
        owner = f"task '{name}'"
        if task.spawn is not None:
            spawn_clash(task.spawn.task, owner, ("tasks", name, "spawn"))
        if task.subscribe is not None:
            branches = Wait(task.subscribe)
            for target in branches.targets("task"):
                if target not in tasks:
                    raise source.error(
                        ("tasks", name, "subscribe"),
                        f"task '{name}' subscribes to task '{target}', "
                        + _undefined(target, tasks, "tasks"),
                    )
            for target in branches.targets("signal"):
                if target not in signals:
                    raise source.error(
                        ("tasks", name, "subscribe"),
                        f"task '{name}' subscribes to signal '{target}', "
                        + _undefined(target, signals, "signals"),
                    )
        for index, entry in enumerate(task.on):
            location = ("tasks", name, "on", index)
            if entry.spawn is not None:
                spawn_clash(entry.spawn.task, owner, location + ("spawn",))
            if (
                entry.interrupt is not None
                and entry.interrupt.task is not None
                and entry.interrupt.task not in tasks
            ):
                raise source.error(
                    location + ("interrupt",),
                    f"the on: action of {owner} interrupts task "
                    f"'{entry.interrupt.task}', "
                    + _undefined(entry.interrupt.task, tasks, "tasks"),
                )
            if entry.fire is not None:
                check_fire(entry.fire, owner, location + ("fire",), {name})

    for name, signal in signals.items():
        owner = f"signal '{name}'"
        anchors = firing_tasks(name)
        for index, entry in enumerate(signal.on):
            location = ("signals", name, "on", index)
            if entry.spawn is not None:
                spawn_clash(entry.spawn.task, owner, location + ("spawn",))
            if (
                entry.interrupt is not None
                and entry.interrupt.task is not None
                and entry.interrupt.task not in tasks
            ):
                raise source.error(
                    location + ("interrupt",),
                    f"the on: action of {owner} interrupts task "
                    f"'{entry.interrupt.task}', "
                    + _undefined(entry.interrupt.task, tasks, "tasks"),
                )
            if entry.fire is not None:
                check_fire(entry.fire, owner, location + ("fire",), anchors)


def _undefined(name: str, defined: Iterable[str], section: str) -> str:
    """The end of a message for a name no section defines: the fix, or the closest."""
    hint = suggestion(name, defined)
    return f"which is not defined under {section}" + (
        hint or f"; add it under {section}"
    )


# Task keys that take time or wait, which a `look` task must not have: a step that
# yields lets the other events of that instant run before the look finishes.
_TIMED_KEYS = (
    "service",
    "service_time",
    "wait",
    "resource",
    "seize",
    "get",
    "put",
    "patience",
    "batch",
    "open",
    "next",
)


def _check_segments(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks `run.segments` and `run.steps` against the rest of the `run` section
    and the tasks a `look` names."""
    run, simulation = blueprint.run, blueprint.simulation
    if run.steps and (run.segments is not None or run.stop or run.drain):
        other = "segments" if run.segments is not None else "stop or run.drain"
        raise source.error(
            ("run", "steps"),
            f"run.steps cannot be used with run.{other}: steps already reads the "
            "run after each event",
        )
    segments = run.segments
    if segments is None:
        return
    if run.stop or run.drain:
        raise source.error(
            ("run", "segments"),
            "run.segments cannot be used with run.stop or run.drain: a segment "
            "ends at its own until or stop",
        )
    if isinstance(segments, SegmentEvery):
        if run.until is None:
            raise source.error(
                ("run", "segments", "every"),
                "every needs run.until, which is where the last segment ends",
            )
        return
    previous = simulation.initial_time
    for index, segment in enumerate(segments):
        if (
            index
            and segments[index - 1].until is None
            and segments[index - 1].stop is None
        ):
            raise source.error(
                ("run", "segments", index),
                "this segment never runs: the segment before it has neither until "
                "nor stop, so it runs to the end of the run",
            )
        if segment.until is not None:
            if segment.until <= previous:
                raise source.error(
                    ("run", "segments", index, "until"),
                    f"segment until ({segment.until:g}) must be after the previous "
                    f"end ({previous:g}); segment untils increase",
                )
            previous = segment.until
        if segment.stop is not None:
            _check_stop(
                segment.stop, ("run", "segments", index, "stop"), blueprint, source
            )
        if segment.look is not None:
            _check_look(segment.look, blueprint, source, index)


def _check_look(name: str, blueprint: Blueprint, source: SourceMap, index: int) -> None:
    """Checks a segment's `look` names a task that takes no time."""
    where = ("run", "segments", index, "look")
    task = blueprint.tasks.get(name)
    if task is None:
        raise source.error(
            where,
            f"look names task '{name}', " + _undefined(name, blueprint.tasks, "tasks"),
        )
    timed = [key for key in _TIMED_KEYS if getattr(task, key, None) is not None]
    if timed:
        raise source.error(
            where,
            f"look task '{name}' has {', '.join(timed)}, which can wait; a look "
            "reads the model before any event at the stop time, so it takes no time",
        )


def _check_run(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks the `run` section against the rest of the blueprint, when it loads.

    A stop names a signal under `signals`, or a condition whose names exist and
    that reads no entity, as it is checked outside every task; `until` is after
    the initial time, as SimPy's `run(until=)` needs.
    """
    run, simulation = blueprint.run, blueprint.simulation
    if run.until is not None and run.until <= simulation.initial_time:
        raise source.error(
            ("run", "until"),
            f"until ({run.until:g}) must be greater than the initial time "
            f"({simulation.initial_time:g}); set until later, or initial_time earlier",
        )
    _check_segments(blueprint, source)
    if run.stop:
        _check_stop(run.stop, ("run", "stop"), blueprint, source)


def _check_stop(
    stop: Dict[str, str], where: tuple, blueprint: Blueprint, source: SourceMap
) -> None:
    """Checks a `stop` names a signal under `signals`, or a condition whose names
    exist and that reads no entity.

    Args:
        stop: The one-entry mapping, `{signal: name}` or `{condition: text}`.
        where: The location of the mapping, for the error's line.
        blueprint: The blueprint the names are looked up in.
        source: The blueprint's source map.
    """
    ((kind, target),) = stop.items()
    where = (*where, kind)
    if kind == "signal":
        if target not in blueprint.signals:
            raise source.error(
                where,
                f"stop names signal '{target}', "
                + _undefined(target, blueprint.signals, "signals"),
            )
        return
    try:
        expression = Expression(target)
    except ValueError as exc:
        raise source.error(where, str(exc)) from None
    _check_run_names(expression, blueprint, source, where)
    entity = [
        f"{kind}.<...>"
        for kind in ("payload", "attr", "item", "wait")
        if expression.references.get(kind)
    ]
    if entity:
        raise source.error(
            where,
            f"a stop condition is checked outside every task, so it cannot read "
            f"{', '.join(entity)}; read levels, registry values, signals or t",
        )


def _generator_texts(
    value: Any, path: Tuple[Any, ...]
) -> Iterator[Tuple[str, Tuple[Any, ...]]]:
    """The `expr` and `when` texts of every generator in a value, and their
    `omit_when`, with where each is written."""
    if isinstance(value, _Gen):
        if value.omit_when is not None:
            yield value.omit_when, path + ("omit_when",)
        if isinstance(value, Expr):
            yield value.expr, path + ("expr",)
        elif isinstance(value, When):
            yield value.condition, path + ("condition",)
    for item, item_path, _field in children(value, path):
        yield from _generator_texts(item, item_path)


def _arrival_expressions(
    blueprint: Blueprint,
) -> Iterator[Tuple[Expression, Tuple[Any, ...]]]:
    """Every expression in the run places of an arrival, parsed: the text of `gap`,
    `stagger`, `count`, `max_events` and `group`, and the `expr`, `when` and `omit_when`
    of any generator in those places, `vars`, `key` and `attributes`. Text that does not parse is left to
    the checks that report it."""

    for name, arrival in blueprint.arrivals.items():
        where: Tuple[Any, ...] = ("arrivals", name)
        found: List[Tuple[str, Tuple[Any, ...]]] = []
        for key in _ARRIVAL_RUN_PLACES:
            value = getattr(arrival, key)
            if isinstance(value, str):
                found.append((value, where + (key,)))
            else:
                found += _generator_texts(value, where + (key,))
        found += _generator_texts(arrival.key, where + ("key",))
        found += _generator_texts(arrival.vars, where + ("vars",))
        found += _generator_texts(arrival.attributes, where + ("attributes",))
        for text, path in found:
            try:
                yield Expression(text, names=ANY_NAME), path
            except ValueError:
                continue


def _rule_expressions(
    blueprint: Blueprint,
) -> Iterator[Tuple[Expression, Tuple[Union[str, int], ...]]]:
    """The `rank` of every `queue`, `get_queue` and `put_queue`, and the `when` and
    `victim` of every `preempt` rule, parsed, with where each is written."""
    for name, resource in blueprint.resources.items():
        if isinstance(resource.queue, QueueRule):
            yield Expression(resource.queue.rank), ("resources", name, "queue", "rank")
        if isinstance(resource.preempt, BaseModel):
            for key in ("when", "victim"):
                text = getattr(resource.preempt, key)
                if text is not None:
                    yield (
                        Expression(text, rule=True),
                        ("resources", name, "preempt", key),
                    )
    for section, holders in (
        ("stores", blueprint.stores),
        ("containers", blueprint.containers),
    ):
        for name, holder in holders.items():
            for key in ("get_queue", "put_queue"):
                rule = getattr(holder, key)
                if rule is not None:
                    yield Expression(rule.rank), (section, name, key, "rank")


def _check_arrival_places(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks the names the run places of an arrival and the value places outside a
    task (a store's `items`, a signal's `on`, a scenario step) read: every `level`,
    `signal`, `users`, `queue`, `putters`, `getters`, `count` and `items` name must
    exist, as in a `stop` condition. The rules of a `queue` and a `preempt` are such
    places too."""
    for expression, where in _arrival_expressions(blueprint):
        _check_run_names(expression, blueprint, source, where)
    for expression, where in _rule_expressions(blueprint):
        _check_run_names(expression, blueprint, source, where)
    for location, value in _value_sites(blueprint):
        for expression, where in _parsed_texts(value, location):
            _check_run_names(expression, blueprint, source, where)


def _parsed_texts(
    value: Any, location: Tuple[Any, ...]
) -> Iterator[Tuple[Expression, Tuple[Any, ...]]]:
    """The expressions of the generators in a value, parsed, with where each is
    written. Text that does not parse is left to the checks that report it."""
    for text, path in _generator_texts(value, location):
        try:
            yield Expression(text, names=ANY_NAME), path
        except ValueError:
            continue


def _check_run_names(
    expression: Expression,
    blueprint: Blueprint,
    source: SourceMap,
    where: Tuple[Union[str, int], ...],
) -> None:
    """Checks the signal, store, container and resource names an expression reads
    outside any task."""
    for signal in expression.references["signal"]:
        if signal not in blueprint.signals:
            raise source.error(
                where,
                f"signal.{signal} names no signal under signals"
                + (suggestion(signal, blueprint.signals) or "; add it under signals"),
            )
    for level in expression.references["level"]:
        if level not in blueprint.stores and level not in blueprint.containers:
            raise source.error(
                where,
                f"level.{level} names no store or container"
                + (
                    suggestion(level, [*blueprint.stores, *blueprint.containers])
                    or "; add it under stores or containers"
                ),
            )
    opened = {name for task in blueprint.tasks.values() for name in task.handles()}
    for handle in sorted(expression.references["handle"] - opened):
        raise source.error(
            where,
            f"handle.{handle} names no handle, which no task opens"
            + (suggestion(handle, opened) or "; open it with open"),
        )
    _check_resource_names(expression, blueprint, source, where)
    _check_store_names(expression, blueprint, source, where)


def _check_resource_names(
    expression: Expression,
    blueprint: Blueprint,
    source: SourceMap,
    location: Tuple[Union[str, int], ...],
) -> None:
    """Checks that `users.<name>` and `queue.<name>` name a resource."""
    for kind in ("users", "queue", "held", "waiting"):
        for name in expression.references[kind]:
            if name not in blueprint.resources:
                raise source.error(
                    location,
                    (
                        f"{kind}.{name}"
                        if kind in ("users", "queue")
                        else f"{kind}({name})"
                    )
                    + " names no resource"
                    + (
                        suggestion(name, blueprint.resources)
                        or "; add it under resources"
                    ),
                )


def _check_store_names(
    expression: Expression,
    blueprint: Blueprint,
    source: SourceMap,
    location: Tuple[Union[str, int], ...],
) -> None:
    """Checks that `putters.<name>` and `getters.<name>` name a store or container, and
    that the name in `count(...)` and `items(...)` is a store."""
    holders = [*blueprint.stores, *blueprint.containers]
    for kind in ("putters", "getters", "putting", "getting"):
        for name in expression.references[kind]:
            if name not in holders:
                raise source.error(
                    location,
                    (
                        f"{kind}.{name}"
                        if kind in ("putters", "getters")
                        else f"{kind}({name})"
                    )
                    + " names no store or container"
                    + (
                        suggestion(name, holders)
                        or "; add it under stores or containers"
                    ),
                )
    for name in expression.references["running"]:
        if name not in blueprint.tasks:
            raise source.error(
                location,
                f"running({name}) names no task"
                + (suggestion(name, blueprint.tasks) or "; add it under tasks"),
            )
    for kind in ("count", "items"):
        for name in expression.references[kind]:
            if name not in blueprint.stores:
                raise source.error(
                    location,
                    f"{kind}({name}) names no store"
                    + (suggestion(name, blueprint.stores) or "; add it under stores"),
                )


def _routes(name: str, task: Any) -> List[Tuple[str, Tuple[Union[str, int], ...]]]:
    """Every task a `next` can lead to, with where it is written."""
    where: Tuple[Union[str, int], ...] = ("tasks", name, "next")
    if task.next is None:
        return []
    if isinstance(task.next, str):
        return [(task.next, where)]
    routes = [
        (rule.task, where + ("when", index, "task"))
        for index, rule in enumerate(task.next.when)
    ]
    if task.next.default is not None:
        routes.append((task.next.default, where + ("default",)))
    for target in task.next.weights or {}:
        routes.append((target, where + ("weights", target)))
    return routes


def _expressions(
    name: str, task: Any
) -> List[Tuple[Expression, Tuple[Union[str, int], ...]]]:
    """Every expression a task holds, parsed, with where it is written."""
    where: Tuple[Union[str, int], ...] = ("tasks", name)
    found = []
    if isinstance(task.priority, str):
        found.append((Expression(task.priority), where + ("priority",)))
    if isinstance(task.preempt, str):
        found.append((Expression(task.preempt), where + ("preempt",)))
    if isinstance(task.patience, str):
        found.append((Expression(task.patience), where + ("patience",)))
    if isinstance(task.service_time, str):
        found.append(
            (
                Expression(task.service_time, names=ANY_NAME),
                where + ("service_time",),
            )
        )
    if task.balk is not None:
        found.append((Expression(task.balk), where + ("balk",)))
    if task.fail is not None:
        found.append((Expression(task.fail, names=ANY_NAME), where + ("fail",)))
    if task.interrupt is not None and task.interrupt.where is not None:
        found.append((Expression(task.interrupt.where), where + ("interrupt", "where")))
    if task.open is not None:
        for delay in Open(task.open).expressions:
            found.append((delay, where + ("open",)))
    if isinstance(task.seize, SeizeSpec):
        for value in (task.seize.priority, task.seize.preempt):
            if isinstance(value, str) and not isinstance(value, bool):
                found.append((Expression(value), where + ("seize",)))
    if task.release is not None and not isinstance(task.release, str):
        if task.release.where is not None:
            found.append((Expression(task.release.where), where + ("release", "where")))
    if task.wait is not None:
        wait = Wait(task.wait)
        for condition in wait.conditions:
            found.append((condition, where + ("wait",)))
        for delay in wait.time_expressions:
            found.append((delay, where + ("wait",)))
        for request in wait.request_expressions:
            found.append((request, where + ("wait",)))
        for predicate in wait.predicates:
            found.append((predicate, where + ("wait", "when")))
    for field in ("get", "put"):
        step = getattr(task, field)
        if step is None:
            continue
        if isinstance(step.amount, str):
            found.append(
                (Expression(step.amount, names=ANY_NAME), where + (field, "amount"))
            )
        if isinstance(step.priority, str):
            found.append(
                (
                    Expression(step.priority, names=ANY_NAME),
                    where + (field, "priority"),
                )
            )
        if step.filter is not None:
            found.append((Expression(step.filter), where + (field, "filter")))
    if task.next is not None and not isinstance(task.next, str):
        for target, weight in (task.next.weights or {}).items():
            if isinstance(weight, str):
                found.append(
                    (
                        Expression(weight, names=ANY_NAME),
                        where + ("next", "weights", target),
                    )
                )
        for index, rule in enumerate(task.next.when):
            found.append(
                (
                    Expression(rule.condition, names=ANY_NAME),
                    where + ("next", "when", index, "condition"),
                )
            )
    return found


def _whole(value: Optional[float]) -> Optional[float]:
    """A capacity as a whole number; None, the default, and infinity stay."""
    return value if value is None or math.isinf(value) else int(value)


def _build_context(
    blueprint: Blueprint, source: SourceMap, preview: Optional[Preview] = None
) -> SimulationContext:
    """Makes the builder calls a Python script would make, in the same order.

    The context does not read the `row` files while it is built, because
    `_check_values` reads them from the blueprint's folder. A Builder call on the
    finished context reads them from the working directory again.
    """
    context = SimulationContext(**blueprint.simulation.model_dump(exclude_unset=True))
    context._rows_deferred = True
    try:
        _add_to_context(context, blueprint, source, preview)
    finally:
        context._rows_deferred = False
    return context


def _add_to_context(
    context: SimulationContext,
    blueprint: Blueprint,
    source: SourceMap,
    preview: Optional[Preview] = None,
) -> None:
    """Makes the builder calls of a blueprint on `context`, in file order."""
    for index, ingress in enumerate(blueprint.ingress):
        if preview is None or preview.sample is None:
            provider = _connector(ingress.type, ingress.config, INGRESS_TYPES)
            context.add_ingress(provider(source, ("ingress", index)))
        else:
            _check_connector_type(
                ingress.type, INGRESS_TYPES, source, ("ingress", index)
            )

    if preview is None:
        for index, egress in enumerate(blueprint.egress):
            provider = _connector(egress.type, egress.config, EGRESS_TYPES)
            context.add_egress(
                provider(source, ("egress", index)),
                when=LABELS if egress.when == LABELS else _when(egress.when, blueprint),
                batch_size=egress.batch_size,
                flush_interval=egress.flush_interval,
                envelope=egress.envelope,
            )
    else:
        # The file's connectors are not constructed, so none of their modules is
        # imported and only their types are checked; a label goes only to a labels
        # sink, so a file with one gets a second console for its labels
        for index, egress in enumerate(blueprint.egress):
            _check_connector_type(egress.type, EGRESS_TYPES, source, ("egress", index))
        context.add_egress(
            ConsoleEgress(format="json"),
            when=_preview_predicate(context, preview.sample),
        )
        if any(egress.when == LABELS for egress in blueprint.egress):
            context.add_egress(ConsoleEgress(format="json"), when=LABELS)

    if blueprint.batching is not None:
        context.with_batching(**blueprint.batching.model_dump(exclude_none=True))
    if blueprint.stats is not None:
        context.with_stats(blueprint.stats.level, blueprint.stats.interval)

    for name, resource in blueprint.resources.items():
        context.add_resource(
            name,
            _whole(resource.current_cap),
            _whole(resource.max_cap),
            queue=_rule_as_mapping(resource.queue),
            preempt=_rule_as_mapping(resource.preempt),
            queue_limit=resource.queue_limit,
            type=resource.type,
        )
        if resource.breakdown is not None:
            context.add_breakdown(
                name,
                time_to_failure={
                    "dist": resource.breakdown.time_to_failure.dist,
                    **resource.breakdown.time_to_failure.parameters(),
                },
                repair={
                    "dist": resource.breakdown.repair.dist,
                    **resource.breakdown.repair.parameters(),
                },
                work=resource.breakdown.work,
            )
    for name, store in blueprint.stores.items():
        context.add_store(
            name,
            _whole(store.current_cap),
            _whole(store.max_cap),
            priority=store.priority,
            items=store.items,
            type=store.type,
            get_queue=_rule_as_mapping(store.get_queue),
            put_queue=_rule_as_mapping(store.put_queue),
        )
    for name, container in blueprint.containers.items():
        context.add_container(
            name,
            container.current_cap,
            container.max_cap,
            level=container.level,
            type=container.type,
            get_queue=_rule_as_mapping(container.get_queue),
            put_queue=_rule_as_mapping(container.put_queue),
        )
    for name, value in blueprint.variables.items():
        context.add_variable(name, value)
    for name, signal in blueprint.signals.items():
        context.add_signal(
            name, once=signal.once, on=[entry.entry() for entry in signal.on]
        )
    # A YAML task's function takes the context after its id, also in a child flow
    context._task_args = (context,)
    for name, dist in blueprint.services.items():
        context.add_service(name, dist.dist, **dist.parameters())
    targeted = _targeted_arrivals(blueprint)
    for name, arrival in blueprint.arrivals.items():
        if arrival.replay is not None:
            replay = arrival.replay
            context.add_arrival_replay(
                name,
                _relative(replay.path, source),
                time=replay.time,
                unit=replay.unit,
                fields=replay.fields,
                on_invalid=replay.on_invalid,
                attributes=arrival.attributes,
                spawn=arrival.spawn,
            )
        elif arrival.gap is None and arrival.dist is None:
            assert arrival.spawn is not None  # the model refuses initial with no spawn
            context.add_initial_arrival(
                name,
                arrival.initial,
                attributes=arrival.attributes,
                spawn=arrival.spawn,
            )
        elif arrival.gap is not None:
            context._add_gap_arrival(
                name,
                _time_place(arrival.gap),
                attributes=arrival.attributes,
                spawn=arrival.spawn,
            )
        else:
            assert arrival.dist is not None  # the model needs dist, replay or gap
            context.add_arrival(
                name,
                arrival.dist,
                attributes=arrival.attributes,
                spawn=arrival.spawn,
                **arrival.parameters(),
            )
        plan = _arrival_plan(arrival, name in targeted)
        if plan is not None:
            context._arrival_plans[name] = plan

    for name, task in blueprint.tasks.items():
        context.task(
            service_id=task.service,
            resource_id=task.resource,
            keep=task.keep,
            preempt=task.preempt,
            on_interrupt=task.on_interrupt,
            on_interrupt_next=task.on_interrupt_next,
            interrupt=None if task.interrupt is None else task.interrupt.action(),
            name=name,
            priority=task.priority,
            patience=task.patience,
            service_time=task.service_time,
            key=task.key,
            balk=task.balk,
            get=None if task.get is None else task.get.transfer(),
            put=None if task.put is None else task.put.transfer(),
            next=task.next_rules(),
            set=task.set,
            set_args=task.set_args,
            activate=task.activate,
            deactivate=task.deactivate,
            history=task.history if "history" in task.model_fields_set else None,
            vars=task.vars,
            vars_once=task.vars_once,
            on_start=task.on_start,
            finish_vars=task.finish_vars,
            wait=task.wait,
            open=task.open,
            cancel=(
                task.cancel.handle
                if task.cancel is not None and not isinstance(task.cancel, str)
                else task.cancel
            ),
            seize=(
                task.seize
                if task.seize is None or isinstance(task.seize, str)
                else task.seize.model_dump(exclude_none=True, exclude={"patience"})
            ),
            release=(
                task.release
                if task.release is None or isinstance(task.release, str)
                else task.release.action()
            ),
            signal=task.signal,
            fail=task.fail,
            fail_reason=task.fail_reason,
            on_fail=task.on_fail,
            key_field=task.key_field,
            spawn=(None if task.spawn is None else task.spawn.action()),
            subscribe=task.subscribe,
            on=[entry.entry() for entry in task.on],
        )(_payload(task.payload, task.id_field, task.attributes, task.item_field, name))
        # The places the context cannot see: the payload, the wait and the put
        context.note_values(
            name,
            task.payload if not callable(task.payload) else None,
            task.wait,
            None if task.put is None else task.put.item,
        )
    for _location, value in _value_sites(blueprint):
        context.note_values(None, value)
    for _location, value, _vars, _run in _arrival_sites(blueprint):
        context.note_values(None, value)

    for process in blueprint.processes:
        context.add_process(process.function, **process.kwargs)

    for entry in blueprint.telemetry:
        context.telemetry_loop(entry.interval)(entry.function)

    if blueprint.scenario:
        context.add_scenario(
            [
                {
                    key: value
                    for key, value in (
                        ("at", step.at),
                        ("path", step.path),
                        ("value", step.value),
                        ("record", step.record),
                        (
                            "interrupt",
                            None if step.interrupt is None else step.interrupt.action(),
                        ),
                    )
                    if value is not None or (key == "value" and step.path is not None)
                }
                for step in blueprint.scenario
            ]
        )


def _rule_as_mapping(value: Any) -> Any:
    """A `queue`, `preempt`, `get_queue` or `put_queue` as the context reads it: a rule
    model becomes its mapping, and a name or a boolean stays."""
    return (
        value.model_dump(exclude_none=True) if isinstance(value, BaseModel) else value
    )


def _time_place(value: Any) -> Any:
    """A time or count place as the run reads it: text is an `Expression`."""
    return Expression(value) if isinstance(value, str) else value


def _targeted_arrivals(blueprint: Blueprint) -> Set[str]:
    """The arrivals an `activate` or `deactivate` of a task or an `on:` entry names."""
    found: Set[str] = set()
    owners = [*blueprint.tasks.values(), *blueprint.signals.values()]
    for owner in owners:
        for entry in getattr(owner, "on", []):
            found.update(_arrival_names(entry.activate))
            found.update(_arrival_names(entry.deactivate))
        found.update(_arrival_names(getattr(owner, "activate", None)))
        found.update(_arrival_names(getattr(owner, "deactivate", None)))
    return found


def _arrival_names(value: Any) -> List[str]:
    """The names of an `activate` or `deactivate`, which is one name or a list."""
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


def _arrival_plan(arrival: Any, targeted: bool = False) -> Optional[ArrivalPlan]:
    """What an arrival does beyond drawing gaps from its distribution, or None when it
    does nothing more. A `targeted` arrival, which an `activate` or `deactivate`
    names, has a plan so its source can be switched."""
    if not (
        arrival.gap is not None
        or arrival.first_at is not None
        or arrival.count is not None
        or arrival.keys is not None
        or arrival.key is not None
        or arrival.vars
        or arrival.stagger != 0
        or arrival.max_events is not None
        or "group" in arrival.model_fields_set
        or "active" in arrival.model_fields_set
        or "off_mode" in arrival.model_fields_set
        or arrival.initial
        or targeted
    ):
        return None
    return ArrivalPlan(
        group=_time_place(arrival.group),
        active=arrival.active,
        off_mode=arrival.off_mode,
        initial=arrival.initial,
        sourceless=arrival.gap is None
        and arrival.dist is None
        and arrival.replay is None,
        gap=None if arrival.gap is None else _time_place(arrival.gap),
        first_at=arrival.first_at,
        count=None if arrival.count is None else _time_place(arrival.count),
        keys=arrival.keys,
        key=arrival.key,
        vars=arrival.vars,
        stagger=_time_place(arrival.stagger),
        max_events=(
            None if arrival.max_events is None else _time_place(arrival.max_events)
        ),
    )


def _locator(source: SourceMap) -> Callable[[Tuple[Any, ...]], str]:
    """Gives the "file:line: dotted path" a generator reports when it fails."""

    def locate(path: Tuple[Any, ...]) -> str:
        return f"{source.path}:{source.line(path)}: " + ".".join(
            str(part) for part in path
        )

    return locate


def _value_sites(blueprint: Blueprint) -> Iterator[Tuple[Tuple[Any, ...], Any]]:
    """Every place in a blueprint that takes values, outside the task keys that
    `task_value_problems` reads: with its location."""
    for name, store in blueprint.stores.items():
        yield ("stores", name, "items"), store.items
    for name, signal in blueprint.signals.items():
        yield ("signals", name, "on"), signal.on
    for index, step in enumerate(blueprint.scenario):
        yield ("scenario", index, "value"), step.value
        yield ("scenario", index, "record"), step.record
        yield ("scenario", index, "interrupt"), step.interrupt


# The arrival places that read the run only, and those that read the arrival's vars
_ARRIVAL_RUN_PLACES = ("gap", "count", "stagger", "max_events", "group")


def _arrival_sites(
    blueprint: Blueprint,
) -> Iterator[Tuple[Tuple[Any, ...], Any, bool, bool]]:
    """Every value place of an arrival, as its location, its value, whether it reads
    the arrival's vars and whether it reads the run only."""
    for name, arrival in blueprint.arrivals.items():
        where: Tuple[Any, ...] = ("arrivals", name)
        yield where + ("attributes",), arrival.attributes, True, False
        yield where + ("vars",), arrival.vars, True, True
        yield where + ("key",), arrival.key, True, True
        for key in _ARRIVAL_RUN_PLACES:
            yield where + (key,), getattr(arrival, key), False, True


def _task_value(task: Any, key: str) -> Any:
    """The value of one place of `TASK_VALUE_PLACES` in a blueprint task."""
    if key == "put.item":
        return task.put.item if task.put is not None else None
    return getattr(task, key)


def _bind_values(blueprint: Blueprint, source: SourceMap) -> None:
    """Gives every generator its place in the file: the dotted path that names its
    random sequence, and the file and line its failures name."""
    locate = _locator(source)
    for name, task in blueprint.tasks.items():
        for key, where, _after, value in task_value_places(
            lambda key: _task_value(task, key)
        ):
            bind_values(value, ("tasks", name) + where, locate)
    for location, value in _value_sites(blueprint):
        bind_values(value, location, locate)
    for location, value, _vars, _run in _arrival_sites(blueprint):
        bind_values(value, location, locate)


def _check_values(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Checks the generators of the file: var names and their order, `expr` names,
    `fake` methods, `sample` services, `param` paths, `row` files and where
    `omit_rate` and `omit_when` stand."""
    registry = context._scratch_registry(isolated=True)
    services = set(blueprint.services)
    base_dir = source.path.parent
    written = context._written_paths()
    problems: List[Tuple[Tuple[Any, ...], str]] = []
    for name, task in blueprint.tasks.items():
        found = task_value_problems(
            vars_once=task.vars_once,
            vars=task.vars,
            on_start=task.on_start,
            finish_vars=task.finish_vars,
            others={
                "payload": task.payload if not callable(task.payload) else {},
                "set": task.set,
                "set_args": task.set_args,
                "interrupt": task.interrupt,
                "on": task.on,
            },
            wait_values={
                **dict(_timeout_values(task.wait, ("wait",))),
                ("patience",): task.patience,
                ("key",): task.key,
            },
            service_time=task.service_time,
            services=services,
            base_dir=base_dir,
            registry=registry,
            early_end=(
                task.patience is not None
                or task.balk is not None
                or task.wait is not None
                or task.on_interrupt == "abandon"
                or any(
                    blueprint.resources[resource].queue_limit is not None
                    or context._limit_path(resource) in written
                    for resource in (
                        *task.resources(),
                        *task.seized(),
                        *task.open_requests(),
                    )
                    if resource in blueprint.resources
                )
            ),
        )
        problems += [(("tasks", name) + where, text) for where, text in found]
    for location, value in _value_sites(blueprint):
        for where, text in check_value(
            value,
            location,
            [],
            in_task=False,
            services=services,
            base_dir=base_dir,
            registry=registry,
        ):
            problems.append((where, text))
    for location, value, reads_vars, run_scope in _arrival_sites(blueprint):
        # An arrival's vars read the vars above them; its key and attributes read all
        arrival = blueprint.arrivals[location[1]]
        visible = list(arrival.vars)
        if location[-1] == "vars":
            problems += _arrival_vars(arrival, location, services, base_dir, registry)
            continue
        for where, text in check_value(
            value,
            location,
            visible if reads_vars else [],
            in_task=False,
            in_arrival=True,
            run_scope=run_scope,
            services=services,
            base_dir=base_dir,
            registry=registry,
        ):
            problems.append((where, text))
    # A place `check_value` does not walk (a spawn's `count` and `args`, a `put` item,
    # the `args` of a `wait` on a task) still holds `row` files to read
    for location, value, _task in _task_sites(blueprint):
        for inner, node in generators_in(value, (Row,)):
            problem = load_rows(node, base_dir)
            if problem is not None and problem not in {text for _, text in problems}:
                problems.append((location + inner + ("file",), problem))
    problems += _check_relationships(blueprint)
    if problems:
        raise BlueprintError(
            "\n".join(
                str(source.error(where, f"{'.'.join(str(p) for p in where)}: {text}"))
                for where, text in problems
            )
        )


def _task_sites(blueprint: Blueprint) -> Iterator[Tuple[Tuple[Any, ...], Any, str]]:
    """Every value place of a task, with where it is written and the task's name."""
    for name, task in blueprint.tasks.items():
        keys = [
            "payload",
            "set",
            "set_args",
            "vars",
            "vars_once",
            "on_start",
            "finish_vars",
            "interrupt",
            "on",
            "wait",
            "spawn",
            "service_time",
            "patience",
            "key",
        ]
        for key in keys:
            value = getattr(task, key)
            if not callable(value) or isinstance(value, (_Gen, dict, list)):
                yield ("tasks", name, key), value, name
        if task.put is not None and task.put.item is not None:
            yield ("tasks", name, "put", "item"), task.put.item, name


def _payload_keys(task: Any) -> Optional[Set[str]]:
    """The top-level fields of a task's finished payload, or None when they are not
    known: a `!python` function or one generator gives the payload."""
    if task.payload is None:
        return set()
    if callable(task.payload) and not isinstance(task.payload, dict):
        return None
    if not isinstance(task.payload, dict):
        return None
    keys = {str(key) for key in task.payload}
    keys |= {field for field in (task.id_field, task.item_field) if field}
    return keys | set(task.attributes)


def _check_relationships(
    blueprint: Blueprint,
) -> List[Tuple[Tuple[Any, ...], str]]:
    """The problems of `lookup`, `aggregate`, `previous` and `upstream` that need the
    other tasks: the task a lookup names exists, publishes a record, and has the field
    read; tasks do not look each other up; `upstream` stands in a task that another task
    routes to."""
    found: List[Tuple[Tuple[Any, ...], str]] = []
    tasks = blueprint.tasks
    reached_by_next: Dict[str, List[str]] = {name: [] for name in tasks}
    reached_otherwise: Set[str] = set()
    for name, task in tasks.items():
        for target, _ in _routes(name, task):
            if target in tasks:
                reached_by_next[target].append(name)
        reached_otherwise |= {
            target
            for target in (task.on_fail, task.on_interrupt_next)
            if target is not None
        }

    def field_problem(
        where: Tuple[Any, ...], owner: str, path: Optional[str], what: str
    ) -> None:
        keys = _payload_keys(tasks[owner])
        if path is None or keys is None:
            return
        head = str(path).split(".")[0]
        if head not in keys:
            found.append(
                (
                    where,
                    f"{what} '{head}' is not a field of the payload of task "
                    f"'{owner}'" + suggestion(head, keys),
                )
            )

    runs: Set[str] = set()
    # A `processes` entry is Python, which can start any task with `context.call`
    unknown_start = bool(blueprint.processes)
    for start in _starts(blueprint):
        reached, unknown = _reach_from(blueprint, start)
        runs |= reached
        unknown_start = unknown_start or unknown
    gates: Dict[str, Set[str]] = {}
    first_lookup: Dict[str, Tuple[Any, ...]] = {}
    sites: List[Tuple[Tuple[Any, ...], Any, Optional[str]]] = [
        (where, item, owner) for where, item, owner in _task_sites(blueprint)
    ]
    sites += [(where, item, None) for where, item in _value_sites(blueprint)]
    sites += [(where, item, None) for where, item, _v, _r in _arrival_sites(blueprint)]
    for location, value, owner in sites:
        for where, node in generators_in(value, (Lookup, Aggregate), location):
            target = node.task
            if target not in tasks:
                found.append(
                    (
                        where + ("task",),
                        f"{node.gen} names task '{target}', "
                        + _undefined(target, tasks, "tasks"),
                    )
                )
                continue
            if tasks[target].payload is None:
                found.append(
                    (
                        where + ("task",),
                        f"{node.gen} names task '{target}', which has payload: null "
                        "and publishes no record to read",
                    )
                )
                continue
            if target not in runs and not unknown_start:
                found.append(
                    (
                        where + ("task",),
                        f"{node.gen} names task '{target}', which no arrival, spawn, "
                        "next or wait starts, so it never has a row",
                    )
                )
                continue
            field_problem(where + ("path",), target, node.path, "path")
            if isinstance(node, Lookup):
                if node.latest_by is not None:
                    field_problem(
                        where + ("latest_by",), target, node.latest_by, "latest_by"
                    )
                if owner is not None:
                    gates.setdefault(owner, set()).add(target)
                    first_lookup.setdefault(owner, where + ("task",))
        if owner is None:
            continue
        for where, node in generators_in(value, (Previous,), location):
            if tasks[owner].payload is None:
                found.append(
                    (
                        where,
                        "previous reads the last record of its own task, and "
                        "payload: null publishes none",
                    )
                )
            else:
                field_problem(where + ("path",), owner, node.path, "path")
        for where, node in generators_in(value, (Upstream,), location):
            if owner in reached_otherwise:
                found.append(
                    (
                        where,
                        f"task '{owner}' is also reached by on_fail or "
                        "on_interrupt_next, which hand over the failure or the "
                        "interrupted task's record and not its payload; upstream "
                        "reads the payload of the task that routed here with next, "
                        "so give the task reached that way no upstream",
                    )
                )
            elif not reached_by_next[owner]:
                found.append(
                    (
                        where,
                        "upstream reads the payload of the task that routed here, "
                        f"and no task routes to '{owner}' with next",
                    )
                )
            else:
                for source in sorted(set(reached_by_next[owner])):
                    field_problem(where + ("path",), source, node.path, "path")
    cycle = lookup_cycle(gates)
    if cycle:
        found.append(
            (
                first_lookup[cycle[0]],
                "tasks look each other up, so none could start before the other had "
                "a row: " + " -> ".join(cycle),
            )
        )
    return found


def _arrival_vars(
    arrival: Any,
    location: Tuple[Any, ...],
    services: Any,
    base_dir: Any,
    registry: Any,
) -> List[Tuple[Tuple[Any, ...], str]]:
    """The problems of an arrival's `vars`, each reading the ones above it."""
    found: List[Tuple[Tuple[Any, ...], str]] = []
    seen: List[str] = []
    for var, value in arrival.vars.items():
        found += check_value(
            value,
            location + (var,),
            seen,
            in_task=False,
            in_arrival=True,
            run_scope=True,
            services=services,
            base_dir=base_dir,
            registry=registry,
        )
        seen.append(var)
    return found


def _timeout_values(
    wait: Any, path: Tuple[Any, ...]
) -> Iterator[Tuple[Tuple[Any, ...], Any]]:
    """The delay and the `value` of every timeout in a wait as written, with where
    each is written."""
    if not isinstance(wait, Mapping):
        return
    timeout = wait.get("timeout")
    if isinstance(timeout, _Gen):
        yield path + ("timeout",), timeout
    elif isinstance(timeout, Mapping):
        if isinstance(timeout.get("after"), _Gen):
            yield path + ("timeout", "after"), timeout["after"]
        if "value" in timeout:
            yield path + ("timeout", "value"), timeout["value"]
    for key in ("any", "all", "of"):
        branches = wait.get(key)
        if isinstance(branches, Mapping):
            for name, branch in branches.items():
                yield from _timeout_values(branch, path + (key, name))


def _check_scenario(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Checks every scenario path and value against the registry the run will build.

    The parameters are registered into a scratch registry with the same code the run
    uses, so a path is accepted exactly when `registry.update` would find it. Each
    value is then applied with `registry.change`, in the order the run applies the
    steps, so the load refuses exactly what the run would refuse and ignore. The
    scratch registry holds a copy of the parameters, so the model keeps its own
    starting values. Without this a misspelt path or a refused value is only logged,
    at the moment it is due.
    """
    if not blueprint.scenario:
        return

    registry = context._scratch_registry(isolated=True)

    # sorted is stable, so steps at equal times keep the order they are written in
    steps = sorted(enumerate(blueprint.scenario), key=lambda item: item[1].at)
    for index, step in steps:
        if step.path is None:
            continue
        try:
            current = registry.get(step.path).value
        except KeyError:
            raise source.error(
                ("scenario", index, "path"),
                f"'{step.path}' is not a registry path. Paths look like "
                f"{context.sim_id}.resources.<name>.current_cap or "
                f"{context.sim_id}.arrival.<name>.rate",
            ) from None
        if has_generator(step.value):
            # The value is known when the step is due, and checked then
            continue
        if (
            current is not None
            and not is_queue_limit(step.path)
            and not isinstance(step.value, type(current))
        ):
            try:
                type(current)(step.value)
            except (TypeError, ValueError):
                raise source.error(
                    ("scenario", index, "value"),
                    f"'{step.path}' holds a {type(current).__name__}, and "
                    f"{step.value!r} cannot be converted to one",
                ) from None
        try:
            registry.change(step.path, step.value)
        except ParamRejected as exc:
            raise source.error(("scenario", index, "value"), exc.reason) from None


def _warn_on_paths(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Warns about an `on:` entry whose `path` is not a registry path, or whose
    `value` cannot become the type the path holds, at the entry's line.

    It is a warning and not a refusal, because these entries load and run today: a
    wrong path ends the run with `KeyError` when the entry first runs, and a value
    the registry cannot convert is logged and ignored.
    """
    registry = context._scratch_registry()
    owners = [("tasks", name, task.on) for name, task in blueprint.tasks.items()]
    owners += [
        ("signals", name, signal.on) for name, signal in blueprint.signals.items()
    ]
    for section, name, entries in owners:
        for index, entry in enumerate(entries):
            if entry.path is None:
                continue
            where = (section, name, "on", index)
            try:
                current = registry.get(entry.path).value
            except KeyError:
                logger.warning(
                    "%s",
                    source.error(
                        (*where, "path"),
                        f"'{entry.path}' is not a registry path, so this on: entry "
                        f"ends the run with KeyError when it runs. Paths look like "
                        f"{context.sim_id}.resources.<name>.current_cap",
                    ),
                )
                continue
            if is_queue_limit(entry.path) and not has_generator(entry.value):
                try:
                    queue_limit_value(entry.path, entry.value)
                except ParamRejected as exc:
                    logger.warning(
                        "%s",
                        source.error(
                            (*where, "value"),
                            f"{exc.reason}, so the registry ignores this value",
                        ),
                    )
                continue
            if (
                current is not None
                and not has_generator(entry.value)
                and not isinstance(entry.value, type(current))
            ):
                try:
                    type(current)(entry.value)
                except (TypeError, ValueError):
                    logger.warning(
                        "%s",
                        source.error(
                            (*where, "value"),
                            f"'{entry.path}' holds a value of type "
                            f"{type(current).__name__}, and {entry.value!r} cannot "
                            f"be converted to it, so the registry ignores this value",
                        ),
                    )


def _check_registry_reads(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Checks every `registry["<path>"]` an expression reads against the registry."""
    registry = context._scratch_registry()
    found = [
        pair
        for name, task in blueprint.tasks.items()
        for pair in _expressions(name, task)
    ]
    found += list(_arrival_expressions(blueprint))
    found += list(_rule_expressions(blueprint))
    for location, value, _owner in _task_sites(blueprint):
        found += list(_parsed_texts(value, location))
    for location, value in _value_sites(blueprint):
        found += list(_parsed_texts(value, location))
    for expression, location in found:
        for path in expression.references["registry"]:
            try:
                registry.get(path)
            except KeyError:
                raise source.error(
                    location, f'registry["{path}"] is not a registry path'
                ) from None


def _relative(path: str, source: SourceMap) -> Path:
    """A path in the blueprint, read relative to the blueprint's folder."""
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return Path(source.path).parent / candidate


# The wall clock's lag behind the simulation, which a preview does not print
_LAG_PATH = "system.simulation.lag_seconds"


def _preview_predicate(
    context: SimulationContext, limit: Optional[int]
) -> Callable[[dict], bool]:
    """The preview console's predicate. It leaves out the lag readings, which
    describe the wall clock rather than the model.

    With a `limit` it counts the events into `context._sample_seen`, ends the run at
    the limit-th, and refuses every record after it, so the events at the same
    instant after the last one are not printed.
    """

    def take(record: dict) -> bool:
        if record.get("path_id") == _LAG_PATH:
            return False
        if limit is None:
            return True
        if context._sample_seen >= limit:
            return False
        if record["stream_type"] == "event":
            context._sample_seen += 1
            if context._sample_seen == limit and context._finish_run is not None:
                context._finish_run()
        return True

    return take


def _when(
    when: Union[str, WhenExpr, Callable[[dict], bool], None], blueprint: Blueprint
) -> Optional[Callable[[dict], bool]]:
    """The egress predicate: a `!python` function, or one for a name or an expression.

    `history` and `live` read each record's timestamp as a time and compare it with
    the go-live instant, so the offset of each, which a daylight-saving change can
    alter during a run, is taken into account. A go-live instant without an offset
    is read in the run's zone, as the environment reads it. `events` and `telemetry`
    compare the record's `stream_type`. `{expr: ...}` evaluates its expression on
    each record, and a failure of it stops the run.
    """
    if isinstance(when, WhenExpr):
        expression = Expression(when.expr, records=True)

        def matches(record: dict) -> bool:
            try:
                return bool(expression.evaluate(Scope(record=record)))
            except ValueError as exc:
                raise ValueError(f"egress when: {exc}") from None

        return matches
    if not isinstance(when, str):
        return when
    if when == "events":
        return lambda record: record["stream_type"] == "event"
    if when == "telemetry":
        return lambda record: record["stream_type"] == "telemetry"

    simulation = blueprint.simulation
    assert simulation.go_live_at is not None  # checked in _check_references
    zone = resolve_zone(simulation.timezone, simulation.logical_start_time)
    go_live_at = in_zone(simulation.go_live_at, zone)
    # Timestamps carry milliseconds, so the go-live instant is cut to them too, and a
    # record stamped in the millisecond of going live is live, as it always was
    go_live_at = go_live_at.replace(microsecond=go_live_at.microsecond // 1000 * 1000)
    read = datetime.fromisoformat

    if when == "history":

        def is_history(record: dict) -> bool:
            return read(record["timestamp"]) < go_live_at

        return is_history

    def is_live(record: dict) -> bool:
        return read(record["timestamp"]) >= go_live_at

    return is_live


def _set_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    """Drops unset fields, so the builder's own defaults apply as in Python."""
    return {key: value for key, value in fields.items() if value is not None}


def _connector(
    kind: Union[str, type],
    config: Dict[str, Any],
    types: Dict[str, Tuple[str, str, str]],
) -> Callable[[SourceMap, Tuple[Union[str, int], ...]], Any]:
    """Returns a function that constructs the connector, reporting failures by line.

    `kind` is a short name from `types`, or a class from a `!python` reference.
    """

    def construct(source: SourceMap, location: Tuple[Union[str, int], ...]) -> Any:
        if isinstance(kind, type):
            return _instantiate(kind, config, source, location)
        _check_connector_type(kind, types, source, location)
        module_name, class_name, extra = types[kind]
        try:
            cls = getattr(importlib.import_module(module_name), class_name)
        except ImportError as exc:
            hint = (
                f' Install it with `pip install "desflow[{extra}]"`.' if extra else ""
            )
            raise source.error(
                location + ("type",),
                f"{kind} needs a package that is missing: {exc}.{hint}",
            ) from None
        return _instantiate(cls, config, source, location)

    return construct


def _check_connector_type(
    kind: Union[str, type],
    types: Dict[str, Tuple[str, str, str]],
    source: SourceMap,
    location: Tuple[Union[str, int], ...],
) -> None:
    """Refuses a short name that is not in `types`; a `!python` class passes."""
    if not isinstance(kind, type) and kind not in types:
        raise source.error(
            location + ("type",),
            f"unknown connector type '{kind}'. Use one of {', '.join(types)}",
        )


def _instantiate(
    cls: type,
    config: Dict[str, Any],
    source: SourceMap,
    location: Tuple[Union[str, int], ...],
) -> Any:
    try:
        return cls(**config)
    except (TypeError, ValueError, ImportError, OSError) as exc:
        raise source.error(
            location + ("config",), f"{cls.__name__} rejected its config: {exc}"
        ) from None


def _payload(
    payload: Union[Dict[str, Any], Callable[..., Any], None],
    id_field: Union[str, None],
    attributes: List[str],
    item_field: Union[str, None] = None,
    task_name: str = "task",
) -> Callable:
    """The task body: a `!python` payload function, or a copy of a mapping payload.

    A mapping payload gains the task id under `id_field`, the item its store get
    took under `item_field`, and the entity attributes `attributes` names, each
    under its own name. A mapping that holds a generator is a `Template` the task
    renders after its service, and these fields are written over the result.
    """
    if payload is None:

        def silent(task_id: int, context: SimulationContext, entity: Any) -> Any:
            return NO_RECORD

        return silent
    if callable(payload):
        return payload

    template = (
        Template(payload, name=task_name, check=False)
        if has_generator(payload)
        else None
    )

    def make(task_id: int, entity: Any, item: Any = None) -> Any:
        event: Dict[str, Any] = {} if template is not None else dict(payload)
        if id_field is not None:
            event[id_field] = task_id
        if item_field is not None:
            event[item_field] = item
        if template is not None:
            # The attributes are read when the payload is built, after the service
            return BoundPayload(template, entity, event, attributes=attributes)
        for name in attributes:
            event[name] = entity.attributes.get(name)
        return event

    # Only a task with an item_field declares `item`, which a get with wait: false
    # cannot give
    if item_field is None:

        def body(
            task_id: int, context: SimulationContext, entity: Any
        ) -> Dict[str, Any]:
            return make(task_id, entity)

    else:

        def body(  # type: ignore[misc]
            task_id: int, context: SimulationContext, entity: Any, item: Any = None
        ) -> Dict[str, Any]:
            return make(task_id, entity, item)

    return body
