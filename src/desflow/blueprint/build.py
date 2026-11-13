"""Turns a blueprint file into a `SimulationContext`.

Everything is built through the public builder methods, so a blueprint and a Python
script that make the same calls produce the same simulation.
"""

import importlib
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

import yaml
from pydantic import BaseModel, ValidationError

from desflow.blueprint.loader import BlueprintError, SourceMap, read_document
from desflow.blueprint.models import (
    Blueprint,
    BlueprintTest,
    HandleRef,
    QueueRule,
    Ref,
    ReleaseSpec,
    Resource,
    Run,
    SeizeSpec,
    SegmentEvery,
    TestsFile,
    WhenExpr,
    _name,
    unknown_key,
)
from desflow.config import LABELS, TRACE
from desflow.connectors.egress.local import ConsoleEgress
from desflow.core.expressions import ANY_NAME, Expression, Scope
from desflow.builder.arrivals import ArrivalPlan
from desflow.core.families import name_of, name_problem, references
from desflow.builder.flow import Open, Release, Transfer, Wait, transfer_expressions
from desflow.builder.holders import check_level, check_queue_limit
from desflow.core.registry import ParamRejected
from desflow.core.rules import check_capacity
from desflow.builder.task import NO_RECORD
from desflow.builder.task_checks import (
    Holding,
    bare_name_problem,
    batch_mismatch,
    batch_mismatch_text,
    check_acquisition_order,
    first_family_problem,
    flow_sets,
    generator_expressions,
    handle_problem,
    scope_problem,
)
from desflow.builder.context import (
    SimulationContext,
    parts_of,
    scratch_registry,
    segment_until_problem,
    reading_problem,
    stop_condition,
    stop_family_problem,
)
from desflow.builder.model_checks import (
    names_a_member,
    priority_mix,
    unmade_member_note,
    written_paths,
)
from desflow.builder.scenario import walk_steps
from desflow.builder.signals import timing_problem
from desflow.core.environment import in_zone, resolve_zone
from desflow.utils import suggestion
from desflow.values.check import (
    Place,
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
    Attr,
    Expr,
    Lookup,
    Previous,
    Row,
    Upstream,
    When,
    Gen,
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

    Every egress entry is replaced by a JSON console on stdout, with one more console
    for labels when the file has a `when: labels` entry and one for the trace when it
    has a `when: trace` entry, so no sink module the file names by a short `type` is
    imported, and no sink is written to.

    Attributes:
        sample: Print this many events, unpaced and with no ingress, then stop.
            None, for `--stdout`, prints every record of the run as it is paced,
            except the `system.simulation.lag_seconds` readings.
    """

    sample: Optional[int] = None


def preview_builder(context: SimulationContext, preview: Preview) -> None:
    """Makes a context that a builder file defined run as a preview, as a blueprint
    built with the same preview runs.

    Every egress sink the file added is replaced by a JSON console on stdout, with
    one more console for labels when the file added a labels sink and one for the
    trace when it added a trace sink. A sample also leaves out the ingress, and runs
    unpaced from `logical_start_time`, with `go_live_at` cleared, until it has
    printed that many events. The file's connectors are already built, because
    importing the file built them, but none of them is set up or written to.

    Args:
        context: The context the builder file defines, not yet run.
        preview: The preview `desflow run --sample` makes.
    """
    sinks = parts_of(context).sinks
    kinds = []
    if any(sink.label for sink in sinks.egress):
        kinds.append(LABELS)
    if any(sink.trace for sink in sinks.egress):
        kinds.append(TRACE)
    sinks.egress = []
    if preview.sample is not None:
        sinks.ingress = []
    _add_preview_consoles(context, preview, kinds)
    run_as_sample(context, preview)


def run_as_sample(context: SimulationContext, preview: Preview) -> None:
    """Sets a context to run as a sample, when the preview is one: unpaced from
    `logical_start_time`, with `go_live_at` cleared and `strict` unset, until it has
    printed `preview.sample` events. A preview for `--stdout` changes nothing here.

    Args:
        context: The context, not yet run.
        preview: The preview.
    """
    if preview.sample is None:
        return
    context.factor = 0.0
    context.go_live_at = None
    context.strict = None
    parts_of(context).defaults.sample_limit = preview.sample


def _add_preview_consoles(
    context: SimulationContext, preview: Preview, kinds: List[str]
) -> None:
    """Adds the JSON console of a preview, and one more for each of `kinds`, the
    labels or the trace, which no other sink receives."""
    context.add_egress(
        ConsoleEgress(format="json"),
        when=_preview_predicate(context, preview.sample),
    )
    for kind in kinds:
        context.add_egress(ConsoleEgress(format="json"), when=kind)


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
    """Builds a blueprint as `desflow run` would, without running it, with two
    more checks.

    Building the connectors refuses an entry whose extra is not installed,
    including a `Parquet`, `Jsonl` or `Iceberg` entry, whose connector imports its
    packages only when it writes. A `tests` entry with no seed in a file with none
    is refused, as `desflow test` refuses it. A run
    with no end is logged as a warning, because `desflow run` of the file runs
    until it is stopped; `desflow run` itself does not warn, since a live model
    may have no end on purpose.

    Args:
        path: The YAML file.
        variables: Values for `${NAME}`, as for `build`.

    Returns:
        SimulationContext: The built context, which `desflow check` only reads.

    Raises:
        BlueprintError: If the file is invalid, a test has no seed, or an entry
            needs an extra that is not installed. The message names the file and
            line.
    """
    built = load_blueprint(path, None, variables)
    _check_test_seeds(built.blueprint, built.source)
    _warn_on_no_end(built.blueprint, built.source)
    return built.context


def missing_seed(test: str) -> str:
    """The refusal of a `tests` entry that has no seed in a file that has none,
    which `desflow check` and `desflow test` both give.

    Args:
        test: The test's name.
    """
    return (
        f"test '{test}' needs a seed: give `seed` in the test or "
        f"simulation.random_seed in the file"
    )


def _check_test_seeds(blueprint: Blueprint, source: SourceMap) -> None:
    """Refuses the first `tests` entry with no seed, when the file has none."""
    if blueprint.simulation.random_seed is not None:
        return
    for index, test in enumerate(blueprint.tests):
        if test.seed is None:
            raise source.error(("tests", index), missing_seed(test.name))


def _warn_on_no_end(blueprint: Blueprint, source: SourceMap) -> None:
    """Logs a warning for each arrival and each telemetry entry that keeps a run with no
    end going."""
    run = blueprint.run
    if run.until is not None or run.stop is not None or run.segments is not None:
        return
    for name, arrival in blueprint.arrivals.items():
        endless = arrival.dist is not None or arrival.gap is not None
        if endless and arrival.max_events is None:
            logger.warning(
                "%s",
                source.error(
                    ("arrivals", name),
                    f"arrivals.{name}: the run has no end: this arrival has no "
                    f"max_events, and run has no until or stop, so desflow run runs "
                    f"until it is stopped. Give run.until, or pass --until to "
                    f"desflow run",
                ),
            )
    for index, entry in enumerate(blueprint.telemetry):
        logger.warning(
            "%s",
            source.error(
                ("telemetry", index),
                f"telemetry[{index}]: the run has no end: this entry publishes every "
                f"{entry.interval:g} s for as long as the run goes, and run has "
                f"no until or stop, so desflow run runs until it is stopped. Give "
                f"run.until, or pass --until to desflow run",
            ),
        )


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
    _check_section_keys(data, source)
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
    _check_declared_names(blueprint, source)
    context = _build_context(blueprint, source, preview)
    _check_batches(context, source)
    _check_priority_kinds(context, source)
    _check_families(blueprint, context, source)
    _check_values(blueprint, context, source)
    _check_scenario(blueprint, context, source)
    _check_registry_reads(blueprint, context, source)
    _check_on_paths(blueprint, context, source)
    return Built(context, blueprint.run, blueprint, source)


_NAMED_SECTIONS = (
    "resources",
    "containers",
    "stores",
    "services",
    "arrivals",
    "variables",
    "signals",
    "tasks",
)


def _read_as_yaml(text: str) -> Any:
    """The value YAML reads from the text of a key, or the text when it cannot read it."""
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        return text


def _check_section_keys(data: Any, source: SourceMap) -> None:
    """Refuses a name that YAML reads as a boolean, a number, a date or null, such as
    an unquoted task called `on`, `no`, `1` or `null`, at its own line and as it is
    written. The load would otherwise name it by the value YAML read."""
    if not isinstance(data, dict):
        return
    for section in _NAMED_SECTIONS:
        entries = data.get(section)
        if not isinstance(entries, dict):
            continue
        for key in entries:
            if isinstance(key, str):
                continue
            written = next(
                (
                    text
                    for text in source.keys((section,))
                    if _read_as_yaml(text) == key
                    and type(_read_as_yaml(text)) is type(key)
                ),
                str(key),
            )
            raise source.error(
                (section, written),
                f"{section}.{written}: YAML reads the name {written} as {key!r}, "
                f'which is not text; write it in quotes, "{written}"',
            )


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
        location = _without_members(_file_location(error["loc"]), source)
        where = ".".join(str(part) for part in location) or "top level"
        text = error["msg"]
        if error["type"] == "extra_forbidden":
            text = unknown_key(model, tuple(error["loc"]))
        messages.append(
            str(source.error(_without_classes(location), f"{where}: {text}"))
        )
    return BlueprintError("\n".join(messages))


_MEMBER_NAMES = frozenset({"str", "int", "float", "bool", "list", "dict", "none"})


def _without_members(location: Tuple[Any, ...], source: SourceMap) -> Tuple[Any, ...]:
    """`location` without the parts pydantic adds for a member of a union: a type or
    model name, and a generic such as `list[...]`, which the file does not have as a
    key. A part the file has as a key stays."""
    from desflow.blueprint import models

    kept: Tuple[Any, ...] = ()
    for part in location:
        member = isinstance(part, str) and (
            "[" in part
            or part in _MEMBER_NAMES
            or isinstance(getattr(models, part, None), type)
        )
        if member and not source.has((*kept, part)):
            continue
        kept = (*kept, part)
    return kept


def _without_classes(location: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """`location` without the class names a wait's union adds, which the file does not
    have, so that the line found is the line of the key inside the wait."""
    from typing import get_args

    from desflow.blueprint.models import TimeoutAfter, WaitBranch

    classes = {member.__name__ for member in get_args(WaitBranch)}
    classes.add(TimeoutAfter.__name__)
    return tuple(part for part in location if part not in classes)


def _task_has_each(task: Any) -> bool:
    """Whether a blueprint task has a `wait`, `subscribe` or `open` with an `each` node,
    which is what gives `each.index` and `each.value` a meaning.

    Args:
        task: The blueprint task.

    Returns:
        bool: True when a wait node of the task is an `each` node.
    """
    waits = [Wait(w) for w in (task.wait, task.subscribe) if w is not None]
    if task.open is not None:
        waits.append(Open(task.open).wait)
    return any(n.is_each for w in waits for n in w.all_nodes)


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
                has_each=_task_has_each(task),
                each_here=where[0] in ("wait", "open"),
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
            opened = Open(task.open)
            for handle in opened.nodes:
                opens.setdefault(handle, []).append((name, opened.opened_as(handle)))
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
        if task.wait is not None:
            uses += [
                (name, "wait", h, "element")
                for h in sorted(Wait(task.wait).indexed_handles())
            ]
        if task.open is not None:
            uses += [
                (name, "open", h, "element")
                for h in sorted(Open(task.open).indexed_handles())
            ]
        if task.cancel is not None:
            handle = task.cancel if isinstance(task.cancel, str) else task.cancel.handle
            uses.append((name, "cancel", handle, "line"))
            if isinstance(task.cancel, HandleRef) and task.cancel.index is not None:
                uses.append((name, "cancel", handle, "element"))
        if isinstance(task.release, ReleaseSpec) and task.release.handle is not None:
            uses.append((name, "release", task.release.handle, "unit"))
            if task.release.index is not None:
                uses.append((name, "release", task.release.handle, "element"))
        if task.listen is not None and task.listen.handle is not None:
            uses.append((name, "listen", task.listen.handle, "any"))
            if task.listen.index is not None:
                uses.append((name, "listen", task.listen.handle, "element"))
        if task.interrupt is not None and task.interrupt.handle is not None:
            uses.append((name, "interrupt", task.interrupt.handle, "child"))
            if task.interrupt.index is not None:
                uses.append((name, "interrupt", task.interrupt.handle, "element"))
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
        for at_where, entries in task.on_lists():
            for index, entry in enumerate(entries):
                for key in ("activate", "deactivate"):
                    check(
                        getattr(entry, key),
                        f"the on: entry of task '{name}'",
                        ("tasks", name, *at_where, index, key),
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
                        task.on_renege,
                        task.on_balk,
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
        [
            target
            for t in blueprint.tasks.values()
            for target in (t.on_interrupt_next, t.on_interrupt_run)
            if target
        ],
    )
    _check_switches(blueprint, source)
    for name in set(blueprint.stores) & set(blueprint.containers):
        raise source.error(
            ("stores", name),
            f"'{name}' is both a store and a container; level.{name} needs one",
        )

    for name, task in blueprint.tasks.items():
        _check_task_names(name, task, blueprint, source)
        _check_task_scopes(
            name, task, blueprint, source, after_get_tasks, cause_targets
        )

    _check_handles(blueprint, source)

    _check_events(blueprint, source)

    _check_run(blueprint, source)

    _check_acquisition_order(blueprint, source)
    _check_whole_capacities(blueprint, source)
    _check_arrival_spawns(blueprint, source)
    _check_stop_signal(blueprint, source)
    _check_scenario_interrupts(blueprint, source)
    _check_egress_when(blueprint, source)


def _check_task_names(
    name: str, task: Any, blueprint: Blueprint, source: SourceMap
) -> None:
    """Checks that every service, resource, store, container, task and signal a task
    names is defined, that a priority is given only where its resource takes one,
    and that a get or put filter is not used on a store that serves by priority."""
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
        and not isinstance(task.release, (str, Ref))
        and task.release.resource is not None
        and task.release.resource not in blueprint.resources
    ):
        raise source.error(
            ("tasks", name, "release"),
            f"task '{name}' uses resource '{task.release.resource}', "
            + _undefined(task.release.resource, blueprint.resources, "resources"),
        )
    if isinstance(task.release, str) and str(task.release) not in blueprint.resources:
        raise source.error(
            ("tasks", name, "release"),
            f"task '{name}' releases '{task.release}', "
            + _undefined(str(task.release), blueprint.resources, "resources"),
        )
    if (
        isinstance(task.seize, SeizeSpec)
        and task.seize.priority is not None
        and task.seize.resource in blueprint.resources
        and _no_priority(task.seize.resource, blueprint.resources[task.seize.resource])
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
        for node in group.all_nodes if group else ():
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
        ("on_interrupt_run", task.on_interrupt_run),
        ("on_renege", task.on_renege),
        ("on_balk", task.on_balk),
        ("resume", task.resume.task if task.resume else None),
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
        for store in _store_names(step.store, target):
            if (
                step.filter is not None
                and store in blueprint.stores
                and blueprint.stores[store].priority
            ):
                raise source.error(
                    ("tasks", name, field, "filter"),
                    f"store '{store}' serves by priority, and SimPy has no store "
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
    for node in opened.wait.all_nodes if opened else []:
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


def _store_names(reference: Any, target: str) -> Tuple[str, ...]:
    """The stores a task's `store` names: every name of a `{of: [...]}` list, and the
    one name otherwise. A name that is not declared is left to the check of the name."""
    if isinstance(reference, Ref):
        return reference.name().names
    return (target,)


def _check_task_scopes(
    name: str,
    task: Any,
    blueprint: Blueprint,
    source: SourceMap,
    after_get_tasks: Set[str],
    cause_targets: Set[str],
) -> None:
    """Checks what each expression of a task may read where it is written: its vars,
    the payload, signals, wait branches, the item a get took, and the names of
    levels, resources and stores."""
    wait = None if task.wait is None else Wait(task.wait)
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
        early_vars = {*task.vars_once, *task.vars}
        for loop in sorted(expression.references["loop"] & var_names):
            raise source.error(
                location,
                f"'{expression.text}': the loop name '{loop}' is a var of the task",
            )
        problem = bare_name_problem(
            expression,
            f"batch {location[3]}" if location[2] == "batch" else str(location[2]),
            str(location[2]),
            var_names,
            early_vars,
        )
        if problem:
            raise source.error(location, problem)
        if task.payload is None and expression.references["payload"]:
            raise source.error(
                location,
                f"'{expression.text}' reads payload, but payload: null gives "
                f"the task no payload",
            )
        for signal in sorted(expression.references["signal"]):
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
        for branch in sorted(expression.references["wait"]):
            # balk and patience are read before the wait starts
            if branch not in branches or (
                location[2] in ("wait", "balk", "patience", "open", "seize", "batch")
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
        # The `when` of a task's own `on` runs after the get, as its `value` does
        reads_item = (
            (
                location[2] in ("put", "next", "fail", "service_time")
                or (location[2] == "on" and location[-1] == "when")
            )
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
            has_each=_task_has_each(task),
            each_here=(
                location[2] in ("wait", "open", "subscribe")
                and location[3:4] not in (("when",), ("group",))
            ),
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
        for level in sorted(expression.references["level"]):
            if level not in blueprint.stores and level not in blueprint.containers:
                raise source.error(
                    location,
                    f"level.{level} names no store or container"
                    + (
                        suggestion(level, [*blueprint.stores, *blueprint.containers])
                        or "; add it under stores or containers"
                    ),
                )
        _check_resource_names(expression, blueprint, source, location)
        _check_store_names(expression, blueprint, source, location)
    for expression, location in _name_expressions(name, task):
        _check_run_names(expression, blueprint, source, location)


def _check_acquisition_order(blueprint: Blueprint, source: SourceMap) -> None:
    """Refuses a task that keeps a resource for a task that requests it in its wait,
    at the `tasks` line."""
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
                    str(task.release)
                    if isinstance(task.release, (str, Ref))
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
                                task.on_renege,
                                task.on_balk,
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


def _check_whole_capacities(blueprint: Blueprint, source: SourceMap) -> None:
    """Runs the checks `add_resource`, `add_store` and `add_container` run, with the
    line of the entry: a capacity is a whole number of 0 or more, or infinity (a
    container's need not be whole), a `queue_limit` a whole number of 0 or more or
    null, and a container's `level` a finite number from 0 to its capacity."""
    for kind in ("resources", "stores", "containers"):
        singular = kind[:-1]
        for name, holder in getattr(blueprint, kind).items():
            cap = holder.current_cap if holder.current_cap is not None else None
            if kind == "containers" and cap is None:
                cap = holder.max_cap
            checks: List[Tuple[str, Callable[[Any], None]]] = []
            for field in ("current_cap", "max_cap"):
                checks.append(
                    (
                        field,
                        lambda value, field=field: check_capacity(  # type: ignore[misc]
                            singular, name, field, value, whole=kind != "containers"
                        ),
                    )
                )
            if kind == "resources":
                checks.append(
                    (
                        "queue_limit",
                        lambda value: check_queue_limit(singular, name, value),
                    )
                )
            if kind == "containers":
                checks.append(("level", lambda value: check_level(name, value, cap)))
            for field, check in checks:
                value = getattr(holder, field, None)
                if isinstance(value, str) and getattr(holder, "family", False):
                    continue  # an expression, checked when each member is made
                try:
                    check(value)
                except ValueError as exc:
                    raise source.error((kind, name, field), str(exc)) from None


def _check_arrival_spawns(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that each arrival spawns a defined task."""
    for name, arrival in blueprint.arrivals.items():
        if arrival.spawn is not None and arrival.spawn not in blueprint.tasks:
            raise source.error(
                ("arrivals", name, "spawn"),
                f"arrival '{name}' spawns task '{arrival.spawn}', "
                + _undefined(arrival.spawn, blueprint.tasks, "tasks"),
            )


def _check_stop_signal(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that the signal `run.stop` names is defined."""
    stop = blueprint.run.stop or {}
    if "signal" in stop:
        stop_signal = str(name_of(stop["signal"], "run.stop.signal"))
        if stop_signal not in blueprint.signals:
            raise source.error(
                ("run", "stop"),
                f"run.stop names signal '{stop_signal}', "
                + _undefined(stop_signal, blueprint.signals, "signals"),
            )


def _check_scenario_interrupts(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that each task a scenario step interrupts is defined."""
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


def _check_egress_when(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that an egress `when` of `history` or `live` has a `go_live_at`, and
    that one of `trace` has `stats.trace`."""
    for index, egress in enumerate(blueprint.egress):
        if (
            egress.when in ("history", "live")
            and blueprint.simulation.go_live_at is None
        ):
            raise source.error(
                ("egress", index, "when"),
                f"when: {egress.when} needs simulation.go_live_at",
            )
        if egress.when == TRACE and not (
            blueprint.stats is not None and blueprint.stats.trace
        ):
            raise source.error(
                ("egress", index, "when"),
                "when: trace receives the records of stats.trace, so the file needs "
                "stats: {trace: true}",
            )


def _starts(blueprint: Blueprint) -> Set[str]:
    """The tasks an entity can start a flow from: an arrival's task, or one a spawn
    starts. Each spawn begins a new entity, so a spawn target is a start of its own."""
    starts = {a.spawn for a in blueprint.arrivals.values() if a.spawn is not None}
    for task in blueprint.tasks.values():
        if task.spawn is not None:
            starts.add(task.spawn.task)
        for _where, entries in task.on_lists():
            starts.update(e.spawn.task for e in entries if e.spawn is not None)
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
        frontier.extend(
            n
            for n in (
                task.on_fail,
                task.on_renege,
                task.on_balk,
                task.on_interrupt_next,
                task.on_interrupt_run,
            )
            if n
        )
        if task.wait is not None:
            frontier.extend(Wait(task.wait).targets("task"))
        if task.open is not None:
            frontier.extend(Open(task.open).targets("task"))
        if task.subscribe is not None:
            frontier.extend(Wait(task.subscribe).targets("task"))
    return seen, unknown


def _check_families(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Checks the families of the file: a name place that names a family uses a
    reference, a reference's `of` is a family of the kind the place takes, a list of
    names holds declared entries that are no family, and no declared entry is named
    like a member of a family. Each is reported at its line.

    Raises:
        BlueprintError: Naming the file and the line.
    """
    parts = parts_of(context)
    found = first_family_problem(
        parts.tasks.specs,
        parts.families.by_name,
        parts.holders,
        parts.signals,
        parts.readings,
        task_reads=_file_generator_reads(blueprint),
        entries=[
            (section, name)
            for section in ("resources", "stores", "containers", "signals")
            for name in getattr(blueprint, section)
        ],
    )
    if found is None:
        return
    section, name = found.holder
    if found.kind == "entry":
        message = f"{section}.{name} {found.message}"
    elif section == "rule":
        # A rule place: a signal's `when` has its own line, the others none
        message = f"{name} {found.message}"
    elif section == "signals":
        message = f"signal '{name}': {found.key} {found.message}"
    else:
        message = f"task '{name}': {found.key} {found.message}"
    raise source.error(found.where, message)


def _file_generator_reads(
    blueprint: Blueprint,
) -> Iterator[Tuple[str, str, Tuple[Any, ...], Expression]]:
    """The expressions inside the generators of each task's value places, as the
    file writes them, each as (task, key, where, expression). The context cannot see
    a payload generator, because a Builder task's body is a function."""
    for name, task in blueprint.tasks.items():
        for key, where, _after, value in task_value_places(
            lambda key: _task_value(task, key)
        ):
            for _where, generator in generators_in(value, (Gen,)):
                for text in generator_expressions(generator):
                    try:
                        read = Expression(text, names=ANY_NAME)
                    except ValueError:
                        continue
                    yield name, key, ("tasks", name) + where, read


def _check_batches(context: Any, source: SourceMap) -> None:
    """Refuses two tasks that share a batch name but differ in `size`, `timeout`,
    `close_when`, `resource` or service, at the second task's `batch` line."""
    found = batch_mismatch(parts_of(context).tasks.specs)
    if found is not None:
        raise source.error(("tasks", found.other, "batch"), batch_mismatch_text(*found))


def _check_priority_kinds(context: Any, source: SourceMap) -> None:
    """Refuses a priority resource or store that is given a number and a list as
    priorities, at the line of the second."""
    found = priority_mix(parts_of(context).tasks.specs, parts_of(context).holders)
    if found is not None:
        raise source.error(found.location, found.message)


def _check_events(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks the `spawn`, `subscribe`, `listen` and `on:` names a task or signal uses,
    at the line of each action.

    A spawned task must exist and must not share an arrival's name; a `subscribe`
    target, a `listen` signal, an `on:` fire's signal and an `on:` interrupt's task must
    exist; a fire's `priority` needs a one-shot signal; and an
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
            for _where, entries in task.on_lists():
                for entry in entries:
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
        problem = timing_problem(
            fire.urgent,
            fire.after,
            fire.priority,
            signals[fire.signal].once,
            "`once: true`",
        )
        if problem is not None:
            raise source.error(location, problem)
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

    def check_entry(entry: Any, owner: str, location: tuple, anchors: set) -> None:
        """Checks one `on:` entry of a task or a signal: the task it spawns, interrupts
        or resumes, and the signal it fires."""
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
        if entry.resume is not None and entry.resume.task not in tasks:
            raise source.error(
                location + ("resume",),
                f"the on: action of {owner} resumes task "
                f"'{entry.resume.task}', "
                + _undefined(entry.resume.task, tasks, "tasks"),
            )
        if entry.fire is not None:
            check_fire(entry.fire, owner, location + ("fire",), anchors)

    for name, task in tasks.items():
        owner = f"task '{name}'"
        if task.spawn is not None:
            spawn_clash(task.spawn.task, owner, ("tasks", name, "spawn"))
        if task.subscribe is not None:
            branches = Wait(task.subscribe)
            for target in sorted(branches.targets("task")):
                if target not in tasks:
                    raise source.error(
                        ("tasks", name, "subscribe"),
                        f"task '{name}' subscribes to task '{target}', "
                        + _undefined(target, tasks, "tasks"),
                    )
            for target in sorted(branches.targets("signal")):
                if target not in signals:
                    raise source.error(
                        ("tasks", name, "subscribe"),
                        f"task '{name}' subscribes to signal '{target}', "
                        + _undefined(target, signals, "signals"),
                    )
        if task.listen is not None and isinstance(task.listen.signal, str):
            if task.listen.signal not in signals:
                raise source.error(
                    ("tasks", name, "listen", "signal"),
                    f"task '{name}' listens to signal '{task.listen.signal}', "
                    + _undefined(task.listen.signal, signals, "signals"),
                )
        for at_where, entries in task.on_lists():
            for index, entry in enumerate(entries):
                location: Tuple[Union[str, int], ...] = (
                    "tasks",
                    name,
                    *at_where,
                    index,
                )
                check_entry(entry, owner, location, {name})

    for name, signal in signals.items():
        owner = f"signal '{name}'"
        anchors = firing_tasks(name)
        for index, entry in enumerate(signal.on):
            location = ("signals", name, "on", index)
            check_entry(entry, owner, location, anchors)


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
        if math.isinf(run.until):
            raise source.error(
                ("run", "segments", "every"),
                "every needs a finite run.until, which is where the last segment "
                f"ends, and run.until gave {run.until:g}",
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
            problem = segment_until_problem(segment.until, previous)
            if problem is not None:
                raise source.error(("run", "segments", index, "until"), problem)
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
    stop: Dict[str, Any], where: tuple, blueprint: Blueprint, source: SourceMap
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
        written = target
        target = name_of(target, "stop.signal")
        if isinstance(written, str) and getattr(
            blueprint.signals.get(target), "family", False
        ):
            raise source.error(where, stop_family_problem(target))
        if target not in blueprint.signals:
            raise source.error(
                where,
                f"stop names signal '{target}', "
                + _undefined(target, blueprint.signals, "signals"),
            )
        for read in references(target):
            for expression in read.expressions:
                _check_run_names(expression, blueprint, source, where)
                entity = [
                    f"{part}.<...>"
                    for part in ("payload", "attr", "item", "wait")
                    if expression.references.get(part)
                ]
                if entity:
                    raise source.error(
                        where,
                        f"the key of a stop signal is read once as the run starts, "
                        f"outside every task, so it cannot read {', '.join(entity)}",
                    )
        return
    try:
        expression = stop_condition(target)
    except ValueError as exc:
        raise source.error(where, str(exc)) from None
    _check_run_names(expression, blueprint, source, where)


def _generator_texts(
    value: Any, path: Tuple[Any, ...]
) -> Iterator[Tuple[str, Tuple[Any, ...]]]:
    """The `expr` and `when` texts of every generator in a value, and their
    `omit_when`, with where each is written."""
    if isinstance(value, Gen):
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
    of any generator in those places, `vars`, `key` and `attributes`; and the `when` of
    each entry of a signal's `on`. Text that does not parse is left to the checks that
    report it."""

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
    for name, signal in blueprint.signals.items():
        for index, entry in enumerate(signal.on):
            at = ("signals", name, "on", index)
            if entry.when is not None:
                yield Expression(entry.when, names=ANY_NAME), (*at, "when")
            if entry.spawn is not None:
                yield from _parsed(
                    _read_texts(entry.spawn.count, (*at, "spawn", "count"))
                )
            if entry.interrupt is not None and entry.interrupt.where is not None:
                yield (
                    Expression(entry.interrupt.where, names=ANY_NAME),
                    (*at, "interrupt", "where"),
                )
            if entry.resume is not None:
                if entry.resume.where is not None:
                    yield (
                        Expression(entry.resume.where, names=ANY_NAME),
                        (*at, "resume", "where"),
                    )
                if isinstance(entry.resume.limit, str):
                    yield (
                        Expression(entry.resume.limit, names=ANY_NAME),
                        (*at, "resume", "limit"),
                    )
    for index, step in enumerate(blueprint.scenario):
        if step.interrupt is not None and step.interrupt.where is not None:
            yield (
                Expression(step.interrupt.where, names=ANY_NAME),
                ("scenario", index, "interrupt", "where"),
            )
    for stop, at in _stops(blueprint):
        if "condition" in stop:
            yield Expression(stop["condition"], names=ANY_NAME), (*at, "condition")


def _stops(blueprint: Blueprint) -> Iterator[Tuple[Dict[str, Any], Tuple[Any, ...]]]:
    """Every `stop` mapping of the run: `run.stop` and the `stop` of each segment."""
    if blueprint.run.stop:
        yield blueprint.run.stop, ("run", "stop")
    segments = blueprint.run.segments
    for index, segment in enumerate(segments if isinstance(segments, list) else ()):
        if segment.stop:
            yield segment.stop, ("run", "segments", index, "stop")


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


def _reading_expressions(
    blueprint: Blueprint,
) -> Iterator[Tuple[Expression, Tuple[Union[str, int], ...]]]:
    """The expression of every `readings` entry of `telemetry`, parsed, with where it
    is written."""
    for index, entry in enumerate(blueprint.telemetry):
        for name, text in (entry.readings or {}).items():
            yield (
                Expression(text, names=ANY_NAME),
                ("telemetry", index, "readings", name),
            )


def _check_arrival_attributes(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that a `gen: attr` in an arrival's `attributes` names an attribute
    written before it in the same mapping, as the arrival reads them in order. A
    replay's row fields are written first; a replay of every column (`fields: all`)
    is not checked, because its columns are known only when the file is read."""
    for name, arrival in blueprint.arrivals.items():
        written: List[str] = []
        if arrival.replay is not None:
            if arrival.replay.fields == "all":
                continue
            written = [arrival.replay.time, *arrival.replay.fields]
        for key, value in (arrival.attributes or {}).items():
            for where, generator in generators_in(value, (Attr,)):
                if generator.name not in written:
                    raise source.error(
                        ("arrivals", name, "attributes", key, *where),
                        f"gen attr: the entity has no attribute '{generator.name}'"
                        + suggestion(generator.name, written),
                    )
            written.append(key)


def _check_arrival_places(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks the names the run places of an arrival and the value places outside a
    task (a store's `items`, a signal's `on`, a scenario step) read: every `level`,
    `signal`, `users`, `queue`, `putters`, `getters`, `count` and `items` name must
    exist, as in a `stop` condition. The rules of a `queue` and a `preempt` and the
    `readings` of `telemetry` are such places too."""
    _check_arrival_attributes(blueprint, source)
    for expression, where in _arrival_expressions(blueprint):
        _check_run_names(expression, blueprint, source, where)
    for expression, where in _rule_expressions(blueprint):
        _check_run_names(expression, blueprint, source, where)
    for expression, where in _reading_expressions(blueprint):
        _check_run_names(expression, blueprint, source, where)
    for expression, where in _family_expressions(blueprint):
        _check_run_names(expression, blueprint, source, where)
    for location, value in _value_sites(blueprint):
        for expression, where in _parsed_texts(value, location):
            _check_run_names(expression, blueprint, source, where)


def _parsed_texts(
    value: Any, location: Tuple[Any, ...]
) -> Iterator[Tuple[Expression, Tuple[Any, ...]]]:
    """The expressions of the generators in a value, parsed, with where each is
    written. Text that does not parse is left to the checks that report it."""
    yield from _parsed(_generator_texts(value, location))


def _read_texts(
    value: Any, path: Tuple[Any, ...]
) -> Iterator[Tuple[str, Tuple[Any, ...]]]:
    """The text of a place written as an expression, or the texts of the generators
    in a place written as a value, with where each is written."""
    if isinstance(value, str):
        yield value, path
    else:
        yield from _generator_texts(value, path)


def _parsed(
    texts: Iterable[Tuple[str, Tuple[Any, ...]]],
) -> Iterator[Tuple[Expression, Tuple[Any, ...]]]:
    """The expression of each text, with where it is written. Text that does not parse
    is left to the checks that report it."""
    for text, path in texts:
        try:
            yield Expression(text, names=ANY_NAME), path
        except ValueError:
            continue


def _family_expressions(
    blueprint: Blueprint,
) -> Iterator[Tuple[Expression, Tuple[Any, ...]]]:
    """The expressions a family writes as text in `members`, `current_cap`, `max_cap`
    and a container's `level`, parsed, with where each is written. They are read once
    for the run, when each member is made. A generator in `members` is a value place."""
    for section in ("resources", "stores", "containers", "signals"):
        for name, entry in getattr(blueprint, section).items():
            for key in ("members", "current_cap", "max_cap", "level"):
                value = getattr(entry, key, None)
                if isinstance(value, str) and getattr(entry, "family", False):
                    yield from _parsed([(value, (section, name, key))])


def _name_expressions(
    name: str, task: Any
) -> Iterator[Tuple[Expression, Tuple[Union[str, int], ...]]]:
    """The expressions of a task that only the name checks read, parsed, with where each
    is written: the `count` of its `spawn` and of each `spawn` in its `on` entries, and
    the `amount`, `priority` and `filter` of the `get` and `put` branches of its `wait`
    and `open`."""
    where: Tuple[Union[str, int], ...] = ("tasks", name)
    if task.spawn is not None:
        yield from _parsed(_read_texts(task.spawn.count, where + ("spawn", "count")))
    for at_where, entries in task.on_lists():
        for index, entry in enumerate(entries):
            if entry.spawn is not None:
                at = where + (*at_where, index, "spawn", "count")
                yield from _parsed(_read_texts(entry.spawn.count, at))
    branches = []
    if task.wait is not None:
        branches.append(("wait", Wait(task.wait)))
    if task.open is not None:
        branches.append(("open", Open(task.open).wait))
    for key, wait in branches:
        for node in wait.all_nodes:
            if isinstance(node.target, Transfer):
                for _field, expression in transfer_expressions(node.target):
                    yield expression, where + (key,)


def _check_run_names(
    expression: Expression,
    blueprint: Blueprint,
    source: SourceMap,
    where: Tuple[Union[str, int], ...],
) -> None:
    """Checks the signal, store, container and resource names an expression reads
    outside any task, and the `wait.<branch>` names it reads."""
    if where[0] != "tasks":
        _check_wait_reads(expression, blueprint, source, where)
    for signal in sorted(expression.references["signal"]):
        if signal not in blueprint.signals:
            raise source.error(
                where,
                f"signal.{signal} names no signal under signals"
                + (suggestion(signal, blueprint.signals) or "; add it under signals"),
            )
    for level in sorted(expression.references["level"]):
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


def _check_wait_reads(
    expression: Expression,
    blueprint: Blueprint,
    source: SourceMap,
    where: Tuple[Union[str, int], ...],
) -> None:
    """Checks the `wait.<branch>` names an expression outside a task's own keys reads.
    A scenario step's interrupt `where` is read in the scope of each instance of the
    task it interrupts, so it reads a branch of that task's wait. Everywhere else no
    wait is in scope, and a `wait.<branch>` is refused."""
    read = expression.references["wait"]
    if not read:
        return
    if where[0] == "scenario" and where[2:] == ("interrupt", "where"):
        interrupt = blueprint.scenario[int(where[1])].interrupt
        # The model refuses a where without a task
        assert interrupt is not None and interrupt.task is not None
        task = blueprint.tasks[interrupt.task]
        branches = set(Wait(task.wait).branches) if task.wait else set()
        for branch in sorted(read - branches):
            raise source.error(
                where,
                f"wait.{branch} is not a branch of the wait of task "
                f"'{interrupt.task}', which the step interrupts"
                + suggestion(branch, branches),
            )
        return
    for branch in sorted(read):
        raise source.error(
            where,
            f"wait.{branch} is read outside a task's own keys, where no wait is in "
            f"scope; a signal's on entries run with no wait",
        )


def _check_resource_names(
    expression: Expression,
    blueprint: Blueprint,
    source: SourceMap,
    location: Tuple[Union[str, int], ...],
) -> None:
    """Checks that `users.<name>` and `queue.<name>` name a resource."""
    for kind in ("users", "queue", "held", "waiting"):
        for name in sorted(expression.references[kind]):
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
        for name in sorted(expression.references[kind]):
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
    for name in sorted(expression.references["running"]):
        if name not in blueprint.tasks:
            raise source.error(
                location,
                f"running({name}) names no task"
                + (suggestion(name, blueprint.tasks) or "; add it under tasks"),
            )
    for kind in ("count", "items"):
        for name in sorted(expression.references[kind]):
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
    if task.batch is not None:
        if isinstance(task.batch.size, str):
            found.append(
                (Expression(task.batch.size, names=ANY_NAME), where + ("batch", "size"))
            )
        if isinstance(task.batch.timeout, str):
            found.append(
                (
                    Expression(task.batch.timeout, names=ANY_NAME),
                    where + ("batch", "timeout"),
                )
            )
        if task.batch.close_when is not None:
            found.append(
                (
                    Expression(task.batch.close_when, names=ANY_NAME, batch=True),
                    where + ("batch", "close_when"),
                )
            )
    if task.fail is not None:
        found.append((Expression(task.fail, names=ANY_NAME), where + ("fail",)))
    if task.interrupt is not None and task.interrupt.where is not None:
        found.append((Expression(task.interrupt.where), where + ("interrupt", "where")))
    if task.resume is not None:
        if task.resume.where is not None:
            found.append((Expression(task.resume.where), where + ("resume", "where")))
        if isinstance(task.resume.limit, str):
            found.append((Expression(task.resume.limit), where + ("resume", "limit")))
    for at_where, entries in task.on_lists():
        for index, entry in enumerate(entries):
            entry_at = where + (*at_where, index)
            if entry.when is not None:
                found.append((Expression(entry.when), entry_at + ("when",)))
            if entry.fire is not None and isinstance(entry.fire.signal, Ref):
                for expression in entry.fire.signal.name().expressions:
                    found.append((expression, where + at_where))
            if entry.interrupt is not None and entry.interrupt.where is not None:
                found.append(
                    (
                        Expression(entry.interrupt.where),
                        entry_at + ("interrupt", "where"),
                    )
                )
            if entry.resume is not None:
                at = entry_at + ("resume",)
                if entry.resume.where is not None:
                    found.append((Expression(entry.resume.where), at + ("where",)))
                if isinstance(entry.resume.limit, str):
                    found.append((Expression(entry.resume.limit), at + ("limit",)))
    if task.listen is not None:
        if isinstance(task.listen.index, str):
            found.append((Expression(task.listen.index), where + ("listen", "index")))
        if isinstance(task.listen.signal, Ref):
            for expression in task.listen.signal.name().expressions:
                found.append((expression, where + ("listen",)))
    if task.interrupt is not None and isinstance(task.interrupt.index, str):
        found.append((Expression(task.interrupt.index), where + ("interrupt", "index")))
    if isinstance(task.cancel, HandleRef) and isinstance(task.cancel.index, str):
        found.append((Expression(task.cancel.index), where + ("cancel", "index")))
    signal = _name(task.signal)
    for reference in references(task.resources(), signal):
        place = "signal" if reference is signal else "resource"
        for expression in reference.expressions:
            found.append((expression, where + (place,)))
    if task.subscribe is not None:
        for expression in Wait(task.subscribe).reference_expressions:
            found.append((expression, where + ("subscribe",)))
    for field in ("get", "put"):
        step = getattr(task, field)
        if step is not None:
            for expression in step.transfer().reference_expressions:
                found.append((expression, where + (field,)))
    if task.open is not None:
        for delay in Open(task.open).expressions:
            found.append((delay, where + ("open",)))
    if isinstance(task.seize, SeizeSpec):
        for value in (task.seize.priority, task.seize.preempt):
            if isinstance(value, str) and not isinstance(value, bool):
                found.append((Expression(value), where + ("seize",)))
    if isinstance(task.seize, SeizeSpec):
        for reference in references(_name(task.seize.resource)):
            found.extend((e, where + ("seize",)) for e in reference.expressions)
    if isinstance(task.release, Ref):
        found.extend((e, where + ("release",)) for e in task.release.name().expressions)
    elif task.release is not None and not isinstance(task.release, str):
        if task.release.where is not None:
            found.append((Expression(task.release.where), where + ("release", "where")))
        for expression in Release(task.release.action()).expressions:
            if expression is not None and not any(expression is e for e, _ in found):
                found.append((expression, where + ("release",)))
    if task.wait is not None:
        wait = Wait(task.wait)
        for condition in wait.conditions:
            found.append((condition, where + ("wait",)))
        for delay in wait.time_expressions:
            found.append((delay, where + ("wait",)))
        for request in wait.request_expressions:
            found.append((request, where + ("wait",)))
        for read in wait.reference_expressions:
            found.append((read, where + ("wait",)))
        for predicate in wait.predicates:
            found.append((predicate, where + ("wait", "when")))
        for count in wait.group_expressions:
            found.append((count, where + ("wait", "group")))
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


def _whole(value: Any) -> Any:
    """A capacity as a whole number; None, the default, infinity and the text of a
    family's capacity stay."""
    if value is None or isinstance(value, str) or math.isinf(value):
        return value
    return int(value)


def _build_context(
    blueprint: Blueprint, source: SourceMap, preview: Optional[Preview] = None
) -> SimulationContext:
    """Makes the builder calls a Python script would make, in the same order.

    The context does not read the `row` files while it is built, because
    `_check_values` reads them from the blueprint's folder. A Builder call on the
    finished context reads them from the working directory again.
    """
    context = SimulationContext(**blueprint.simulation.model_dump(exclude_unset=True))
    with parts_of(context).tasks.deferred_rows():
        _add_to_context(context, blueprint, source, preview)
    return context


def _add_to_context(
    context: SimulationContext,
    blueprint: Blueprint,
    source: SourceMap,
    preview: Optional[Preview] = None,
) -> None:
    """Makes the builder calls of a blueprint on `context`, in file order."""
    _add_connectors(context, blueprint, source, preview)
    _add_holders(context, blueprint)
    for name, value in blueprint.variables.items():
        context.add_variable(name, value)
    for name, signal in blueprint.signals.items():
        context.add_signal(
            name,
            once=signal.once,
            on=[entry.entry() for entry in signal.on],
            family=signal.family,
            members=signal.members,
            defused=signal.defused,
        )
    # A YAML task's function takes the context after its id, also in a child flow
    parts_of(context).tasks.takes_context = True
    _add_arrivals(context, blueprint, source)
    for name, task in blueprint.tasks.items():
        _add_task(context, name, task, source)
    for _location, value in _value_sites(blueprint):
        context.note_values(None, value)
    for _location, value, _vars, _run in _arrival_sites(blueprint):
        context.note_values(None, value)
    _add_processes(context, blueprint)
    _add_scenario(context, blueprint)


def _add_connectors(
    context: SimulationContext,
    blueprint: Blueprint,
    source: SourceMap,
    preview: Optional[Preview],
) -> None:
    """Adds the ingress and egress connectors, or for a preview the consoles that
    replace them, then the batching and the statistics."""
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
                when=(
                    egress.when
                    if egress.when in (LABELS, TRACE)
                    else _when(egress.when, blueprint)
                ),
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
        kinds = []
        for kind in (LABELS, TRACE):
            if any(egress.when == kind for egress in blueprint.egress):
                kinds.append(kind)
        _add_preview_consoles(context, preview, kinds)

    if blueprint.batching is not None:
        context.with_batching(**blueprint.batching.model_dump(exclude_none=True))
    if blueprint.stats is not None:
        context.with_stats(
            blueprint.stats.level,
            blueprint.stats.interval,
            on_change=blueprint.stats.on_change,
            trace=blueprint.stats.trace,
        )


def _add_holders(context: SimulationContext, blueprint: Blueprint) -> None:
    """Adds the resources with their breakdowns, the stores and the containers."""
    for name, resource in blueprint.resources.items():
        context.add_resource(
            name,
            _whole(resource.current_cap),
            _whole(resource.max_cap),
            queue=_rule_as_mapping(resource.queue),
            preempt=_rule_as_mapping(resource.preempt),
            queue_limit=resource.queue_limit,
            type=resource.type,
            family=resource.family,
            members=resource.members,
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
                clock=resource.breakdown.clock,
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
            family=store.family,
            members=store.members,
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
            family=container.family,
            members=container.members,
        )


def _add_arrivals(
    context: SimulationContext, blueprint: Blueprint, source: SourceMap
) -> None:
    """Adds the services, then each arrival with its plan."""
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
            line = source.line(("arrivals", name, "replay", "path"))
            parts_of(context).arrivals.replay_places[name] = (
                f"{source.path}:{line}: arrivals.{name}.replay.path: {replay.path}"
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
            context.add_arrival(
                name,
                gap=_time_place(arrival.gap),
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
            parts_of(context).arrivals.plans[name] = plan


def _add_task(
    context: SimulationContext, name: str, task: Any, source: SourceMap
) -> None:
    """Declares one task, locates its expressions that read the payload, and notes the
    value places the context cannot see."""
    context.task(
        service_id=task.service,
        resource_id=_resource_names(task.resource),
        keep=task.keep,
        preempt=task.preempt,
        on_interrupt=task.on_interrupt,
        on_interrupt_next=task.on_interrupt_next,
        on_interrupt_run=task.on_interrupt_run,
        interrupt=None if task.interrupt is None else task.interrupt.action(),
        resume=None if task.resume is None else task.resume.action(),
        batch=None if task.batch is None else task.batch.action(),
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
        listen=None if task.listen is None else task.listen.mapping(),
        cancel=_cancel_argument(task.cancel),
        seize=_seize_argument(task.seize),
        release=_release_argument(task.release),
        signal=_name(task.signal),
        fail=task.fail,
        fail_reason=task.fail_reason,
        on_fail=task.on_fail,
        on_renege=task.on_renege,
        on_balk=task.on_balk,
        key_field=task.key_field,
        spawn=(None if task.spawn is None else task.spawn.action()),
        subscribe=task.subscribe,
        on=[entry.entry() for entry in task.on],
    )(_payload(task.payload, task.id_field, task.attributes, task.item_field, name))
    _locate_expressions(parts_of(context).tasks.specs[name], name, source)
    # The places the context cannot see: the payload, the wait and the put
    context.note_values(
        name,
        task.payload if not callable(task.payload) else None,
        task.wait,
        None if task.put is None else task.put.item,
    )


def _cancel_argument(cancel: Any) -> Any:
    """The `cancel` the builder takes: a handle with its index as a mapping, else the
    handle's name."""
    if isinstance(cancel, HandleRef) and cancel.index is not None:
        return cancel.model_dump(exclude_none=True)
    if cancel is not None and not isinstance(cancel, str):
        return cancel.handle
    return cancel


def _seize_argument(seize: Any) -> Any:
    """The `seize` the builder takes: a name, or the mapping without `patience`."""
    if seize is None or isinstance(seize, str):
        return seize
    return seize.model_dump(exclude_none=True, exclude={"patience"})


def _release_argument(release: Any) -> Any:
    """The `release` the builder takes: a name, a reference's mapping, or the action."""
    if release is None or isinstance(release, str):
        return release
    if isinstance(release, Ref):
        return release.name().mapping()
    return release.action()


def _add_processes(context: SimulationContext, blueprint: Blueprint) -> None:
    """Adds the processes, then the telemetry readings and loops."""
    for process in blueprint.processes:
        context.add_process(process.function, **process.kwargs)

    for entry in blueprint.telemetry:
        if entry.readings is not None:
            context.add_readings(entry.interval, entry.readings)
        else:
            assert entry.function is not None  # the model takes one of the two
            context.telemetry_loop(entry.interval)(entry.function)


def _add_scenario(context: SimulationContext, blueprint: Blueprint) -> None:
    """Adds the scenario steps, each with the keys it gives."""
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


def _resource_names(value: Any) -> Any:
    """A task's `resource` as the builder takes it: a name, a computed member as its
    mapping, or a list of them."""
    if isinstance(value, Ref):
        return value.name().mapping()
    if isinstance(value, list):
        return [_resource_names(item) for item in value]
    return value


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


def _locate_expressions(spec: Any, name: str, source: SourceMap) -> None:
    """Gives each expression of the task its place in the file, which its evaluation
    errors name, as a generator's errors do."""
    locate = _locator(source)
    for found in spec.expressions:
        if found.place is not None:
            found.expression.where = locate(("tasks", name, *found.place))
    spec.router.where = locate(("tasks", name, "next", "weights"))


def _value_sites(blueprint: Blueprint) -> Iterator[Tuple[Tuple[Any, ...], Any]]:
    """Every place in a blueprint that takes values, outside the task keys that
    `task_value_problems` reads: with its location."""
    for name, store in blueprint.stores.items():
        yield ("stores", name, "items"), store.items
    for section in ("resources", "stores", "containers", "signals"):
        for name, entry in getattr(blueprint, section).items():
            if getattr(entry, "members", None) is not None:
                yield (section, name, "members"), entry.members
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
    if key.startswith("batch."):
        return None if task.batch is None else getattr(task.batch, key[6:])
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
    registry = scratch_registry(context)
    services = set(blueprint.services)
    base_dir = source.path.parent
    parts = parts_of(context)
    written = written_paths(parts.scenario_steps, parts.tasks, parts.signals)
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
                "resume": task.resume,
                "on": task.on,
                "fail_reason": task.fail_reason,
                "listen": task.listen,
            },
            wait_values={
                **dict(_timeout_values(task.wait, ("wait",))),
                ("patience",): task.patience,
                ("key",): task.key,
                ("batch", "timeout"): None
                if task.batch is None
                else task.batch.timeout,
            },
            service_time=task.service_time,
            services=services,
            base_dir=base_dir,
            registry=registry,
            early_end=_can_end_early(task, blueprint, context, written),
        )
        problems += [(("tasks", name) + where, text) for where, text in found]
    for location, value in _value_sites(blueprint):
        for where, text in check_value(
            value,
            location,
            [],
            Place(
                in_task=False, services=services, base_dir=base_dir, registry=registry
            ),
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
            Place(
                in_task=False,
                in_arrival=True,
                run_scope=run_scope,
                services=services,
                base_dir=base_dir,
                registry=registry,
            ),
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


def _can_end_early(
    task: Any, blueprint: Blueprint, context: SimulationContext, written: Set[str]
) -> bool:
    """Whether an instance of the task can end before its service: by `patience`, a
    `balk`, a `wait`, `on_interrupt: abandon`, or a request for a resource whose
    `queue_limit` is set or written by the model."""
    return (
        task.patience is not None
        or task.balk is not None
        or task.wait is not None
        or task.on_interrupt == "abandon"
        or parts_of(context).holders.ends_early(
            context.sim_id,
            [
                resource
                for resource in (
                    *task.resources(),
                    *task.seized(),
                    *task.open_requests(),
                )
                if resource in blueprint.resources
            ],
            written,
        )
    )


def _task_sites(blueprint: Blueprint) -> Iterator[Tuple[Tuple[Any, ...], Any, str]]:
    """Every value place of a task, with where it is written and the task's name. The
    places are those of `TASK_VALUE_PLACES`."""
    for name, task in blueprint.tasks.items():
        for _key, where, _after, value in task_value_places(
            lambda key: _task_value(task, key)
        ):
            if value is None:
                continue
            if not callable(value) or isinstance(value, (Gen, dict, list)):
                yield ("tasks", name) + where, value, name


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
            for target in (
                task.on_fail,
                task.on_renege,
                task.on_balk,
                task.on_interrupt_next,
            )
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
                        f"task '{owner}' is also reached by on_fail, "
                        "on_renege, on_balk or on_interrupt_next, which hand over the "
                        "failure, the renege, the balk or the interrupted task's "
                        "record and not its payload; upstream "
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
            Place(
                in_task=False,
                in_arrival=True,
                run_scope=True,
                services=services,
                base_dir=base_dir,
                registry=registry,
            ),
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
    if isinstance(timeout, Gen):
        yield path + ("timeout",), timeout
    elif isinstance(timeout, Mapping):
        if isinstance(timeout.get("after"), Gen):
            yield path + ("timeout", "after"), timeout["after"]
        if "value" in timeout:
            yield path + ("timeout", "value"), timeout["value"]
    for key in ("any", "all", "of"):
        branches = wait.get(key)
        if isinstance(branches, Mapping):
            for name, branch in branches.items():
                yield from _timeout_values(branch, path + (key, name))
    if "each" in wait and isinstance(wait.get("branch"), Mapping):
        yield from _timeout_values(wait["branch"], path + ("branch",))


def _names_a_member(path: str, context: SimulationContext) -> bool:
    """Whether a registry path names an attribute of a member of a declared family,
    such as `S.resources.desk[3].current_cap`, which the scratch registry does not
    hold because the member is made when the run starts or at its first use.

    Args:
        path: The registry path.
        context: The context with the declared families.

    Returns:
        bool: True when the path names an attribute of a member of a family.
    """
    parts = parts_of(context)
    return names_a_member(
        str(path),
        parts.families,
        parts.holders,
        context.sim_id,
        parts.tasks,
        parts.signals,
    )


def _unmade_note(path: str, context: SimulationContext) -> str:
    """The end of a refusal of `path`: why its family never makes the member it names,
    or "" when it names no such member.

    Args:
        path: The registry path.
        context: The context with the declared families.

    Returns:
        str: The sentence that follows the refusal, or "".
    """
    parts = parts_of(context)
    return unmade_member_note(
        str(path),
        parts.families,
        parts.holders,
        context.sim_id,
        parts.tasks,
        parts.signals,
    )


def _check_declared_names(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that no name is in two of resources, stores, containers and signals, and
    that none holds a dot or a space, which an expression cannot name, or a character
    that an expression reads as another, such as `µ`, and that no task or arrival has an
    empty name. A name in two sections is refused at its second place."""
    for section, article in (("tasks", "a task"), ("arrivals", "an arrival")):
        if "" in getattr(blueprint, section):
            raise source.error(
                (section, ""),
                f"the name of {article} is at least one character, got ''",
            )
    kinds: Dict[str, str] = {}
    for section, kind in (
        ("resources", "resource"),
        ("stores", "store"),
        ("containers", "container"),
        ("signals", "signal"),
    ):
        for name in getattr(blueprint, section):
            problem = name_problem(kind, name, kinds.get(name))
            if problem is not None:
                raise source.error((section, name), problem)
            kinds[name] = kind
    earlier: List[Dict[str, Any]] = []
    for index, entry in enumerate(blueprint.telemetry):
        for name in entry.readings or {}:
            problem = reading_problem(name, earlier)
            if problem is not None:
                raise source.error(("telemetry", index, "readings", name), problem)
        earlier.append(dict(entry.readings or {}))


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

    registry = scratch_registry(context)
    for index, problem, detail in walk_steps(
        [(step.at, step.path, step.value) for step in blueprint.scenario],
        registry,
        lambda path: _names_a_member(path, context),
    ):
        step = blueprint.scenario[index]
        if problem == "path":
            raise source.error(
                ("scenario", index, "path"),
                f"'{step.path}' is not a registry path. Paths look like "
                f"{context.sim_id}.resources.<name>.current_cap or "
                f"{context.sim_id}.arrival.<name>.rate"
                + _unmade_note(str(step.path), context),
            )
        if problem == "value":
            raise source.error(("scenario", index, "value"), detail)


def _check_on_paths(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Refuses an `on:` entry whose `path` is not a registry path, or whose `value`
    the path does not accept, at the entry's line. An entry on a path the registry
    lacks would end the run with `KeyError` when it first runs. Each list is checked
    against a fresh registry that its earlier literal values have changed, as the
    run applies them in order."""
    owners: List[Tuple[str, str, Tuple[str, ...], List[Any]]] = [
        ("tasks", name, at_where, entries)
        for name, task in blueprint.tasks.items()
        for at_where, entries in task.on_lists()
    ]
    owners += [
        ("signals", name, ("on",), signal.on)
        for name, signal in blueprint.signals.items()
    ]
    for section, name, at_where, entries in owners:
        registry = scratch_registry(context)
        # A generated value is not known before the run, so a later literal on a path
        # of the same parent is not checked against a value the run may have changed
        unknown: Set[str] = set()
        for index, entry in enumerate(entries):
            if entry.path is None or _names_a_member(entry.path, context):
                continue
            where: Tuple[Union[str, int], ...] = (section, name, *at_where, index)
            try:
                registry.get(entry.path)
            except KeyError:
                raise source.error(
                    (*where, "path"),
                    f"'{entry.path}' is not a registry path. Paths look like "
                    f"{context.sim_id}.resources.<name>.current_cap"
                    + _unmade_note(entry.path, context),
                ) from None
            parent = entry.path.rpartition(".")[0]
            if has_generator(entry.value):
                unknown.add(parent)
                continue
            if parent in unknown:
                continue
            try:
                if entry.when is not None:
                    # A conditional entry may not run: its value is checked, and the
                    # entries after it do not count on it
                    registry.check_value(entry.path, entry.value)
                    unknown.add(parent)
                else:
                    registry.change(entry.path, entry.value)
            except ParamRejected as exc:
                raise source.error((*where, "value"), exc.reason) from None


def _check_registry_reads(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Checks every `registry["<path>"]` an expression reads against the registry."""
    registry = scratch_registry(context)
    found = [
        pair
        for name, task in blueprint.tasks.items()
        for pair in _expressions(name, task)
    ]
    found += [
        pair
        for name, task in blueprint.tasks.items()
        for pair in _name_expressions(name, task)
    ]
    found += list(_arrival_expressions(blueprint))
    found += list(_rule_expressions(blueprint))
    found += list(_reading_expressions(blueprint))
    found += list(_family_expressions(blueprint))
    for location, value, _owner in _task_sites(blueprint):
        found += list(_parsed_texts(value, location))
    for location, value in _value_sites(blueprint):
        found += list(_parsed_texts(value, location))
    for expression, location in found:
        for path in sorted(expression.references["registry"]):
            if _names_a_member(path, context):
                continue
            try:
                registry.get(path)
            except KeyError:
                raise source.error(
                    location,
                    f'registry["{path}"] is not a registry path'
                    + _unmade_note(path, context),
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

    With a `limit` it counts the events into the open run's `sample_seen`, ends the run at
    the limit-th, and refuses every record after it, so the events at the same
    instant after the last one are not printed.
    """

    def take(record: dict) -> bool:
        if record.get("path_id") == _LAG_PATH:
            return False
        if limit is None:
            return True
        run = parts_of(context).run
        if run.sample_seen >= limit:
            return False
        if record["stream_type"] == "event":
            run.sample_seen += 1
            if run.sample_seen == limit and run.finish_run is not None:
                run.finish_run()
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
        beside = _files_beside(config, source)
        if isinstance(kind, type):
            return _instantiate(kind, beside, source, location)
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
        return _instantiate(cls, beside, source, location)

    return construct


_SERIALIZER_FILES = ("schema_file", "descriptor_file", "verify", "cert")
_SSL_FILES = ("cafile", "certfile", "keyfile")


def _files_beside(config: Dict[str, Any], source: SourceMap) -> Dict[str, Any]:
    """The connector's config with its file paths read from the blueprint's folder, as
    a `row` file and an `!include` are, not from the working directory.

    The paths are `schema_file`, `descriptor_file`, `verify` and `cert` of a Kafka
    `serializer` and of each `topics.<name>.serializer`, and `cafile`, `certfile` and
    `keyfile` of `ssl`. `topic_serializers` holds serializer objects, which read no
    file.

    Args:
        config: The connector's `config` mapping.
        source: The blueprint file the connector is in.

    Returns:
        Dict[str, Any]: The config, a copy where a path was changed.
    """

    def beside(value: Any) -> Any:
        if isinstance(value, str):
            return str(_relative(value, source))
        if isinstance(value, list):
            # A `cert` of the certificate file, the key file and the key's password
            return [beside(part) for part in value[:2]] + value[2:]
        return value

    def files(spec: Any, keys: Tuple[str, ...]) -> Any:
        if not isinstance(spec, Mapping):
            return spec
        return {key: beside(v) if key in keys else v for key, v in spec.items()}

    def topic(settings: Any) -> Any:
        if not isinstance(settings, Mapping) or "serializer" not in settings:
            return settings
        return {
            **settings,
            "serializer": files(settings["serializer"], _SERIALIZER_FILES),
        }

    made = dict(config)
    if "serializer" in made:
        made["serializer"] = files(made["serializer"], _SERIALIZER_FILES)
    if isinstance(made.get("topics"), Mapping):
        made["topics"] = {name: topic(s) for name, s in made["topics"].items()}
    if "ssl" in made:
        made["ssl"] = files(made["ssl"], _SSL_FILES)
    return made


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
