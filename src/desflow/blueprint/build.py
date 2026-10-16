"""Turns a blueprint file into a `SimulationContext`.

Everything is built through the public builder methods, so a blueprint and a Python
script that make the same calls produce the same simulation.
"""

import importlib
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

import simpy
from pydantic import ValidationError

from desflow.blueprint.loader import BlueprintError, SourceMap, read_document
from desflow.blueprint.models import Blueprint, Run
from desflow.config import LABELS
from desflow.core.expressions import Expression
from desflow.core.context import SimulationContext
from desflow.core.registry import SimulationRegistry

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
    "Kafka": ("desflow.connectors.ingress.kafka", "KafkaIngress", "kafka"),
    "Postgres": (
        "desflow.connectors.ingress.postgres",
        "PostgresIngress",
        "postgres",
    ),
    "Redis": ("desflow.connectors.ingress.redis", "RedisIngress", "redis"),
}


def build(path: Union[str, Path]) -> Tuple[SimulationContext, Run]:
    """Reads, validates and builds a blueprint file.

    Args:
        path: The YAML file.

    Returns:
        The built context, and the `run` section for whoever starts it.

    Raises:
        BlueprintError: If the file is invalid. The message names the file and line.
    """
    data, source = read_document(path)
    try:
        # One `now` for the whole file, so relative times agree with each other.
        blueprint = Blueprint.model_validate(data, context={"now": datetime.now()})
    except ValidationError as exc:
        raise _validation_error(exc, source) from None

    _check_references(blueprint, source)
    context = _build_context(blueprint, source)
    _check_scenario(blueprint, context, source)
    _check_registry_reads(blueprint, context, source)
    return context, blueprint.run


def _validation_error(exc: ValidationError, source: SourceMap) -> BlueprintError:
    """Lists every validation error, each with its line."""
    messages = []
    for error in exc.errors():
        # A task's `next` is a name or rules, and pydantic adds which one to the
        # location; the file has no such key, so it is left out.
        location = tuple(
            part
            for index, part in enumerate(error["loc"])
            if not (
                index
                and error["loc"][index - 1] == "next"
                and part in ("name", "rules")
            )
        )
        where = ".".join(str(part) for part in location) or "top level"
        messages.append(str(source.error(location, f"{where}: {error['msg']}")))
    return BlueprintError("\n".join(messages))


def _check_references(blueprint: Blueprint, source: SourceMap) -> None:
    """Checks that every name one section uses is defined in another."""
    for name in set(blueprint.stores) & set(blueprint.containers):
        raise source.error(
            ("stores", name),
            f"'{name}' is both a store and a container; level.{name} needs one",
        )

    for name, task in blueprint.tasks.items():
        if task.service is not None and task.service not in blueprint.services:
            raise source.error(
                ("tasks", name, "service"),
                f"task '{name}' uses service '{task.service}', which is not "
                f"defined under services",
            )
        if task.resource is not None and task.resource not in blueprint.resources:
            raise source.error(
                ("tasks", name, "resource"),
                f"task '{name}' uses resource '{task.resource}', which is not "
                f"defined under resources",
            )
        if (
            task.priority is not None
            and blueprint.resources[task.resource or ""].queue != "priority"
        ):
            raise source.error(
                ("tasks", name, "priority"),
                f"task '{name}' has a priority, but resource '{task.resource}' "
                f"does not set queue: priority",
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
                    f"task '{name}' uses {kind} '{target}', which is not defined "
                    f"under {kind}s",
                )
        for target, location in _routes(name, task):
            if target not in blueprint.tasks:
                raise source.error(
                    location,
                    f"task '{name}' routes to '{target}', which is not defined "
                    f"under tasks",
                )
        for expression, location in _expressions(name, task):
            for level in expression.references["level"]:
                if level not in blueprint.stores and level not in blueprint.containers:
                    raise source.error(
                        location,
                        f"level.{level} names no store or container",
                    )

    for kind in ("resources", "stores"):
        for name, capacity in getattr(blueprint, kind).items():
            for field in ("current_cap", "max_cap"):
                if not float(getattr(capacity, field)).is_integer():
                    raise source.error(
                        (kind, name, field),
                        f"{kind[:-1]} '{name}' needs a whole number for {field}",
                    )

    for name, arrival in blueprint.arrivals.items():
        if arrival.spawn is not None and arrival.spawn not in blueprint.tasks:
            raise source.error(
                ("arrivals", name, "spawn"),
                f"arrival '{name}' spawns task '{arrival.spawn}', which is not "
                f"defined under tasks",
            )

    go_live_at = blueprint.simulation.go_live_at
    start = blueprint.simulation.logical_start_time
    # The run starts at datetime.now(), which is naive, when no start time is given
    if go_live_at is not None and (go_live_at.tzinfo is None) != (
        start is None or start.tzinfo is None
    ):
        raise source.error(
            ("simulation", "go_live_at"),
            "go_live_at and logical_start_time must both have a time zone or both "
            "have none, and a run with no logical_start_time starts without one",
        )

    for index, egress in enumerate(blueprint.egress):
        if (
            isinstance(egress.when, str)
            and egress.when != LABELS
            and blueprint.simulation.go_live_at is None
        ):
            raise source.error(
                ("egress", index, "when"),
                f"when: {egress.when} needs simulation.go_live_at",
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
    for field in ("get", "put"):
        step = getattr(task, field)
        if step is not None and isinstance(step.amount, str):
            found.append((Expression(step.amount), where + (field, "amount")))
    if task.next is not None and not isinstance(task.next, str):
        for index, rule in enumerate(task.next.when):
            found.append(
                (
                    Expression(rule.condition),
                    where + ("next", "when", index, "condition"),
                )
            )
    return found


def _build_context(blueprint: Blueprint, source: SourceMap) -> SimulationContext:
    """Makes the builder calls a Python script would make, in the same order."""
    context = SimulationContext(**blueprint.simulation.model_dump(exclude_unset=True))

    for index, ingress in enumerate(blueprint.ingress):
        provider = _connector(ingress.type, ingress.config, INGRESS_TYPES)
        context.add_ingress(provider(source, ("ingress", index)))

    for index, egress in enumerate(blueprint.egress):
        provider = _connector(egress.type, egress.config, EGRESS_TYPES)
        context.add_egress(
            provider(source, ("egress", index)),
            when=LABELS if egress.when == LABELS else _when(egress.when, blueprint),
            batch_size=egress.batch_size,
            flush_interval=egress.flush_interval,
        )

    if blueprint.batching is not None:
        context.with_batching(**blueprint.batching.model_dump(exclude_none=True))
    if blueprint.stats is not None:
        context.with_stats(blueprint.stats.level, blueprint.stats.interval)

    for name, resource in blueprint.resources.items():
        context.add_resource(
            name, int(resource.current_cap), int(resource.max_cap), queue=resource.queue
        )
    for name, store in blueprint.stores.items():
        context.add_store(name, int(store.current_cap), int(store.max_cap))
    for name, container in blueprint.containers.items():
        context.add_container(
            name, container.current_cap, container.max_cap, level=container.level
        )
    for name, value in blueprint.variables.items():
        context.add_variable(name, value)
    for name, dist in blueprint.services.items():
        context.add_service(name, dist.dist, **dist.parameters())
    for name, arrival in blueprint.arrivals.items():
        context.add_arrival(
            name, arrival.dist, attributes=arrival.attributes, **arrival.parameters()
        )

    tasks = {
        name: context.task(
            service_id=task.service,
            resource_id=task.resource,
            name=name,
            priority=task.priority,
            get=None if task.get is None else task.get.transfer(),
            put=None if task.put is None else task.put.transfer(),
            next=task.next_rules(),
            set=task.set,
        )(_payload(task.payload, task.id_field, task.attributes))
        for name, task in blueprint.tasks.items()
    }

    for name, arrival in blueprint.arrivals.items():
        if arrival.spawn is not None:
            context.arrival_loop(name)(
                _spawn_loop(name, arrival.spawn, tasks[arrival.spawn])
            )

    for process in blueprint.processes:
        context.add_process(process.function, **process.kwargs)

    for entry in blueprint.telemetry:
        context.telemetry_loop(entry.interval)(entry.function)

    if blueprint.scenario:
        steps = sorted(
            (
                (step.at, step.path, step.value, step.record)
                for step in blueprint.scenario
            ),
            key=lambda step: step[0],
        )
        context.add_process(_run_scenario, steps=steps)

    return context


def _check_scenario(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Checks every scenario path against the registry the run will build.

    The parameters are registered into a scratch registry with the same code the run
    uses, so a path is accepted exactly when `registry.update` would find it. Without
    this a misspelt path is only logged as a warning, at the moment it is due.
    """
    if not blueprint.scenario:
        return

    registry = SimulationRegistry(simpy.Environment())
    registry.register_sim_parameter(context.compile_parameters())

    for index, step in enumerate(blueprint.scenario):
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
        if current is not None and not isinstance(step.value, type(current)):
            try:
                type(current)(step.value)
            except (TypeError, ValueError):
                raise source.error(
                    ("scenario", index, "value"),
                    f"'{step.path}' holds a {type(current).__name__}, and "
                    f"{step.value!r} cannot be converted to one",
                ) from None


def _check_registry_reads(
    blueprint: Blueprint, context: SimulationContext, source: SourceMap
) -> None:
    """Checks every `registry["<path>"]` an expression reads against the registry."""
    registry = SimulationRegistry(simpy.Environment())
    registry.register_sim_parameter(context.compile_parameters())
    for name, task in blueprint.tasks.items():
        for expression, location in _expressions(name, task):
            for path in expression.references["registry"]:
                try:
                    registry.get(path)
                except KeyError:
                    raise source.error(
                        location, f'registry["{path}"] is not a registry path'
                    ) from None


def _run_scenario(
    context: SimulationContext,
    steps: List[Tuple[float, Optional[str], Any, Optional[Dict[str, Any]]]],
) -> Generator:
    """Applies each scenario step at its simulation time.

    It waits on the simulation clock, so a scenario repeats exactly and works at
    `factor=0`, where `LocalIngress`, which waits on the wall clock, would not. A
    step with a `record` publishes a ground-truth record of the change.
    """
    registry = context.env.registry
    for at, path, value, record in steps:
        if at > context.env.now:
            yield context.env.timeout(at - context.env.now)
        old = new = None
        if path is not None:
            old = registry.get(path).value
            registry.update(path, value)
            new = registry.get(path).value
        if record is not None:
            context.env.publish_label(record, path=path, old=old, new=new)


def _when(
    when: Union[str, Callable[[dict], bool], None], blueprint: Blueprint
) -> Optional[Callable[[dict], bool]]:
    """The egress predicate: a `!python` function, or one for `history` or `live`.

    Records carry their logical time as an ISO string in one layout, so comparing the
    text compares the times. The go-live instant is formatted in the same layout and,
    when both times carry a time zone, in the zone of `logical_start_time`.
    """
    if not isinstance(when, str):
        return when

    go_live_at = blueprint.simulation.go_live_at
    start = blueprint.simulation.logical_start_time
    assert go_live_at is not None  # checked in _check_references
    if go_live_at.tzinfo is not None and start is not None and start.tzinfo:
        go_live_at = go_live_at.astimezone(start.tzinfo)
    go_live_iso = go_live_at.isoformat(timespec="milliseconds")

    if when == "history":

        def is_history(record: dict) -> bool:
            return record["timestamp"] < go_live_iso

        return is_history

    def is_live(record: dict) -> bool:
        return record["timestamp"] >= go_live_iso

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
        if kind not in types:
            raise source.error(
                location + ("type",),
                f"unknown connector type '{kind}'. Use one of {', '.join(types)}",
            )
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


def _instantiate(
    cls: type,
    config: Dict[str, Any],
    source: SourceMap,
    location: Tuple[Union[str, int], ...],
) -> Any:
    try:
        return cls(**config)
    except (TypeError, ValueError) as exc:
        raise source.error(
            location + ("config",), f"{cls.__name__} rejected its config: {exc}"
        ) from None


def _payload(
    payload: Union[Dict[str, Any], Callable[..., Any]],
    id_field: Union[str, None],
    attributes: List[str],
) -> Callable:
    """The task body: a `!python` payload function, or a copy of a mapping payload.

    A mapping payload gains the task id under `id_field`, and the entity attributes
    `attributes` names, each under its own name.
    """
    if callable(payload):
        return payload

    def body(task_id: int, context: SimulationContext, entity: Any) -> Dict[str, Any]:
        event = dict(payload)
        if id_field is not None:
            event[id_field] = task_id
        for name in attributes:
            event[name] = entity.attributes.get(name)
        return event

    return body


def _spawn_loop(arrival: str, name: str, task: Callable) -> Callable:
    """The arrival loop a builder example writes by hand.

    The instance id comes from the task's one counter on the context, which `next`
    and every other arrival that spawns the task share.
    """

    def loop(context: SimulationContext):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(name), context, entity=entity))

    return loop
