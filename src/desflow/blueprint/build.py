"""Turns a blueprint file into a `SimulationContext`.

Everything is built through the public builder methods, so a blueprint and a Python
script that make the same calls produce the same simulation.
"""

import importlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import simpy
from pydantic import ValidationError

from desflow.blueprint.loader import BlueprintError, SourceMap, read_document
from desflow.blueprint.models import Blueprint, Run
from desflow.config import LABELS
from desflow.core.expressions import Expression
from desflow.core.flow import Wait
from desflow.core.task import check_acquisition_order
from desflow.core.context import SimulationContext
from desflow.core.environment import in_zone, resolve_zone
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
        # One `now` for the whole file, so relative times agree with each other;
        # an instant, read in the run's zone
        now = datetime.now(timezone.utc)
        blueprint = Blueprint.model_validate(data, context={"now": now})
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
        for resource in task.resources():
            if resource not in blueprint.resources:
                raise source.error(
                    ("tasks", name, "resource"),
                    f"task '{name}' uses resource '{resource}', which is not "
                    f"defined under resources",
                )
            if (
                task.priority is not None
                and blueprint.resources[resource].queue != "priority"
            ):
                raise source.error(
                    ("tasks", name, "priority"),
                    f"task '{name}' has a priority, but resource '{resource}' "
                    f"does not set queue: priority",
                )
        for key, target in (
            ("on_interrupt_next", task.on_interrupt_next),
            ("interrupt", task.interrupt.task if task.interrupt else None),
        ):
            if target is not None and target not in blueprint.tasks:
                raise source.error(
                    ("tasks", name, key),
                    f"task '{name}' names task '{target}', which is not defined "
                    f"under tasks",
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
            if step.filter is not None and blueprint.stores[target].priority:
                raise source.error(
                    ("tasks", name, field, "filter"),
                    f"store '{target}' serves by priority, and SimPy has no store "
                    f"that both ranks and filters its items",
                )
        for target, location in _routes(name, task):
            if target not in blueprint.tasks:
                raise source.error(
                    location,
                    f"task '{name}' routes to '{target}', which is not defined "
                    f"under tasks",
                )
        wait = None if task.wait is None else Wait(task.wait)
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
        ]
        for kind, key, target in named:
            defined = blueprint.tasks if kind == "task" else blueprint.signals
            if target is not None and target not in defined:
                raise source.error(
                    ("tasks", name, key),
                    f"task '{name}' names {kind} '{target}', which is not defined "
                    f"under {kind}s",
                )
        branches = set(wait.branches) if wait else set()
        takes_item = task.get is not None and task.get.store is not None
        for expression, location in _expressions(name, task):
            for signal in expression.references["signal"]:
                if signal not in blueprint.signals:
                    raise source.error(
                        location, f"signal.{signal} names no signal under signals"
                    )
            for branch in expression.references["wait"]:
                # balk and patience are read before the wait starts
                if branch not in branches or location[2] in (
                    "wait",
                    "balk",
                    "patience",
                ):
                    raise source.error(
                        location,
                        f"wait.{branch} is not a branch of this task's wait, read "
                        f"after it",
                    )
            # item.<field> reads what a store get took: a get's own filter, then
            # the put and next after it
            reads_item = location[2] in ("put", "next", "fail") or location[2:4] == (
                "get",
                "filter",
            )
            if expression.references["item"] and not (takes_item and reads_item):
                raise source.error(
                    location,
                    "item.<field> reads the item a store get took, so it is "
                    "read in the get's filter, the put and next of a task with one",
                )
            for level in expression.references["level"]:
                if level not in blueprint.stores and level not in blueprint.containers:
                    raise source.error(
                        location,
                        f"level.{level} names no store or container",
                    )

    _check_run(blueprint, source)

    try:
        check_acquisition_order(
            {name: task.resources() for name, task in blueprint.tasks.items()},
            {
                name: (task.keep, {target for target, _ in _routes(name, task)})
                for name, task in blueprint.tasks.items()
            },
        )
    except ValueError as exc:
        raise source.error(("tasks",), str(exc)) from None

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

    stop = blueprint.run.stop or {}
    if "signal" in stop and stop["signal"] not in blueprint.signals:
        raise source.error(
            ("run", "stop"),
            f"run.stop names signal '{stop['signal']}', which is not defined under "
            f"signals",
        )

    for index, step in enumerate(blueprint.scenario):
        if step.interrupt is not None and step.interrupt.task not in blueprint.tasks:
            raise source.error(
                ("scenario", index, "interrupt"),
                f"the step interrupts task '{step.interrupt.task}', which is not "
                f"defined under tasks",
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
    if not run.stop:
        return
    ((kind, target),) = run.stop.items()
    where = ("run", "stop", kind)
    if kind == "signal":
        if target not in blueprint.signals:
            raise source.error(
                where,
                f"stop names signal '{target}', which is not defined under signals",
            )
        return
    try:
        expression = Expression(target)
    except ValueError as exc:
        raise source.error(where, str(exc)) from None
    for signal in expression.references["signal"]:
        if signal not in blueprint.signals:
            raise source.error(where, f"signal.{signal} names no signal under signals")
    for level in expression.references["level"]:
        if level not in blueprint.stores and level not in blueprint.containers:
            raise source.error(where, f"level.{level} names no store or container")
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
    if isinstance(task.patience, str):
        found.append((Expression(task.patience), where + ("patience",)))
    if task.balk is not None:
        found.append((Expression(task.balk), where + ("balk",)))
    if task.fail is not None:
        found.append((Expression(task.fail), where + ("fail",)))
    if task.interrupt is not None and task.interrupt.where is not None:
        found.append((Expression(task.interrupt.where), where + ("interrupt", "where")))
    if task.wait is not None:
        wait = Wait(task.wait)
        for branch, (kind, target) in wait.branches.items():
            if kind == "condition":
                found.append((target, where + ("wait",)))
    for field in ("get", "put"):
        step = getattr(task, field)
        if step is None:
            continue
        if isinstance(step.amount, str):
            found.append((Expression(step.amount), where + (field, "amount")))
        if isinstance(step.priority, str):
            found.append((Expression(step.priority), where + (field, "priority")))
        if step.filter is not None:
            found.append((Expression(step.filter), where + (field, "filter")))
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
            name,
            int(resource.current_cap),
            int(resource.max_cap),
            queue=resource.queue,
            preempt=resource.preempt,
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
            int(store.current_cap),
            int(store.max_cap),
            priority=store.priority,
            items=store.items,
        )
    for name, container in blueprint.containers.items():
        context.add_container(
            name, container.current_cap, container.max_cap, level=container.level
        )
    for name, value in blueprint.variables.items():
        context.add_variable(name, value)
    for name in blueprint.signals:
        context.add_signal(name)
    # A YAML task's function takes the context after its id, also in a child flow
    context._task_args = (context,)
    for name, dist in blueprint.services.items():
        context.add_service(name, dist.dist, **dist.parameters())
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
            )
            continue
        assert arrival.dist is not None  # the model needs dist or replay
        context.add_arrival(
            name, arrival.dist, attributes=arrival.attributes, **arrival.parameters()
        )

    tasks = {
        name: context.task(
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
            balk=task.balk,
            get=None if task.get is None else task.get.transfer(),
            put=None if task.put is None else task.put.transfer(),
            next=task.next_rules(),
            set=task.set,
            wait=task.wait,
            signal=task.signal,
            fail=task.fail,
            fail_reason=task.fail_reason,
            on_fail=task.on_fail,
        )(_payload(task.payload, task.id_field, task.attributes, task.item_field))
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


def _relative(path: str, source: SourceMap) -> Path:
    """A path in the blueprint, read relative to the blueprint's folder."""
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return Path(source.path).parent / candidate


def _when(
    when: Union[str, Callable[[dict], bool], None], blueprint: Blueprint
) -> Optional[Callable[[dict], bool]]:
    """The egress predicate: a `!python` function, or one for `history` or `live`.

    Each record's timestamp is read as a time and compared with the go-live
    instant, so the offset of each, which a daylight-saving change can alter during
    a run, is taken into account. A go-live instant without an offset is read in
    the run's zone, as the environment reads it.
    """
    if not isinstance(when, str):
        return when

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
    item_field: Union[str, None] = None,
) -> Callable:
    """The task body: a `!python` payload function, or a copy of a mapping payload.

    A mapping payload gains the task id under `id_field`, the item its store get
    took under `item_field`, and the entity attributes `attributes` names, each
    under its own name.
    """
    if callable(payload):
        return payload

    def body(
        task_id: int, context: SimulationContext, entity: Any, item: Any = None
    ) -> Dict[str, Any]:
        event = dict(payload)
        if id_field is not None:
            event[id_field] = task_id
        if item_field is not None:
            event[item_field] = item
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
