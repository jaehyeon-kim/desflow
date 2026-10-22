"""Turns a blueprint file into a `SimulationContext`.

Everything is built through the public builder methods, so a blueprint and a Python
script that make the same calls produce the same simulation.
"""

import importlib
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import simpy
from pydantic import ValidationError

from desflow.blueprint.loader import BlueprintError, SourceMap, read_document
from desflow.blueprint.models import Blueprint, Run, unknown_key
from desflow.config import LABELS
from desflow.core.expressions import Expression
from desflow.core.flow import Wait
from desflow.core.task import check_acquisition_order
from desflow.core.context import SimulationContext
from desflow.core.environment import in_zone, resolve_zone
from desflow.core.registry import SimulationRegistry
from desflow.utils import suggestion

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
        text = error["msg"]
        if error["type"] == "extra_forbidden":
            text = unknown_key(Blueprint, tuple(error["loc"]))
        messages.append(str(source.error(location, f"{where}: {text}")))
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
            if (
                task.priority is not None
                and blueprint.resources[resource].queue != "priority"
            ):
                raise source.error(
                    ("tasks", name, "priority"),
                    f"task '{name}' has a priority, but resource '{resource}' "
                    f"does not set queue: priority; set it, or remove the priority",
                )
        for key, target in (
            ("on_interrupt_next", task.on_interrupt_next),
            ("interrupt", task.interrupt.task if task.interrupt else None),
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
                    f"task '{name}' names {kind} '{target}', "
                    + _undefined(target, defined, f"{kind}s"),
                )
        branches = set(wait.branches) if wait else set()
        takes_item = task.get is not None and task.get.store is not None
        for expression, location in _expressions(name, task):
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
                        f"after it; read it in the task's payload, put, next or "
                        f"fail, after the wait" + suggestion(branch, branches),
                    )
            # item.<field> reads what a store get took: a get's own filter, then
            # the put and next after it
            # A get with wait: false gives the task no item, so only its own filter
            # reads one
            reads_item = (
                location[2] in ("put", "next", "fail")
                and takes_item
                and task.get is not None
                and task.get.wait
            ) or location[2:4] == ("get", "filter")
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

    _check_events(blueprint, source)

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
        if step.interrupt is not None and step.interrupt.task not in blueprint.tasks:
            raise source.error(
                ("scenario", index, "interrupt"),
                f"the step interrupts task '{step.interrupt.task}', "
                + _undefined(step.interrupt.task, blueprint.tasks, "tasks"),
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

    # The tasks an entity can start a flow from: an arrival's task, or one a spawn
    # starts. Each spawn begins a new entity, so a spawn target is a start of its own.
    starts = {a.spawn for a in blueprint.arrivals.values() if a.spawn is not None}
    for task in tasks.values():
        if task.spawn is not None:
            starts.add(task.spawn.task)
        starts.update(e.spawn.task for e in task.on if e.spawn is not None)
    for signal in signals.values():
        starts.update(e.spawn.task for e in signal.on if e.spawn is not None)

    def reach_from(start: str) -> tuple:
        """The tasks reachable on one entity from `start`, and whether a reached task
        has a `!python` payload, which can start any task with `context.call`."""
        seen: set = set()
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
            if task.subscribe is not None:
                frontier.extend(Wait(task.subscribe).targets("task"))
        return seen, unknown

    reach = {start: reach_from(start) for start in starts}

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
            if entry.interrupt is not None and entry.interrupt.task not in tasks:
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
            if entry.interrupt is not None and entry.interrupt.task not in tasks:
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
                f"stop names signal '{target}', "
                + _undefined(target, blueprint.signals, "signals"),
            )
        return
    try:
        expression = Expression(target)
    except ValueError as exc:
        raise source.error(where, str(exc)) from None
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
    _check_resource_names(expression, blueprint, source, where)
    _check_store_names(expression, blueprint, source, where)
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


def _check_resource_names(
    expression: Expression,
    blueprint: Blueprint,
    source: SourceMap,
    location: Tuple[Union[str, int], ...],
) -> None:
    """Checks that `users.<name>` and `queue.<name>` name a resource."""
    for kind in ("users", "queue"):
        for name in expression.references[kind]:
            if name not in blueprint.resources:
                raise source.error(
                    location,
                    f"{kind}.{name} names no resource"
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
    for kind in ("putters", "getters"):
        for name in expression.references[kind]:
            if name not in holders:
                raise source.error(
                    location,
                    f"{kind}.{name} names no store or container"
                    + (
                        suggestion(name, holders)
                        or "; add it under stores or containers"
                    ),
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
        for condition in wait.conditions:
            found.append((condition, where + ("wait",)))
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


def _whole(value: Optional[float]) -> Optional[float]:
    """A capacity as a whole number; None, the default, and infinity stay."""
    return value if value is None or math.isinf(value) else int(value)


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
            _whole(resource.current_cap),
            _whole(resource.max_cap),
            queue=resource.queue,
            preempt=resource.preempt,
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
        )
    for name, container in blueprint.containers.items():
        context.add_container(
            name,
            container.current_cap,
            container.max_cap,
            level=container.level,
            type=container.type,
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
            spawn=(
                None
                if task.spawn is None
                else {"task": task.spawn.task, "after": task.spawn.after}
            ),
            subscribe=task.subscribe,
            on=[entry.entry() for entry in task.on],
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

    def make(task_id: int, entity: Any, item: Any = None) -> Dict[str, Any]:
        event = dict(payload)
        if id_field is not None:
            event[id_field] = task_id
        if item_field is not None:
            event[item_field] = item
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
