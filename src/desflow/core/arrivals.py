"""Arrivals that start a fixed number of entities, run one loop for each entity, or
take their gaps from an expression.

A blueprint arrival with `count`, `keys`, `key`, `vars`, `stagger`, `first_at`,
`max_events` or `gap` has an `ArrivalPlan`. `SimulationContext` registers the arrival's
loop with `_spawn_loop`, which runs `source` for an arrival that starts one entity at
each gap, and `start_entities` for one with `count` or `keys`, where each entity has a
loop of its own. An arrival with no plan runs the plain loop in `context.py`.
"""

import numbers
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Union,
)

import simpy

if TYPE_CHECKING:
    from desflow.core.context import SimulationContext

ARRIVALS_STOPPED = "arrivals stopped"


@dataclass
class ArrivalPlan:
    """What an arrival does beyond drawing gaps from its distribution.

    Attributes:
        gap: The time to the next arrival in place of a distribution: a number, an
            `Expression` or a generator, read at the start of each gap.
        first_at: The time of the first arrival, then gaps follow. With `count`, the
            time each entity's first run starts.
        count: How many entities, each with a loop of its own: a whole number, an
            `Expression` or a generator, read once when the arrival starts.
        keys: The entity keys, which also give the number of entities.
        key: The key of each entity, a value read once for each entity.
        vars: Named values read once for each entity, before its attributes and key,
            which read them as bare names.
        stagger: The time between the starts of two entities, read after each start.
        max_events: How many times the arrival starts its task: in all, or for each
            entity when the arrival has several.
    """

    gap: Any = None
    first_at: Optional[Union[int, float]] = None
    count: Any = None
    keys: Optional[List[Union[int, str]]] = None
    key: Any = None
    vars: Dict[str, Any] = field(default_factory=dict)
    stagger: Any = 0
    max_events: Any = None

    @property
    def has_entities(self) -> bool:
        """Whether the arrival runs one loop for each entity."""
        return self.count is not None or self.keys is not None

    @property
    def makes_entities(self) -> bool:
        """Whether an entity needs its vars or key made when it starts."""
        return bool(self.vars) or self.key is not None or self.keys is not None


class EntityLoop:
    """The state of one entity's loop. The entity holds its key while the loop runs
    and while any run it started is still in the model.

    Attributes:
        done: Whether the loop has ended.
        admitted: Whether the entity started, so it holds its key.
        active: How many of its runs have started and not yet left the model.
    """

    __slots__ = ("done", "admitted", "active")

    def __init__(self) -> None:
        self.done = False
        self.admitted = False
        self.active = 0

    @property
    def running(self) -> bool:
        """Whether the entity still holds its key."""
        return not self.done or self.active > 0

    def release(self, context: "SimulationContext", key: Any) -> None:
        """Forgets the values kept for the entity's key once it holds the key no
        longer, so a later entity with the same key starts with none."""
        if self.admitted and not self.running:
            context.values.forget(key)


def _not_left(entity: Any) -> Callable[[], bool]:
    """Tells whether a source's entity is still in the model."""
    return lambda: not entity.left


def _holds_key(state: "EntityLoop") -> Callable[[], bool]:
    """Tells whether an entity's loop, or a run it started, is still going."""
    return lambda: state.running


def whole(value: Any, what: str) -> int:
    """A count place's result: a whole number, a boolean counting as 1, as `range`
    takes it.

    Args:
        value: The result of the place.
        what: Where it was written, for the message.

    Raises:
        TypeError: If it is not a whole number, as `range(2.0)` raises.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, numbers.Integral):
        return int(value)
    raise TypeError(f"{what} gave {value!r}, which is not a whole number")


def defuse_stopped(event: Any) -> None:
    """Marks an arrival loop ended by `stop_arrivals` as handled, and nothing else."""
    value = getattr(event, "_value", None)
    if (
        not event.ok
        and isinstance(value, simpy.Interrupt)
        and value.cause == ARRIVALS_STOPPED
    ):
        event.defused = True


def _limit(
    context: "SimulationContext", arrival: str, plan: ArrivalPlan
) -> Optional[int]:
    """How many times the arrival, or an entity, starts its task: None for no limit.
    A result of 0 or less starts none, as `range` gives none."""
    if plan.max_events is None:
        return None
    count = whole(
        context._arrival_value(plan.max_events), f"arrival '{arrival}' max_events"
    )
    return max(count, 0)


def source(
    context: "SimulationContext", arrival: str, task: str, plan: ArrivalPlan
) -> Generator[Any, Any, None]:
    """An arrival that starts a new entity at each gap, up to `max_events`.

    The loop ends in the step of the last start and draws no further gap. A start
    whose key a running entity of the arrival has is skipped and counts as one.
    """
    limit = _limit(context, arrival, plan)
    attempts = 0
    while limit is None or attempts < limit:
        entity = yield context.wait_for_arrival(arrival)
        attempts += 1
        if context._admit(arrival, entity, _not_left(entity)):
            context._start(task, entity, None)


def start_entities(
    context: "SimulationContext", arrival: str, task: str, plan: ArrivalPlan
) -> Generator[Any, Any, None]:
    """Starts the loops of an arrival with `count` or `keys`, `stagger` apart.

    Each loop is an arrival process of its own, so a drain stops them with the rest.
    """
    if plan.keys is not None:
        number = len(plan.keys)
    else:
        number = max(
            whole(context._arrival_value(plan.count), f"arrival '{arrival}' count"), 0
        )
    for index in range(number):
        process = context.env.process(entity_loop(context, arrival, task, plan, index))
        process.callbacks.append(defuse_stopped)
        context._arrival_processes.append(process)
        if index < number - 1:
            apart = context._arrival_seconds(
                plan.stagger, f"arrival '{arrival}' stagger"
            )
            if apart > 0:
                yield context.env.timeout(apart)


def entity_loop(
    context: "SimulationContext",
    arrival: str,
    task: str,
    plan: ArrivalPlan,
    index: int,
) -> Generator[Any, Any, None]:
    """One entity of an arrival with several: it starts `task` after each gap, up to
    `max_events` times, and keeps its attributes between the runs.

    The first run starts at `first_at` when the plan has one, otherwise after a gap.
    Each run is a flow of its own, with the entity's key and attributes.
    """
    env = context.env
    entity = context._new_entity(arrival, context._arrival_entities.get(arrival), False)
    state = EntityLoop()
    entity.loop = state
    limit = _limit(context, arrival, plan)
    try:
        started = 0
        while limit is None or started < limit:
            if started == 0 and plan.first_at is not None:
                delay: Union[int, float] = max(plan.first_at - env.now, 0)
            else:
                delay = context._gap(arrival)
            yield env.timeout(delay)
            if started == 0:
                context._make_entity(entity, index=index)
                if not context._admit(arrival, entity, _holds_key(state)):
                    return
                state.admitted = True
            context._start(task, context._visit(entity), None)
            started += 1
    finally:
        state.done = True
        state.release(context, entity.key)
