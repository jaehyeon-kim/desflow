"""Arrivals that start a fixed number of entities, run one loop for each entity, or
take their gaps from an expression.

A blueprint arrival with `count`, `keys`, `key`, `vars`, `stagger`, `first_at`,
`max_events` or `gap` has an `ArrivalPlan`. `Arrivals` holds what a model declares,
and each run's `ArrivalRun` holds what the run keeps. The context starts
`ArrivalRun.loop` for each arrival with `spawn`. The loop runs `source` for an arrival
that starts one entity at each gap, `start_entities` for one with `count` or `keys`,
and a plain loop for an arrival with no plan.
"""

import collections.abc
import functools
import logging
import numbers

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Generator,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import simpy

from desflow.models.distributions import TIME_LIST_PARAMETERS, TIME_PARAMETERS
from desflow.core.environment import trigger
from desflow.core.expressions import Expression
from desflow.core.families import read_place, whole
from desflow.core.replay import UNITS, Row, check_fields
from desflow.models.flow import Entity
from desflow.models.params import DistributionConfig
from desflow.utils import _seconds, checked_seconds, suggestion
from desflow.values.models import Gen, has_generator
from desflow.values.runtime import RunStopped

if TYPE_CHECKING:
    from desflow.core.environment import DynamicRealtimeEnvironment
    from desflow.core.sampler import Sampler
    from desflow.values.runtime import ValueRuntime

# The messages of the arrival methods keep the logger they had in the context, so a
# filter or a level set on `desflow.builder.context` still reaches them
logger = logging.getLogger("desflow.builder.context")

ARRIVALS_STOPPED = "arrivals stopped"

# Binds the generators of a place at the place a blueprint gives it, as the context's
# `_bind_values` does: the value, the place, and whether to replace a binding
Bind = Callable[..., None]


def _fresh(value: Any) -> Any:
    """A new copy of each list, mapping, set and tuple in `value`, at any depth.

    Each entity starts with containers of its own, so one entity's change to a list
    attribute is not seen by the next. Any other object, such as a lock or a shared
    counter a Builder model passes, stays the same object for every entity.
    """
    if isinstance(value, dict):
        return {key: _fresh(part) for key, part in value.items()}
    if isinstance(value, list):
        return [_fresh(part) for part in value]
    if isinstance(value, set):
        return {_fresh(part) for part in value}
    if isinstance(value, tuple):
        return tuple(_fresh(part) for part in value)
    return value


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
        group: How many entities one arrival starts: a whole number, an `Expression`
            or a generator, read once at each arrival, after the gap.
        active: Whether the source starts on.
        off_mode: `drop`, where the clock runs while the source is off and an arrival
            then starts nobody, or `restart`, where turning it off drops the pending
            arrival and turning it on starts a full new gap.
        initial: How many entities are created when the run is built.
        sourceless: True for an arrival with `initial` alone, which draws no gaps.
    """

    gap: Any = None
    first_at: Optional[Union[int, float]] = None
    count: Any = None
    keys: Optional[List[Union[int, str]]] = None
    key: Any = None
    vars: Dict[str, Any] = field(default_factory=dict)
    stagger: Any = 0
    max_events: Any = None
    group: Any = 1
    active: bool = True
    off_mode: str = "drop"
    initial: int = 0
    sourceless: bool = False

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

    def release(self, values: "ValueRuntime", key: Any) -> None:
        """Forgets the values kept for the entity's key once it holds the key no
        longer, so a later entity with the same key starts with none.

        Args:
            values: The run's values, which keep them.
            key: The entity's key.
        """
        if self.admitted and not self.running:
            values.forget(key)


def _not_left(entity: Any) -> Callable[[], bool]:
    """Tells whether a source's entity is still in the model."""
    return lambda: not entity.left


def _holds_key(state: "EntityLoop") -> Callable[[], bool]:
    """Tells whether an entity's loop, or a run it started, is still going."""
    return lambda: state.running


def defuse_stopped(event: Any) -> None:
    """Marks an arrival loop ended by `stop_arrivals` as handled, and nothing else.

    Args:
        event: The arrival process that ended.
    """
    value = getattr(event, "_value", None)
    if (
        not event.ok
        and isinstance(value, simpy.Interrupt)
        and value.cause == ARRIVALS_STOPPED
    ):
        event.defused = True


class _Counter:
    """The next number of an arrival's entities, which a dropped arrival gives back
    while no entity has taken a later number."""

    __slots__ = ("last", "holder")

    def __init__(self) -> None:
        self.last = 0
        # the entity that took the last number
        self.holder: Optional[Entity] = None

    def take(self) -> int:
        """The next number. `hold` records who took it."""
        self.last += 1
        self.holder = None
        return self.last

    def hold(self, entity: Entity) -> None:
        """Records the entity that took the number `take` returned last.

        Args:
            entity: The entity that took it.
        """
        self.holder = entity

    def give_back(self, entity: Entity) -> None:
        """Frees the number `entity` took, when it is still the last one taken, so
        that no two entities share a number.

        Args:
            entity: The dropped entity.
        """
        if self.holder is entity:
            self.last -= 1
            self.holder = None


def _check_name(name: str) -> None:
    """Refuses an empty arrival name, which the startup loops read as no arrival."""
    if name == "":
        raise ValueError("an arrival name is at least one character, got ''")


def _replaces_whole_or_nothing(method: Callable[..., None]) -> Callable[..., None]:
    """Makes a method that declares an arrival under a name keep the arrival the name
    held before when the call is refused, so a refused call changes nothing."""

    @functools.wraps(method)
    def declare(self: "Arrivals", bind: Bind, name: str, *args: Any, **kwargs: Any):
        before = self.snapshot(name)
        try:
            return method(self, bind, name, *args, **kwargs)
        except Exception:
            self.restore(name, before)
            raise

    return declare


class Arrivals:
    """The arrivals of a model: how each makes its entities and when.

    It holds the declarations only. What a run needs while it goes on, with the loops
    that start the entities, is the run's `ArrivalRun`, which `new_run` makes.

    Attributes:
        configs: Each arrival's distribution of gaps.
        entities: Per arrival, the attributes each entity starts with, and the key
            function.
        spawns: Per arrival given `spawn`, the task each of its entities starts.
        plans: Per arrival, the keys of a blueprint that limit or repeat it, or take
            its gaps from an expression.
        replays: Per replayed arrival, how to read its rows.
        replay_places: Per replayed arrival of a blueprint, where its `replay.path` is
            written, to start a refusal of its rows with.
    """

    def __init__(self) -> None:
        self.configs: Dict[str, DistributionConfig] = {}
        self.entities: Dict[
            str, Tuple[Dict[str, Any], Optional[Callable[[int], str]]]
        ] = {}
        self.spawns: Dict[str, str] = {}
        self.plans: Dict[str, ArrivalPlan] = {}
        self.replays: Dict[str, Dict[str, Any]] = {}
        self.replay_places: Dict[str, str] = {}

    def new_run(
        self,
        env: "DynamicRealtimeEnvironment",
        values: "ValueRuntime",
        sampler: "Sampler",
        sim_id: str,
        start_task: Callable[[str, Entity, Any], Any],
    ) -> "ArrivalRun":
        """The state a new run starts with: each `first_at` due, and each source on
        or off as declared.

        Args:
            env: The run's environment.
            values: The run's values, which evaluate the arrivals' places.
            sampler: The run's sampler, which draws the gaps.
            sim_id: The context's `sim_id`, under which the run registers the gaps.
            start_task: Starts a task for an entity, given the task, the entity and
                the upstream instance.
        """
        return ArrivalRun(
            self,
            env,
            values,
            sampler,
            sim_id,
            start_task,
            first_due={
                name: plan.first_at
                for name, plan in self.plans.items()
                if plan.first_at is not None and not plan.has_entities
            },
            on={name: plan.active for name, plan in self.plans.items()},
        )

    @_replaces_whole_or_nothing
    def add_arrival(
        self,
        bind: Bind,
        name: str,
        dist: Optional[str] = None,
        *,
        rate: Optional[float] = None,
        mean: Optional[Union[float, str]] = None,
        std: Optional[Union[float, str]] = None,
        low: Optional[Union[float, str]] = None,
        value: Optional[Union[float, str]] = None,
        high: Optional[Union[float, str]] = None,
        mode: Optional[Union[float, str]] = None,
        shape: Optional[float] = None,
        scale: Optional[Union[float, str]] = None,
        values: Optional[Sequence[Union[float, str]]] = None,
        weights: Optional[List[float]] = None,
        edges: Optional[Sequence[Union[float, str]]] = None,
        counts: Optional[List[float]] = None,
        p: Optional[float] = None,
        gap: Any = None,
        attributes: Optional[Mapping[str, Any]] = None,
        key: Any = None,
        spawn: Optional[str] = None,
        first_at: Union[float, str, None] = None,
        group: Any = 1,
        active: bool = True,
        off_mode: str = "drop",
        initial: int = 0,
        max_events: Any = None,
        vars: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """See `SimulationContext.add_arrival`.

        Args:
            bind: Binds the generators of a place at the place a blueprint gives it (the
                context's `_bind_values`). The others are as for
                `SimulationContext.add_arrival`.

        Raises:
            ValueError: If `name` is empty.
        """
        _check_name(name)
        self.forget(name)
        params = {
            k: v
            for k, v in locals().items()
            if k
            not in (
                "bind",
                "attributes",
                "key",
                "spawn",
                "first_at",
                "group",
                "active",
                "off_mode",
                "initial",
                "gap",
                "max_events",
                "vars",
            )
        }
        if (dist is None) == (gap is None):
            raise ValueError(
                f"arrival '{name}' takes a dist or a gap, "
                + ("not both" if dist is not None else "and neither is given")
            )
        if gap is not None:
            given = sorted(
                k
                for k, v in params.items()
                if v is not None and k not in ("self", "name", "dist")
            )
            if given:
                raise ValueError(
                    f"arrival '{name}' has a gap, which takes no distribution "
                    f"parameter: {', '.join(given)}"
                )
            if isinstance(gap, str):
                from desflow.core.rules import duration_text

                gap = duration_text(gap)  # a duration or a plain number in text
                if isinstance(gap, str):  # not a number, so an expression
                    gap = _run_expression(name, "gap", gap)
            if isinstance(gap, bool) or not isinstance(
                gap, (numbers.Real, Gen, Expression)
            ):
                raise ValueError(
                    f"arrival '{name}': gap is a number of seconds, a duration, "
                    f"an expression or a generator, got {gap!r}"
                )
            if isinstance(gap, numbers.Real):
                try:
                    from_zero = float(gap) >= 0
                except OverflowError:
                    raise ValueError(
                        f"arrival '{name}': gap is a whole number too large for a "
                        "float, so it is not a number of seconds"
                    ) from None
                if not from_zero:
                    raise ValueError(f"arrival '{name}': gap is 0 or more, got {gap!r}")
        else:
            assert dist is not None
            self.configs[name] = distribution(dist, params)
        bind(attributes or {}, ("arrivals", name, "attributes"), overwrite=False)
        value_key, key = self._key_and_vars(bind, name, key, vars)
        self.entities[name] = (dict(attributes or {}), key)
        if first_at is not None:
            try:
                seconds = checked_seconds(first_at, "first_at")
            except ValueError:
                raise ValueError(
                    f"first_at is a time of 0 or more, got {first_at!r}"
                ) from None
            first_at = seconds
        from desflow.core.rules import check_count_place

        for what, count in (("group", group), ("max_events", max_events)):
            try:
                check_count_place(count, what)  # _source_plan reads a text group
            except ValueError as error:
                raise ValueError(f"arrival '{name}': {error}") from None
        try:
            plan = _source_plan(spawn, group, active, off_mode, initial)
        except ValueError as error:
            raise ValueError(f"arrival '{name}': {error}") from None
        if plan is not None:
            bind(plan.group, ("arrivals", name, "group"), overwrite=False)
        plan = self._with_key_and_vars(plan, value_key, vars)
        if first_at is not None:
            plan = plan or ArrivalPlan()
            plan.first_at = first_at
        if gap is not None:
            plan = plan or ArrivalPlan()
            plan.gap = gap
            bind(gap, ("arrivals", name, "gap"), overwrite=False)
        if max_events is not None:
            if spawn is None:
                raise ValueError(f"arrival '{name}': max_events needs a spawn")
            if isinstance(max_events, str):
                max_events = Expression(max_events)
            elif not isinstance(max_events, (numbers.Integral, Gen, Expression)):
                raise ValueError(
                    f"arrival '{name}': max_events is a whole number of 0 or more, "
                    f"an expression or a generator, got {max_events!r}"
                )
            plan = plan or ArrivalPlan()
            plan.max_events = (
                int(max_events)
                if isinstance(max_events, numbers.Integral)
                else max_events
            )
            bind(plan.max_events, ("arrivals", name, "max_events"), overwrite=False)
        if plan is not None:
            self.plans[name] = plan
        else:
            self.plans.pop(name, None)  # a second call replaces the first
        self._add_spawn(name, spawn)

    def _key_and_vars(
        self, bind: Bind, name: str, key: Any, vars: Optional[Mapping[str, Any]]
    ) -> Tuple[Any, Optional[Callable[[int], str]]]:
        """Checks an arrival's `key` and `vars` and binds their generators at the
        places a blueprint gives them, for every Builder form of an arrival.

        Args:
            bind: Binds the generators of a place.
            name: The arrival's name.
            key: A function of the entity's counter, or a value as a blueprint's
                `key` reads it.
            vars: The arrival's `vars`, or None.

        Returns:
            Tuple: The value key, or None, and the function key, or None.

        Raises:
            ValueError: If `vars` is not a mapping of names to values, a name is not
                one an expression can read, or a value is a plain mapping with `gen`.
        """
        value_key = None
        if key is not None and not callable(key):
            value_key, key = key, None  # a value place, as the blueprint's key
            bind(value_key, ("arrivals", name, "key"), overwrite=False)
        if vars is not None:
            from desflow.core.rules import check_var_names

            if not isinstance(vars, Mapping) or not all(
                isinstance(k, str) for k in vars
            ):
                raise ValueError(
                    f"arrival '{name}': vars is a mapping of names to values, "
                    f"got {vars!r}"
                )
            try:
                check_var_names(vars, "vars")
            except ValueError as error:
                raise ValueError(f"arrival '{name}': {error}") from None
            for var, value in vars.items():
                if isinstance(value, Mapping) and "gen" in value:
                    raise ValueError(
                        f"arrival '{name}': vars: '{var}' is a mapping with a gen, "
                        f"which is kept as data; write generator({dict(value)!r})"
                    )
            bind(dict(vars), ("arrivals", name, "vars"), overwrite=False)
        return value_key, key

    @staticmethod
    def _with_key_and_vars(
        plan: Optional[ArrivalPlan], value_key: Any, vars: Optional[Mapping[str, Any]]
    ) -> Optional[ArrivalPlan]:
        """The arrival's plan with its value key and `vars`, made when it has either
        and no plan yet."""
        if value_key is not None or vars:
            plan = plan or ArrivalPlan()
            plan.key = value_key
            plan.vars = dict(vars or {})
        return plan

    @_replaces_whole_or_nothing
    def add_initial_arrival(
        self,
        bind: Bind,
        name: str,
        initial: int,
        *,
        spawn: str,
        attributes: Optional[Mapping[str, Any]] = None,
        key: Any = None,
        gap: Any = None,
        vars: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """See `SimulationContext.add_initial_arrival`.

        Args:
            bind: Binds the generators of a place at the place a blueprint gives it (the
                context's `_bind_values`). The others are as for
                `SimulationContext.add_initial_arrival`.
        """
        _check_name(name)
        if (
            isinstance(initial, bool)
            or not isinstance(initial, numbers.Integral)
            or initial < 1
        ):
            raise ValueError(f"initial is a whole number above 0, got {initial!r}")
        # a NumPy integer too, as `group` and `max_events` take it
        initial = int(initial)
        self.forget(name)
        if gap is not None:
            return self.add_arrival(
                bind,
                name,
                gap=gap,
                attributes=attributes,
                key=key,
                spawn=spawn,
                initial=initial,
                vars=vars,
            )
        bind(attributes or {}, ("arrivals", name, "attributes"), overwrite=False)
        value_key, key = self._key_and_vars(bind, name, key, vars)
        self.entities[name] = (dict(attributes or {}), key)
        plan = self._with_key_and_vars(
            ArrivalPlan(initial=initial, sourceless=True), value_key, vars
        )
        assert plan is not None  # it was given one
        self.plans[name] = plan
        self._add_spawn(name, spawn)

    def _tables(self) -> Tuple[Dict[str, Any], ...]:
        """The tables that hold what is declared under an arrival's name."""
        return (
            self.configs,
            self.entities,
            self.spawns,
            self.plans,
            self.replays,
            self.replay_places,
        )

    def snapshot(self, name: str) -> List[Tuple[bool, Any]]:
        """What each table holds under an arrival's name, for `restore`.

        Args:
            name: The arrival's name.

        Returns:
            List[Tuple[bool, Any]]: For each table, whether it holds the name and the
                value it holds.
        """
        return [(name in table, table.get(name)) for table in self._tables()]

    def restore(self, name: str, saved: List[Tuple[bool, Any]]) -> None:
        """Puts back what `snapshot` found under an arrival's name.

        Args:
            name: The arrival's name.
            saved: The result of `snapshot`.
        """
        for table, (had, value) in zip(self._tables(), saved):
            if had:
                table[name] = value
            else:
                table.pop(name, None)

    def forget(self, name: str) -> None:
        """Drops everything declared under an arrival's name, so a second declaration
        of the name replaces the first entirely.

        Args:
            name: The arrival's name.
        """
        for table in self._tables():
            table.pop(name, None)

    def _add_spawn(self, arrival: str, task: Optional[str]) -> None:
        """Records that each entity of the arrival starts `task`. The context adds the
        arrival's loop to its startup loops when the arrival is added, so the arrival
        loops start in the order the arrivals were added."""
        if task is None:
            return
        self.spawns[arrival] = task

    @_replaces_whole_or_nothing
    def add_arrival_replay(
        self,
        bind: Bind,
        name: str,
        source: Union[str, Path, Iterable[Mapping[str, Any]]],
        *,
        time: str = "time",
        unit: str = "s",
        fields: Union[str, List[str]] = "all",
        on_invalid: str = "error",
        attributes: Optional[Mapping[str, Any]] = None,
        key: Any = None,
        spawn: Optional[str] = None,
        active: bool = True,
        off_mode: str = "drop",
        vars: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """See `SimulationContext.add_arrival_replay`.

        Args:
            bind: Binds the generators of a place at the place a blueprint gives it (the
                context's `_bind_values`). The others are as for
                `SimulationContext.add_arrival_replay`.

        Raises:
            ValueError: If `name` is empty.
        """
        _check_name(name)
        self.forget(name)
        if unit not in UNITS and unit != "timestamp":
            raise ValueError(
                f"unit must be one of {', '.join([*UNITS, 'timestamp'])}, got '{unit}'"
            )
        if on_invalid not in ("error", "skip"):
            raise ValueError(f"on_invalid must be error or skip, got '{on_invalid}'")
        check_fields(fields)
        if isinstance(source, collections.abc.Iterator):
            # an iterator is spent by one run, and every later run replays its rows
            source = list(source)
        self.replays[name] = dict(
            source=source, time=time, unit=unit, fields=fields, on_invalid=on_invalid
        )
        bind(attributes or {}, ("arrivals", name, "attributes"), overwrite=False)
        value_key, key = self._key_and_vars(bind, name, key, vars)
        self.entities[name] = (dict(attributes or {}), key)
        plan = self._with_key_and_vars(
            _source_plan(spawn, 1, active, off_mode, 0), value_key, vars
        )
        if plan is not None:
            self.plans[name] = plan
        else:
            self.plans.pop(name, None)
        self._add_spawn(name, spawn)


@dataclass
class ArrivalRun:
    """What the arrivals need while one run goes on, and the loops that start the
    entities. Each run's build makes a new one, with `Arrivals.new_run`.

    Attributes:
        arrivals: The arrivals' declarations.
        env: The run's environment.
        values: The run's values.
        sampler: The run's sampler.
        sim_id: The context's `sim_id`.
        start_task: Starts a task for an entity.
        first_due: The time each arrival with `first_at` starts its first entity.
        live_keys: Per arrival, whether each key's entity is still in the model.
        key_skips: Per arrival, how many starts a running entity's key skipped.
        on: Whether each arrival's source is on.
        changed: The event a restarting source waits on for a change, by arrival.
        entity_counters: The next entity number of each arrival.
        replay_rows: The rows left to replay, by arrival.
        replays_done: The replayed arrivals whose last row has started.
        processes: The arrival processes, which a drain stops.
    """

    arrivals: Arrivals
    env: "DynamicRealtimeEnvironment"
    values: "ValueRuntime"
    sampler: "Sampler"
    sim_id: str
    start_task: Callable[[str, Entity, Any], Any]
    first_due: Dict[str, Union[int, float]] = field(default_factory=dict)
    live_keys: Dict[str, Dict[str, Callable[[], bool]]] = field(default_factory=dict)
    key_skips: Dict[str, int] = field(default_factory=dict)
    on: Dict[str, bool] = field(default_factory=dict)
    changed: Dict[str, simpy.Event] = field(default_factory=dict)
    entity_counters: Dict[str, _Counter] = field(default_factory=dict)
    replay_rows: Dict[str, Deque[Row]] = field(default_factory=dict)
    replays_done: Set[str] = field(default_factory=set)
    processes: List[simpy.Process] = field(default_factory=list)

    def value(self, place: Any, names: Optional[Dict[str, Any]] = None) -> Any:
        """Reads a place of an arrival: a number as it is, an expression or a
        generator over the run, with `names` as its bare names.

        Args:
            place: The place: a number, an expression or a generator.
            names: The bare names the place reads; None for none.
        """
        return read_place(self.values, place, names)

    def _seconds(self, place: Any, what: str) -> Union[int, float]:
        """A time place of an arrival, read now, as seconds.

        Raises:
            TypeError: If the result is not a number.
            ValueError: If it is below 0.
        """
        value = self.value(place)
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            raise TypeError(f"{what} gave {value!r}, which is not a number of seconds")
        return checked_seconds(value, what)

    def _gap(self, arrival: str) -> Union[int, float]:
        """The time to the arrival's next entity: its `gap`, or a draw from its
        distribution."""
        plan = self.arrivals.plans.get(arrival)
        if plan is not None and plan.gap is not None:
            return self._seconds(plan.gap, f"arrival '{arrival}' gap")
        sampler = self.sampler
        assert sampler is not None  # a run is under way
        return sampler.sample(
            self.env.registry.get_config(f"{self.sim_id}.arrival.{arrival}")
        )

    def _make_entity(
        self,
        entity: Entity,
        attributes: Optional[Mapping[str, Any]] = None,
        index: Optional[int] = None,
    ) -> None:
        """Gives an entity that starts now its vars, then its attributes, then its key.

        Args:
            entity: The entity.
            attributes: The attributes to evaluate, when they are not the arrival's.
            index: The entity's place among the entities of an arrival with `keys`.
        """
        plan = self.arrivals.plans.get(entity.arrival)
        names: Optional[Dict[str, Any]] = None
        if plan is not None and plan.vars:
            names = self.values.evaluate_vars(plan.vars, self.values.scope_of())
        if attributes is None:
            attributes = self.arrivals.entities.get(entity.arrival, ({}, None))[0]
        self._generate_attributes(entity, attributes, names)
        if plan is None:
            return
        if plan.keys is not None and index is not None:
            entity.key = str(plan.keys[index])
        elif plan.key is not None:
            made = self.values.evaluate(
                plan.key, self.values.scope_of(entity, names=names)
            )
            if isinstance(made, bool) or not isinstance(made, (int, str)):
                raise ValueError(
                    f"arrival '{entity.arrival}' key gave {made!r}, which is not an "
                    f"integer or text"
                )
            entity.key = str(made)

    def _admit(self, arrival: str, entity: Entity, in_use: Callable[[], bool]) -> bool:
        """Whether an entity may start: False, with a warning, when an entity of the
        arrival that is still running has its key. Only an arrival with `key` or
        `keys` checks.

        Args:
            arrival: The arrival.
            entity: The entity that would start.
            in_use: Tells whether the entity holds its key, from now on.
        """
        plan = self.arrivals.plans.get(arrival)
        if plan is None or (plan.key is None and plan.keys is None):
            return True
        live = self.live_keys.setdefault(arrival, {})
        holder = live.get(entity.key)
        if holder is not None and holder():
            skipped = self.key_skips.get(arrival, 0) + 1
            self.key_skips[arrival] = skipped
            if skipped == 1 or skipped in (10, 100, 1000) or skipped % 10000 == 0:
                logger.warning(
                    f"Arrival '{arrival}' skipped a start because an entity that is "
                    f"still running has the key '{entity.key}'; {skipped} skipped so "
                    f"far."
                )
            return False
        live[entity.key] = in_use
        if len(live) > 1024 and len(live) % 1024 == 0:
            for key in [key for key, held in live.items() if not held()]:
                del live[key]
        return True

    def _visit(self, entity: Entity) -> Entity:
        """A run of an entity that repeats: the same key and attributes with the
        state of a new flow, so each run enters and leaves the model on its own."""
        if entity.loop is not None:
            entity.loop.active += 1
        return replace(
            entity,
            held={},
            entered=False,
            instances=None,
            spawned_by=None,
            ends_pending=0,
            ending=False,
            last_ended=None,
        )

    def wait_for_arrival(self, arrival_id: str) -> simpy.Event:
        """See `SimulationContext.wait_for_arrival`, which checks that a run exists.

        Args:
            arrival_id: The arrival's name.
        """
        if arrival_id in self.arrivals.replays:
            return self._next_replayed(arrival_id)
        options = self.arrivals.entities.get(arrival_id)
        entity = self.new_entity(arrival_id, options, generate=False)
        first = self.first_due.pop(arrival_id, None)
        if first is not None:
            delay: Union[int, float] = max(first - self.env.now, 0)
        else:
            delay = self._gap(arrival_id)
        event = self.env.timeout(delay, value=entity)
        self._generate_when_due(event, entity, options)
        return event

    def _next_replayed(self, arrival_id: str) -> simpy.Event:
        """The wait for the next row of a replayed arrival; it never ends after the
        last row, so the arrival stops."""
        rows = self.replay_rows[arrival_id]
        if not rows:
            if arrival_id not in self.replays_done:
                self.replays_done.add(arrival_id)
                logger.info(
                    f"Arrival '{arrival_id}' replayed its last row at "
                    f"{self.env.now:g} s; it stops."
                )
            return self.env.event()
        row = rows.popleft()
        attributes = {
            **self.arrivals.entities.get(arrival_id, ({}, None))[0],
            **row.fields,
        }
        options = (attributes, self.arrivals.entities[arrival_id][1])
        entity = self.new_entity(arrival_id, options, generate=False)
        if row.time <= self.env.now:
            # A row that starts nobody draws nothing, as one due later does
            if self.on.get(arrival_id, True):
                self._make_entity(entity, attributes)
            return self._next_replayed_tied(entity)
        event = self.env.timeout(row.time - self.env.now, value=entity)
        self._generate_when_due(event, entity, options)
        return event

    def _next_replayed_tied(self, entity: Entity) -> simpy.Event:
        """The wait for a row due at the step in progress: an event already processed,
        so the arrival loop goes on in the same step. The rows of one time then start
        their entities in one step, in file order, as a SimPy loop that starts them
        one after another does, before any of them takes its first step."""
        event = self.env.event()
        trigger(event, True, entity)
        event.callbacks = None  # type: ignore[assignment]
        return event

    def new_entity(
        self,
        arrival: str,
        options: Optional[Tuple[Dict[str, Any], Optional[Callable[[int], str]]]],
        generate: bool = True,
    ) -> Entity:
        """Starts an entity: the next counter of `arrival`, its key and attributes.

        An attribute written as a generator is evaluated once, here, or with
        `generate=False` by `_generate_attributes`, which the caller runs when the
        entity's arrival is due. Until then the attribute is None.

        Args:
            arrival: The arrival's name.
            options: The arrival's attributes and key function, as `Arrivals.entities` holds
                them; None for none.
            generate: False leaves an attribute written as a generator None until the
                caller evaluates it.
        """
        counter = self.entity_counters.setdefault(arrival, _Counter())
        number = counter.take()
        attributes, key = options if options is not None else ({}, None)
        entity = Entity(
            key=f"{arrival}-{number}" if key is None else key(number),
            arrival=arrival,
            attributes={
                name: None if has_generator(value) else _fresh(value)
                for name, value in attributes.items()
            },
        )
        counter.hold(entity)
        if generate:
            self._generate_attributes(entity, attributes)
        return entity

    def _generate_attributes(
        self,
        entity: Entity,
        attributes: Mapping[str, Any],
        names: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Evaluates the generated attributes of an entity, in the order written, each
        seeing the attributes before it as `attr`, and `names` as bare names."""
        for name, value in attributes.items():
            if has_generator(value):
                entity.attributes[name] = self.values.evaluate(
                    value, self.values.scope_of(entity, names=names)
                )

    def _drop(
        self, arrival: str, entity: Entity, pending: Optional[simpy.Event] = None
    ) -> None:
        """Drops an arrival that starts nobody: its entity is forgotten and the key
        number it took is free for the next arrival, so a dropped arrival uses none.
        `pending` is its event when that has not been processed yet, so its attributes
        are not drawn when it is."""
        if pending is not None:
            pending.dropped = True  # type: ignore[attr-defined]
        counter = self.entity_counters.get(arrival)
        if counter is not None:
            counter.give_back(entity)

    def _change(self, arrival: str) -> simpy.Event:
        """The event that succeeds when the arrival's source is next turned on or
        off, for a source with `off_mode: restart`."""
        event = self.changed.get(arrival)
        if event is None or event.triggered:
            event = self.changed[arrival] = self.env.event()
        return event

    def set_arrivals(self, names: Iterable[str], on: bool) -> None:
        """Turns arrival sources on or off, as `activate` and `deactivate` do. A
        source already in that state is left as it is.

        Args:
            names: The arrivals to turn on or off.
            on: True turns them on, False off.

        Raises:
            ValueError: If a name is not an arrival that has a task to spawn.
        """
        for name in names:
            if name not in self.arrivals.spawns:
                raise ValueError(
                    f"cannot {'activate' if on else 'deactivate'} '{name}', which "
                    f"is not an arrival with a spawn"
                    + suggestion(name, self.arrivals.spawns)
                )
            if self.on.get(name, True) == on:
                continue
            self.on[name] = on
            event = self.changed.pop(name, None)
            if event is not None and not event.triggered:
                event.succeed()

    def start_initial(self, arrival: str, plan: ArrivalPlan) -> None:
        """Creates the `initial` entities of an arrival and starts the arrival's task
        for each, in order, with the arrival's keys and attributes.

        Args:
            arrival: The arrival's name.
            plan: The arrival's plan, which gives the count of `initial` entities.
        """
        task = self.arrivals.spawns[arrival]
        for _ in range(plan.initial):
            entity = self.new_entity(
                arrival, self.arrivals.entities.get(arrival), generate=False
            )
            try:
                self._make_entity(entity)
            except RunStopped:
                return  # a `row` with on_end: stop ended the run before it began
            self.start_task(task, entity, None)

    def _generate_when_due(
        self,
        event: simpy.Event,
        entity: Entity,
        options: Optional[Tuple[Dict[str, Any], Optional[Callable[[int], str]]]],
    ) -> None:
        """Has the arrival's generated attributes evaluated when the event is
        processed, before the arrival loop resumes, so a generator reads the clock
        and the registry as they are when the entity arrives."""
        if options is None:
            return
        plan = self.arrivals.plans.get(entity.arrival)
        if (plan is not None and plan.makes_entities) or has_generator(options[0]):
            attributes = options[0]
            arrival = entity.arrival

            def make(_event: Any) -> None:
                # An arrival that starts nobody draws nothing: the source is off, or
                # a restarting source dropped it
                if getattr(_event, "dropped", False):
                    return
                # A group reads its size first, and draws attributes for members only
                if plan is not None and plan.group != 1:
                    return
                if self.on.get(arrival, True):
                    self._make_entity(entity, attributes)

            event.callbacks.append(make)

    def stop(self) -> None:
        """See `SimulationContext.stop_arrivals`."""
        for process in self.processes:
            if process.is_alive:
                process.interrupt(ARRIVALS_STOPPED)

    def _limit(self, arrival: str, plan: ArrivalPlan) -> Optional[int]:
        """How many times the arrival, or an entity, starts its task: None for no limit.
        A result of 0 or less starts none, as `range` gives none."""
        if plan.max_events is None:
            return None
        count = whole(self.value(plan.max_events), f"arrival '{arrival}' max_events")
        return max(count, 0)

    def _members(self, arrival: str, plan: ArrivalPlan, entity: Any) -> List[Any]:
        """The entities one arrival starts: `entity` and the others of its `group`, made
        now in order, each with the arrival's next key. An empty list when the group is 0
        or less, which frees the key number `entity` took."""
        if plan.group == 1:
            return [entity]
        size = whole(self.value(plan.group), f"arrival '{arrival}' group")
        if size <= 0:
            self._drop(arrival, entity)
            return []
        self._make_entity(entity)
        members = [entity]
        options = self.arrivals.entities.get(arrival)
        for _ in range(size - 1):
            member = self.new_entity(arrival, options, generate=False)
            self._make_entity(member)
            members.append(member)
        return members

    def source(
        self, arrival: str, task: str, plan: ArrivalPlan
    ) -> Generator[Any, Any, None]:
        """An arrival that starts a new entity at each gap, up to `max_events`.

        The loop ends in the step of the last start and draws no further gap. A start
        whose key a running entity of the arrival has is skipped and counts as one. An
        arrival that falls while the source is off starts nobody, counts toward no limit
        and takes no key number. With `off_mode: restart` the pending arrival is dropped
        when the source goes off, and a source that is on starts a full new gap.

        Args:
            arrival: The arrival's name.
            task: The task each entity starts.
            plan: The arrival's settings.

        Yields:
            Any: The events the loop waits for.
        """
        limit = self._limit(arrival, plan)
        attempts = 0
        restart = plan.off_mode == "restart"
        while limit is None or attempts < limit:
            if restart:
                while not self.on.get(arrival, True):
                    yield self._change(arrival)
            due = self.wait_for_arrival(arrival)
            if restart:
                change = self._change(arrival)
                yield due | change
                if not due.processed:
                    # The source went off first: this arrival is dropped
                    self._drop(arrival, due._value, pending=due)
                    continue
                entity = due.value
            else:
                entity = yield due
            if not self.on.get(arrival, True):
                self._drop(arrival, entity)
                continue
            attempts += 1
            for member in self._members(arrival, plan, entity):
                if self._admit(arrival, member, _not_left(member)):
                    self.start_task(task, member, None)

    def start_entities(
        self, arrival: str, task: str, plan: ArrivalPlan
    ) -> Generator[Any, Any, None]:
        """Starts the loops of an arrival with `count` or `keys`, `stagger` apart.

        Each loop is an arrival process of its own, so a drain stops them with the rest.

        Args:
            arrival: The arrival's name.
            task: The task each entity starts.
            plan: The arrival's settings.

        Yields:
            Any: The events the loop waits for.
        """
        if plan.keys is not None:
            number = len(plan.keys)
        else:
            number = max(whole(self.value(plan.count), f"arrival '{arrival}' count"), 0)
        for index in range(number):
            process = self.env.process(self.entity_loop(arrival, task, plan, index))
            process.callbacks.append(defuse_stopped)
            self.processes.append(process)
            if index < number - 1:
                apart = self._seconds(plan.stagger, f"arrival '{arrival}' stagger")
                if apart > 0:
                    yield self.env.timeout(apart)

    def entity_loop(
        self,
        arrival: str,
        task: str,
        plan: ArrivalPlan,
        index: int,
    ) -> Generator[Any, Any, None]:
        """One entity of an arrival with several: it starts `task` after each gap, up to
        `max_events` times, and keeps its attributes between the runs.

        The first run starts at `first_at` when the plan has one, otherwise after a gap.
        Each run is a flow of its own, with the entity's key and attributes.

        Args:
            arrival: The arrival's name.
            task: The task the entity starts.
            plan: The arrival's settings.
            index: The entity's place among the arrival's entities.

        Yields:
            Any: The events the loop waits for.
        """
        env = self.env
        entity = self.new_entity(arrival, self.arrivals.entities.get(arrival), False)
        state = EntityLoop()
        entity.loop = state
        limit = self._limit(arrival, plan)
        restart = plan.off_mode == "restart"
        try:
            started = 0
            timed = False  # whether a first gap or `first_at` has been waited for
            while limit is None or started < limit:
                if restart:
                    while not self.on.get(arrival, True):
                        yield self._change(arrival)
                if not timed and plan.first_at is not None:
                    delay: Union[int, float] = max(plan.first_at - env.now, 0)
                else:
                    delay = self._gap(arrival)
                timed = True
                if restart:
                    change = self._change(arrival)
                    yield env.timeout(delay) | change
                    if change.processed:
                        # The source went off first: this start is dropped, and the gap
                        # starts again when the source is on
                        continue
                else:
                    yield env.timeout(delay)
                if not self.on.get(arrival, True):
                    # An arrival that falls while the source is off starts nobody and
                    # counts toward no limit
                    continue
                if started == 0:
                    self._make_entity(entity, index=index)
                    if not self._admit(arrival, entity, _holds_key(state)):
                        return
                    state.admitted = True
                self.start_task(task, self._visit(entity), None)
                started += 1
        finally:
            state.done = True
            state.release(self.values, entity.key)

    def loop(self, arrival: str, task: str) -> Generator[Any, Any, None]:
        """The arrival loop of an arrival with `spawn`: each entity starts `task`.

        The instance id comes from the task's one counter on the context, which `next`
        and every other arrival that spawns the task share.

        Args:
            arrival: The arrival's name.
            task: The task each entity starts.

        Yields:
            Any: The events the loop waits for.
        """
        plan = self.arrivals.plans.get(arrival)
        if plan is None:
            while True:
                entity = yield self.wait_for_arrival(arrival)
                if not self.on.get(arrival, True):
                    self._drop(arrival, entity)
                    continue
                self.start_task(task, entity, None)
        elif plan.sourceless:
            return
        elif plan.has_entities:
            yield from self.start_entities(arrival, task, plan)
        else:
            yield from self.source(arrival, task, plan)


def _run_expression(arrival: str, what: str, text: str) -> Expression:
    """The expression of a Builder arrival's `gap` given as text, checked as a
    blueprint checks it: it reads the run only, so not `attr`.

    Args:
        arrival: The arrival's name, for the message.
        what: The argument.
        text: The expression.

    Raises:
        ValueError: If the text is not an expression, or reads the entity or a task.
    """
    from desflow.core.rules import check_run_text

    try:
        check_run_text(text, what, True)
        return Expression(text)
    except ValueError as error:
        raise ValueError(f"arrival '{arrival}': {error}") from None


def _source_plan(
    spawn: Optional[str],
    group: Any = 1,
    active: bool = True,
    off_mode: str = "drop",
    initial: int = 0,
) -> Optional[ArrivalPlan]:
    """The plan a Builder arrival has for `group`, `active`, `off_mode` and `initial`,
    or None when it keeps all four defaults.

    Raises:
        ValueError: If one is malformed, or one is set with no `spawn`.
    """
    if isinstance(group, str):
        group = Expression(group)
    elif isinstance(group, numbers.Integral) and not isinstance(group, bool):
        group = int(group)  # a NumPy integer too, as `max_events` takes it
    elif isinstance(group, bool) or not (isinstance(group, Gen) or callable(group)):
        raise ValueError(
            f"group is a whole number, an expression or a generator, got {group!r}"
        )
    if not isinstance(active, bool):
        raise ValueError(f"active is true or false, got {active!r}")
    if off_mode not in ("drop", "restart"):
        raise ValueError(f"off_mode is 'drop' or 'restart', got {off_mode!r}")
    if (
        isinstance(initial, bool)
        or not isinstance(initial, numbers.Integral)
        or initial < 0
    ):
        raise ValueError(f"initial is a whole number of 0 or more, got {initial!r}")
    initial = int(initial)
    if group == 1 and active and off_mode == "drop" and not initial:
        return None
    if spawn is None:
        raise ValueError("group, active, off_mode and initial need a spawn")
    return ArrivalPlan(group=group, active=active, off_mode=off_mode, initial=initial)


def _in_seconds(given: Mapping[str, Any]) -> Dict[str, Any]:
    """The parameters of a distribution with each time-valued one read as seconds, so a
    duration such as `"4 min"` is accepted as the YAML `Distribution` accepts it."""
    read = dict(given)
    for key in TIME_PARAMETERS:
        if key in read and read[key] is not None:
            read[key] = _seconds(read[key])
    for key in TIME_LIST_PARAMETERS:
        if isinstance(read.get(key), (list, tuple)):
            read[key] = [_seconds(item) for item in read[key]]
    return read


def distribution_of(spec: Mapping[str, Any]) -> DistributionConfig:
    """Builds a config from a mapping such as `{"dist": "exponential", "rate": 1}`.

    Args:
        spec: The mapping: `dist` and the family's parameters.
    """
    if not isinstance(spec, Mapping) or "dist" not in spec:
        raise ValueError(f"a distribution is a mapping with a dist, got {spec!r}")
    given = {key: value for key, value in spec.items() if key != "dist"}
    return DistributionConfig(dist=spec["dist"], **_in_seconds(given))


def distribution(dist: str, params: Mapping[str, Any]) -> DistributionConfig:
    """Builds a config from the parameters a builder method was given.

    Args:
        dist: The family's name.
        params: The parameters the builder method was given; `self`, `name`, `dist`
            and those that are None are left out.
    """
    given = {
        key: value
        for key, value in params.items()
        if key not in ("self", "name", "dist") and value is not None
    }
    return DistributionConfig(dist=dist, **_in_seconds(given))
