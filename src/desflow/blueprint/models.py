"""Pydantic models for a blueprint file.

Every model forbids unknown keys, so a misspelt key is reported rather than ignored.
Distributions are checked by building the `DistributionConfig` the builder registers,
so YAML and Python accept the same families and parameters. Fields that take Python
objects are filled by `!python` references and checked here, so a wrong reference
fails when the file is loaded rather than during the run.
"""

import inspect
import keyword
import numbers
import re
from datetime import datetime, timedelta
from types import UnionType
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    StrictFloat,
    StrictInt,
    Tag,
    TypeAdapter,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

from desflow.config import (
    DEFAULT_QUEUE_ORDER,
    DEFAULT_STATS_INTERVAL,
    DEFAULT_STATS_LEVEL,
    INTERRUPT_POLICIES,
    LABELS,
    QUEUE_ORDERS,
    STATS_LEVELS,
    TRACE,
)
from desflow.core.distributions import (
    PARAMETERS,
    TIME_LIST_PARAMETERS,
    TIME_PARAMETERS,
)
from desflow.core.expressions import (
    ANY_NAME,
    RESERVED_NAMES,
    Expression,
    boolean_word,
    rank_problem,
)
from desflow.core.environment import (
    DynamicRealtimeEnvironment,
    in_zone,
    resolve_zone,
)
from desflow.core.families import Reference
from desflow.core.flow import Open, Release, Router, Transfer, Wait
from desflow.models.params import DistributionConfig
from desflow.resources.base import check_ties
from desflow.resources.container import ContainerBase, check_container_type
from desflow.resources.resource import ResourceBase, check_resource_type
from desflow.resources.store import StoreBase, check_store_type
from desflow.values.models import GENERATORS, Generator, Payload, Value, _Gen
from desflow.utils import _seconds, suggestion, time_to_seconds

# The names an egress `when` takes instead of a `!python` function.
WHEN_NAMES = ("history", "live", "events", "telemetry", LABELS, TRACE)


def _place_kind(value: Any) -> str:
    """Which branch of a time or count place a value is: a generator (a `_Gen` or a
    mapping with `gen`), or a plain number or text."""
    if isinstance(value, _Gen) or (isinstance(value, Mapping) and "gen" in value):
        return "<gen>"
    return "<plain>"


# A place that takes a time: seconds, a duration such as `5 min`, an expression, or a
# `{gen: ...}` generator. A mapping with `gen` is always checked as a generator, so a
# misspelt argument is reported and not passed on as another form.
TimePlace = Annotated[
    Union[
        Annotated[Generator, Tag("<gen>")],
        Annotated[Union[int, float, str], Tag("<plain>")],
    ],
    Discriminator(_place_kind),
]

# A place that takes a whole number: an integer (not a float, and not true or false),
# an expression, or a generator.
CountPlace = Annotated[
    Union[
        Annotated[Generator, Tag("<gen>")],
        Annotated[Union[StrictInt, str], Tag("<plain>")],
    ],
    Discriminator(_place_kind),
]


# Text that is only a number, so a name such as `inf` stays an expression
_PLAIN_NUMBER = re.compile(r"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")


def _duration_text(value: Any) -> Any:
    """A time place as the runtime reads it: a duration in text becomes seconds, and
    other text stays an expression, checked here so a mistake is reported at load."""
    if isinstance(value, str):
        try:
            return time_to_seconds(value)
        except ValueError:
            pass
        if _PLAIN_NUMBER.fullmatch(value.strip()):
            return float(value)  # a number in text, such as '-1'
    return value


def _refuse_boolean(value: Any, what: Optional[str]) -> None:
    """Refuses true and false in a time place, which pydantic would read as 1 and 0."""
    if isinstance(value, bool):
        raise ValueError(
            f"{what} must be a number of seconds, a duration or an expression, "
            f"got {value!r}"
        )


def _check_time_place(value: Any, what: str, run_scope: bool = False) -> None:
    """Refuses a negative number and text that is not an expression.

    Args:
        value: The place as validated.
        what: The key, for the message.
        run_scope: Whether the place reads the run only, so `attr` is refused.
    """
    if isinstance(value, str):
        check_run_text(value, what, run_scope)
    elif isinstance(value, numbers.Real) and not float(value) >= 0:
        raise ValueError(f"{what} must be 0 or more, got {value!r}")


def check_run_text(text: str, what: str, run_scope: bool) -> None:
    """Refuses text that is not an expression, or that reads the entity or a task
    where the place reads the run only.

    A blueprint's places and the `add_arrival` arguments that mirror them both check
    through this.

    Args:
        text: The expression.
        what: The key, for the message.
        run_scope: Whether the place reads the run only, so `attr` is refused.

    Raises:
        ValueError: If the text does not parse, or reads outside the run where
            `run_scope` is True.
    """
    try:
        parsed = Expression(text, names=ANY_NAME)
    except ValueError as exc:
        raise ValueError(f"{what}: {exc}") from None
    outside = parsed.outside_run() if run_scope else None
    if outside:
        raise ValueError(
            f"{what} '{text}' reads {outside}, but this place reads the run (level, "
            f"users, queue, signal, registry and t), not the entity or a task"
        )


def check_var_names(names: Iterable[str], what: str) -> None:
    """Refuses a variable name that an expression cannot read, or that it already uses.

    An arrival's `vars` in a blueprint and in `add_arrival` both check through this.

    Args:
        names: The names.
        what: The key they are under, for the message.

    Raises:
        ValueError: If a name is not an identifier, is a keyword, or is one of the
            names an expression already reads.
    """
    for name in names:
        if not name.isidentifier() or keyword.iskeyword(name):
            raise ValueError(
                f"{what}: '{name}' is not a name an expression can read; use "
                "letters, digits and _, not starting with a digit"
            )
        if name in RESERVED_NAMES:
            raise ValueError(
                f"{what}: '{name}' is a name an expression already uses; pick another"
            )


def check_count_place(value: Any, what: str) -> None:
    """Refuses what a count place does not take: text that is not an expression of the
    run, a bool, a number that is not whole, and a number below 0.

    A count place is an arrival's `count`, `max_events` or `group`. The `Arrival`
    validator and `add_arrival` both check through this; a generator, an
    `Expression` and None pass.

    Args:
        value: The place as given.
        what: The key, for the message.

    Raises:
        ValueError: If the value is one of those the place refuses.
    """
    if isinstance(value, str):
        check_run_text(value, what, True)
    elif isinstance(value, bool) or (
        isinstance(value, numbers.Real) and not float(value).is_integer()
    ):
        raise ValueError(
            f"{what} is a whole number of 0 or more, an expression or a generator, "
            f"got {value!r}"
        )
    elif isinstance(value, numbers.Real) and value < 0:
        raise ValueError(f"{what} must be 0 or more, got {value!r}")


_DATETIME = TypeAdapter(datetime)


def _instant(value: Any, now: datetime) -> Any:
    """Reads `now`, a signed duration from now such as `-1d`, or an ISO datetime.

    A datetime, from a YAML timestamp or a `!python` reference, is returned as it is.
    """
    if not isinstance(value, str):
        return value
    text = value.strip()
    if text.lower() == "now":
        return now
    if text[:1] in ("+", "-"):
        try:
            offset = time_to_seconds(text[1:])
        except ValueError:
            raise ValueError(
                f"{value!r} is not a duration. Write a sign and a duration, such "
                f"as -1d, -7d or -10m"
            ) from None
        return now + timedelta(seconds=offset if text[0] == "+" else -offset)
    try:
        return _DATETIME.validate_python(text)
    except ValidationError:
        raise ValueError(
            f"{value!r} is not a time. Use now, a signed duration such as -1d, or "
            f"an ISO datetime such as 2026-01-01T00:00:00"
        ) from None


def _generator_function(value: Any) -> Any:
    if not inspect.isgeneratorfunction(value):
        raise ValueError(
            f"{value!r} must be a generator function, one that uses `yield`"
        )
    return value


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


class Simulation(_Model):
    """The `simulation` section: the arguments of `SimulationContext`.

    `logical_start_time` and `go_live_at` take `now`, a signed duration from now such
    as `-1d` or `-10m`, an ISO datetime, or a `!python` datetime. Both read the same
    `now`, the moment the file is validated, so `-10m` and `now` are exactly ten
    minutes apart. `now` and a time without an offset are read in `timezone`, an IANA
    name, UTC by default.

    `strict` makes a step raise `RuntimeError` when the run is more than `factor`
    seconds behind the wall clock, as SimPy's real-time environment does. Left unset
    it is true for a paced run or one with `go_live_at`, and false at `factor: 0`
    with no `go_live_at`. At `factor: 0`, `strict: true` raises once the wall clock
    has moved, as SimPy's does.
    `environment` is a subclass of `DynamicRealtimeEnvironment` the run is built on.
    """

    sim_id: str
    factor: float = 1.0
    random_seed: Optional[int] = None
    # Before the times, so `now` and times without an offset can be read in it
    timezone: Optional[str] = None
    logical_start_time: Optional[datetime] = None
    go_live_at: Optional[datetime] = None
    warmup: Optional[Union[int, float]] = Field(default=None, ge=0)
    initial_time: Union[int, float] = 0.0
    # Seconds between lag readings; unset is off at factor 0 and 1 s otherwise
    lag_interval: Optional[float] = Field(default=None, ge=0)
    # Unset is true when paced or live and false otherwise; false lets a blocking sink fall behind
    strict: Optional[bool] = None
    # A subclass of DynamicRealtimeEnvironment, as `!python module.Env`
    environment: Optional[Type[DynamicRealtimeEnvironment]] = None

    @field_validator("environment")
    @classmethod
    def _check_environment(cls, value: Any) -> Any:
        if value is not None and not issubclass(value, DynamicRealtimeEnvironment):
            raise ValueError(
                "environment must be a subclass of DynamicRealtimeEnvironment, "
                f"got {value!r}"
            )
        return value

    @field_validator("logical_start_time", "go_live_at", mode="before")
    @classmethod
    def _parse_instant(cls, value: Any, info: ValidationInfo) -> Any:
        zone = resolve_zone(info.data.get("timezone"))
        now = (info.context or {}).get("now") or datetime.now(zone)
        # `now` and relative times are read in the run's zone
        now = in_zone(now, zone).astimezone(zone)
        return _instant(value, now)

    @field_validator("timezone")
    @classmethod
    def _check_zone(cls, value: Optional[str]) -> Optional[str]:
        resolve_zone(value)
        return value

    @field_validator("warmup", "initial_time", "lag_interval", mode="before")
    @classmethod
    def _parse_duration(cls, value: Any) -> Any:
        return None if value is None else _seconds(value)


class Distribution(_Model):
    """A `services` entry: a distribution family and its parameters.

    Time-valued parameters (`mean`, `std`, `low`, `mode`, `high`, `scale`, `value`,
    and an `empirical`'s `values` and `edges`) take seconds or a duration such as
    `5 min`; `rate` is a number per second.
    """

    dist: str
    rate: Optional[Union[int, float]] = None
    mean: Optional[Union[int, float]] = None
    std: Optional[Union[int, float]] = None
    low: Optional[Union[int, float]] = None
    value: Optional[Union[int, float]] = None
    high: Optional[Union[int, float]] = None
    mode: Optional[Union[int, float]] = None
    shape: Optional[Union[int, float]] = None
    scale: Optional[Union[int, float]] = None
    values: Optional[List[Union[int, float]]] = None
    weights: Optional[List[float]] = None
    edges: Optional[List[float]] = None
    counts: Optional[List[float]] = None
    p: Optional[Union[int, float]] = None

    @field_validator(*TIME_PARAMETERS, mode="before")
    @classmethod
    def _duration(cls, value: Any) -> Any:
        return None if value is None else _seconds(value)

    @field_validator(*TIME_LIST_PARAMETERS, mode="before")
    @classmethod
    def _durations(cls, value: Any) -> Any:
        if not isinstance(value, list):
            return value
        return [_seconds(item) for item in value]

    @model_validator(mode="after")
    def _distribution_in_range(self) -> "Distribution":
        # Built as the builder builds it, so a bad parameter is reported with its line
        self.config()
        return self

    def config(self) -> DistributionConfig:
        """The `DistributionConfig` the builder registers for this entry."""
        return DistributionConfig(dist=self.dist, **self.parameters())

    def parameters(self) -> Dict[str, Any]:
        """The parameters given, by name, for `add_service` or `add_arrival`."""
        return {
            name: getattr(self, name)
            for name in PARAMETERS
            if getattr(self, name) is not None
        }


class Replay(_Model):
    """An arrival's `replay`: the rows of a local file, each one arrival at its time.

    `path` is a CSV or Parquet file, relative to the blueprint; `time` its column of
    times; `unit` `ms`, `s`, `min` or `h` from the start of the run, or `timestamp`
    for a date and time read from `simulation.logical_start_time`; `fields` the
    columns each entity carries, or `all`; `on_invalid` `error` or `skip` for a row
    with no valid time.
    """

    path: str
    time: str = "time"
    unit: Literal["ms", "s", "min", "h", "timestamp"] = "s"
    fields: Union[Literal["all"], List[str]] = "all"
    on_invalid: Literal["error", "skip"] = "error"


class Arrival(Distribution):
    """An `arrivals` entry: a distribution, or a `replay` of recorded rows, the task
    it spawns, and the attributes each entity it starts carries.

    File order is creation order: arrivals are made in the order they are written,
    then the `processes`. Events due at one time run in the order they were
    scheduled, so this order decides only ties between first due times.

    An `attributes` value is a constant or a `{gen: ...}` generator, drawn once for each
    entity when it starts, in the order the entities are made. A mapping with a `gen`
    key is a generator; `{gen: literal, value: ...}` keeps a mapping as data.

    The gaps come from exactly one of `dist`, `replay` or `gap`. `gap` is a number of
    seconds, a duration such as `5 min`, an expression or a generator, read at the
    start of each gap, so a gap may read `registry[...]`, `level.<name>` and `t`.
    `first_at` is the simulation time of the first arrival, which waits for it in place
    of a gap; the gaps after it follow. A time already past arrives at once.
    `max_events` is how many times the arrival starts its task, in all: the arrival
    stops after the last, and its last start draws no further gap. A start whose key
    belongs to an entity of the arrival that is still running is skipped with a
    warning and counts as one.

    `group` is how many entities one arrival starts, a count place read once at each
    arrival, after the gap: 0 or less starts nobody and the next gap still runs, and
    a dropped or empty arrival uses no key number. `initial` creates that many entities
    when the run is built, after the arrival's source starts and in file order, with the
    arrival's keys, and an arrival with `initial` alone needs no `dist`, `replay` or
    `gap`. `active: false` makes a source that starts off, and a task or an `on:` entry
    turns it on or off with `activate` and `deactivate`. With `off_mode: drop`, the
    default, the clock keeps running and an arrival while off starts nobody. With
    `restart` the pending arrival is dropped when the source goes off and `activate`
    starts a full new gap. An action on a source already in that state does nothing,
    and an arrival that falls at the instant of an action whose event comes first is
    dropped.

    `count` makes that many entities, each with a loop of its own: it starts its task
    after each gap, `first_at` is the time of its first start, and `max_events` is how
    many times each entity starts. `keys` is a list of keys instead, one entity for
    each, and gives the number. `stagger` is the time between the starts of two
    entities, read after each one. `key` is a value place read once for each entity,
    for its key; `vars` are named values read once for each entity before its
    attributes and key, which read them as bare names. Every one of these places reads
    the run only: `attr` is refused in them, because the entity has none yet. `count`
    has no argument of `add_arrival`; `max_events` and `vars` do.
    `count`, `keys`, `first_at` and `stagger` do not apply to a replay. `count`,
    `keys`, `max_events` and `stagger` set how the arrival starts its task, so each
    needs `spawn`; `stagger` also needs `count` or `keys`. `key`, `vars`, `gap` and
    `first_at` are read by `wait_for_arrival` too, so an arrival without `spawn` may
    carry them.
    """

    dist: Optional[str] = None  # type: ignore[assignment]
    replay: Optional[Replay] = None
    gap: Optional[TimePlace] = None
    first_at: Optional[Union[int, float]] = None
    count: Optional[CountPlace] = None
    keys: Optional[List[Union[StrictInt, str]]] = None
    key: Value = None
    vars: Dict[str, Value] = Field(default_factory=dict)
    stagger: TimePlace = 0
    max_events: Optional[CountPlace] = None
    spawn: Optional[str] = None
    attributes: Dict[str, Value] = Field(default_factory=dict)
    group: CountPlace = 1
    active: bool = True
    off_mode: Literal["drop", "restart"] = "drop"
    initial: StrictInt = Field(default=0, ge=0)

    @field_validator("gap", "stagger", mode="before")
    @classmethod
    def _place_duration(cls, value: Any, info: ValidationInfo) -> Any:
        _refuse_boolean(value, info.field_name)
        return _duration_text(value)

    @field_validator("first_at", mode="before")
    @classmethod
    def _first_at_time(cls, value: Any) -> Any:
        return None if value is None else _seconds(value)

    @model_validator(mode="after")
    def _distribution_in_range(self) -> "Arrival":
        given = [
            name
            for name, value in (
                ("dist", self.dist),
                ("replay", self.replay),
                ("gap", self.gap),
            )
            if value is not None
        ]
        if len(given) != 1 and not (not given and self.initial > 0):
            raise ValueError(
                "an arrival takes exactly one of dist, replay or gap"
                + (f", not {' and '.join(given)}" if given else ", got none")
                + ("" if given else " (an arrival with initial alone needs none)")
            )
        if not given:
            for name in (
                "group",
                "active",
                "off_mode",
                "max_events",
                "count",
                "keys",
                "stagger",
                "first_at",
            ):
                if name in self.model_fields_set:
                    raise ValueError(
                        f"{name} sets how a source starts entities, and an arrival "
                        "with initial alone has no source; add a dist, a replay or "
                        "a gap, or remove it"
                    )
        if self.dist is None and self.parameters():
            raise ValueError(
                f"an arrival with {given[0] if given else 'initial'} takes no "
                f"distribution parameters; remove them, or the "
                f"{given[0] if given else 'initial'}"
            )
        if self.dist is not None:
            self.config()
        if self.keys is not None and self.key is not None:
            raise ValueError("an arrival takes key or keys, not both")
        if self.keys is not None and self.count is not None:
            raise ValueError(
                "keys gives the number of entities, so count is not needed"
            )
        if self.keys is not None and len(set(map(str, self.keys))) != len(self.keys):
            raise ValueError("keys lists each key once")
        if self.keys is not None and not self.keys:
            raise ValueError("keys needs at least one key")
        if self.spawn is None:
            for name in (
                "count",
                "keys",
                "max_events",
                "stagger",
                "group",
                "active",
                "off_mode",
                "initial",
            ):
                if name in self.model_fields_set:
                    raise ValueError(
                        f"{name} sets how an arrival starts its task, so it needs "
                        f"spawn; an arrival without spawn is read by "
                        f"wait_for_arrival, which uses gap, first_at, key and vars"
                    )
        if "group" in self.model_fields_set and (
            self.count is not None or self.keys is not None or self.replay is not None
        ):
            raise ValueError(
                "group is how many entities one arrival starts, so it does not "
                "apply with count, keys or a replay"
            )
        if (
            "stagger" in self.model_fields_set
            and self.count is None
            and self.keys is None
            and self.replay is None
        ):
            raise ValueError(
                "stagger is the time between the starts of two entities, so it "
                "needs count or keys"
            )
        if self.replay is not None:
            for name in ("count", "keys", "first_at"):
                if getattr(self, name) is not None:
                    raise ValueError(
                        f"{name} does not apply to a replay, whose rows give the "
                        f"times and the number of entities"
                    )
            if self.stagger != 0:
                raise ValueError("stagger does not apply to a replay")
        for name in ("gap", "stagger"):
            _check_time_place(getattr(self, name), name, run_scope=True)
        for name in ("count", "max_events", "group"):
            check_count_place(getattr(self, name), name)
        if self.first_at is not None and not self.first_at >= 0:
            raise ValueError(f"first_at must be 0 or more, got {self.first_at!r}")
        check_var_names(self.vars, "vars")
        return self


class Breakdown(_Model):
    """A resource's `breakdown`: the time to each failure, the repair time, whether
    the tasks being served are interrupted or finish, and which time the time to
    failure counts. `clock: calendar` (the default) counts it from the start or the
    last repair, busy or idle. `clock: busy` counts it only while the resource holds
    at least one unit; a time to failure of 0 then fails at the grant."""

    time_to_failure: Distribution
    repair: Distribution
    work: Literal["interrupt", "finish"] = "interrupt"
    clock: Literal["calendar", "busy"] = "calendar"


class QueueRule(_Model):
    """A `queue` of a resource, or a `get_queue` or `put_queue` of a store or container,
    written as a rank: `rank` is an expression read once as each request, get or put
    is made, in the scope of the task that makes it, and the lowest rank is served
    first. `ties` orders equal ranks: `fifo` (the default) serves the oldest first and
    `lifo` the latest first. On a resource, `ties` orders the queue only: it changes
    neither a request's key nor who preempts."""

    rank: str
    ties: Literal["fifo", "lifo"] = "fifo"

    @field_validator("rank")
    @classmethod
    def _rank_parses(cls, value: str) -> str:
        problem = rank_problem(Expression(value))
        if problem:
            raise ValueError(problem)
        return value


class PreemptRule(_Model):
    """A `preempt` of a resource written as a rule. `when` is tested when a request
    finds no free unit, and the request takes the unit if it holds. `victim` gives each
    holder a value, and the holder with the highest value is the one `when` is tested
    on, the first granted among equals. Both read `request.rank`, `request.time`,
    `request.preempt` and `request.key`, and `holder.rank`, `holder.time`,
    `holder.preempt`, `holder.key`, `holder.since` and `holder.attr.<name>`, and the run's
    own names, `t`, `users.`, `queue.`, `level.` and `registry[...]`. With neither, the
    rule is SimPy's: the holder whose key sorts last is taken when it sorts after the
    request's."""

    when: Optional[str] = None
    victim: Optional[str] = None

    @field_validator("when", "victim")
    @classmethod
    def _parses(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            outside = Expression(value, rule=True).outside_run()
            if outside:
                raise ValueError(
                    f"'{value}' reads {outside}, which a preempt rule cannot: it "
                    "reads request., holder., t and the run"
                )
        return value


def _capacity_text(value: Any) -> Any:
    """A capacity written as text that is only a number (as `${NAME}` gives) is that
    number; any other text stays, for a family to read as an expression."""
    if isinstance(value, str) and _PLAIN_NUMBER.fullmatch(value.strip()):
        number = float(value)
        return int(number) if number.is_integer() and "." not in value else number
    return value


class Ref(_Model):
    """A name a task computes, where a task names a resource, a store, a container or a
    signal: `{of: <family>, key: <expression>}` is the member of a family that `key`
    gives, made when it is first used, and `{of: <family>, lowest: <expression>}` is
    the member whose value is the lowest, `lowest` being read once for each member as
    `member.key`, `member.users`, `member.queue`, `member.level`, `member.count`,
    `member.putters` and `member.getters`. `of` may be a list of declared names, and
    `key` then gives one of them. A reference takes `key` or `lowest`, not both.
    """

    of: Union[str, List[str]]
    key: Optional[Union[str, int]] = None
    lowest: Optional[str] = None

    @model_validator(mode="after")
    def _form(self) -> "Ref":
        self.name()
        return self

    def name(self) -> Reference:
        """The `Reference` the builder and the run take."""
        return Reference(self.of, self.key, self.lowest)

    def _first(self) -> str:
        return self.of if isinstance(self.of, str) else self.of[0]

    def __str__(self) -> str:
        return self._first()

    def __hash__(self) -> int:
        return hash(self._first())

    def __eq__(self, other: object) -> bool:
        """A reference equals the name it starts with, so a check that looks a name up
        in a section finds the declared entry, as it does for a `Reference`."""
        if isinstance(other, str):
            return other == self._first()
        if isinstance(other, Ref):
            return self.model_dump() == other.model_dump()
        return NotImplemented


def _name(value: Any) -> Any:
    """A name place as the builder takes it: a name, or the `Reference` of a `Ref`."""
    return value.name() if isinstance(value, Ref) else value


class Resource(_Model):
    """A `resources` entry: its capacity, the order its queue is served in, whether
    a request of a lower priority value preempts a user, how many requests may wait,
    the class it is built from, and how it breaks down.

    `current_cap` defaults to 1 and `max_cap` to `current_cap`, so `{}` is a
    resource of one unit. `.inf` makes a resource with no limit. `type` is a
    `!python` subclass of `ResourceBase`. `queue` is `fifo`, `lifo`, `priority`, or a
    `{rank, ties}` rule that ranks each request by an expression; `preempt` is true or
    false, or a `{when, victim}` rule, and needs `queue: priority` or a rank.
    `queue_limit` is also the registry path `<sim_id>.resources.<name>.queue_limit`,
    so a scenario, an `on:` entry or an ingress message can change it: the new limit
    applies from the next request and removes nobody who waits, and `null` is no limit.

    `family: true` makes the entry a family: one resource for each key, made from this
    entry. `members` is a count place read once when the run is built, for the keys 0
    to n - 1; without it the family starts empty. A task names a member with `{of:
    <family>, key: <expression>}`, and a member is made at its first use. In a family,
    `current_cap` and `max_cap` may be expression text, read once when a member is made.
    """

    current_cap: Optional[Union[int, float, str]] = None
    max_cap: Optional[Union[int, float, str]] = None
    queue: Union[str, QueueRule] = DEFAULT_QUEUE_ORDER
    preempt: Union[bool, PreemptRule] = False
    queue_limit: Optional[int] = None
    type: Optional[Type[ResourceBase]] = None
    breakdown: Optional[Breakdown] = None
    family: bool = False
    members: Optional[CountPlace] = None

    _capacity_numbers = field_validator("current_cap", "max_cap", mode="before")(
        _capacity_text
    )

    @model_validator(mode="after")
    def _family_form(self) -> "Resource":
        _check_family(self, ("current_cap", "max_cap"))
        return self

    @model_validator(mode="after")
    def _preempt_needs_priority(self) -> "Resource":
        if self.preempt and not (
            isinstance(self.queue, QueueRule) or self.queue == "priority"
        ):
            raise ValueError(
                "preempt ranks users by priority, so it needs queue: priority or a "
                "queue rank"
            )
        return self

    @model_validator(mode="after")
    def _capacity_and_queue_fit(self) -> "Resource":
        if (
            self.current_cap is None
            and isinstance(self.max_cap, (int, float))
            and self.max_cap < 1
        ):
            raise ValueError(
                f"max_cap {self.max_cap} is below 1, the default current_cap; give "
                f"current_cap {self.max_cap} or less"
            )
        check_resource_type(self.type, self.queue_limit)
        return self

    @field_validator("queue")
    @classmethod
    def _known_order(cls, value: Union[str, QueueRule]) -> Union[str, QueueRule]:
        if isinstance(value, str) and value not in QUEUE_ORDERS:
            raise ValueError(
                f"queue must be one of {', '.join(QUEUE_ORDERS)}, or a mapping with "
                f"a rank"
            )
        return value


def _check_family(model: Any, fields: Any) -> None:
    """Checks the `family` and `members` of an entry, and that only a family has text
    for a capacity.

    Args:
        model: The `Resource`, `Store`, `Container` or `SignalSpec`.
        fields: The capacity fields of the entry that may be text in a family.

    Raises:
        ValueError: If `members` is given without `family`, is below 0, or a capacity
            is text in an entry that is not a family.
    """
    if model.members is not None and not model.family:
        raise ValueError("members needs family: true")
    if isinstance(model.members, int) and model.members < 0:
        raise ValueError(f"members is 0 or more, got {model.members}")
    if isinstance(model.members, str):
        Expression(model.members)
    for name in fields:
        value = getattr(model, name)
        if isinstance(value, str):
            if not model.family:
                raise ValueError(
                    f"{name} is a number, or text in a family: add family: true, or "
                    f"give a number"
                )
            parsed = Expression(value)
            outside = parsed.outside_run() if model.members is not None else None
            if outside:
                raise ValueError(
                    f"{name} '{value}' reads {outside}, but a family with members "
                    f"reads the run for every member, so it reads level, users, queue, "
                    f"signal, registry and t, not an entity"
                )


def _room(current_cap: Any, max_cap: Any) -> float:
    """The capacity a store or container starts with, as the registry resolves it: a
    missing `current_cap` is the `max_cap`, and with neither there is no limit. A
    capacity written as text, in a family, has no limit here."""
    if isinstance(current_cap, str) or isinstance(max_cap, str):
        return float("inf")
    if current_cap is None:
        current_cap = max_cap if max_cap is not None else float("inf")
    if max_cap is None:
        max_cap = current_cap
    return min(current_cap, max_cap)


class Store(_Model):
    """A `stores` entry: how many items it holds, whether it serves by priority, the
    items it holds when the run starts, and the class it is built from.

    `current_cap` defaults to `max_cap`, and with neither the store has no limit;
    `.inf` is no limit too. `max_cap` defaults to `current_cap`. A capacity is a whole
    number or `.inf`. `items` are returned in the order given; a priority store's
    items are `[priority, item]` pairs. `type` is a `!python` subclass of `StoreBase`,
    which sets its own order, so it cannot be given with `priority`. An item is a
    constant or a `{gen: ...}` generator, drawn once when the run starts.
    `get_queue` and `put_queue` rank the waiting gets and puts by an expression, as a
    resource's `queue` ranks its requests; `priority` still ranks the items. They are
    not allowed with `type`. With `family: true` the entry is a family of stores, as for
    a resource, each member made with the `items` given.
    """

    current_cap: Optional[Union[int, float, str]] = None
    max_cap: Optional[Union[int, float, str]] = None
    priority: bool = False
    items: List[Value] = Field(default_factory=list)
    type: Optional[Type[StoreBase]] = None
    get_queue: Optional[QueueRule] = None
    put_queue: Optional[QueueRule] = None
    family: bool = False
    members: Optional[CountPlace] = None

    _capacity_numbers = field_validator("current_cap", "max_cap", mode="before")(
        _capacity_text
    )

    @model_validator(mode="after")
    def _family_form(self) -> "Store":
        _check_family(self, ("current_cap", "max_cap"))
        return self

    @model_validator(mode="after")
    def _items_fit(self) -> "Store":
        check_store_type(self.type, self.priority)
        check_ties(
            None if self.get_queue is None else self.get_queue.ties,
            None if self.put_queue is None else self.put_queue.ties,
            self.type,
        )
        room = _room(self.current_cap, self.max_cap)
        if len(self.items) > room:
            raise ValueError(
                f"the store holds {room:g} items, so it cannot start with "
                f"{len(self.items)}; raise current_cap and max_cap, or give fewer "
                f"items"
            )
        if self.priority and not all(
            (isinstance(item, list) and len(item) == 2) or isinstance(item, _Gen)
            for item in self.items
        ):
            raise ValueError("a priority store starts with [priority, item] pairs")
        return self


class Container(_Model):
    """A `containers` entry: its capacity, the largest level, its starting level and
    the class it is built from.

    `current_cap` defaults to `max_cap`, and with neither the container has no limit;
    `.inf` is no limit too. `max_cap` defaults to `current_cap`. `type` is a
    `!python` subclass of `ContainerBase`. `get_queue` and `put_queue` rank the waiting
    gets and puts by an expression, as a resource's `queue` ranks its requests; they are
    not allowed with `type`. With `family: true` the entry is a family of containers, as
    for a resource, and `current_cap`, `max_cap` and `level` may be expression text.
    """

    current_cap: Optional[Union[int, float, str]] = None
    max_cap: Optional[Union[int, float, str]] = None
    # An int stays an int, as simpy.Container keeps it
    level: Union[int, float, str] = 0
    type: Optional[Type[ContainerBase]] = None
    get_queue: Optional[QueueRule] = None
    put_queue: Optional[QueueRule] = None
    family: bool = False
    members: Optional[CountPlace] = None

    _capacity_numbers = field_validator(
        "current_cap", "max_cap", "level", mode="before"
    )(_capacity_text)

    @model_validator(mode="after")
    def _family_form(self) -> "Container":
        _check_family(self, ("current_cap", "max_cap", "level"))
        return self

    @model_validator(mode="after")
    def _type_fits(self) -> "Container":
        check_container_type(self.type)
        check_ties(
            None if self.get_queue is None else self.get_queue.ties,
            None if self.put_queue is None else self.put_queue.ties,
            self.type,
        )
        return self


def _generator_or_data(value: Any, handler: Any) -> Any:
    """A validator for the `value` of an `on:` entry: a generator is parsed, and a
    mapping with no `gen` key is data, kept as written even if a generator is nested in
    it, so it is stored and not evaluated."""
    if isinstance(value, Mapping) and "gen" not in value:
        return value
    return handler(value)


class TransferStep(_Model):
    """A task's `get` or `put`: one `container` with an `amount`, or one `store`.

    `amount` is a number or an expression such as `payload.quantity`, and 1 when it
    is left out. `priority` is for a put into a priority store, lowest served first,
    and 0 when it is left out. Text is an expression such as `attr.priority`; any
    other value is used as written: a number, a list (which orders element by
    element, as a tuple does), a mapping, null, a date or a bool. `filter` is a condition
    for a store get, such as `item.size == attr.size`, where `item.<field>` reads
    the item offered: the get takes the oldest item that matches. `wait: false` makes
    the get or put and does not wait for it: the task goes on at once. `item` is for a
    store `put`: the value to put, a constant, text, a list, a mapping or a `{gen: ...}`
    generator read as the put is made, a generator nested at any depth of a mapping
    or list included; left out, the put adds the task's payload. The
    bare name `item` in `{gen: expr, expr: item}` is the whole item a get took, so
    `item: {gen: expr, expr: item}` puts it back.
    """

    container: Optional[Union[str, Ref]] = None
    store: Optional[Union[str, Ref]] = None
    amount: Union[int, float, str] = 1.0
    priority: Any = 0.0
    filter: Optional[str] = None
    wait: bool = True
    item: Value = None

    @field_validator("priority", mode="before")
    @classmethod
    def _priority_is_a_literal(cls, value: Any) -> Any:
        if callable(value):
            raise ValueError("a priority in YAML is a literal or an expression")
        return value

    @model_validator(mode="after")
    def _one_target(self) -> "TransferStep":
        self.transfer()
        return self

    def transfer(self) -> Transfer:
        """The `Transfer` the builder takes."""
        return Transfer(
            container=_name(self.container),
            store=_name(self.store),
            amount=self.amount,
            priority=self.priority,
            filter=self.filter,
            wait=self.wait,
            item=self.item,
        )


class Rule(_Model):
    """A `next.when` entry: a condition and the task it leads to."""

    condition: str
    task: str

    @field_validator("condition")
    @classmethod
    def _parses(cls, value: str) -> str:
        Expression(value, names=ANY_NAME)
        return value


class Next(_Model):
    """A task's `next`: `when` rules with an optional `default`, or `weights`."""

    when: List[Rule] = Field(default_factory=list)
    default: Optional[str] = None
    weights: Optional[Dict[str, Union[float, str]]] = None

    @field_validator("weights", mode="before")
    @classmethod
    def _numeric_text(cls, value: Any) -> Any:
        """Text a number can be read from is that number, as it was before text was
        allowed; other text is an expression read when the task finishes."""
        if not isinstance(value, Mapping):
            return value
        read: Dict[Any, Any] = {}
        for name, weight in value.items():
            if isinstance(weight, str):
                try:
                    weight = float(weight)
                except ValueError:
                    pass
            read[name] = weight
        return read

    @model_validator(mode="after")
    def _one_form(self) -> "Next":
        if self.weights is not None and (self.when or self.default is not None):
            raise ValueError(
                "next takes weights, or when rules with a default, not both"
            )
        Router(self.rules())
        return self

    def rules(self) -> Dict[str, Any]:
        """The rules in the form `Router` and the builder take."""
        if self.weights is not None:
            return {"weights": self.weights}
        rules: Dict[str, Any] = {"when": [rule.model_dump() for rule in self.when]}
        if self.default is not None:
            rules["default"] = self.default
        return rules


def _next_form(value: Any) -> str:
    """Which form of `next` a value is, so an error reports that form only."""
    return "name" if isinstance(value, str) else "rules"


class InterruptAction(_Model):
    """An `interrupt` on a task or a scenario step: interrupt every running instance
    of `task` whose `where` condition holds, with `cause`, a constant or a
    `{gen: ...}` generator evaluated at the moment of the interrupt. A task's own
    `interrupt` may instead give `handle`, the name of a child flow the task or an
    earlier task of its flow opened with `open`, and interrupts that one child, as
    `process.interrupt(cause)` does; it takes no `where`, and it fails the run when
    the child has ended. A scenario step and an `on:` entry take `task` only."""

    task: Optional[str] = None
    handle: Optional[str] = None
    index: Optional[Union[StrictInt, str]] = None
    where: Optional[str] = None
    cause: Value = None

    @model_validator(mode="after")
    def _task_or_handle(self) -> "InterruptAction":
        if self.index is not None and self.handle is None:
            raise ValueError("interrupt takes an index with a handle only")
        if isinstance(self.index, str):
            Expression(self.index)
        if self.task is None and self.handle is None:
            raise ValueError("interrupt names a task or a handle, and has neither")
        if self.task is not None and self.handle is not None:
            raise ValueError("interrupt takes a task or a handle, and not both")
        if self.handle is not None and self.where is not None:
            raise ValueError(
                "interrupt takes no where with a handle, which names one child flow"
            )
        return self

    @field_validator("where")
    @classmethod
    def _where_parses(cls, value: Any) -> Any:
        if value is not None:
            Expression(value)
        return value

    def action(self) -> Dict[str, Any]:
        """The mapping the builder's `interrupt` takes."""
        found = {"task": self.task, "where": self.where, "cause": self.cause}
        if self.handle is not None:
            found["handle"] = self.handle
        if self.index is not None:
            found["index"] = self.index
        return found


class ResumeSpec(_Model):
    """A `resume`, a task key and an `on:` entry: wakes the instances of `task` that are
    suspended in a `wait: {suspend: true}`, in the order they suspended, as
    `event.succeed(value)` wakes the process that waits for the event. `where` keeps the
    instances whose scope it holds for (as `interrupt` reads it), `limit` is how many to
    wake at most (no `limit` wakes all, and 0 wakes nobody), and `value` is what each
    one's `wait.suspend.value` reads. `limit` and `value` are read once for each
    `resume`. A `resume` that finds nobody suspended does nothing."""

    task: str
    where: Optional[str] = None
    value: Value = None
    limit: Optional[CountPlace] = None

    @field_validator("where")
    @classmethod
    def _where_parses(cls, value: Any) -> Any:
        if value is not None:
            Expression(value)
        return value

    @field_validator("limit")
    @classmethod
    def _limit_parses(cls, value: Any) -> Any:
        if isinstance(value, str):
            Expression(value)
        return value

    def action(self) -> Dict[str, Any]:
        """The mapping the builder's `resume` takes."""
        return {
            "task": self.task,
            "where": self.where,
            "value": self.value,
            "limit": self.limit,
        }


class BatchSpec(_Model):
    """A task's `batch`: its members wait together and one run of the task's `resource`
    and service serves them all.

    The batch closes when `size` members have joined, `timeout` after its first member,
    or when `close_when` holds as a member joins, whichever is first. `size` is a
    number, or text read as an expression in the scope of the first member when it
    joins; 1 or below closes the batch at its first member. `close_when` reads
    `batch.size` (how many members it holds) and `batch.age` (seconds since its first
    member). `name` names the batch and defaults to the task's name: tasks with the same
    `name` share one open batch, so they must have the same `size`, `timeout`,
    `close_when`, `resource` and service (`service` or `service_time`), or the file
    is refused at load. Values are compared by what they mean, so a generator written
    the same way, an expression with other spacing and a number written as text agree. Each member keeps its own `on_interrupt`, `patience`, payload
    and `next`, and follows its own `next` in the order the members joined.
    """

    name: Optional[str] = None
    size: Union[int, float, str]
    timeout: Optional[TimePlace] = None
    close_when: Optional[str] = None

    @field_validator("size", mode="before")
    @classmethod
    def _size_not_boolean(cls, value: Any) -> Any:
        _refuse_boolean(value, "batch size")
        return value

    @field_validator("timeout", mode="before")
    @classmethod
    def _timeout_duration(cls, value: Any) -> Any:
        _refuse_boolean(value, "batch timeout")
        return _duration_text(value)

    @field_validator("name")
    @classmethod
    def _name_is_text(cls, value: Any) -> Any:
        if value is not None and not value.strip():
            raise ValueError("batch name is text, not empty")
        return value

    @model_validator(mode="after")
    def _expressions_parse(self) -> "BatchSpec":
        if isinstance(self.size, str):
            check_run_text(self.size, "batch size", False)
        _check_time_place(self.timeout, "batch timeout")
        if self.close_when is not None:
            try:
                Expression(self.close_when, names=ANY_NAME, batch=True)
            except ValueError as exc:
                raise ValueError(f"batch close_when: {exc}") from None
        return self

    def action(self) -> Dict[str, Any]:
        """The mapping the builder's `batch` takes."""
        given = {
            "name": self.name,
            "size": self.size,
            "timeout": self.timeout,
            "close_when": self.close_when,
        }
        return {key: value for key, value in given.items() if value is not None}


class SpawnAction(_Model):
    """A `spawn:` key or an `on:` spawn action: start `task` as a new entity, after
    an optional delay. `count` is how many entities one spawn starts: a whole number,
    an expression or a generator, read once in the parent's step (0 or less starts
    none, as `range` gives none). `copy_attributes: true` gives each child a copy of the
    parent's attributes, one level deep, so a nested mapping stays shared with the
    parent; `parent` is the parent's key as always. `args` gives each child its local
    names, read there as `arg.<name>`; each value is read in the parent's step, once for
    each child. `after` applies to each child."""

    task: str
    after: Optional[Union[int, float, str]] = None
    count: CountPlace = 1
    copy_attributes: bool = False
    args: Optional[Dict[str, Value]] = None

    @field_validator("after", mode="before")
    @classmethod
    def _after_duration(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError(f"a spawn after is seconds or a duration, got {value!r}")
        return _seconds(value) if isinstance(value, str) else value

    @field_validator("after")
    @classmethod
    def _after_positive(cls, value: Any) -> Any:
        if value is not None and not value > 0:
            raise ValueError(f"a spawn after is more than 0, got {value:g}")
        return value

    @field_validator("count")
    @classmethod
    def _count_reads_the_parent(cls, value: Any) -> Any:
        if isinstance(value, int) and value < 0:
            raise ValueError(f"a spawn count must be 0 or more, got {value!r}")
        if isinstance(value, str):
            Expression(value)
        return value

    def action(self) -> Dict[str, Any]:
        """The mapping the builder takes."""
        return {
            "task": self.task,
            "after": self.after,
            "count": self.count,
            "copy_attributes": self.copy_attributes,
            "args": self.args,
        }


def _names(value: Union[str, List[str]]) -> List[str]:
    """The arrival names of an `activate` or `deactivate`, as a list."""
    return [value] if isinstance(value, str) else list(value)


class FireAction(_Model):
    """An `on:` fire action: fire `signal` with a `value` (a constant, or a
    `{gen: ...}` generator evaluated when the action runs; a mapping with no `gen`
    key is data, and a generator nested in it is stored as written), or with the
    outcome `from` copies from a signal or a task. `urgent` runs the
    firing ahead of the other events at its time, `priority` orders it among them (the
    lowest number first), and `after` delays it, as SimPy's
    `env.schedule(event, priority, delay)` does. All three need a one-shot signal.
    `urgent` is priority 0, `priority` is an integer that is 1 when it is left out, and
    the two are not given together.

    `fail: true` fails the signal in place of firing it, as `Event.fail` does, and
    `value` is the reason; the task that fired goes on. `defused: true` is for a failure
    that no waiter handles: it does not stop the run, as `event.defused = True` does
    (`signals.<name>.defused` does the same for every failure of a signal).
    """

    signal: Union[str, Ref]
    value: Value = None
    from_: Optional[str] = Field(default=None, alias="from")
    urgent: bool = False
    after: Optional[Union[int, float, str]] = None
    priority: Optional[StrictInt] = None
    fail: bool = False
    defused: bool = False

    _value_form = field_validator("value", mode="wrap")(_generator_or_data)

    @field_validator("after", mode="before")
    @classmethod
    def _after_duration(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError(f"a fire after is seconds or a duration, got {value!r}")
        return _seconds(value) if isinstance(value, str) else value

    @field_validator("after")
    @classmethod
    def _after_positive(cls, value: Any) -> Any:
        if value is not None and not value > 0:
            raise ValueError(f"a fire after is more than 0, got {value:g}")
        return value

    @model_validator(mode="after")
    def _value_or_from(self) -> "FireAction":
        if self.value is not None and self.from_ is not None:
            raise ValueError("a fire action takes value or from, not both")
        if self.fail and self.from_ is not None:
            raise ValueError(
                "fail: true fails the signal with the value as the reason, and from "
                "copies an outcome, so a fire action takes one of them"
            )
        if self.defused and not self.fail:
            raise ValueError("defused is for a failure, so it needs fail: true")
        if self.priority is not None and self.urgent:
            raise ValueError("urgent is priority 0, so a fire takes urgent or priority")
        return self


class OnEntry(_Model):
    """One entry of an `on:` list: exactly one of a fire, spawn, interrupt or resume
    action, or a `path` with a `value` as a scenario step sets a registry value. A `value` is a
    constant or a `{gen: ...}` generator evaluated when the entry runs; a mapping with no
    `gen` key is data, and a generator nested in it is stored as written. `when` is an
    expression read when the entry would run: a false result skips the entry, and one that
    fails is a run failure."""

    fire: Optional[FireAction] = None
    spawn: Optional[SpawnAction] = None
    interrupt: Optional[InterruptAction] = None
    resume: Optional[ResumeSpec] = None
    activate: Optional[Union[str, List[str]]] = None
    deactivate: Optional[Union[str, List[str]]] = None
    path: Optional[str] = None
    value: Value = None
    when: Optional[str] = None

    _value_form = field_validator("value", mode="wrap")(_generator_or_data)

    @field_validator("when")
    @classmethod
    def _when_parses(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            Expression(value)
        return value

    @model_validator(mode="after")
    def _one_action(self) -> "OnEntry":
        actions = [
            name
            for name in (
                "fire",
                "spawn",
                "interrupt",
                "resume",
                "activate",
                "deactivate",
                "path",
            )
            if getattr(self, name) is not None
        ]
        if len(actions) != 1:
            raise ValueError(
                "an on: entry takes exactly one of fire, spawn, interrupt, resume, "
                "activate, deactivate or path"
            )
        if self.value is not None and self.path is None and self.fire is None:
            raise ValueError("on: value belongs to a path or a fire action")
        if self.interrupt is not None and self.interrupt.task is None:
            raise ValueError(
                "an on: entry interrupts a task; a handle is the interrupt key of a "
                "task, which knows the names it opened"
            )
        return self

    def entry(self) -> Dict[str, Any]:
        """The normalized mapping the builder and the runtime take."""
        found = self._action()
        if self.when is not None:
            found["when"] = self.when
        return found

    def _action(self) -> Dict[str, Any]:
        """The mapping of the action alone."""
        if self.fire is not None:
            return {
                "fire": {
                    "signal": _name(self.fire.signal),
                    "value": self.fire.value,
                    "from": self.fire.from_,
                    "urgent": self.fire.urgent,
                    "after": self.fire.after,
                    "priority": self.fire.priority,
                    "fail": self.fire.fail,
                    "defused": self.fire.defused,
                }
            }
        if self.spawn is not None:
            return {"spawn": self.spawn.action()}
        if self.interrupt is not None:
            return {"interrupt": self.interrupt.action()}
        if self.resume is not None:
            return {"resume": self.resume.action()}
        if self.activate is not None:
            return {"activate": _names(self.activate)}
        if self.deactivate is not None:
            return {"deactivate": _names(self.deactivate)}
        return {"path": self.path, "value": self.value}


class RunningTarget(_Model):
    """A `running` wait branch target: the task whose running instances to wait for,
    narrowed by an optional `where` condition read in each instance's scope. The
    wait is for the end of the instance's task, or, with `whole_flow: true`, for the
    end of the whole flow the instance belongs to (its `next` chain and the tasks it
    waits for in child flows), as `yield process` waits for a SimPy process. A flow
    selected that way is any flow of the model, and one that has ended already
    ends the wait at once; its last task's payload is the branch's value."""

    task: str
    where: Optional[str] = None
    whole_flow: bool = False

    @field_validator("where")
    @classmethod
    def _where_parses(cls, value: Any) -> Any:
        if value is not None:
            Expression(value)
        return value


class RunningWait(_Model):
    """A `wait` branch for the running instances of a task: done when they all end."""

    running: RunningTarget


class SignalWait(_Model):
    """A `wait` branch for a signal's next firing, or for a member of a family of
    signals, `{signal: {of: <family>, key: <expression>}}`."""

    signal: Union[str, Ref]


class TaskWait(_Model):
    """A `wait` branch for a child flow: it starts `task` for the same entity. `args`
    are value places read in the parent's scope when the child starts, and the child
    flow reads them as `arg.<name>`; each child has its own, as a SimPy process has its
    own arguments."""

    task: str
    args: Optional[Dict[str, Value]] = None


class TimeoutAfter(_Model):
    """A `wait` timeout with a value: `{after: 40 s, value: ...}`, as SimPy's
    `env.timeout(delay, value)` carries a value the waiter receives. The value is a
    constant or a `{gen: ...}` generator evaluated when the wait starts, so it may read
    `vars_once` names but no `vars`. `after` is a time place: seconds, a duration, an
    expression or a generator, read when the wait starts."""

    after: TimePlace
    value: Value = None


def _timeout_kind(value: Any) -> str:
    if isinstance(value, TimeoutAfter):
        return "<after>"
    if isinstance(value, Mapping) and "gen" not in value:
        return "<after>"
    return _place_kind(value)


class TimeoutWait(_Model):
    """A `wait` branch of a time place: a number of seconds, a duration such as
    `30 s`, an expression, a `{gen: ...}` generator, or `{after, value}` with a value
    the waiter receives. An expression or a generator is read when the wait starts."""

    timeout: Annotated[
        Union[
            Annotated[Generator, Tag("<gen>")],
            Annotated[Union[int, float, str], Tag("<plain>")],
            Annotated[TimeoutAfter, Tag("<after>")],
        ],
        Discriminator(_timeout_kind),
    ]


class ConditionWait(_Model):
    """A `wait` branch for a condition, checked again after each change."""

    condition: str


class AnyWait(_Model):
    """A `wait` done when the first of its named branches is."""

    any: "Dict[str, WaitBranch]"


class AllWait(_Model):
    """A `wait` done when every one of its named branches is."""

    all: "Dict[str, WaitBranch]"


class AtLeastWait(_Model):
    """A `wait` done when at least `at_least` of its named branches are."""

    at_least: int
    of: "Dict[str, WaitBranch]"


class WhenWait(_Model):
    """A `wait` done when the expression `when` holds. It is read each time a branch of
    `of` is processed, and once at the start for a branch already processed, as SimPy's
    `Condition(env, evaluate, events)` does. It reads `wait.<branch>.done`, `.ok` and
    `.value` of the branches and `done_count`. A branch that fails fails the wait, and an
    empty `of` succeeds at once."""

    when: str
    of: "Dict[str, WaitBranch]"


class RequestSpec(_Model):
    """A `request` of a `wait` or an `open` with options: the `resource`, a `priority`
    (a number or an expression read as the request is made) and a `preempt` flag (true
    or false, or an expression). With a mapping, the task's `priority` and `preempt` do
    not apply to this request: a key it leaves out is SimPy's default, priority 0 and
    preempt true. With a name they do."""

    resource: Union[str, Ref]
    priority: Optional[Union[int, float, str]] = None
    preempt: Optional[Union[bool, str]] = None

    @field_validator("preempt", mode="before")
    @classmethod
    def _preempt_words(cls, value: Any) -> Any:
        return _boolean_words(value)

    @model_validator(mode="after")
    def _expressions_parse(self) -> "RequestSpec":
        for what in ("priority", "preempt"):
            value = getattr(self, what)
            if isinstance(value, str):
                Expression(value)
        return self


class RequestWait(_Model):
    """A `wait` branch for a resource unit: the task holds it if it wins."""

    request: Union[str, Ref, RequestSpec]


class HandleWait(_Model):
    """A `wait` branch for an event that `open` made: `handle` is the name it was
    opened under. A handle already processed gives its value at once, and a handle
    waited for through an interrupt under `resume` or `restart` is the same event
    after it, with its first end and its place in the queue. `index` names one element
    of an opened `each` group; it is read when the wait starts."""

    handle: str
    index: Optional[Union[StrictInt, str]] = None

    @field_validator("index")
    @classmethod
    def _index_parses(cls, value: Any) -> Any:
        if isinstance(value, str):
            Expression(value)
        return value


class SuspendWait(_Model):
    """A `wait` branch that suspends the instance until a `resume` of another task
    wakes it (`suspend: true`). The branch's value is the `value` of that `resume`, so
    `wait.suspend.value` reads it. An interrupt that reaches a suspended instance
    follows the task's `on_interrupt`: under `resume` the instance stays suspended in
    its place, under `restart` it suspends again behind the others, and under
    `abandon` it ends. `open` does not take it."""

    suspend: Literal[True]


class GetWait(_Model):
    """A `wait` branch for a store or container get."""

    get: TransferStep


class PutWait(_Model):
    """A `wait` branch for a store or container put."""

    put: TransferStep


class AtLeastGroup(_Model):
    """The `group` of an `each` wait done when at least `at_least` elements are, a
    whole number or an expression read when the wait starts."""

    at_least: Union[StrictInt, str]

    @field_validator("at_least")
    @classmethod
    def _count_parses(cls, value: Any) -> Any:
        if isinstance(value, str):
            Expression(value)
        elif value < 1:
            raise ValueError(f"at_least is 1 or more, got {value}")
        return value


class WhenGroup(_Model):
    """The `group` of an `each` wait done when the expression `when` holds, read as
    for a `when` wait, with `done_count` and `wait.<name>.values` and `.flags`."""

    when: str

    @field_validator("when")
    @classmethod
    def _when_parses(cls, value: Any) -> Any:
        Expression(value)
        return value


# A place that takes a whole number or a list: for the `each` of a wait
EachPlace = Annotated[
    Union[
        Annotated[Generator, Tag("<gen>")],
        Annotated[Union[StrictInt, str, List[Any]], Tag("<plain>")],
    ],
    Discriminator(_place_kind),
]


class EachWait(_Model):
    """A `wait` on a computed number of events: `branch` is made once for each element
    of `each`, in order, when the wait starts. `each` is a whole number, a list, an
    expression or a generator, read once. Inside `branch`, `each.index` is the place
    from 0 and `each.value` is the element, or the index when `each` is a number.
    `group` is `all` (the default), `any`, `{at_least: n}` or `{when: expression}`. An
    empty `each` is done at once. `wait.<name>.values` and `.flags` are lists in element
    order."""

    each: EachPlace
    group: Union[Literal["all", "any"], AtLeastGroup, WhenGroup] = "all"
    branch: "WaitBranch"


# The kinds a 0.23.0 wait takes. A plain union, no discriminator: each model
# accepts only its own key under extra="forbid", so exactly one matches a valid
# mapping, on every Pydantic from 2.6.0.
WaitBranch = Union[
    SignalWait,
    TaskWait,
    TimeoutWait,
    ConditionWait,
    AnyWait,
    AllWait,
    AtLeastWait,
    WhenWait,
    RequestWait,
    GetWait,
    PutWait,
    RunningWait,
    HandleWait,
    SuspendWait,
    EachWait,
]

# What an `open` branch is written as: every wait branch but `suspend`, which a
# handle cannot hold.
OpenBranch = Union[
    SignalWait,
    TaskWait,
    TimeoutWait,
    ConditionWait,
    AnyWait,
    AllWait,
    AtLeastWait,
    WhenWait,
    RequestWait,
    GetWait,
    PutWait,
    RunningWait,
    HandleWait,
    EachWait,
]

AnyWait.model_rebuild()
EachWait.model_rebuild()
AllWait.model_rebuild()
AtLeastWait.model_rebuild()
WhenWait.model_rebuild()


def _generators_as_zero(raw: Any) -> Any:
    """`raw`, a wait branch as written, with each `{gen: ...}` timeout or `after`
    replaced by 0, so `Wait` checks the rest of the branch; the generator itself is
    checked by the typed union.

    Args:
        raw: The branch as written.
    """
    if not isinstance(raw, Mapping):
        return raw
    if "each" in raw and "branch" in raw:
        found = dict(raw)
        if isinstance(raw["each"], Mapping):
            found["each"] = 0
        found["branch"] = _generators_as_zero(raw["branch"])
        return found
    request = raw.get("request")
    if isinstance(request, Mapping) and isinstance(request.get("preempt"), str):
        # A boolean word is a boolean, as in the typed model
        words = _boolean_words(request["preempt"])
        return {**raw, "request": {**request, "preempt": words}}
    timeout = raw.get("timeout")
    if isinstance(timeout, Mapping):
        if "gen" in timeout:
            return {**raw, "timeout": 0}
        if isinstance(timeout.get("after"), Mapping) and "gen" in timeout["after"]:
            return {**raw, "timeout": {**timeout, "after": 0}}
        return raw
    out = dict(raw)
    for key in ("any", "all", "of"):
        if isinstance(raw.get(key), Mapping):
            out[key] = {
                name: _generators_as_zero(branch) for name, branch in raw[key].items()
            }
    return out


def _with_typed_values(raw: Any, typed: Any) -> Any:
    """`raw`, a wait branch as written, with each timeout `value` replaced by the
    validated one, so a generator in it is a generator and not a mapping.

    Args:
        raw: The branch as written: a mapping of one key, or `at_least` and `of`.
        typed: What the typed union made of it.
    """
    if not isinstance(raw, Mapping):
        return raw
    if "each" in raw and "branch" in raw:
        found = dict(raw)
        found["each"] = typed.each
        found["branch"] = _with_typed_values(raw["branch"], typed.branch)
        return found
    if "timeout" in raw and isinstance(raw["timeout"], Mapping):
        if "gen" in raw["timeout"]:
            return {"timeout": typed.timeout}
        after = raw["timeout"].get("after")
        typed_after = typed.timeout.after
        out = dict(raw["timeout"])
        if isinstance(after, Mapping) and isinstance(typed_after, _Gen):
            out["after"] = typed_after
        if "value" in out:
            out["value"] = typed.timeout.value
        return {"timeout": out}
    for key in ("any", "all"):
        if key in raw and isinstance(raw[key], Mapping):
            inner = getattr(typed, key)
            return {
                key: {
                    name: _with_typed_values(branch, inner[name])
                    for name, branch in raw[key].items()
                }
            }
    if "task" in raw and "args" in raw:
        return {"task": raw["task"], "args": typed.args}
    if isinstance(raw.get("request"), Mapping):
        # `preempt: "t"` is the boolean word, so the validated mapping replaces the raw
        return {"request": typed.request.model_dump(exclude_none=True)}
    if "of" in raw and isinstance(raw["of"], Mapping):
        return {
            **raw,
            "of": {
                name: _with_typed_values(branch, typed.of[name])
                for name, branch in raw["of"].items()
            },
        }
    return raw


_boolean_words = boolean_word


class SeizeSpec(_Model):
    """A `seize` with options: the `resource`, a `priority` (a number or an expression
    read as the request is made), a `preempt` flag (true or false, or an expression),
    and a `patience`, which is the task's patience for the time it waits for the unit.
    `priority` and `preempt` default to the task's. A task gives its `patience` on the
    task or here, not both."""

    resource: Union[str, Ref]
    priority: Optional[Union[int, float, str]] = None
    preempt: Optional[Union[bool, str]] = None
    patience: Optional[TimePlace] = None

    @field_validator("preempt", mode="before")
    @classmethod
    def _preempt_words(cls, value: Any) -> Any:
        return _boolean_words(value)

    @field_validator("patience", mode="before")
    @classmethod
    def _patience_duration(cls, value: Any) -> Any:
        return _duration_text(value)

    @model_validator(mode="after")
    def _expressions_parse(self) -> "SeizeSpec":
        for what in ("priority", "preempt", "patience"):
            value = getattr(self, what)
            if isinstance(value, str):
                Expression(value)
        _check_time_place(self.patience, "seize patience")
        return self


class ReleaseSpec(_Model):
    """A `release` with options: `{handle: <name>}` frees the unit of that opened
    request, and `{resource, task, where}` frees the units of `resource` that the
    instances of `task` hold, however they asked (`resource`, `seize`, `open` or a
    `request` in a wait) and whichever flow they are in, oldest grant first; `where`
    is read in the instance's scope, and without it every holder of that task is
    freed. The holder is not told. A resource name alone frees the oldest unit of it
    that the flow holds."""

    handle: Optional[str] = None
    index: Optional[Union[StrictInt, str]] = None
    resource: Optional[Union[str, Ref]] = None
    task: Optional[str] = None
    where: Optional[str] = None

    @model_validator(mode="after")
    def _one_form(self) -> "ReleaseSpec":
        Release(self.action())
        return self

    def action(self) -> Dict[str, Any]:
        """The mapping the builder's `release` takes."""
        if self.handle is not None:
            found: Dict[str, Any] = {"handle": self.handle}
            if self.index is not None:
                found["index"] = self.index
            if self.resource is not None or self.task is not None:
                found["resource"] = _name(self.resource)
            return found
        found = {"resource": _name(self.resource), "task": self.task}
        if self.where is not None:
            found["where"] = self.where
        return found


class HandleRef(_Model):
    """The name of an opened event, as `{handle: <name>}`, or one element of an opened
    `each` group, as `{handle: <name>, index: <expression>}`."""

    handle: str
    index: Optional[Union[StrictInt, str]] = None

    @field_validator("index")
    @classmethod
    def _index_parses(cls, value: Any) -> Any:
        if isinstance(value, str):
            Expression(value)
        return value


class ListenSpec(_Model):
    """A task's `listen`: callbacks added to an event, as `event.callbacks.append(f)`
    does, when the task starts, after its `open` and before it waits.

    Exactly one of `signal` and `handle` names the event: a signal, or a member of a
    family of signals `{of: <family>, key: <expression>}`, takes the callbacks on its next
    firing; a `handle` is an event the flow opened, and `index` picks one element of an
    opened `each` group. `on` lists the entries to run, each read in the scope of the
    entity that listens. They run once, on the next firing of the signal or the event of
    the handle, in the order the callbacks were added: before the waiters added later and
    after the ones added earlier. To go on listening, a flow listens again. A signal
    whose firing was processed already, or an event that was processed, takes no
    callback, which fails the task as `event.callbacks.append` raising does.
    """

    signal: Optional[Union[str, Ref]] = None
    handle: Optional[str] = None
    index: Optional[Union[StrictInt, str]] = None
    on: List[OnEntry] = Field(default_factory=list)

    @field_validator("index")
    @classmethod
    def _index_parses(cls, value: Any) -> Any:
        if isinstance(value, str):
            Expression(value)
        return value

    def mapping(self) -> Dict[str, Any]:
        """The mapping `SimulationContext.task(listen=...)` takes."""
        found: Dict[str, Any] = {"on": [entry.entry() for entry in self.on]}
        if self.signal is not None:
            found["signal"] = _name(self.signal)
        if self.handle is not None:
            found["handle"] = self.handle
        if self.index is not None:
            found["index"] = self.index
        return found

    @model_validator(mode="after")
    def _one_event(self) -> "ListenSpec":
        if (self.signal is None) == (self.handle is None):
            raise ValueError("listen takes exactly one of signal and handle")
        if self.index is not None and self.handle is None:
            raise ValueError("listen index picks an element of a handle")
        return self


def _payload_form(value: Any) -> str:
    """Which payload form a value is: `<python>` for a function, else a value."""
    if callable(value) and not isinstance(value, (dict, list, _Gen)):
        return "<python>"
    return "<value>"


class Task(_Model):
    """A `tasks` entry, built with `SimulationContext.task`.

    Text in a value place stays text. A time place reads a number as seconds, text
    `time_to_seconds` accepts as a duration, and other text as an expression.

    `payload` is the value of the `finished` event: a mapping, or a `!python`
    function called as `payload(task_id, context)`. `payload: null` publishes no
    `finished` record and the task runs its other steps; a store `put` on such a task
    is refused, because the put carries the payload. `id_field` adds the task id to a
    mapping payload under that key, `key_field` names the payload field whose value
    becomes the key of the finished event, and so the Kafka message key (the
    `queued` and `started` records keep the entity's key; a payload without the
    field fails the run), `item_field` adds the item its store `get` took,
    and `attributes` adds the named entity attributes. `key` is a value place for the
    key of the task's records, drawn once for each instance at its first record and
    written as text; it replaces `key_field`, and the two are not given together. A
    task with a `service` and no `resource` is a timed step; one with
    neither, and no `get`, emits its payload as soon as it is spawned.

    `service_time` is the task's own time, in place of a `service`, and the two are
    not given together: seconds, a duration such as `30 s`, an expression, or a
    `{gen: ...}` generator, read after the task has its resource and, for a `get`, its
    item. It may read `vars_once` and `vars` as bare names, and `item.<field>` after a
    store get. A result below 0 fails the run. A task with a `resource` needs a
    `service` or a `service_time`.

    `priority` is a number or an expression such as `attr.priority`, for a resource
    whose `queue` is `priority`. `patience` is seconds, a duration such as `30 s`, an
    expression or a generator; `balk` is a condition checked before the task joins the queue.
    `resource` is one resource or a list, acquired in the order listed; `keep` hands
    units to the next task. `on_interrupt` is `resume`, `restart` or `abandon`, and
    `interrupt` interrupts the running instances of a task when this one finishes.
    `wait` is what the task waits for first: a signal, a child task, a timeout or a
    condition, or any or all of several. `signal` names a signal the task fires
    when it finishes. `fail` is a condition checked after the service: when it holds
    the task publishes `failed` with `fail_reason`, and goes to `on_fail`. `fail_reason`
    is text, or a `{gen: ...}` generator read in the step the failure happens, and it
    is the `reason` of the record, of the failed `signal` and of `upstream.reason`. `get`
    and `put` take from and add to a store or container. `next` is a task name, `when` rules or `weights`, whose values are numbers
    or expressions read when the task finishes, with one draw for the choice. `set` gives the
    entity attribute values when the task finishes. `set_args` writes this process's own
    local names, read as `arg.<name>` by this task and by the tasks that follow in the
    same flow (not by a child flow, which has its own). It is read after `set`, so a
    payload reads the value from before the write. A parent passes names to a child with
    `args` on a `task` branch of its `wait`, or on a `spawn`; a read of a name that was
    neither passed nor written is a run failure. `activate` and `deactivate` turn the
    sources of the named arrivals on and off when the task finishes, after `set_args`.
    `cause.<name>` reads why the task was interrupted, in a task that an
    `on_interrupt_next` edge reaches: `by`, `usage_since` and `resource` after a
    preemption, `value` after an `interrupt`, and `left`, the service time that was
    left. `upstream.<field>` is the payload of the task that routed here, `item` is the
    whole item a store get took, and it lasts through the `next` chain.

    `on_interrupt_run` names a task that runs as a child flow of the same entity after
    each interrupt or preemption the task meets under `resume` or `restart`, and the
    task then goes on with the time left, or a new time under `restart`, as a
    SimPy loop `try: yield env.timeout(left) except Interrupt: yield from repair()`
    does. The child runs in the task's process. An interrupt that arrives while it
    runs reaches the child at its next wait and follows the child's own
    `on_interrupt`. The child reads `cause.<name>` as a task reached by
    `on_interrupt_next` does. It is refused with `abandon`, with a `resource` and with
    a request in the `wait`, because it is for work that holds no unit of its own. A
    child that fails fails the task.
    `on_renege` and `on_balk` name the task an instance goes to after it reneges (its
    `patience` ran out) or balks (its `balk` condition holds, or the line of its
    resource, `seize` or `open` is full), in place of the end of its flow. The
    instance still publishes `reneged` or `balked`, and that task reads the record as
    `upstream.status`. `on_fail` needs no `fail`: a failed `wait`, a failed child flow,
    a step that raises and an expression that cannot be evaluated go to it, and it reads
    the failure as `upstream.status` and `upstream.reason`. An interrupt is not a
    failure, and `on_fail` does not see it. `suspend` and `resume` are described on
    their models.

    `open` names branches that start at once and are not waited for, so a later step
    can wait for, cancel, release or interrupt them by name. A branch of `open` is a
    `timeout`, `request`, `get`, `put`, `task` or `signal`, or an `any` or `all` group
    of `timeout`, `task`, `signal` and `handle` branches. The other kinds a `wait` takes
    (`condition`, `running`, `at_least`, `when`) are refused at load, as is a
    `request`, `get` or `put` inside a group.

    `listen` adds callbacks to a signal or to an opened name when the task starts, after
    `open` and before `wait`: its `on` entries run on the next firing of the event, in
    the order the callbacks were added, as `event.callbacks.append(f)` does. An `on`
    entry of a task, a signal or a `listen` may carry a `when` expression that skips the
    entry when it is false.

    The cause of an interrupt, the holders of a resource and the entries of a queue are
    lists and mappings an expression reads: `held(m)` and `waiting(m)` for a resource,
    `putting(s)` and `getting(s)` for a store or container, `running(task)` and
    `running(task, condition)`, `peek()`, `process.key` and `process.flow_name`,
    `signal.<name>.processed`, and in the `when` of a wait group `done_count`.

    A value place takes a constant, a list, a mapping or a `{gen: ...}` generator: a
    mapping with a `gen` key is a generator, and `{gen: literal, value: ...}` keeps a
    mapping as data. The kinds of `gen` are `uniform`, `integer`, `normal`, `sample`,
    `one_of`, `weighted`, `pick`, `switch` (key `on`), `when`, `boolean`, `uuid`,
    `sequence`, `now`, `format_time`, `format`, `lower`, `upper`, `fake`, `add`,
    `subtract`, `multiply`, `divide`, `min`, `max`, `expr`, `var`, `attr`, `entity`,
    `param`, `repeat`, `literal`, `object`, `call`, `row`, `lookup`, `previous`,
    `upstream` and `aggregate`; `null_rate`, `unique`, `omit_rate`
    and `omit_when` go on any of them. `payload` may be such a mapping, or one generator that gives a
    mapping or null. A null gives a `finished` record whose value is null, which a
    Kafka sink with `envelope: false` writes as a tombstone. `id_field`, `item_field`
    and `attributes` need a mapping, so with them a null fails the task, which
    `on_fail` handles as any failure. `key_field` needs a mapping too, and with it a
    null fails the run, as a payload without the field does. A store `put` on the
    task puts the null as its item. `set` values and the `cause` of an interrupt are evaluated when the task
    finishes or interrupts. `vars_once` names are drawn once for each entity and task,
    at the first use, and may be read by a `wait` timeout `value`. `vars` are drawn in
    order, once for each instance of the task: when it starts if `on_start` is given,
    otherwise after the service, just before the payload; a `service_time` that reads
    a var draws it first. `{gen: entity}` gives the entity's key, in a task only. A var reads the vars above it
    with `{gen: var, name: ...}` or as a bare name in an `expr`. `on_start` values are
    published as one event record when the task starts. `finish_vars` are drawn after
    the service and may read the vars. A bare var name also reads in `put`, `fail` and
    `next.when`, but not in `next.weights`, which read the entity and the run. A var name is an identifier, not a name an expression already uses,
    and is in one of the three groups only. Every generator node draws from its own
    stream of the run's seed, so adding a node does not change the draws of the others.

    A task keeps its last `history` finished payloads (default 1,000,000) when a
    `lookup` or `aggregate` in the file names it, or when `history` is written. A task
    that looks up another does not start before the other has a row: an arrival's start
    is dropped, and one INFO line says how long the task waited in simulation time. A
    task reached by `next` that has no row to look up ends its flow in the same way.
    `previous` reads the last finished payload of the same task on the same stream: the
    arrival, or the entity for an arrival with `count` or `keys`. `upstream` reads the
    payload of the task that routed here, in a task reached by `next`. `aggregate` is
    `mean`, `sum`, `min`, `max` or `count` over the last rows of a task that `where`, an
    expression over the row's fields, keeps.

    An expression uses Python's arithmetic: `/` gives a float, and `//` rounds down
    and gives an integer for two integers. `and` and `or` stop at the first operand
    that decides the result and do not absorb an error: an operand that cannot be
    evaluated fails the expression. `**` raises to a power (an integer power above
    10000 is refused), `abs`, `min`, `max`, `round`, `floor`, `ceil`, `sqrt`, `exp`,
    `log`, `log10`, `sin`, `cos` and `tan` are calls on numbers, and `has(attr.x)` is
    true when the entity has the attribute `x`.

    An expression also reads a process's local names `arg.<name>`, the interrupt that
    reached the task as `cause.<name>`, the payload of the task that routed here as
    `upstream.<field>` (at any depth), `process.key` and `process.flow_name`, the
    bare `item` and `item_priority` after a store get, and `signal.<name>.processed`.
    `held(<resource>)`, `waiting(<resource>)`, `putting(<store or container>)` and
    `getting(<store or container>)` give lists of mappings (with `entity`, and `rank`,
    `time`, `preempt`, `key` and `since` for a resource, `item` for a store put and
    `amount` for a container); `running(<task>[, <condition>])` counts the instances
    of a task, and `peek()` is the time of the next event. Lists `[1, 2]`, mappings
    `{'a': 1}`, one-clause comprehensions `[x * 2 for x in list if x > 1]`,
    subscripts and slices, `is None`, `in`, and dotted reads at any depth work on
    them. The functions `len`, `sum`, `sorted`, `range`, `any`, `all`, `str`, `int`,
    `float`, `bool`, `get_key(mapping, key, default)`, `set_key(mapping, key, value)`
    and `drop_key(mapping, key)` work on lists and mappings; `min` and `max` take a
    list. `set_key` and `drop_key` give a new mapping. A reading makes at most
    1,000,000 items, and `%` on text is refused.
    """

    service: Optional[str] = None
    service_time: Optional[TimePlace] = None
    resource: Optional[Union[str, Ref, List[Union[str, Ref]]]] = None
    payload: Optional[
        Annotated[
            Union[
                Annotated[Payload, Tag("<value>")],
                Annotated[Callable[..., Any], Tag("<python>")],
            ],
            Discriminator(
                _payload_form,
                custom_error_type="payload_kind",
                custom_error_message=(
                    "a payload is a mapping, a generator that gives a mapping or "
                    "null (a record whose value is null), a !python function, or "
                    "null for no record"
                ),
            ),
        ]
    ]
    id_field: Optional[str] = None
    key: Value = None
    key_field: Optional[str] = None
    item_field: Optional[str] = None
    attributes: List[str] = Field(default_factory=list)
    priority: Optional[Union[int, float, str]] = None
    patience: Optional[TimePlace] = None
    balk: Optional[str] = None
    get: Optional[TransferStep] = None
    put: Optional[TransferStep] = None
    next: Optional[
        Annotated[
            Union[Annotated[str, Tag("name")], Annotated[Next, Tag("rules")]],
            Discriminator(_next_form),
        ]
    ] = None
    set: Dict[str, Value] = Field(default_factory=dict)
    set_args: Dict[str, Value] = Field(default_factory=dict)
    activate: Optional[Union[str, List[str]]] = None
    deactivate: Optional[Union[str, List[str]]] = None
    vars: Dict[str, Value] = Field(default_factory=dict)
    vars_once: Dict[str, Value] = Field(default_factory=dict)
    on_start: Dict[str, Value] = Field(default_factory=dict)
    finish_vars: Dict[str, Value] = Field(default_factory=dict)
    history: StrictInt = Field(default=1_000_000, ge=1)
    wait: Optional[WaitBranch] = None
    open: Optional[Dict[str, OpenBranch]] = None
    listen: Optional[ListenSpec] = None
    cancel: Optional[Union[str, HandleRef]] = None
    seize: Optional[Union[str, SeizeSpec]] = None
    release: Optional[Union[str, Ref, ReleaseSpec]] = None
    signal: Optional[Union[str, Ref]] = None
    fail: Optional[str] = None
    fail_reason: Value = None
    on_fail: Optional[str] = None
    on_renege: Optional[str] = None
    on_balk: Optional[str] = None
    keep: List[str] = Field(default_factory=list)
    preempt: Union[bool, str] = True
    on_interrupt: str = "resume"
    on_interrupt_next: Optional[str] = None
    on_interrupt_run: Optional[str] = None
    interrupt: Optional[InterruptAction] = None
    resume: Optional[ResumeSpec] = None
    batch: Optional[BatchSpec] = None
    spawn: Optional[SpawnAction] = None
    subscribe: Optional[Dict[str, Any]] = None
    on: List[OnEntry] = Field(default_factory=list)

    @field_validator("subscribe")
    @classmethod
    def _subscribe_parses(cls, value: Any) -> Any:
        if value is not None:
            if any(node.is_each for node in Wait(value).all_nodes):
                raise ValueError("subscribe does not take an each branch")
        return value

    def resources(self) -> List[str]:
        """The resources the task holds, in the order it acquires them; a computed
        member is a `Reference`, whose text is the first name it can take."""
        if self.resource is None:
            return []
        found = (
            [self.resource]
            if isinstance(self.resource, (str, Ref))
            else list(self.resource)
        )
        return [_name(item) for item in found]

    def on_lists(self) -> List[Tuple[Tuple[str, ...], List[OnEntry]]]:
        """Every list of `on:` entries the task holds, with where it is written below
        the task: its own `on`, and the `on` of its `listen`."""
        found: List[Tuple[Tuple[str, ...], List[OnEntry]]] = [(("on",), self.on)]
        if self.listen is not None:
            found.append((("listen", "on"), self.listen.on))
        return found

    def wait_requests(self) -> List[str]:
        """The resources the task's wait requests, in the order the wait lists them."""
        return [] if self.wait is None else Wait(self.wait).requests()

    @field_validator("on_interrupt")
    @classmethod
    def _known_policy(cls, value: str) -> str:
        if value not in INTERRUPT_POLICIES:
            raise ValueError(
                f"on_interrupt must be one of {', '.join(INTERRUPT_POLICIES)}"
            )
        return value

    @model_validator(mode="after")
    def _keep_and_policy(self) -> "Task":
        computed = [r for r in self.resources() if isinstance(r, Reference)]
        plain = [r for r in self.resources() if not isinstance(r, Reference)]
        if set(self.keep) & {str(r) for r in computed}:
            raise ValueError(
                "keep hands a unit to the next task, so it names a resource and not a "
                "computed member"
            )
        if not set(self.keep) <= set(self.resources()):
            raise ValueError(
                "keep names resources the task holds; list only resources in its "
                "resource"
            )
        if len(set(plain)) != len(plain):
            raise ValueError("a task lists each resource once")
        both = [
            name
            for name in self.wait_requests()
            if not isinstance(name, Reference) and name in plain
        ]
        if both:
            raise ValueError(
                f"a task holds each resource once, but resource '{both[0]}' is both "
                "requested in the wait and listed in resource; list it in only one"
            )
        if self.on_interrupt_next is not None and self.on_interrupt != "abandon":
            raise ValueError("on_interrupt_next needs on_interrupt: abandon")
        if self.on_interrupt_run is not None:
            if self.on_interrupt == "abandon":
                raise ValueError(
                    "on_interrupt_run runs a flow and then goes on with the task, so "
                    "it needs on_interrupt: resume or restart; with abandon, use "
                    "on_interrupt_next"
                )
            if self.resources():
                raise ValueError(
                    "on_interrupt_run is for work that holds no unit of its own, so "
                    "a task with resource does not take it; a repair task preempts "
                    "the unit instead, and a unit an earlier seize took is kept"
                )
            if self.wait_requests():
                raise ValueError(
                    "on_interrupt_run is for a task that holds no unit of its own, "
                    "and a request in its wait holds one"
                )
        return self

    @field_validator("wait", mode="wrap")
    @classmethod
    def _wait_parses(cls, value: Any, handler: Any) -> Any:
        # `Wait` is the one runtime parser, so its errors, with their "did you
        # mean", reach the user instead of the union's. It runs first; the handler
        # then validates the typed union. The mapping is returned unchanged, so the
        # runtime and `build.py` read the same value as before the union was typed.
        if value is None:
            return None
        Wait(_generators_as_zero(value))
        return _with_typed_values(value, handler(value))

    @field_validator("open", mode="wrap")
    @classmethod
    def _open_parses(cls, value: Any, handler: Any) -> Any:
        # As for `wait`: `Open` is the one runtime parser and runs first, and the typed
        # union then validates the generators inside the branches
        if value is None:
            return None
        if not isinstance(value, Mapping) or not value:
            Open(value)
        Open({name: _generators_as_zero(branch) for name, branch in value.items()})
        typed = handler(value)
        return {
            name: _with_typed_values(branch, typed[name])
            for name, branch in value.items()
        }

    def handles(self) -> List[str]:
        """The names the task's `open` makes, in the order written."""
        return [] if self.open is None else list(self.open)

    def seized(self) -> List[str]:
        """The resource the task's `seize` asks for, as a list of at most one."""
        if self.seize is None:
            return []
        return [
            _name(self.seize if isinstance(self.seize, str) else self.seize.resource)
        ]

    @field_validator("fail")
    @classmethod
    def _fail_parses(cls, value: Any) -> Any:
        if value is not None:
            Expression(value, names=ANY_NAME)
        return value

    @field_validator("patience", "service_time", mode="before")
    @classmethod
    def _patience_duration(cls, value: Any, info: ValidationInfo) -> Any:
        """Reads a duration such as `30 s`; any other text is an expression. A boolean
        is refused for `service_time`; a `patience` of `true` loaded before."""
        if info.field_name == "service_time":
            _refuse_boolean(value, "service_time")
        return _duration_text(value)

    @field_validator("preempt", mode="before")
    @classmethod
    def _preempt_words(cls, value: Any) -> Any:
        """Text that YAML validation reads as a boolean (`t`, `yes`, `off`, `1`) stays
        that boolean; any other text is an expression, read as each request is made."""
        return _boolean_words(value)

    def plain_requests(self) -> List[str]:
        """The resources a `wait` or an `open` request written as a name, which take the
        task's `priority` and `preempt`."""
        return ([] if self.wait is None else Wait(self.wait).plain_requests()) + (
            [] if self.open is None else Open(self.open).plain_requests()
        )

    def open_requests(self) -> List[str]:
        """The resources the task's `open` requests, in the order written."""
        return [] if self.open is None else Open(self.open).requests()

    @model_validator(mode="after")
    def _holding_keys(self) -> "Task":
        """Checks `release`, `cancel` and `seize`, and gives the task the patience its
        `seize` states, so one patience covers every wait of the task."""
        if isinstance(self.release, ReleaseSpec):
            Release(self.release.action())
        if isinstance(self.seize, SeizeSpec) and self.seize.patience is not None:
            if self.patience is not None:
                raise ValueError(
                    "patience is on the task and on its seize; give it in one place"
                )
            self.patience = self.seize.patience
        return self

    @model_validator(mode="after")
    def _resource_needs_a_service(self) -> "Task":
        if self.service is not None and self.service_time is not None:
            raise ValueError(
                "a task takes a service or a service_time, not both: service_time "
                "is the task's own time"
            )
        if (
            self.resource is not None
            and self.service is None
            and self.service_time is None
        ):
            raise ValueError("a task with a resource needs a service or a service_time")
        if self.payload is None and self.put is not None and self.put.store is not None:
            raise ValueError(
                "payload: null publishes no record, so there is no payload for a "
                "store put to carry"
            )
        if self.payload is None and self.key_field is not None:
            raise ValueError(
                "key_field names a payload field, and payload: null gives no payload"
            )
        if self.key is not None and self.key_field is not None:
            raise ValueError(
                "key and key_field both set the key of the finished record; use one"
            )
        _check_time_place(self.service_time, "service_time")
        if (
            self.priority is not None
            and self.resource is None
            and not self.wait_requests()
            and self.seize is None
            and not self.open_requests()
        ):
            raise ValueError(
                "priority orders a resource's queue, so it needs a resource, a seize "
                "or a request in its wait or open"
            )
        if (
            self.patience is not None
            and self.resource is None
            and (self.get is None or not self.get.wait)
            and self.wait is None
            and self.seize is None
            and self.batch is None
        ):
            raise ValueError(
                "patience is how long a task waits, so it needs a wait, a seize, a "
                "resource or a get"
            )
        if self.batch is not None:
            for key in ("get", "seize", "open", "cancel"):
                if getattr(self, key) is not None:
                    raise ValueError(
                        f"a batch serves its members with one run of the resource and "
                        f"service, so it takes no {key}"
                    )
        if self.fail is None and self.fail_reason is not None:
            raise ValueError("fail_reason needs a fail condition")
        if self.on_renege is not None and self.patience is None:
            raise ValueError(
                "on_renege is where a reneging task goes, so it needs patience"
            )
        if (
            self.on_balk is not None
            and self.balk is None
            and self.resource is None
            and self.seize is None
            and self.open is None
        ):
            raise ValueError(
                "on_balk is where a balking task goes, so it needs a balk, a "
                "resource, a seize or an open"
            )
        if self.put is not None and self.put.filter is not None:
            raise ValueError(
                "a put takes no filter; a filter picks the item a get takes"
            )
        if self.item_field is not None and (self.get is None or self.get.store is None):
            raise ValueError(
                "item_field adds the item a store get took, so it needs one"
            )
        if self.item_field is not None and not self.get.wait:  # type: ignore[union-attr]
            raise ValueError(
                "item_field adds the item a store get took, and a get with wait: "
                "false discards it"
            )
        if self.balk is not None and self.resource is None and self.open is None:
            raise ValueError(
                "balking refuses a resource queue, so it needs a resource or an open"
            )
        if (
            (self.priority is not None or self.preempt is not True)
            and self.resource is None
            and self.seize is None
            and (self.wait_requests() or self.open_requests())
            and not self.plain_requests()
        ):
            raise ValueError(
                "priority and preempt of a task apply to a resource, a seize or a "
                "request written as a name; every request of this task is a mapping, "
                "which takes its own priority and preempt, so put them there"
            )
        if isinstance(self.priority, str):
            Expression(self.priority)
        if isinstance(self.preempt, str):
            Expression(self.preempt)
        if isinstance(self.patience, str):
            Expression(self.patience)
        if self.balk is not None:
            Expression(self.balk)
        return self

    @model_validator(mode="after")
    def _var_names(self) -> "Task":
        owner: Dict[str, str] = {}
        for tier in ("vars_once", "vars", "finish_vars"):
            for name in getattr(self, tier):
                if not name.isidentifier() or keyword.iskeyword(name):
                    raise ValueError(
                        f"{tier}: '{name}' is not a name an expression can read; use "
                        "letters, digits and _, not starting with a digit"
                    )
                if name in RESERVED_NAMES:
                    raise ValueError(
                        f"{tier}: '{name}' is a name an expression already uses; "
                        "pick another"
                    )
                if name in owner:
                    raise ValueError(
                        f"'{name}' is in both {owner[name]} and {tier}; a name "
                        "belongs to one"
                    )
                owner[name] = tier
        return self

    @model_validator(mode="after")
    def _attributes_need_a_mapping(self) -> "Task":
        if self.attributes and not isinstance(self.payload, (dict, _Gen)):
            raise ValueError("attributes applies only to a mapping payload")
        return self

    def next_rules(self) -> Any:
        """`next` in the form the builder takes."""
        return self.next.rules() if isinstance(self.next, Next) else self.next

    @model_validator(mode="after")
    def _id_field_needs_a_mapping(self) -> "Task":
        if self.id_field is not None and not isinstance(self.payload, (dict, _Gen)):
            raise ValueError("id_field applies only to a mapping payload")
        if self.item_field is not None and not isinstance(self.payload, (dict, _Gen)):
            raise ValueError("item_field applies only to a mapping payload")
        return self


class Process(_Model):
    """A `processes` entry: a generator function and extra keyword arguments.

    The function is called as `function(context, **kwargs)` when the run starts. The
    processes are made after the arrivals, in the order they are written.
    """

    function: Callable[..., Any]
    kwargs: Dict[str, Any] = Field(default_factory=dict)

    _check_function = field_validator("function")(_generator_function)


class Connector(_Model):
    """An `ingress` entry: a connector type and its constructor arguments.

    `type` is a short name such as `Kafka`, or a `!python` class for a connector
    from another package.
    """

    type: Union[str, Type[Any]]
    config: Dict[str, Any] = Field(default_factory=dict)


class WhenExpr(_Model):
    """An egress `when` written as an expression: `when: {expr: <expression>}`.

    The expression reads the record the sink is offered: `record.stream_type`
    (`event` or `telemetry`), `record.key`, `record.task` and `record.value.<field>`.
    A name the record lacks reads as None, so `record.value.event_type ==
    'order_packed'` is false for a telemetry record and for an event without that
    field. The sink receives the record when the expression is true. An expression
    that fails on a record, such as `record.value.amount > 100` on a record whose
    value has no `amount`, fails the run.
    """

    expr: str

    @field_validator("expr")
    @classmethod
    def _parses(cls, value: str) -> str:
        Expression(value, records=True)
        return value


class EgressConnector(Connector):
    """An `egress` entry, with the per-provider options of `add_egress`.

    `when` is `history`, `live`, `events`, `telemetry`, `labels`, `trace`, a mapping
    `{expr: ...}` or a `!python` function taking one record. `history` sends the
    records stamped before `simulation.go_live_at`, and `live` sends the records
    stamped at or after it. `events` sends the event records and `telemetry` the
    telemetry records. `labels` sends the ground-truth records of scenario steps
    with a `record`, and nothing else. `trace` sends the records of `stats.trace`,
    one for each event the environment processes, and nothing else; no other entry
    receives them.

    `envelope: false` makes the entry plain output: the sink writes each event's
    value without the record around it (`stream_type`, `sim_ts`, `timestamp`, `key`,
    `task` and `instance`), and it receives no telemetry record and no `queued` or
    `started` record of a task. A Kafka sink sends a null value as a tombstone, a
    message with the record's key and a null value, and sends any other value that
    is not a mapping as JSON. A Redis sink sends a value that is not a mapping as
    JSON, null included. A storage, Iceberg or PostgreSQL sink skips it, with one
    warning. The
    message key of Kafka stays the record's key, and a `topic_router` still receives
    the whole record. Ground-truth records of a `when: labels` entry are not
    affected.
    """

    when: Optional[
        Union[
            Literal["history", "live", "events", "telemetry", "labels", "trace"],
            WhenExpr,
            Callable[[dict], bool],
        ]
    ] = None
    envelope: bool = True
    batch_size: Optional[int] = Field(default=None, gt=0)
    flush_interval: Optional[float] = Field(default=None, gt=0)

    @field_validator("when", mode="before")
    @classmethod
    def _known_name(cls, value: Any) -> Any:
        if isinstance(value, str) and value not in WHEN_NAMES:
            raise ValueError(
                f"'{value}' is not a when. Use history, live, events, telemetry, "
                f"labels, trace, a mapping with expr, or a !python function"
            )
        return value


class Batching(_Model):
    """The `batching` section: the arguments of `with_batching`."""

    batch_size: int = Field(gt=0)
    flush_interval: float = Field(gt=0)
    max_queued_batches: Optional[int] = Field(default=None, gt=0)
    drain_stall_seconds: Optional[float] = Field(default=None, gt=0)


class Telemetry(_Model):
    """A `telemetry` entry: custom readings published every `interval` seconds.

    It takes exactly one of `function` and `readings`. `function` is a `!python`
    function called as `function(context)`, which publishes with `context.publish`.
    `readings` maps a reading name to an expression read over the run (`level.`,
    `users.`, `queue.`, `signal.`, `registry[...]`, `t` and the functions of the
    expression language, and no name of an entity or a task). Each interval publishes
    one telemetry record for each reading, named `<sim_id>.<reading>`, as
    `context.publish` names one, in the order the readings are written. An expression
    that cannot be evaluated stops the run. Resource, store and container readings
    come from `stats` instead.
    """

    interval: float = Field(gt=0)
    function: Optional[Callable[..., Any]] = None
    readings: Optional[Dict[str, str]] = None

    @model_validator(mode="after")
    def _one_source(self) -> "Telemetry":
        if (self.function is None) == (self.readings is None):
            raise ValueError(
                "a telemetry entry takes exactly one of function and readings"
            )
        for name, text in (self.readings or {}).items():
            if not name:
                raise ValueError("a reading needs a name")
            check_run_text(text, f"reading '{name}'", True)
        return self


class Stats(_Model):
    """The `stats` section: the built-in statistics level and how often to publish.

    Written as a level alone (`stats: basic`) or as a mapping
    (`stats: {level: all, interval: 60}`). The interval is in seconds or a duration
    such as `5 min`. A mapping needs a `level`, unless it sets only `trace`,
    when the level is `none`. `on_change: true` also publishes a resource's readings
    at each `request` and `release` call on it, so it needs a level above `none`.
    `trace: true` publishes one record for each event the environment processes, of
    the kind `trace`, to the egress entries with `when: trace`.
    """

    level: str = DEFAULT_STATS_LEVEL
    interval: Union[int, float] = Field(default=DEFAULT_STATS_INTERVAL, gt=0)
    on_change: bool = False
    trace: bool = False

    @field_validator("interval", mode="before")
    @classmethod
    def _parse_interval(cls, value: Any) -> Any:
        return _seconds(value)

    @field_validator("level")
    @classmethod
    def _known_level(cls, value: str) -> str:
        if value not in STATS_LEVELS:
            raise ValueError(f"level must be one of {', '.join(STATS_LEVELS)}")
        return value

    @model_validator(mode="after")
    def _level_for_what_is_set(self) -> "Stats":
        if "level" not in self.model_fields_set and not (
            self.trace and not self.on_change
        ):
            raise ValueError(
                "stats needs a level, unless it sets only trace, when the "
                f"level is {DEFAULT_STATS_LEVEL}"
            )
        if self.on_change and self.level == "none":
            raise ValueError(
                "on_change publishes the statistics at each request and release, so "
                "it needs a level above none"
            )
        return self


class ScenarioStep(_Model):
    """A `scenario` entry: at simulation time `at`, set `path` to `value`, publish a
    ground-truth record of `record`, or both.

    `at` is seconds from the start of the run, or a duration such as `"10 min"`. A
    step with a `record` publishes a ground-truth record, `stream_type: label`, with
    the path, the value before and after, and the `record` mapping, to the sinks
    with `when: labels` only. A step with an `interrupt` interrupts the running
    instances of a task, as a task's `interrupt` does. A `value` and each `record`
    value are constants or `{gen: ...}` generators: `value` is evaluated just before
    the change and `record` just after it.
    """

    at: Union[int, float]
    path: Optional[str] = None
    value: Value = None
    record: Optional[Dict[str, Value]] = None
    interrupt: Optional[InterruptAction] = None

    @field_validator("at", mode="before")
    @classmethod
    def _parse_at(cls, value: Any) -> Any:
        return _seconds(value)

    @model_validator(mode="after")
    def _something_to_do(self) -> "ScenarioStep":
        if self.path is None and self.record is None and self.interrupt is None:
            raise ValueError(
                "a step needs a path and value, a record, an interrupt, or more than one"
            )
        if self.path is None and "value" in self.model_fields_set:
            raise ValueError("a value needs a path")
        if self.path is not None and "value" not in self.model_fields_set:
            raise ValueError("a path needs a value")
        if self.interrupt is not None and self.interrupt.task is None:
            raise ValueError(
                "a scenario step interrupts a task; a handle is the interrupt key of "
                "a task, which knows the names it opened"
            )
        return self


class Drain(_Model):
    """A `run.drain` mapping: the longest a drain may run after `until`."""

    limit: Optional[float] = Field(default=None, gt=0)

    @field_validator("limit", mode="before")
    @classmethod
    def _parse_limit(cls, value: Any) -> Any:
        return None if value is None else _seconds(value)


def _check_stop(value: Any) -> Any:
    """Checks a `stop` is one signal or one condition, whose text parses."""
    if value is None:
        return value
    if len(value) != 1 or not set(value) <= {"signal", "condition"}:
        raise ValueError("stop takes {signal: name} or {condition: ...}")
    if "condition" in value:
        Expression(value["condition"])
    if isinstance(value.get("signal"), Mapping):
        name_of_stop = Ref.model_validate(value["signal"])
        value = {**value, "signal": name_of_stop.name().mapping()}
    return value


class RunSegment(_Model):
    """A `run.segments` entry: the run goes on to `until` or to `stop`, then waits for
    the next entry.

    An entry has at most one of `until` and `stop`; `{}` runs to the end. After the
    segment stops, `look` names a task whose new entity reads the model before any
    event at the stop time runs, and `sync` re-anchors the wall clock at the start of
    the segment. A record at `system.simulation.segment` reports `index`, `now` and
    `peek` (null when no model event is left) as the segment ends.
    """

    until: Optional[Union[int, float, str]] = None
    stop: Optional[Dict[str, Any]] = None
    look: Optional[str] = None
    sync: bool = False

    @field_validator("until", mode="before")
    @classmethod
    def _parse_until(cls, value: Any) -> Any:
        return None if value is None else _seconds(value)

    @field_validator("stop")
    @classmethod
    def _one_stop(cls, value: Any) -> Any:
        return _check_stop(value)

    @model_validator(mode="after")
    def _one_end(self) -> "RunSegment":
        if self.until is not None and self.stop is not None:
            raise ValueError("a segment takes until or stop, not both")
        return self


class SegmentEvery(_Model):
    """`run.segments: {every: 10 s}`: a segment every `every` seconds up to `run.until`."""

    every: Union[int, float, str]

    @field_validator("every", mode="before")
    @classmethod
    def _parse_every(cls, value: Any) -> Any:
        number = _seconds(value)
        if number <= 0:
            raise ValueError("every must be above 0")
        return number


class Run(_Model):
    """The `run` section.

    `before` lists `!python` functions called with no arguments before the run
    starts, for setup such as creating topics or tables. `stop` ends the run before
    `until` at the next firing of a signal, `{signal: name}`, or when a condition
    first holds, `{condition: ...}`. `drain`, `true` or `{limit: 1 h}`, stops the
    arrivals at `until` and runs on until no entity is left in the model.

    `segments` runs the model in parts and publishes a reading after each, as a
    SimPy program that calls `env.run(until=...)` in a loop does: a list of
    `RunSegment`, or `{every: <time>}` for equal parts up to `until`. `steps: true`
    publishes `system.simulation.step`, `{step, now, peek}`, after each model event, as
    a loop of `env.step()` and `env.peek()` reads them. `sync: true` calls `env.sync()`
    as the run starts. A run with no `until` ends when no model event is left, except
    that a list of segments with no `until` whose last entry has an `until` or a
    `stop` ends there, as a SimPy program that stops calling `env.run` does; a last
    entry `{}` runs to the end.
    """

    until: Optional[Union[int, float]] = Field(default=None, gt=0)
    before: List[Callable[[], Any]] = Field(default_factory=list)
    stop: Optional[Dict[str, Any]] = None
    drain: Union[bool, Drain] = False
    on_unhandled_failure: Literal["stop", "continue"] = "stop"
    segments: Optional[Union[List[RunSegment], SegmentEvery]] = None
    steps: bool = False
    sync: bool = False

    @field_validator("stop")
    @classmethod
    def _one_stop(cls, value: Any) -> Any:
        return _check_stop(value)

    @model_validator(mode="after")
    def _drain_needs_until(self) -> "Run":
        if self.drain is not False and self.until is None:
            raise ValueError("drain starts at until, so it needs until")
        return self

    def drain_setting(self) -> Any:
        """`drain` in the form `SimulationContext.run` takes."""
        if isinstance(self.drain, Drain):
            return {"limit": self.drain.limit}
        return self.drain or None

    @field_validator("until", mode="before")
    @classmethod
    def _parse_until(cls, value: Any) -> Any:
        return None if value is None else _seconds(value)


class SignalSpec(_Model):
    """A `signals` entry in the mapping form: `{closing: {once: true, on: [...]}}`.

    A signal fires once, as SimPy's `Event` does, unless it says `once: false`, which
    re-arms it after each firing. `on` lists actions run when the signal fires, as a
    task's `on` lists actions run when it ends. `defused: true` makes every failure of
    the signal that no waiter handles go unraised, as a callback's `event.defused = True`
    does; without it, a one-shot signal's failure with no waiter stops the run under
    `on_unhandled_failure: stop`. `family: true` makes the entry a family
    of signals, one for each key, as for a resource; `members` makes the keys 0 to n - 1
    when the run is built.
    """

    once: bool = True
    on: List[OnEntry] = Field(default_factory=list)
    defused: bool = False
    family: bool = False
    members: Optional[CountPlace] = None

    @model_validator(mode="after")
    def _family_form(self) -> "SignalSpec":
        _check_family(self, ())
        return self


class ExpectedWithin(_Model):
    """An `expect` value with a tolerance: the measured statistic passes when it is
    within `within` of `value`, in absolute terms. Use it for a value whose last
    digits change with the platform's floating point."""

    value: Union[StrictInt, StrictFloat]
    within: Union[StrictInt, StrictFloat] = Field(ge=0)


class BlueprintTest(_Model):
    """A `tests` entry: the statistics a model must give at one seed, checked by
    `desflow test`.

    `expect` maps a statistic name, as `measures` in an experiment file names it
    (`tasks.pack_order.finished`), to the number it must have, or to `{value, within}`
    for a tolerance. A number is compared exactly. `seed` defaults to the file's
    `simulation.random_seed` and `until` to `run.until`. The entry runs the model
    once, unpaced, with no ingress and no egress, so a test contacts no service.

    A test states the measures at one seed in one version of desflow. A new release
    may draw its random numbers differently, so a test does not promise that exact
    seeded records or values stay the same across versions; run
    `desflow test --fix` to write the new values and read them before you keep them.
    """

    name: str
    seed: Optional[StrictInt] = None
    until: Optional[Union[int, float, str]] = None
    expect: Dict[str, Union[ExpectedWithin, StrictInt, StrictFloat]] = Field(
        default_factory=dict
    )

    @field_validator("until", mode="before")
    @classmethod
    def _parse_until(cls, value: Any) -> Any:
        if value is None:
            return None
        number = _seconds(value)
        if not number > 0:
            raise ValueError("until must be above 0")
        return number


def _distinct_test_names(tests: List[BlueprintTest]) -> List[BlueprintTest]:
    names = [test.name for test in tests]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise ValueError(
            f"each test needs its own name; repeated: {', '.join(repeated)}"
        )
    return tests


class TestsFile(_Model):
    """The `tests.yaml` that `desflow test` reads beside a builder file, which has
    no `tests` key of its own: a mapping with the same `tests` list a blueprint
    holds."""

    __test__ = False  # pytest would otherwise collect this class

    tests: List[BlueprintTest]

    @field_validator("tests")
    @classmethod
    def _test_names_differ(cls, value: List[BlueprintTest]) -> List[BlueprintTest]:
        return _distinct_test_names(value)


class Blueprint(_Model):
    """A whole blueprint file.

    File order is creation order: the arrivals are made in the order they are
    written, then the `processes`. Events due at one time run in the order they were
    scheduled, so this order decides only ties between first due times.
    """

    simulation: Simulation
    resources: Dict[str, Resource] = Field(default_factory=dict)
    containers: Dict[str, Container] = Field(default_factory=dict)
    stores: Dict[str, Store] = Field(default_factory=dict)
    services: Dict[str, Distribution] = Field(default_factory=dict)
    arrivals: Dict[str, Arrival] = Field(default_factory=dict)
    variables: Dict[str, Any] = Field(default_factory=dict)
    signals: Dict[str, SignalSpec] = Field(default_factory=dict)
    tasks: Dict[str, Task] = Field(default_factory=dict)
    telemetry: List[Telemetry] = Field(default_factory=list)
    processes: List[Process] = Field(default_factory=list)
    scenario: List[ScenarioStep] = Field(default_factory=list)
    ingress: List[Connector] = Field(default_factory=list)
    egress: List[EgressConnector] = Field(default_factory=list)
    batching: Optional[Batching] = None
    stats: Optional[Stats] = None
    run: Run = Field(default_factory=Run)
    tests: List[BlueprintTest] = Field(default_factory=list)

    @field_validator("tests")
    @classmethod
    def _test_names_differ(cls, value: List[BlueprintTest]) -> List[BlueprintTest]:
        return _distinct_test_names(value)

    @field_validator("signals", mode="before")
    @classmethod
    def _signal_mapping(cls, value: Any) -> Any:
        """Lets `signals` be written as a list of names, as before, or a mapping of
        name to `{once, on}`; a name alone is a one-shot signal."""
        if isinstance(value, list):
            return {name: {} for name in value}
        return value

    @field_validator("stats", mode="before")
    @classmethod
    def _bare_level(cls, value: Any) -> Any:
        """Lets `stats` be written as a level alone."""
        return {"level": value} if isinstance(value, str) else value

    @field_validator("processes", mode="before")
    @classmethod
    def _bare_functions(cls, value: Any) -> Any:
        """Lets a process be written as a bare `!python` reference."""
        if isinstance(value, list):
            return [
                item if isinstance(item, dict) else {"function": item} for item in value
            ]
        return value


def unknown_key(model: Type[BaseModel], location: tuple) -> str:
    """The message for a key no model at `location` takes, with the closest one.

    Args:
        model: The model of the whole file, such as `Blueprint`.
        location: The key's location in the file, as pydantic gives it.

    Returns:
        str: "unknown key 'x'", then the closest key that place takes, or the keys
            it takes when none is close.
    """
    types: List[Any] = [model]
    kind_next = False
    for part in location[:-1]:
        if isinstance(part, str) and part.startswith("<") and part.endswith(">"):
            kind_next = part == "<gen>"
            continue
        if kind_next:
            # The part after `<gen>` is the value of `gen`, which names the generator
            kind_next = False
            if part in GENERATORS:
                types = [GENERATORS[part]]
            continue
        inner = [found for kind in types for found in _inner(kind, part)]
        # A union's tag, such as `rules`, is in the location but not in the file
        types = inner or (types if isinstance(part, str) else [])
    keys = sorted(
        {
            field.alias or name
            for kind in types
            if inspect.isclass(kind) and issubclass(kind, BaseModel)
            for name, field in kind.model_fields.items()
        }
    )
    key = str(location[-1])
    message = f"unknown key '{key}'"
    hint = suggestion(key, keys)
    if hint:
        return message + hint
    return f"{message}; this place takes {', '.join(keys)}" if keys else message


def _leaves(kind: Any) -> List[Any]:
    """The types `kind` can be, with Optional, Union and Annotated taken apart."""
    origin = get_origin(kind)
    if origin is Annotated:
        return _leaves(get_args(kind)[0])
    if origin is Union or origin is UnionType:
        return [leaf for arg in get_args(kind) for leaf in _leaves(arg)]
    return [kind]


def _inner(kind: Any, part: Any) -> List[Any]:
    """The types at `part` inside a value of type `kind`."""
    found = []
    for leaf in _leaves(kind):
        origin = get_origin(leaf)
        if inspect.isclass(leaf) and issubclass(leaf, BaseModel):
            for name, field in leaf.model_fields.items():
                if part in (name, field.alias):
                    found.append(field.annotation)
        elif origin in (dict, Dict) and isinstance(part, str):
            found.append(get_args(leaf)[1])
        elif origin in (list, List, tuple) and isinstance(part, int):
            found.append(get_args(leaf)[0])
    return found
