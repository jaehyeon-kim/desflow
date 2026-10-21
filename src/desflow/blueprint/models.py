"""Pydantic models for a blueprint file.

Every model forbids unknown keys, so a misspelt key is reported rather than ignored.
Distributions are checked by building the `DistributionConfig` the builder registers,
so YAML and Python accept the same families and parameters. Fields that take Python
objects are filled by `!python` references and checked here, so a wrong reference
fails when the file is loaded rather than during the run.
"""

import inspect
from datetime import datetime, timedelta
from types import UnionType
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
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
    INTERRUPT_POLICIES,
    LABELS,
    QUEUE_ORDERS,
    STATS_LEVELS,
)
from desflow.core.distributions import (
    PARAMETERS,
    TIME_LIST_PARAMETERS,
    TIME_PARAMETERS,
)
from desflow.core.expressions import Expression
from desflow.core.environment import in_zone, resolve_zone
from desflow.core.flow import Router, Transfer, Wait
from desflow.models.params import DistributionConfig
from desflow.resources.resource import ResourceBase, check_resource_type
from desflow.utils import suggestion, time_to_seconds

# The names an egress `when` takes instead of a `!python` function.
WHEN_NAMES = ("history", "live", LABELS)


def _seconds(value: Union[float, str]) -> float:
    """Accepts a number of seconds or a duration such as `"10 min"`."""
    if isinstance(value, str):
        return time_to_seconds(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    raise ValueError("expected seconds or a duration such as '10 min'")


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
    """

    sim_id: str
    factor: float = 1.0
    random_seed: Optional[int] = None
    # Before the times, so `now` and times without an offset can be read in it
    timezone: Optional[str] = None
    logical_start_time: Optional[datetime] = None
    go_live_at: Optional[datetime] = None
    warmup: Optional[float] = Field(default=None, ge=0)
    initial_time: float = 0.0
    # Seconds between lag readings; unset is off at factor 0 and 1 s otherwise
    lag_interval: Optional[float] = Field(default=None, ge=0)

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
    rate: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    low: Optional[float] = None
    value: Optional[float] = None
    high: Optional[float] = None
    mode: Optional[float] = None
    shape: Optional[float] = None
    scale: Optional[float] = None
    values: Optional[List[float]] = None
    weights: Optional[List[float]] = None
    edges: Optional[List[float]] = None
    counts: Optional[List[float]] = None

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
    it spawns, and the attributes each entity it starts carries."""

    dist: Optional[str] = None  # type: ignore[assignment]
    replay: Optional[Replay] = None
    spawn: Optional[str] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _distribution_in_range(self) -> "Arrival":
        if (self.dist is None) == (self.replay is None):
            raise ValueError("an arrival takes a dist or a replay, not both or neither")
        if self.replay is not None and self.parameters():
            raise ValueError(
                "a replayed arrival takes no distribution parameters; remove them, "
                "or the replay"
            )
        if self.dist is not None:
            self.config()
        return self


class Breakdown(_Model):
    """A resource's `breakdown`: the time to each failure, the repair time, and
    whether the tasks being served are interrupted or finish."""

    time_to_failure: Distribution
    repair: Distribution
    work: Literal["interrupt", "finish"] = "interrupt"


class Resource(_Model):
    """A `resources` entry: its capacity, the order its queue is served in, whether
    a request of a lower priority value preempts a user, how many requests may wait,
    the class it is built from, and how it breaks down.

    `current_cap` defaults to 1 and `max_cap` to `current_cap`, so `{}` is a
    resource of one unit. `.inf` makes a resource with no limit. `type` is a
    `!python` subclass of `ResourceBase`.
    """

    current_cap: Optional[float] = None
    max_cap: Optional[float] = None
    queue: str = DEFAULT_QUEUE_ORDER
    preempt: bool = False
    queue_limit: Optional[int] = None
    type: Optional[Type[ResourceBase]] = None
    breakdown: Optional[Breakdown] = None

    @model_validator(mode="after")
    def _preempt_needs_priority(self) -> "Resource":
        if self.preempt and self.queue != "priority":
            raise ValueError(
                "preempt ranks users by priority, so it needs queue: priority"
            )
        return self

    @model_validator(mode="after")
    def _capacity_and_queue_fit(self) -> "Resource":
        if self.current_cap is None and self.max_cap is not None and self.max_cap < 1:
            raise ValueError(
                f"max_cap {self.max_cap} is below 1, the default current_cap; give "
                f"current_cap {self.max_cap} or less"
            )
        check_resource_type(self.type, self.queue_limit)
        return self

    @field_validator("queue")
    @classmethod
    def _known_order(cls, value: str) -> str:
        if value not in QUEUE_ORDERS:
            raise ValueError(f"queue must be one of {', '.join(QUEUE_ORDERS)}")
        return value


class Store(_Model):
    """A `stores` entry: how many items it holds, whether it serves by priority, and
    the items it holds when the run starts.

    `items` are returned in the order given; a priority store's items are
    `[priority, item]` pairs.
    """

    current_cap: float
    max_cap: float
    priority: bool = False
    items: List[Any] = Field(default_factory=list)

    @model_validator(mode="after")
    def _items_fit(self) -> "Store":
        room = min(self.current_cap, self.max_cap)
        if len(self.items) > room:
            raise ValueError(
                f"the store holds {room:g} items, so it cannot start with "
                f"{len(self.items)}; raise current_cap and max_cap, or give fewer "
                f"items"
            )
        if self.priority and not all(
            isinstance(item, list) and len(item) == 2 for item in self.items
        ):
            raise ValueError("a priority store starts with [priority, item] pairs")
        return self


class Container(_Model):
    """A `containers` entry: its capacity, the largest level, and its starting level."""

    current_cap: float
    max_cap: float
    # An int stays an int, as simpy.Container keeps it
    level: Union[int, float] = 0


class TransferStep(_Model):
    """A task's `get` or `put`: one `container` with an `amount`, or one `store`.

    `amount` is a number or an expression such as `payload.quantity`, and 1 when it
    is left out. `priority` is a number or an expression for a put into a priority
    store, lowest served first, and 0 when it is left out. `filter` is a condition
    for a store get, such as `item.size == attr.size`, where `item.<field>` reads
    the item offered: the get takes the oldest item that matches.
    """

    container: Optional[str] = None
    store: Optional[str] = None
    amount: Union[float, str] = 1.0
    priority: Union[float, str] = 0.0
    filter: Optional[str] = None

    @model_validator(mode="after")
    def _one_target(self) -> "TransferStep":
        self.transfer()
        return self

    def transfer(self) -> Transfer:
        """The `Transfer` the builder takes."""
        return Transfer(
            container=self.container,
            store=self.store,
            amount=self.amount,
            priority=self.priority,
            filter=self.filter,
        )


class Rule(_Model):
    """A `next.when` entry: a condition and the task it leads to."""

    condition: str
    task: str

    @field_validator("condition")
    @classmethod
    def _parses(cls, value: str) -> str:
        Expression(value)
        return value


class Next(_Model):
    """A task's `next`: `when` rules with an optional `default`, or `weights`."""

    when: List[Rule] = Field(default_factory=list)
    default: Optional[str] = None
    weights: Optional[Dict[str, float]] = None

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
    of `task` whose `where` condition holds, with `cause`."""

    task: str
    where: Optional[str] = None
    cause: Any = None

    @field_validator("where")
    @classmethod
    def _where_parses(cls, value: Any) -> Any:
        if value is not None:
            Expression(value)
        return value

    def action(self) -> Dict[str, Any]:
        """The mapping the builder's `interrupt` takes."""
        return {"task": self.task, "where": self.where, "cause": self.cause}


class SpawnAction(_Model):
    """A `spawn:` key or an `on:` spawn action: start `task` as a new entity, after
    an optional delay."""

    task: str
    after: Optional[Union[float, str]] = None

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


class FireAction(_Model):
    """An `on:` fire action: fire `signal` with a literal `value`, or with the
    outcome `from` copies from a signal or a task."""

    signal: str
    value: Any = None
    from_: Optional[str] = Field(default=None, alias="from")

    @model_validator(mode="after")
    def _value_or_from(self) -> "FireAction":
        if self.value is not None and self.from_ is not None:
            raise ValueError("a fire action takes value or from, not both")
        return self


class OnEntry(_Model):
    """One entry of an `on:` list: exactly one of a fire, spawn or interrupt action,
    or a `path` with a `value` as a scenario step sets a registry value."""

    fire: Optional[FireAction] = None
    spawn: Optional[SpawnAction] = None
    interrupt: Optional[InterruptAction] = None
    path: Optional[str] = None
    value: Any = None

    @model_validator(mode="after")
    def _one_action(self) -> "OnEntry":
        actions = [
            name
            for name in ("fire", "spawn", "interrupt", "path")
            if getattr(self, name) is not None
        ]
        if len(actions) != 1:
            raise ValueError(
                "an on: entry takes exactly one of fire, spawn, interrupt or path"
            )
        if self.value is not None and self.path is None and self.fire is None:
            raise ValueError("on: value belongs to a path or a fire action")
        return self

    def entry(self) -> Dict[str, Any]:
        """The normalized mapping the builder and the runtime take."""
        if self.fire is not None:
            return {
                "fire": {
                    "signal": self.fire.signal,
                    "value": self.fire.value,
                    "from": self.fire.from_,
                }
            }
        if self.spawn is not None:
            return {"spawn": {"task": self.spawn.task, "after": self.spawn.after}}
        if self.interrupt is not None:
            return {"interrupt": self.interrupt.action()}
        return {"path": self.path, "value": self.value}


class RunningTarget(_Model):
    """A `running` wait branch target: the task whose running instances to wait for,
    narrowed by an optional `where` condition read in each instance's scope."""

    task: str
    where: Optional[str] = None

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
    """A `wait` branch for a signal's next firing."""

    signal: str


class TaskWait(_Model):
    """A `wait` branch for a child flow: it starts `task` for the same entity."""

    task: str


class TimeoutAfter(_Model):
    """A `wait` timeout with a value: `{after: 40 s, value: ...}`, as SimPy's
    `env.timeout(delay, value)` carries a value the waiter receives."""

    after: Union[float, str]
    value: Any = None


class TimeoutWait(_Model):
    """A `wait` branch of a number of seconds, a duration such as `30 s`, or
    `{after, value}` with a value the waiter receives."""

    timeout: Union[float, str, TimeoutAfter]


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


class RequestWait(_Model):
    """A `wait` branch for a resource unit: the task holds it if it wins."""

    request: str


class GetWait(_Model):
    """A `wait` branch for a store or container get."""

    get: TransferStep


class PutWait(_Model):
    """A `wait` branch for a store or container put."""

    put: TransferStep


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
    RequestWait,
    GetWait,
    PutWait,
    RunningWait,
]

AnyWait.model_rebuild()
AllWait.model_rebuild()
AtLeastWait.model_rebuild()


class Task(_Model):
    """A `tasks` entry, built with `SimulationContext.task`.

    `payload` is the value of the `finished` event: a mapping, or a `!python`
    function called as `payload(task_id, context)`. `id_field` adds the task id to a
    mapping payload under that key, `item_field` adds the item its store `get` took,
    and `attributes` adds the named entity attributes. A task with a `service` and no `resource` is a timed step; one with
    neither, and no `get`, emits its payload as soon as it is spawned.

    `priority` is a number or an expression such as `attr.priority`, for a resource
    whose `queue` is `priority`. `patience` is seconds, a duration such as `30 s` or
    an expression; `balk` is a condition checked before the task joins the queue.
    `resource` is one resource or a list, acquired in the order listed; `keep` hands
    units to the next task. `on_interrupt` is `resume`, `restart` or `abandon`, and
    `interrupt` interrupts the running instances of a task when this one finishes.
    `wait` is what the task waits for first: a signal, a child task, a timeout or a
    condition, or any or all of several. `signal` names a signal the task fires
    when it finishes. `fail` is a condition checked after the service: when it holds
    the task publishes `failed` with `fail_reason`, and goes to `on_fail`. `get` and `put` take from and add to a store or
    container. `next` is a task name, `when` rules or `weights`. `set` gives the
    entity attribute values when the task finishes.
    """

    service: Optional[str] = None
    resource: Optional[Union[str, List[str]]] = None
    payload: Union[Dict[str, Any], Callable[..., Any]]
    id_field: Optional[str] = None
    item_field: Optional[str] = None
    attributes: List[str] = Field(default_factory=list)
    priority: Optional[Union[float, str]] = None
    patience: Optional[Union[float, str]] = None
    balk: Optional[str] = None
    get: Optional[TransferStep] = None
    put: Optional[TransferStep] = None
    next: Optional[
        Annotated[
            Union[Annotated[str, Tag("name")], Annotated[Next, Tag("rules")]],
            Discriminator(_next_form),
        ]
    ] = None
    set: Dict[str, Any] = Field(default_factory=dict)
    wait: Optional[WaitBranch] = None
    signal: Optional[str] = None
    fail: Optional[str] = None
    fail_reason: Optional[str] = None
    on_fail: Optional[str] = None
    keep: List[str] = Field(default_factory=list)
    preempt: bool = True
    on_interrupt: str = "resume"
    on_interrupt_next: Optional[str] = None
    interrupt: Optional[InterruptAction] = None
    spawn: Optional[SpawnAction] = None
    subscribe: Optional[Dict[str, Any]] = None
    on: List[OnEntry] = Field(default_factory=list)

    @field_validator("subscribe")
    @classmethod
    def _subscribe_parses(cls, value: Any) -> Any:
        if value is not None:
            Wait(value)
        return value

    def resources(self) -> List[str]:
        """The resources the task holds, in the order it acquires them."""
        if self.resource is None:
            return []
        return (
            [self.resource] if isinstance(self.resource, str) else list(self.resource)
        )

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
        if not set(self.keep) <= set(self.resources()):
            raise ValueError(
                "keep names resources the task holds; list only resources in its "
                "resource"
            )
        if len(set(self.resources())) != len(self.resources()):
            raise ValueError("a task lists each resource once")
        if self.on_interrupt_next is not None and self.on_interrupt != "abandon":
            raise ValueError("on_interrupt_next needs on_interrupt: abandon")
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
        Wait(value)
        handler(value)
        return value

    @field_validator("fail")
    @classmethod
    def _fail_parses(cls, value: Any) -> Any:
        if value is not None:
            Expression(value)
        return value

    @field_validator("patience", mode="before")
    @classmethod
    def _patience_duration(cls, value: Any) -> Any:
        """Reads a duration such as `30 s`; any other text is an expression."""
        if isinstance(value, str):
            try:
                return time_to_seconds(value)
            except ValueError:
                return value
        return value

    @model_validator(mode="after")
    def _resource_needs_a_service(self) -> "Task":
        if self.resource is not None and self.service is None:
            raise ValueError("a task with a resource needs a service")
        if self.priority is not None and self.resource is None:
            raise ValueError(
                "priority orders a resource's queue, so it needs a resource"
            )
        if (
            self.patience is not None
            and self.resource is None
            and self.get is None
            and self.wait is None
        ):
            raise ValueError(
                "patience is how long a task waits, so it needs a wait, a resource "
                "or a get"
            )
        if self.fail is None and (
            self.fail_reason is not None or self.on_fail is not None
        ):
            raise ValueError("fail_reason and on_fail need a fail condition")
        if self.put is not None and self.put.filter is not None:
            raise ValueError(
                "a put takes no filter; a filter picks the item a get takes"
            )
        if self.item_field is not None and (self.get is None or self.get.store is None):
            raise ValueError(
                "item_field adds the item a store get took, so it needs one"
            )
        if self.balk is not None and self.resource is None:
            raise ValueError("balking refuses a resource queue, so it needs a resource")
        if isinstance(self.priority, str):
            Expression(self.priority)
        if isinstance(self.patience, str):
            Expression(self.patience)
        if self.balk is not None:
            Expression(self.balk)
        return self

    @model_validator(mode="after")
    def _attributes_need_a_mapping(self) -> "Task":
        if self.attributes and not isinstance(self.payload, dict):
            raise ValueError("attributes applies only to a mapping payload")
        return self

    def next_rules(self) -> Any:
        """`next` in the form the builder takes."""
        return self.next.rules() if isinstance(self.next, Next) else self.next

    @model_validator(mode="after")
    def _id_field_needs_a_mapping(self) -> "Task":
        if self.id_field is not None and not isinstance(self.payload, dict):
            raise ValueError("id_field applies only to a mapping payload")
        if self.item_field is not None and not isinstance(self.payload, dict):
            raise ValueError("item_field applies only to a mapping payload")
        return self


class Process(_Model):
    """A `processes` entry: a generator function and extra keyword arguments.

    The function is called as `function(context, **kwargs)` when the run starts.
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


class EgressConnector(Connector):
    """An `egress` entry, with the per-provider options of `add_egress`.

    `when` is `history`, `live`, `labels` or a `!python` function taking one record.
    `history` sends the records stamped before `simulation.go_live_at`, and `live`
    sends the records stamped at or after it. `labels` sends the ground-truth records
    of scenario steps with a `record`, and nothing else.
    """

    when: Optional[
        Union[Literal["history", "live", "labels"], Callable[[dict], bool]]
    ] = None
    batch_size: Optional[int] = Field(default=None, gt=0)
    flush_interval: Optional[float] = Field(default=None, gt=0)

    @field_validator("when", mode="before")
    @classmethod
    def _known_name(cls, value: Any) -> Any:
        if isinstance(value, str) and value not in WHEN_NAMES:
            raise ValueError(
                f"'{value}' is not a when. Use history, live, labels or a !python "
                f"function"
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

    `function` is a `!python` function called as `function(context)`, which
    publishes with `context.publish`. Resource, store and container readings come
    from `stats` instead.
    """

    interval: float = Field(gt=0)
    function: Callable[..., Any]


class Stats(_Model):
    """The `stats` section: the built-in statistics level and how often to publish.

    Written as a level alone (`stats: basic`) or as a mapping
    (`stats: {level: all, interval: 60}`). The interval is in seconds or a duration
    such as `5 min`.
    """

    level: str
    interval: float = Field(default=DEFAULT_STATS_INTERVAL, gt=0)

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


class ScenarioStep(_Model):
    """A `scenario` entry: at simulation time `at`, set `path` to `value`, publish a
    ground-truth record of `record`, or both.

    `at` is seconds from the start of the run, or a duration such as `"10 min"`. A
    step with a `record` publishes a ground-truth record, `stream_type: label`, with
    the path, the value before and after, and the `record` mapping, to the sinks
    with `when: labels` only. A step with an `interrupt` interrupts the running
    instances of a task, as a task's `interrupt` does.
    """

    at: float
    path: Optional[str] = None
    value: Any = None
    record: Optional[Dict[str, Any]] = None
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
        return self


class Drain(_Model):
    """A `run.drain` mapping: the longest a drain may run after `until`."""

    limit: Optional[float] = Field(default=None, gt=0)

    @field_validator("limit", mode="before")
    @classmethod
    def _parse_limit(cls, value: Any) -> Any:
        return None if value is None else _seconds(value)


class Run(_Model):
    """The `run` section.

    `before` lists `!python` functions called with no arguments before the run
    starts, for setup such as creating topics or tables. `stop` ends the run before
    `until` at the next firing of a signal, `{signal: name}`, or when a condition
    first holds, `{condition: ...}`. `drain`, `true` or `{limit: 1 h}`, stops the
    arrivals at `until` and runs on until no entity is left in the model.
    """

    until: Optional[float] = Field(default=None, gt=0)
    before: List[Callable[[], Any]] = Field(default_factory=list)
    stop: Optional[Dict[str, str]] = None
    drain: Union[bool, Drain] = False
    on_unhandled_failure: Literal["stop", "continue"] = "continue"

    @field_validator("stop")
    @classmethod
    def _one_stop(cls, value: Any) -> Any:
        if value is None:
            return value
        if len(value) != 1 or not set(value) <= {"signal", "condition"}:
            raise ValueError("stop takes {signal: name} or {condition: ...}")
        if "condition" in value:
            Expression(value["condition"])
        return value

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

    `once` makes the signal fire once, as SimPy's `Event` does; `on` lists actions
    run when the signal fires, as a task's `on` lists actions run when it ends.
    """

    once: bool = False
    on: List[OnEntry] = Field(default_factory=list)


class Blueprint(_Model):
    """A whole blueprint file."""

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

    @field_validator("signals", mode="before")
    @classmethod
    def _signal_mapping(cls, value: Any) -> Any:
        """Lets `signals` be written as a list of names, as before, or a mapping of
        name to `{once, on}`."""
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
    for part in location[:-1]:
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
