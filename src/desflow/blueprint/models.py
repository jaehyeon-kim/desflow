"""Pydantic models for a blueprint file.

Every model forbids unknown keys, so a misspelt key is reported rather than ignored.
Distributions are checked by building the `DistributionConfig` the builder registers,
so YAML and Python accept the same families and parameters. Fields that take Python
objects are filled by `!python` references and checked here, so a wrong reference
fails when the file is loaded rather than during the run.
"""

import inspect
from datetime import datetime, timedelta
from typing import Annotated, Any, Callable, Dict, List, Literal, Optional, Type, Union

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
from desflow.core.flow import Router, Transfer
from desflow.models.params import DistributionConfig
from desflow.utils import time_to_seconds

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
    minutes apart.
    """

    sim_id: str
    factor: float = 1.0
    random_seed: Optional[int] = None
    logical_start_time: Optional[datetime] = None
    go_live_at: Optional[datetime] = None
    warmup: Optional[float] = Field(default=None, ge=0)
    initial_time: float = 0.0

    @field_validator("logical_start_time", "go_live_at", mode="before")
    @classmethod
    def _parse_instant(cls, value: Any, info: ValidationInfo) -> Any:
        now = (info.context or {}).get("now") or datetime.now()
        return _instant(value, now)

    @field_validator("warmup", mode="before")
    @classmethod
    def _parse_warmup(cls, value: Any) -> Any:
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


class Arrival(Distribution):
    """An `arrivals` entry: a distribution, the task it spawns, and the attributes
    each entity it starts carries."""

    spawn: Optional[str] = None
    attributes: Dict[str, Any] = Field(default_factory=dict)


class Resource(_Model):
    """A `resources` entry: its capacity and the order its queue is served in."""

    current_cap: float
    max_cap: float
    queue: str = DEFAULT_QUEUE_ORDER

    @field_validator("queue")
    @classmethod
    def _known_order(cls, value: str) -> str:
        if value not in QUEUE_ORDERS:
            raise ValueError(f"queue must be one of {', '.join(QUEUE_ORDERS)}")
        return value


class Store(_Model):
    """A `stores` entry: how many items it holds, and whether it serves by priority."""

    current_cap: float
    max_cap: float
    priority: bool = False


class Container(_Model):
    """A `containers` entry: its capacity, the largest level, and its starting level."""

    current_cap: float
    max_cap: float
    level: float = 0.0


class TransferStep(_Model):
    """A task's `get` or `put`: one `container` with an `amount`, or one `store`.

    `amount` is a number or an expression such as `payload.quantity`, and 1 when it
    is left out. `priority` is a number or an expression for a put into a priority
    store, lowest served first, and 0 when it is left out.
    """

    container: Optional[str] = None
    store: Optional[str] = None
    amount: Union[float, str] = 1.0
    priority: Union[float, str] = 0.0

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


class Task(_Model):
    """A `tasks` entry, built with `SimulationContext.task`.

    `payload` is the value of the `finished` event: a mapping, or a `!python`
    function called as `payload(task_id, context)`. `id_field` adds the task id to a
    mapping payload under that key, and `attributes` adds the named entity
    attributes. A task with a `service` and no `resource` is a timed step; one with
    neither, and no `get`, emits its payload as soon as it is spawned.

    `priority` is a number or an expression such as `attr.priority`, for a resource
    whose `queue` is `priority`. `get` and `put` take from and add to a store or
    container. `next` is a task name, `when` rules or `weights`. `set` gives the
    entity attribute values when the task finishes.
    """

    service: Optional[str] = None
    resource: Optional[str] = None
    payload: Union[Dict[str, Any], Callable[..., Any]]
    id_field: Optional[str] = None
    attributes: List[str] = Field(default_factory=list)
    priority: Optional[Union[float, str]] = None
    get: Optional[TransferStep] = None
    put: Optional[TransferStep] = None
    next: Optional[
        Annotated[
            Union[Annotated[str, Tag("name")], Annotated[Next, Tag("rules")]],
            Discriminator(_next_form),
        ]
    ] = None
    set: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _resource_needs_a_service(self) -> "Task":
        if self.resource is not None and self.service is None:
            raise ValueError("a task with a resource needs a service")
        if self.priority is not None and self.resource is None:
            raise ValueError(
                "priority orders a resource's queue, so it needs a resource"
            )
        if isinstance(self.priority, str):
            Expression(self.priority)
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
    with `when: labels` only.
    """

    at: float
    path: Optional[str] = None
    value: Any = None
    record: Optional[Dict[str, Any]] = None

    @field_validator("at", mode="before")
    @classmethod
    def _parse_at(cls, value: Any) -> Any:
        return _seconds(value)

    @model_validator(mode="after")
    def _something_to_do(self) -> "ScenarioStep":
        if self.path is None and self.record is None:
            raise ValueError("a step needs a path and value, a record, or both")
        if self.path is None and "value" in self.model_fields_set:
            raise ValueError("a value needs a path")
        if self.path is not None and "value" not in self.model_fields_set:
            raise ValueError("a path needs a value")
        return self


class Run(_Model):
    """The `run` section.

    `before` lists `!python` functions called with no arguments before the run
    starts, for setup such as creating topics or tables.
    """

    until: Optional[float] = Field(default=None, gt=0)
    before: List[Callable[[], Any]] = Field(default_factory=list)

    @field_validator("until", mode="before")
    @classmethod
    def _parse_until(cls, value: Any) -> Any:
        return None if value is None else _seconds(value)


class Blueprint(_Model):
    """A whole blueprint file."""

    simulation: Simulation
    resources: Dict[str, Resource] = Field(default_factory=dict)
    containers: Dict[str, Container] = Field(default_factory=dict)
    stores: Dict[str, Store] = Field(default_factory=dict)
    services: Dict[str, Distribution] = Field(default_factory=dict)
    arrivals: Dict[str, Arrival] = Field(default_factory=dict)
    variables: Dict[str, Any] = Field(default_factory=dict)
    tasks: Dict[str, Task] = Field(default_factory=dict)
    telemetry: List[Telemetry] = Field(default_factory=list)
    processes: List[Process] = Field(default_factory=list)
    scenario: List[ScenarioStep] = Field(default_factory=list)
    ingress: List[Connector] = Field(default_factory=list)
    egress: List[EgressConnector] = Field(default_factory=list)
    batching: Optional[Batching] = None
    stats: Optional[Stats] = None
    run: Run = Field(default_factory=Run)

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
