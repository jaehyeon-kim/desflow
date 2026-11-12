"""The value generators of a blueprint: one model for each `gen`, and the `Value` type.

A mapping with a `gen` key is a generator, and its other keys are its arguments. Any
other mapping and any list is walked, and a scalar is a constant. Arguments that take
a value also take a generator, so `high: {gen: var, name: k}` works. `{gen: literal,
value: ...}` writes a mapping that has a `gen` key without evaluating it.

Every generator also takes the modifiers `null_rate`, `unique`, `omit_rate` and
`omit_when`.

A generator draws from a random sequence of its own, made from the run's seed and the
generator's key path in the file, so adding or removing a random field of a mapping
leaves the draws of every other field unchanged. The stream follows the path, so
inserting an item into a list, a scenario step or an `on:` entry moves the streams of the
items after it. Without a seed every run differs.

Each model has an `evaluate(run, scope)` method, which `ValueRuntime.evaluate` calls
after the modifiers have been applied.
"""

import keyword
import numbers
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    PrivateAttr,
    StrictInt,
    Tag,
    field_validator,
    model_validator,
)
from typing_extensions import TypeAliasType

from desflow.models.distributions import (
    LIST_PARAMETERS,
    PARAMETERS,
    TIME_LIST_PARAMETERS,
    TIME_PARAMETERS,
    get_distribution,
)
from desflow.core.expressions import ANY_NAME, RESERVED_NAMES, Expression, Scope
from desflow.core.rules import check_name_folds
from desflow.core.environment import resolve_zone
from desflow.models.params import DistributionConfig, finite_number
from desflow.utils import time_to_seconds

if TYPE_CHECKING:
    from desflow.values.runtime import ValueRuntime

EPOCH = datetime(1970, 1, 1, tzinfo=dt_timezone.utc)
_MILLISECOND = timedelta(milliseconds=1)
_SECOND = timedelta(seconds=1)


class ValueGenerationError(ValueError):
    """A generator could not give its value: a bad argument, a missing var, a draw
    from an empty choice. The message names the place in the file."""


def _not_boolean(value: Any) -> Any:
    """Refuses a boolean where a number is written; YAML reads `yes` and `on` as one."""
    if isinstance(value, bool):
        raise ValueError(f"must be a number, not the boolean {value!r}")
    return value


def _whole_number(value: Any) -> Any:
    """Refuses a boolean or a float where a whole number is written, such as `2.0`."""
    if isinstance(value, float):
        raise ValueError(
            f"must be a whole number written without a point, got {value!r}"
        )
    return _not_boolean(value)


def _number_fields(*names: str) -> Any:
    """A validator for plain-typed number fields, which Pydantic would read a boolean
    into: `_x = _number_fields("rate")` in a model refuses a boolean in `rate`."""
    return field_validator(*names, mode="before")(
        classmethod(lambda cls, value: _not_boolean(value))
    )


def _whole_number_fields(*names: str) -> Any:
    """The same for fields that take a whole number, which also refuses a float."""
    return field_validator(*names, mode="before")(
        classmethod(lambda cls, value: _whole_number(value))
    )


def _is_number(value: Any) -> bool:
    return isinstance(value, numbers.Real) and not isinstance(value, bool)


def rounded(value: Any, decimals: Optional[int]) -> Any:
    """`round(value, decimals)`, an integer when `decimals` is 0; the value as it is
    when `decimals` is None or the value is not a number.

    Args:
        value: The value.
        decimals: How many decimals to keep, or None.
    """
    if decimals is None or not _is_number(value):
        return value
    if decimals == 0:
        return int(round(value))
    return round(value, decimals)


def part_of(value: Any, path: Optional[str]) -> Any:
    """The part of `value` at a dotted `path`; an integer part indexes a list.

    Args:
        value: The value.
        path: Where the value is written, as a path of keys.

    Raises:
        KeyError: If a part is missing, with the part in its message.
    """
    if path is None:
        return value
    for step in str(path).split("."):
        try:
            value = value[int(step)] if isinstance(value, list) else value[step]
        except (KeyError, IndexError, TypeError, ValueError):
            raise KeyError(f"no part '{step}' in '{path}'") from None
    return value


def seconds_of(value: Any) -> Union[int, float]:
    """Seconds from a number or a duration such as `1 h`, which may be signed (`-1 h`).

    Args:
        value: The value.
    """
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("-"):
            return -time_to_seconds(text[1:].strip())
        return time_to_seconds(text)
    if not _is_number(value):
        raise ValueError(f"expected seconds or a duration, got {value!r}")
    return value


class Gen(BaseModel):
    """A generator: the `gen` key names its kind, and the modifiers apply to any."""

    model_config = ConfigDict(
        extra="forbid", arbitrary_types_allowed=True, populate_by_name=True
    )

    null_rate: Optional[float] = Field(default=None, ge=0, le=1)
    unique: bool = False
    omit_rate: Optional[float] = Field(default=None, ge=0, le=1)
    _plain_rates = _number_fields("null_rate", "omit_rate")
    omit_when: Optional[str] = None

    # Where the generator is written, set when the file is loaded: its dotted path,
    # which names its random sequence, and "file:line: path" for a message
    _path: str = PrivateAttr(default="")
    _where: str = PrivateAttr(default="")

    @field_validator("omit_when")
    @classmethod
    def _omit_when_parses(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            Expression(value, names=ANY_NAME)
        return value

    # The fields that hold a number argument, such as a bound or a weight, which a
    # literal NaN or infinity cannot be; the values a generator gives are not here
    _number_arguments: ClassVar[Tuple[str, ...]] = ()

    @model_validator(mode="after")
    def _finite_arguments(self) -> "Gen":
        # A literal NaN or infinity in a number argument is refused at load, by the
        # check every distribution parameter uses; a value an expression gives is
        # checked by `number` when it is read
        for name in self._number_arguments:
            for place, value in self._argument_values(name):
                if isinstance(value, float):
                    finite_number(place, value)
        return self

    def _argument_values(self, name: str) -> List[Tuple[str, Any]]:
        """Each literal of the number argument `name`, with its place."""
        value = getattr(self, name)
        if isinstance(value, dict):
            return [(f"{name}.{key}", item) for key, item in value.items()]
        if isinstance(value, list):
            return [(name, getattr(item, "weight", item)) for item in value]
        return [(name, value)]

    def fail(self, message: str) -> ValueGenerationError:
        """The error for this generator, with its place in the file.

        Args:
            message: What is wrong.

        Returns:
            ValueGenerationError: The error, for the caller to raise.
        """
        gen = getattr(self, "gen", "?")
        return ValueGenerationError(
            f"{self._where or self._path or 'a template'}: gen {gen}: {message}"
        )

    def number(self, value: Any, what: str) -> Union[int, float]:
        """A number argument, as given; a boolean, NaN, infinity or other type is
        refused.

        Args:
            value: The argument's value.
            what: The argument's name, for the message.

        Returns:
            Union[int, float]: The value as given.

        Raises:
            ValueGenerationError: If the value is not a finite number.
        """
        if not _is_number(value):
            raise self.fail(f"{what} must be a number, got {value!r}")
        try:
            finite_number(what, float(value))
        except ValueError as exc:
            raise self.fail(str(exc)) from None
        return value

    def whole(self, value: Any, what: str) -> int:
        """A whole-number argument; a whole float such as 3.0 is refused.

        Args:
            value: The argument's value.
            what: The argument's name, for the message.

        Returns:
            int: The value.

        Raises:
            ValueGenerationError: If the value is not an integer.
        """
        if isinstance(value, bool) or not isinstance(value, numbers.Integral):
            raise self.fail(f"{what} must be a whole number, got {value!r}")
        return int(value)

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:  # pragma: no cover
        """The generator's value; each kind defines it.

        Args:
            run: The state the generators of the run share.
            scope: What an expression in an argument reads.

        Returns:
            Any: The value.
        """
        raise NotImplementedError


# --- numbers ----------------------------------------------------------------


class Uniform(Gen):
    """`uniform`: a float in [low, high), from `low + (high - low) * random()`;
    `decimals` rounds it, and 0 gives an integer."""

    _number_arguments: ClassVar[Tuple[str, ...]] = ("low", "high")

    gen: Literal["uniform"]
    low: "Value"
    high: "Value"
    decimals: Optional[int] = Field(default=None, ge=0)
    _whole_decimals = _whole_number_fields("decimals")

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        low = self.number(run.evaluate(self.low, scope), "low")
        high = self.number(run.evaluate(self.high, scope), "high")
        if high < low:
            raise self.fail(f"high {high!r} is below low {low!r}")
        draw = run.rng(self).random()
        return rounded(float(low + (high - low) * draw), self.decimals)


class Integer(Gen):
    """`integer`: a whole number between `bounds[0]` and `bounds[1]`, both included."""

    gen: Literal["integer"]
    bounds: List["Value"] = Field(min_length=2, max_length=2)

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        low = self.whole(run.evaluate(self.bounds[0], scope), "the lower bound")
        high = self.whole(run.evaluate(self.bounds[1], scope), "the upper bound")
        if high < low:
            raise self.fail(f"the upper bound {high} is below the lower bound {low}")
        return int(run.rng(self).integers(low, high + 1))


class Normal(Gen):
    """`normal`: a float from a normal distribution with `mean` and `std`."""

    _number_arguments: ClassVar[Tuple[str, ...]] = ("mean", "std")

    gen: Literal["normal"]
    mean: "Value"
    std: "Value"
    decimals: Optional[int] = Field(default=None, ge=0)
    _whole_decimals = _whole_number_fields("decimals")

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        mean = self.number(run.evaluate(self.mean, scope), "mean")
        std = self.number(run.evaluate(self.std, scope), "std")
        if not std >= 0:
            raise self.fail(f"std must be 0 or more, got {std!r}")
        return rounded(float(run.rng(self).normal(mean, std)), self.decimals)


class Sample(Gen):
    """`sample`: one draw from a distribution family with its parameters, or from
    the live distribution of a service."""

    _number_arguments: ClassVar[Tuple[str, ...]] = (
        "rate",
        "mean",
        "std",
        "low",
        "value",
        "high",
        "mode",
        "shape",
        "scale",
        "p",
        "weights",
        "counts",
    )

    gen: Literal["sample"]
    dist: Optional[str] = None
    service: Optional[str] = None
    rate: "Value" = None
    mean: "Value" = None
    std: "Value" = None
    low: "Value" = None
    value: "Value" = None
    high: "Value" = None
    mode: "Value" = None
    shape: "Value" = None
    scale: "Value" = None
    p: "Value" = None
    values: Optional[List[Union[int, float, str]]] = None
    weights: Optional[List[float]] = None
    edges: Optional[List[Union[int, float, str]]] = None
    counts: Optional[List[float]] = None
    decimals: Optional[int] = Field(default=None, ge=0)
    _whole_decimals = _whole_number_fields("decimals")

    def parameters(self) -> Dict[str, Any]:
        """The parameters written, by name."""
        return {
            name: getattr(self, name)
            for name in PARAMETERS
            if getattr(self, name) is not None
        }

    @model_validator(mode="after")
    def _one_source(self) -> "Sample":
        if (self.dist is None) == (self.service is None):
            raise ValueError("sample takes a dist or a service, not both or neither")
        if self.service is not None:
            if self.parameters():
                raise ValueError(
                    "sample of a service takes no parameters; its distribution is "
                    "the service's"
                )
            return self
        family = get_distribution(self.dist)
        extra = [name for name in self.parameters() if name not in family.parameters]
        if extra:
            raise ValueError(
                f"{self.dist} does not take {', '.join(extra)}; it takes "
                f"{', '.join(family.parameters)}"
            )
        if not any(has_generator(v) for v in self.parameters().values()):
            self.config({})  # checked at load when every parameter is a constant
        return self

    def config(self, evaluated: Dict[str, Any]) -> DistributionConfig:
        """The distribution config for the parameters, with durations in seconds.

        Args:
            evaluated: The parameters after they were evaluated, by name.
        """
        given: Dict[str, Any] = {}
        for name, value in self.parameters().items():
            value = evaluated.get(name, value)
            if name in TIME_LIST_PARAMETERS and isinstance(value, list):
                value = [seconds_of(item) for item in value]
            elif name in TIME_PARAMETERS and isinstance(value, str):
                value = time_to_seconds(value)
            given[name] = value
        return DistributionConfig(dist=self.dist, **given)  # type: ignore[arg-type]

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        if self.service is not None:
            value = run.draw_service(self, self.service)
        else:
            evaluated = {
                name: run.evaluate(value, scope)
                for name, value in self.parameters().items()
                if name not in LIST_PARAMETERS
            }
            for name, value in evaluated.items():
                if isinstance(value, bool):
                    raise self.fail(f"{name} must be a number, got {value!r}")
            try:
                config = self.config(evaluated)
            except (TypeError, ValueError) as exc:
                raise self.fail(str(exc)) from None
            value = get_distribution(self.dist).draw(config, run.rng(self))
        return rounded(value, self.decimals)


class Boolean(Gen):
    """`boolean`: true with probability `p` (0.5 when not given)."""

    _number_arguments: ClassVar[Tuple[str, ...]] = ("p",)

    gen: Literal["boolean"]
    p: "Value" = 0.5

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        p = self.number(run.evaluate(self.p, scope), "p")
        if not 0 <= p <= 1:
            raise self.fail(f"p must be between 0 and 1, got {p!r}")
        return bool(run.rng(self).random() < p)


# --- choices ----------------------------------------------------------------


class OneOf(Gen):
    """`one_of`: one of `choices`, each equally likely. Only the choice picked is
    evaluated."""

    gen: Literal["one_of"]
    choices: List["Value"] = Field(min_length=1)

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        index = int(run.rng(self).integers(len(self.choices)))
        return run.evaluate(self.choices[index], scope)


class WeightedChoice(BaseModel):
    """One entry of `weighted` `choices`: a value and its weight."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    value: "Value"
    weight: "Value"


class Weighted(Gen):
    """`weighted`: one choice, by weight. `options` maps each value to its weight, or
    `choices` lists `{value, weight}` for values that are mappings or generators. Each
    weight is 0 or more and at least one is above 0; weights are divided by their
    sum. Only the value picked is evaluated. In a blueprint, an option key written
    `yes`, `no`, `on` or `off` is that text, not a boolean."""

    _number_arguments: ClassVar[Tuple[str, ...]] = ("options", "choices")

    gen: Literal["weighted"]
    options: Optional[Dict[Any, "Value"]] = None
    choices: Optional[List[WeightedChoice]] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _one_form(self) -> "Weighted":
        if (self.options is None) == (self.choices is None):
            raise ValueError("weighted takes options or choices, not both or neither")
        if self.options is not None and not self.options:
            raise ValueError("weighted needs at least one option")
        return self

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        if self.options is not None:
            values: List[Any] = list(self.options)
            raw = list(self.options.values())
        else:
            assert self.choices is not None
            values = [choice.value for choice in self.choices]
            raw = [choice.weight for choice in self.choices]
        weights = [self.number(run.evaluate(w, scope), "a weight") for w in raw]
        if any(not w >= 0 for w in weights) or not sum(weights) > 0:
            raise self.fail(
                f"weights must be 0 or more with a sum above 0, got {weights!r}"
            )
        point = run.rng(self).random() * sum(weights)
        total = 0.0
        picked = len(weights) - 1
        for index, weight in enumerate(weights):
            total += weight
            if weight > 0 and point < total:
                picked = index
                break
        else:
            while weights[picked] == 0:  # rounding put the point past the last weight
                picked -= 1
        if self.options is not None:
            return values[picked]
        return run.evaluate(values[picked], scope)


class Pick(Gen):
    """`pick`: a list of `count` different `choices`, drawn without replacement, in
    the order drawn. Only the choices picked are evaluated."""

    gen: Literal["pick"]
    choices: List["Value"] = Field(min_length=1)
    count: "Value" = 1

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        count = self.whole(run.evaluate(self.count, scope), "count")
        if not 1 <= count <= len(self.choices):
            raise self.fail(
                f"count must be from 1 to the number of choices "
                f"({len(self.choices)}), got {count}"
            )
        picked = run.rng(self).choice(len(self.choices), size=count, replace=False)
        return [run.evaluate(self.choices[int(i)], scope) for i in picked]


class SwitchRange(BaseModel):
    """One range case of `switch`: the first case whose `min` is at or below `on`
    and whose `max`, when given, is above it."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    min: Optional[Union[int, float]] = None
    max: Optional[Union[int, float]] = None
    _plain_bounds = _number_fields("min", "max")
    value: "Value"


def _is_case(key: Any, on: Any) -> bool:
    """Whether the case `key` of a `switch` matches the value `on`.

    A boolean `on` also matches the YAML words for it, as a blueprint reads a case
    `yes:` as the text `yes`: `yes` and `on` for true, `no` and `off` for false.
    """
    if key == on or str(key) == str(on):
        return True
    if isinstance(on, bool) and isinstance(key, str):
        return key.lower() in (("yes", "on") if on else ("no", "off"))
    return False


class Switch(Gen):
    """`switch`: the value of the first case that matches `on`, evaluated only when
    picked. `cases` is a mapping from a value to its result, or a list of ranges;
    `default` is used when none matches (null when it is left out). In a blueprint,
    a key written `yes`, `no`, `on` or `off` is that text, not a boolean."""

    gen: Literal["switch"]
    on: "Value"
    cases: Union[Dict[Any, "Value"], List[SwitchRange]]
    default: "Value" = None

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        on = run.evaluate(self.on, scope)
        if isinstance(self.cases, dict):
            for key, result in self.cases.items():
                if _is_case(key, on):
                    return run.evaluate(result, scope)
        else:
            number = self.number(on, "on, for ranges,")
            for case in self.cases:
                if (case.min is None or number >= case.min) and (
                    case.max is None or number < case.max
                ):
                    return run.evaluate(case.value, scope)
        return run.evaluate(self.default, scope)


class When(Gen):
    """`when`: `then` when the `condition` (an expression) holds, else `else`.
    Only the branch taken is evaluated."""

    gen: Literal["when"]
    condition: str
    then: "Value"
    else_: "Value" = Field(default=None, alias="else")

    @field_validator("condition")
    @classmethod
    def _condition_parses(cls, value: str) -> str:
        Expression(value, names=ANY_NAME)
        return value

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        holds = run.condition(self, self.condition, scope)
        return run.evaluate(self.then if holds else self.else_, scope)


# --- identifiers, counters and text -------------------------------------------


class Uuid(Gen):
    """`uuid`: a version 4 UUID from the generator's random sequence, so it repeats
    with the seed, or a version 7 UUID whose 48-bit millisecond field comes from the
    logical time."""

    gen: Literal["uuid"]
    version: Literal[4, 7] = 4

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        rng = run.rng(self)
        if self.version == 4:
            return str(uuid.UUID(bytes=rng.bytes(16), version=4))
        stamp = (run.now_time() - EPOCH) // _MILLISECOND
        bits = int.from_bytes(rng.bytes(10), "big")
        value = (
            (stamp & (2**48 - 1)) << 80
            | 0x7 << 76
            | ((bits >> 62) & 0xFFF) << 64
            | 0b10 << 62
            | (bits & (2**62 - 1))
        )
        return str(uuid.UUID(int=value))


class Sequence(Gen):
    """`sequence`: the next integer of a counter, `start` plus `step` times the
    count of calls so far, restarting after `loop_after` calls. With `format`, the
    text `format.format(n)`, such as `"sensor-{}"`. There is one counter for the run,
    shared by every entity, so ids stay unique."""

    gen: Literal["sequence"]
    start: int = 0
    step: int = 1
    loop_after: Optional[int] = Field(default=None, ge=1)
    _whole_counts = _whole_number_fields("start", "step", "loop_after")
    format: Optional[str] = None

    @field_validator("format")
    @classmethod
    def _format_takes_the_number(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            try:
                value.format(0)
            except (AttributeError, IndexError, KeyError, ValueError) as exc:
                raise ValueError(f"format cannot take a number: {exc}") from None
        return value

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        count = run.count(self)
        if self.loop_after is not None:
            count %= self.loop_after
        number = self.start + self.step * count
        return number if self.format is None else self.format.format(number)


class Now(Gen):
    """`now`: the logical time plus `offset` (seconds or a duration, signed), as the
    ISO text a record's `timestamp` uses. It reads the simulation clock, never the
    wall clock."""

    _number_arguments: ClassVar[Tuple[str, ...]] = ("offset",)

    gen: Literal["now"]
    offset: "Value" = 0

    @field_validator("offset")
    @classmethod
    def _offset_reads(cls, value: Any) -> Any:
        if isinstance(value, str):
            seconds_of(value)
        return value

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        try:
            offset = seconds_of(run.evaluate(self.offset, scope))
        except ValueError as exc:
            raise self.fail(f"offset: {exc}") from None
        return run.iso_time(offset)


class FormatTime(Gen):
    """`format_time`: a time as text or a number. `time` is the text `{gen: now}`
    gives, or seconds of simulation time (default: now). `format` is a `strftime`
    pattern, `iso`, `iso_utc` (milliseconds and a `Z`), `epoch_s` or `epoch_ms`
    (integers). `tz` is an IANA name and defaults to the run's time zone."""

    gen: Literal["format_time"]
    time: "Value" = None
    format: str = "iso"
    tz: Optional[str] = None

    @field_validator("tz")
    @classmethod
    def _zone_exists(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            resolve_zone(value)
        return value

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        given = run.evaluate(self.time, scope)
        try:
            if given is None:
                when = run.now_time()
            elif isinstance(given, str):
                when = datetime.fromisoformat(given)
                if when.tzinfo is None:
                    when = when.replace(tzinfo=run.zone)
            elif _is_number(given):
                when = run.logical_time(given)
            else:
                raise ValueError(f"time must be text or seconds, got {given!r}")
        except ValueError as exc:
            raise self.fail(str(exc)) from None
        zone = resolve_zone(self.tz) if self.tz else run.zone
        when = when.astimezone(zone)
        if self.format == "iso":
            return when.isoformat(timespec="milliseconds")
        if self.format == "iso_utc":
            utc = when.astimezone(dt_timezone.utc)
            return utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        if self.format == "epoch_s":
            return int((when - EPOCH) // _SECOND)
        if self.format == "epoch_ms":
            return int((when - EPOCH) // _MILLISECOND)
        try:
            return when.strftime(self.format)
        except ValueError as exc:
            raise self.fail(f"format {self.format!r}: {exc}") from None


class Format(Gen):
    """`format`: `template.format_map(...)` over `names`, then the vars of the task.
    A name in the template is a var, or an entry of `names`, which wins."""

    gen: Literal["format"]
    template: str
    names: Dict[str, "Value"] = Field(default_factory=dict)

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        names = {k: run.evaluate(v, scope) for k, v in self.names.items()}
        try:
            return self.template.format_map({**scope.names, **names})
        except (KeyError, IndexError, AttributeError, ValueError, TypeError) as exc:
            raise self.fail(f"template {self.template!r}: {exc!r}") from None


class Lower(Gen):
    """`lower`: the text of `value` in lower case."""

    gen: Literal["lower"]
    value: "Value"

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        return str(run.evaluate(self.value, scope)).lower()


class Upper(Gen):
    """`upper`: the text of `value` in upper case."""

    gen: Literal["upper"]
    value: "Value"

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        return str(run.evaluate(self.value, scope)).upper()


class Fake(Gen):
    """`fake`: a value from the Faker library. `method` names any method of Faker's
    providers, such as `name`, `email`, `isbn13` or `credit_card_number`, and `args`
    and `kwargs` are passed to it. `locale` picks the language and country, `en_US`
    by default. It needs the `fake` extra.

    Each `fake` generator has a Faker of its own, seeded from the run's seed and its
    place in the file, so the same seed gives the same values and adding a field
    changes no other. With no seed, the values change on every run. A date, a time
    or a decimal result is written as text or a float."""

    gen: Literal["fake"]
    method: str
    args: List["Value"] = Field(default_factory=list)
    kwargs: Dict[str, "Value"] = Field(default_factory=dict)
    locale: str = "en_US"

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        faker = run.faker(self, self.locale)
        args = [run.evaluate(a, scope) for a in self.args]
        kwargs = {k: run.evaluate(v, scope) for k, v in self.kwargs.items()}
        return getattr(faker, self.method)(*args, **kwargs)


# --- arithmetic and expressions ------------------------------------------------


class _Arithmetic(Gen):
    """The arithmetic kinds: `args` has at least two entries, and `decimals` rounds
    the result."""

    _number_arguments: ClassVar[Tuple[str, ...]] = ("args",)

    args: List["Value"] = Field(min_length=2)
    decimals: Optional[int] = Field(default=None, ge=0)
    _whole_decimals = _whole_number_fields("decimals")

    def operands(self, run: "ValueRuntime", scope: Scope) -> Tuple[Any, List[Any]]:
        """The arguments, each evaluated once. When the first is the ISO text of a
        time, the result is the time and the seconds that follow it; otherwise it is
        None and the numbers. A time with no zone is read in the run's zone.

        Args:
            run: The value runtime.
            scope: What the expressions here read.
        """
        values = [run.evaluate(arg, scope) for arg in self.args]
        if isinstance(values[0], str):
            first = values[0]
            try:
                when = datetime.fromisoformat(first)
            except ValueError:
                raise self.fail(
                    f"argument 1 {first!r} is not a time or a number"
                ) from None
            if when.tzinfo is None:
                when = when.replace(tzinfo=run.zone)
            rest = [
                self.number(value, f"argument {index}")
                for index, value in enumerate(values[1:], 2)
            ]
            return when, rest
        return None, [
            self.number(value, f"argument {index}")
            for index, value in enumerate(values, 1)
        ]

    def numbers_of(self, run: "ValueRuntime", scope: Scope) -> List[Any]:
        """The arguments, evaluated once and checked to be numbers.

        Args:
            run: The value runtime.
            scope: What the expressions here read.
        """
        return [
            self.number(run.evaluate(arg, scope), f"argument {index}")
            for index, arg in enumerate(self.args, 1)
        ]


class Add(_Arithmetic):
    """`add`: the sum of `args`. When the first is a time (the text `{gen: now}`
    gives), the rest are seconds and the result is a time."""

    gen: Literal["add"]

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        when, numbers_ = self.operands(run, scope)
        if when is not None:
            return run.time_text(when + timedelta(seconds=sum(numbers_)))
        return rounded(sum(numbers_), self.decimals)


class Subtract(_Arithmetic):
    """`subtract`: the first of `args` less the others. When the first is a time,
    the rest are seconds and the result is a time."""

    gen: Literal["subtract"]

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        when, numbers_ = self.operands(run, scope)
        if when is not None:
            return run.time_text(when - timedelta(seconds=sum(numbers_)))
        return rounded(numbers_[0] - sum(numbers_[1:]), self.decimals)


class Multiply(_Arithmetic):
    """`multiply`: the product of `args`."""

    gen: Literal["multiply"]

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        result: Any = 1
        for number in self.numbers_of(run, scope):
            result = result * number
        return rounded(result, self.decimals)


class Divide(_Arithmetic):
    """`divide`: the first of `args` divided by each of the others in turn (`/`)."""

    gen: Literal["divide"]

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        numbers_ = self.numbers_of(run, scope)
        result = numbers_[0]
        for number in numbers_[1:]:
            if number == 0:
                raise self.fail("it divides by 0")
            result = result / number
        return rounded(result, self.decimals)


class Min(_Arithmetic):
    """`min`: the smallest of `args`."""

    gen: Literal["min"]

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        return rounded(min(self.numbers_of(run, scope)), self.decimals)


class Max(_Arithmetic):
    """`max`: the largest of `args`."""

    gen: Literal["max"]

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        return rounded(max(self.numbers_of(run, scope)), self.decimals)


class Expr(Gen):
    """`expr`: the value of an expression, with the names of the language, the vars of
    the task and the entries of `names` (evaluated first) as bare names, and `t` for
    the simulation time. It can give any type; a number is rounded to `decimals`."""

    gen: Literal["expr"]
    expr: str
    names: Dict[str, "Value"] = Field(default_factory=dict)
    decimals: Optional[int] = Field(default=None, ge=0)
    _whole_decimals = _whole_number_fields("decimals")

    _parsed: Optional[Expression] = PrivateAttr(default=None)

    @field_validator("names")
    @classmethod
    def _names_are_free(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        for name in value:
            if (
                not name.isidentifier()
                or keyword.iskeyword(name)
                or name in RESERVED_NAMES
            ):
                raise ValueError(
                    f"'{name}' cannot be a name here: a name is an identifier that "
                    "no expression already uses"
                )
            check_name_folds(name, "names")
        return value

    @field_validator("expr")
    @classmethod
    def _expr_parses(cls, value: str) -> str:
        Expression(value, names=ANY_NAME)
        return value

    def parsed(self) -> Expression:
        """The expression, parsed once."""
        if self._parsed is None:
            self._parsed = Expression(self.expr, names=ANY_NAME)
        return self._parsed

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        names = {k: run.evaluate(v, scope) for k, v in self.names.items()}
        inner = replace(scope, names={**scope.names, **names})
        try:
            value = self.parsed().evaluate(inner)
        except ValueError as exc:
            raise self.fail(str(exc)) from None
        return rounded(value, self.decimals)


# --- reading the model ---------------------------------------------------------


class Var(Gen):
    """`var`: a var of the task, or the part of it at `path` (dotted; an integer part
    indexes a list). Every reference to a var in one event gives the same value."""

    gen: Literal["var"]
    name: str
    path: Optional[str] = None

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        if self.name not in scope.names:
            raise self.fail(f"'{self.name}' is not a var that has a value yet")
        try:
            return run.copy(part_of(scope.names[self.name], self.path))
        except KeyError as exc:
            raise self.fail(f"var '{self.name}': {exc.args[0]}") from None


class Attr(Gen):
    """`attr`: the entity's attribute `name`, or the part of it at `path`."""

    gen: Literal["attr"]
    name: str
    path: Optional[str] = None

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        if self.name not in scope.attributes:
            raise self.fail(f"the entity has no attribute '{self.name}'")
        try:
            return run.copy(part_of(scope.attributes[self.name], self.path))
        except KeyError as exc:
            raise self.fail(f"attribute '{self.name}': {exc.args[0]}") from None


class Entity(Gen):
    """`entity`: the key of the entity the task runs for, such as `sensor-1`. It is
    given only inside a task, so an arrival's `attributes`, `vars` and `key` refuse it."""

    gen: Literal["entity"]

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The entity's key.
        """
        if scope.entity is None:
            raise self.fail("there is no entity here; entity is read inside a task")
        return scope.entity


class Param(Gen):
    """`param`: the live registry value at `path` (a full registry path, such as
    `Shop.variables.error_rate`) at the moment of the call, or the `field` key of a
    mapping value. A scenario step or an ingress update shows in the next call."""

    gen: Literal["param"]
    path: str
    field: "Value" = Field(default=None)

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        if scope.registry is None:
            raise self.fail("there is no registry to read")
        try:
            value = scope.registry.get(self.path).value
        except KeyError:
            raise self.fail(f"'{self.path}' is not a registry path") from None
        key = run.evaluate(self.field, scope)
        if key is None:
            return run.copy(value)
        try:
            return run.copy(value[key])
        except (KeyError, IndexError, TypeError):
            raise self.fail(f"the value at '{self.path}' has no key {key!r}") from None


# --- structure -------------------------------------------------------------------


class Repeat(Gen):
    """`repeat`: a list of `times` values, each evaluated on its own."""

    gen: Literal["repeat"]
    times: "Value"
    value: "Value"

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        times = self.whole(run.evaluate(self.times, scope), "times")
        if times < 0:
            raise self.fail(f"times must be 0 or more, got {times}")
        with run.distinct_frame():
            return [run.evaluate(self.value, scope) for _ in range(times)]


class Literal_(Gen):
    """`literal`: `value` as written, not evaluated, so a mapping that has a `gen`
    key can be a payload field."""

    gen: Literal["literal"]
    value: Any = None

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        return run.copy(self.value)


class Object(Gen):
    """`object`: the mapping `fields`, each evaluated as a plain mapping is. It lets
    a modifier such as `null_rate` or `omit_rate` apply to the whole mapping."""

    gen: Literal["object"]
    fields: Dict[str, "Value"]

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        return run.evaluate(self.fields, scope)


class Call(Gen):
    """`call`: what a `!python` function returns, called as `function(context,
    **kwargs)` with the kwargs evaluated first."""

    gen: Literal["call"]
    function: Callable[..., Any]
    kwargs: Dict[str, "Value"] = Field(default_factory=dict)

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        kwargs = {k: run.evaluate(v, scope) for k, v in self.kwargs.items()}
        return self.function(run.context, **kwargs)


class Row(Gen):
    """`row`: one row of a CSV or Parquet file as a mapping, read once at load. A cell
    of digits is read as a number, so a postal code with leading zeros loses them;
    write them with `{gen: format, template: "{n:05d}"}`. `where` is an expression over
    the row's columns that keeps some rows; `weight` names a column of weights; `order`
    is `random` (the default), `sequential`, `shuffle` (every row once, in a random
    order) or `circular` (in file order, again from the first); `on_end` says what a
    `sequential` or `shuffle` order does when the rows run out: `error` (the default),
    `loop` or `stop`, which ends the run."""

    gen: Literal["row"]
    file: str
    where: Optional[str] = None
    weight: Optional[str] = None
    order: Literal["random", "sequential", "shuffle", "circular"] = "random"
    on_end: Literal["stop", "loop", "error"] = "error"

    _rows: Optional[List[Dict[str, Any]]] = PrivateAttr(default=None)

    @field_validator("where")
    @classmethod
    def _where_parses(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            Expression(value, names=ANY_NAME)
        return value

    @model_validator(mode="after")
    def _weight_needs_random(self) -> "Row":
        if self.weight is not None and self.order != "random":
            raise ValueError("a weight column applies to order: random only")
        return self

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        if self._rows is None:
            raise self.fail("the file was not read when the blueprint was loaded")
        return run.pick_row(self, self._rows)


# --- relationships between records ---------------------------------------------


class HistogramBin(BaseModel):
    """One bin of a `lookup` histogram: `bin` is the share of the rows it covers, in
    the order they were written, and `frequency` is the chance that a draw falls in it.
    Both are scaled to add up to 1, so `{bin: 2, frequency: 8}, {bin: 8, frequency: 2}`
    means the same as 0.2 with 0.8, and 0.8 with 0.2."""

    model_config = ConfigDict(extra="forbid")

    bin: float = Field(gt=0, allow_inf_nan=False)
    frequency: float = Field(ge=0, allow_inf_nan=False)
    _plain_numbers = _number_fields("bin", "frequency")


class Lookup(Gen):
    """`lookup`: a row of the finished payloads of the task `task`, or the part of it
    at `path` (dotted; an integer part indexes a list).

    A row is readable from the instant the task's finished record is published. The
    task keeps its last `history` rows (default 1,000,000).

    `strategy` is `random` (the default), `first` (the oldest row kept) or `last` (the
    newest). `histogram` splits the rows kept, in the order they were written, into
    bins of the stated shares; a draw picks a bin by its frequency and then a row of the
    bin at random, so it is for `random` only. A bin that holds no row yet is left out
    of the draw, and while no bin with a frequency above 0 holds a row (a few rows and
    a last bin of frequency 0), the draw is over all the rows. `latest_by` keeps only
    the newest row for each value of that field of the row, so a lookup sees a
    customer's latest address; `strategy` and `histogram` then choose among those rows,
    oldest first by the time each was written. `distinct` draws without replacement
    within one `repeat`, so the items of one order differ."""

    gen: Literal["lookup"]
    task: str
    path: Optional[str] = None
    strategy: Literal["random", "first", "last"] = "random"
    histogram: Optional[List[HistogramBin]] = None
    latest_by: Optional[str] = None
    distinct: bool = False

    @model_validator(mode="after")
    def _arguments_fit(self) -> "Lookup":
        if self.latest_by is not None and "." in self.latest_by:
            raise ValueError(
                "latest_by names one field of the row, so it has no dot; "
                f"{self.latest_by!r} reads a part of a field"
            )
        if self.histogram is not None:
            if self.strategy != "random":
                raise ValueError("a histogram applies to strategy: random only")
            if not self.histogram:
                raise ValueError("a histogram needs at least one bin")
            if not sum(b.frequency for b in self.histogram) > 0:
                raise ValueError(
                    "the frequencies of a histogram must add up to above 0"
                )
        return self

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        return run.lookup(self, scope)


class Previous(Gen):
    """`previous`: the last finished payload of the same task for the same stream, or
    the part of it at `path`. The stream is the arrival, so all the entities it starts
    share one, except for an arrival with `count` or `keys`, where each entity has its
    own. `default` is evaluated only when the stream has no finished payload yet, and
    without one the first reading is null."""

    gen: Literal["previous"]
    path: Optional[str] = None
    default: "Value" = Field(default=None)

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        return run.previous(self, scope)


class Upstream(Gen):
    """`upstream`: the finished payload of the task that routed to this one with
    `next`, or the part of it at `path`. A start by an arrival has no upstream, and a
    task that `on_fail` or `on_interrupt_next` reaches is handed a failure or an
    interrupted record, so a blueprint refuses `upstream` there."""

    gen: Literal["upstream"]
    path: Optional[str] = None

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        if scope.upstream is None:
            raise self.fail(
                "this task was not reached by another task, so there is no upstream "
                "payload"
            )
        try:
            return run.copy(part_of(scope.upstream, self.path))
        except KeyError as exc:
            raise self.fail(f"upstream: {exc.args[0]}") from None


class Aggregate(Gen):
    """`aggregate`: `mean`, `sum`, `min`, `max` or `count` over the last `last` rows of
    the task `task` that `where` keeps. `where` is an expression over the row's own
    fields, such as `lead_days == 3 and precipitation > 0`; without it every row counts.
    `count` counts rows and takes no `path`; the others read the number at `path`. A
    reading looks back through the rows the task keeps (its `history`) until `last` rows
    are kept, and takes the rows there are when `where` keeps fewer."""

    gen: Literal["aggregate"]
    task: str
    path: Optional[str] = None
    last: StrictInt = Field(ge=1)
    function: Literal["mean", "sum", "min", "max", "count"]
    where: Optional[str] = None

    @field_validator("where")
    @classmethod
    def _where_parses(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            Expression(value, names=ANY_NAME)
        return value

    @model_validator(mode="after")
    def _path_fits_function(self) -> "Aggregate":
        if self.function == "count" and self.path is not None:
            raise ValueError("count counts rows and takes no path")
        if self.function != "count" and self.path is None:
            raise ValueError(f"{self.function} needs the path of the number to read")
        return self

    def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
        """The value of this generator.

        Args:
            run: The value runtime, which holds the generator's random stream.
            scope: What the expressions here read.

        Returns:
            Any: The value.
        """
        return run.aggregate(self, scope)


_KINDS = (
    Uniform,
    Integer,
    Normal,
    Sample,
    OneOf,
    Weighted,
    Pick,
    Switch,
    When,
    Boolean,
    Uuid,
    Sequence,
    Now,
    FormatTime,
    Format,
    Lower,
    Upper,
    Fake,
    Add,
    Subtract,
    Multiply,
    Divide,
    Min,
    Max,
    Expr,
    Var,
    Attr,
    Entity,
    Param,
    Repeat,
    Literal_,
    Object,
    Call,
    Row,
    Lookup,
    Previous,
    Upstream,
    Aggregate,
)

Generator = Annotated[
    Union[
        Uniform,
        Integer,
        Normal,
        Sample,
        OneOf,
        Weighted,
        Pick,
        Switch,
        When,
        Boolean,
        Uuid,
        Sequence,
        Now,
        FormatTime,
        Format,
        Lower,
        Upper,
        Fake,
        Add,
        Subtract,
        Multiply,
        Divide,
        Min,
        Max,
        Expr,
        Var,
        Attr,
        Entity,
        Param,
        Repeat,
        Literal_,
        Object,
        Call,
        Row,
        Lookup,
        Previous,
        Upstream,
        Aggregate,
    ],
    Discriminator("gen"),
]


def _kind(value: Any) -> str:
    if isinstance(value, Gen):
        return "<gen>"
    if isinstance(value, dict):
        return "<gen>" if "gen" in value else "<map>"
    return "<list>" if isinstance(value, list) else "<const>"


_NOT_A_PAYLOAD = (
    "a payload is a mapping or a generator that gives a mapping or null "
    "(a record whose value is null)"
)

# mypy cannot resolve a recursive `TypeAliasType`, so it reads both as `Any`; Pydantic
# reads the definitions below
if TYPE_CHECKING:
    Value = Any
    Payload = Any
else:
    # The outer discriminator is a function, so a mapping with `gen` is always validated
    # as a generator. With a plain union a misspelt argument would fail the generator
    # branch and then pass as a constant mapping, and the error would never be reported.
    Value = TypeAliasType(
        "Value",
        Annotated[
            Union[
                Annotated[Generator, Tag("<gen>")],
                Annotated[Dict[Any, "Value"], Tag("<map>")],
                Annotated[List["Value"], Tag("<list>")],
                Annotated[Any, Tag("<const>")],
            ],
            Discriminator(_kind),
        ],
    )

    # A payload built from values: a generator or a mapping, nothing else
    Payload = TypeAliasType(
        "Payload",
        Annotated[
            Union[
                Annotated[Generator, Tag("<gen>")],
                Annotated[Dict[Any, "Value"], Tag("<map>")],
            ],
            Discriminator(
                _kind,
                custom_error_type="payload_kind",
                custom_error_message=_NOT_A_PAYLOAD,
            ),
        ],
    )

for _model in (*_KINDS, WeightedChoice, SwitchRange, HistogramBin):
    _model.model_rebuild(_types_namespace={"Value": Value})

# The generator classes by the value of `gen`
GENERATORS: Dict[str, type] = {
    model.model_fields["gen"].annotation.__args__[0]: model  # type: ignore[union-attr]
    for model in _KINDS
}


def has_generator(value: Any) -> bool:
    """Whether a value holds a generator at any depth, so it must be evaluated; a
    constant mapping or list can be copied as it is.

    Args:
        value: The value.
    """
    if isinstance(value, Gen):
        return True
    if isinstance(value, dict):
        return any(has_generator(v) for v in value.values())
    if isinstance(value, list):
        return any(has_generator(v) for v in value)
    return False


_ADAPTERS: Dict[str, Any] = {}


def payload_adapter() -> Any:
    """The Pydantic adapter that validates a payload (cached)."""
    from pydantic import TypeAdapter

    if "payload" not in _ADAPTERS:
        _ADAPTERS["payload"] = TypeAdapter(Payload)
    return _ADAPTERS["payload"]


def generator_adapter() -> Any:
    """The Pydantic adapter that validates one generator (cached)."""
    from pydantic import TypeAdapter

    if "generator" not in _ADAPTERS:
        _ADAPTERS["generator"] = TypeAdapter(Generator)
    return _ADAPTERS["generator"]


def values_adapter() -> Any:
    """The Pydantic adapter that validates a mapping of names to values (cached)."""
    from pydantic import TypeAdapter

    if "values" not in _ADAPTERS:
        _ADAPTERS["values"] = TypeAdapter(Dict[str, Value])
    return _ADAPTERS["values"]
