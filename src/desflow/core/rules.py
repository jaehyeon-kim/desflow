"""Checks that a blueprint and the builder share.

A blueprint key and the builder argument that mirrors it check through the same
function, so YAML and Python accept and refuse the same values. `blueprint.models`
imports these from here.
"""

import keyword
import numbers
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from desflow.core.expressions import ANY_NAME, RESERVED_NAMES, Expression
from desflow.core.families import Reference
from desflow.utils import checked_seconds, time_to_seconds


# Text that is only a number, so a name such as `inf` stays an expression
PLAIN_NUMBER = re.compile(r"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")


def duration_text(value: Any) -> Any:
    """Reads a time place as the runtime reads it: a duration in text becomes seconds, a
    number in text becomes that number, and other text is returned unchanged, for the
    caller to check as an expression.

    Args:
        value: The place as given.

    Returns:
        Any: Seconds for a duration or a number in text, else the value as given.
    """
    if isinstance(value, str):
        try:
            return time_to_seconds(value)
        except ValueError:
            pass
        if PLAIN_NUMBER.fullmatch(value.strip()):
            return float(value)  # a number in text, such as '-1'
    return value


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


class StrictModel(BaseModel):
    """The base of the blueprint models: a key a model does not declare is refused."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


class Ref(StrictModel):
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


def check_stop(value: Any) -> Any:
    """Checks a `stop` is one signal or one condition, whose text parses.

    Args:
        value: The `stop` mapping, or None.

    Returns:
        Any: The mapping, with a `{of: ...}` signal read as its reference, or None.

    Raises:
        ValueError: If the mapping is not one signal or one condition, its
            condition does not parse, or its signal is a mapping that is not a
            valid reference.
    """
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


class RunSegment(StrictModel):
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
    def _parse_until(cls, value: Any, info: Any) -> Any:
        return None if value is None else checked_seconds(value, "a segment until")

    @field_validator("stop")
    @classmethod
    def _one_stop(cls, value: Any) -> Any:
        return check_stop(value)

    @model_validator(mode="after")
    def _one_end(self) -> "RunSegment":
        if self.until is not None and self.stop is not None:
            raise ValueError("a segment takes until or stop, not both")
        return self


class SegmentEvery(StrictModel):
    """`run.segments: {every: 10 s}`: a segment every `every` seconds up to `run.until`."""

    every: Union[int, float, str]

    @field_validator("every", mode="before")
    @classmethod
    def _parse_every(cls, value: Any, info: Any) -> Any:
        number = checked_seconds(value, "every", ends=True)
        if number <= 0:
            raise ValueError("every must be above 0")
        return number
