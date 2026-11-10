import difflib
import math
import numbers
import re
from decimal import Decimal
from typing import Any, Iterable, Union

from desflow.connectors._checks import checked_whole  # noqa: F401

MULTIPLIERS = {
    "s": 1,
    "sec": 1,
    "secs": 1,
    "second": 1,
    "seconds": 1,
    "m": 60,
    "min": 60,
    "mins": 60,
    "minute": 60,
    "minutes": 60,
    "h": 3600,
    "hr": 3600,
    "hrs": 3600,
    "hour": 3600,
    "hours": 3600,
    "d": 86400,
    "day": 86400,
    "days": 86400,
    "w": 604800,
    "wk": 604800,
    "week": 604800,
    "weeks": 604800,
    "mo": 2592000,
    "month": 2592000,
    "months": 2592000,
    "y": 31536000,
    "yr": 31536000,
    "year": 31536000,
    "years": 31536000,
}

_PATTERN = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*([a-z]+)\s*$",
    re.IGNORECASE,
)


def time_to_seconds(time_str: str) -> float:
    """Converts a human-readable duration into seconds.

    Examples:
        "1 week"     -> 604800.0
        "2.5 hours"  -> 9000.0
        "30 mins"    -> 1800.0

    Args:
        time_str: A number and a unit, such as `10 min`, `2.5 hours` or `90 s`.

    Returns:
        float: The seconds.

    Raises:
        ValueError: If the text is not a number and a known unit.
    """
    match = _PATTERN.fullmatch(time_str)
    if not match:
        raise ValueError(f"Invalid time format: {time_str!r}")

    value = float(match.group(1))
    unit = match.group(2).lower()

    try:
        return value * MULTIPLIERS[unit]
    except KeyError:
        raise ValueError(f"Unknown time unit: {unit!r}") from None


def _seconds(value: Union[float, Decimal, str]) -> float:
    """Accepts a number of seconds or a duration such as `"10 min"`.

    A real number other than a bool (an `int`, a `float`, a NumPy number, a
    `Fraction`) is returned as given, so an integer stays an integer, as in SimPy. A
    `Decimal` is returned as a float. A duration in text is a float."""
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return time_to_seconds(value)
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return float(value)
    raise ValueError("expected seconds or a duration such as '10 min'")


def checked_seconds(
    value: Any, what: str, *, ends: bool = False, below_zero: bool = False
) -> Union[float, int]:
    """The seconds of a time value, refused when they cannot be a time.

    One check for every place that takes seconds: a bool, a value that is not a real
    number or a duration, NaN, a value below 0 (unless `below_zero`) and infinity
    (when `ends`) are each refused with a message that starts with `what`.

    Args:
        value: A number of seconds, a duration such as `"10 min"`, or a plain number
            in text.
        what: The place, such as `"task 't' service_time"`.
        ends: Whether the value must be finite. Left False, infinity is kept, as
            no limit or never.
        below_zero: Whether a value below 0 is allowed.

    Returns:
        The seconds, an integer staying an integer.

    Raises:
        ValueError: With `what` in the message.
    """
    if isinstance(value, bool) or not isinstance(value, (numbers.Real, Decimal, str)):
        raise ValueError(f"{what} is seconds or a duration, got {value!r}")
    try:
        seconds = _seconds(value)
    except ValueError:
        raise ValueError(f"{what} is seconds or a duration, got {value!r}") from None
    if math.isnan(seconds):
        raise ValueError(f"{what} gave nan, which is not a number of seconds")
    if seconds < 0 and not below_zero:
        raise ValueError(f"{what} gave {seconds:g}, which is below 0")
    if ends and math.isinf(seconds):
        raise ValueError(
            f"{what} gave {'inf' if seconds > 0 else '-inf'}, and it must end"
        )
    return seconds


def checked_factor(value: Any) -> Union[float, int, Decimal]:
    """A run's `factor`: a real number of 0 or more, or a plain number in text.

    It is a number of wall-clock seconds for each simulated second, not a time, so
    a duration such as `"2 min"` is refused. A bool, NaN, a value below 0 and
    infinity are refused too.

    Args:
        value: The factor as given.

    Returns:
        The number as given, or the number a text holds.

    Raises:
        ValueError: Naming `factor`.
    """
    number = value
    if isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            raise ValueError(f"factor is a number, got {value!r}") from None
    if isinstance(number, bool) or not isinstance(number, (numbers.Real, Decimal)):
        raise ValueError(f"factor is a number, got {value!r}")
    if not 0 <= number < math.inf:
        raise ValueError(
            f"factor gave {value!r}, and it must be a finite number of 0 or more"
        )
    return number


def checked_count(value: Any, what: str) -> int:
    """A whole number of 1 or more, refused for a bool, a fraction or a smaller value.

    Args:
        value: The size or count.
        what: The setting, such as `"batching batch_size"`, for the message.

    Raises:
        ValueError: With `what` in the message.
    """
    return checked_whole(value, what, 1, whole_floats=True)


def checked_interval(value: Any, what: str) -> Union[float, int]:
    """Seconds above 0, refused for NaN, a bool and 0 or less; infinity is never.

    Args:
        value: The interval as given.
        what: The place, as the message names it.
    """
    seconds = checked_seconds(value, what)
    if seconds <= 0:
        raise ValueError(f"{what} gave {seconds:g}, and it must be above 0")
    return seconds


def suggestion(name: str, choices: Iterable[str]) -> str:
    """The closest of `choices` to a misspelt `name`, as an error message's end.

    Args:
        name: The name that matched nothing.
        choices: The names that would have matched.

    Returns:
        str: `"; did you mean 'x'?"` for the closest choice, or `""` when none is
            close.
    """
    close = difflib.get_close_matches(str(name), [str(c) for c in choices], n=1)
    return f"; did you mean '{close[0]}'?" if close else ""
