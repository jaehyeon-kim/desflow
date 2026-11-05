import difflib
import numbers
import re
from decimal import Decimal
from typing import Iterable, Union

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
        return time_to_seconds(value)
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return float(value)
    raise ValueError("expected seconds or a duration such as '10 min'")


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
