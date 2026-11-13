"""Checks that connector arguments share."""

import math
import numbers
from typing import Any


def checked_whole(
    value: object, what: str, minimum: int, whole_floats: bool = False
) -> int:
    """Returns `value` when it is a whole number of `minimum` or more, else refuses it.

    A bool, a fraction, NaN and infinity are not whole numbers. A whole float such as
    `3.0` is accepted only with `whole_floats`, for a size that YAML may write as a
    float; a count of runs, tasks or retries is an integer.

    Args:
        value: The value to check.
        what: The name the refusal gives, such as `max_tasks` or `--seed`.
        minimum: The smallest value accepted.
        whole_floats: Accept a float that holds a whole number.

    Returns:
        int: `value`.

    Raises:
        ValueError: If `value` is not a whole number of `minimum` or more.
    """
    whole = isinstance(value, numbers.Integral) or (
        whole_floats and isinstance(value, float) and value.is_integer()
    )
    if isinstance(value, bool) or not whole:
        raise ValueError(f"{what} is a whole number of {minimum} or more, got {value}")
    if value < minimum:  # type: ignore[operator]
        raise ValueError(f"{what} is a whole number of {minimum} or more, got {value}")
    return int(value)  # type: ignore[call-overload]


def checked_poll(value: object, what: str) -> float:
    """Returns `value` when it is a positive, finite number of seconds, else refuses it.

    A poll of 0 or below would make the connector check its source in a loop with
    no wait, and `nan` or infinity would make it wait for ever. Text such as `"1"`
    is not a number here.

    Args:
        value: The seconds between two checks.
        what: The name the refusal gives, such as `poll_interval`.

    Returns:
        float: The seconds.

    Raises:
        ValueError: If `value` is not a number, or is not above 0, or is not finite, or
            is an integer too large for a float.
    """
    if (
        isinstance(value, bool)
        or not isinstance(value, numbers.Real)
        or not is_finite(value)
        or value <= 0
    ):
        raise ValueError(
            f"{what} is a number of seconds above 0 that is not infinite, got {value!r}"
        )
    return float(value)


def is_finite(number: Any) -> bool:
    """Returns whether a real number is finite; an integer too large for a float is not."""
    try:
        return math.isfinite(number)
    except OverflowError:
        return False
