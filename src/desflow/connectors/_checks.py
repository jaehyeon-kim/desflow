"""Checks that connector arguments share."""

import numbers


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
