import pytest

from desflow.connectors._checks import checked_poll


@pytest.mark.parametrize("poll", [0, -1, float("nan"), float("inf"), "1", True])
def test_checked_poll_refuses_what_is_not_a_positive_finite_number(poll):
    """A poll of 0 or below would loop with no wait, `nan` or infinity would never
    poll again, and text would end the task with a TypeError. Each connector's
    constructor calls `checked_poll`, so its test needs one refused value only."""
    with pytest.raises(ValueError, match="poll is a number of seconds above 0"):
        checked_poll(poll, "poll")


def test_checked_poll_names_the_setting_for_an_integer_too_large_for_a_float():
    """`math.isfinite` raises OverflowError for `10**400`; the refusal names the setting."""
    with pytest.raises(ValueError, match="poll_interval is a number of seconds"):
        checked_poll(10**400, "poll_interval")


@pytest.mark.parametrize("poll", [1, 0.01])
def test_checked_poll_returns_the_seconds_as_a_float(poll):
    """A whole or a fractional number of seconds is accepted and returned as a float."""
    assert checked_poll(poll, "poll") == float(poll)
    assert isinstance(checked_poll(poll, "poll"), float)
