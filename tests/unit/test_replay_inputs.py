"""A replay row's time and fields are read only as CSV numbers."""

import logging

import pytest

from desflow.core.replay import plain_value, read_trace

BAD_TIMES = ["nan", "inf", "", "1_0", "-inf", "0x1", "Infinity"]


@pytest.mark.parametrize("bad", BAD_TIMES)
def test_a_bad_time_is_refused_under_error(bad):
    with pytest.raises(ValueError, match="row 2 has no valid time"):
        read_trace([{"time": "1"}, {"time": bad}])


@pytest.mark.parametrize("bad", BAD_TIMES)
def test_a_bad_time_is_skipped_with_a_warning_under_skip(bad, caplog):
    with caplog.at_level(logging.WARNING):
        rows = read_trace(
            [{"time": "1"}, {"time": bad}, {"time": "3"}], on_invalid="skip"
        )
    assert [row.time for row in rows] == [1.0, 3.0]
    assert "Skipped row 2" in caplog.text


def test_a_row_earlier_than_the_row_above_is_refused():
    with pytest.raises(ValueError, match="time order"):
        read_trace([{"time": "5"}, {"time": "1"}])


@pytest.mark.parametrize("text", ["12_34", "0x1", "nan", "Infinity", "1e", "--5"])
def test_a_field_that_is_not_a_csv_number_stays_text(text):
    assert plain_value(text) == text


@pytest.mark.parametrize(
    "text, value", [("12", 12), ("-3", -3), ("1.5", 1.5), (".5", 0.5), ("1e3", 1000.0)]
)
def test_a_csv_number_is_read_as_a_number(text, value):
    assert plain_value(text) == value


@pytest.mark.parametrize("bad", [True, False, 10**400], ids=["true", "false", "huge"])
def test_a_bool_or_a_number_too_large_for_a_float_is_not_a_time(bad, caplog):
    """`True` is not 1 second, and `10**400` does not raise from `float()`: each is
    refused under `error` and skipped under `skip`."""
    with pytest.raises(ValueError, match="row 1 has no valid time"):
        read_trace([{"time": bad}])
    with caplog.at_level(logging.WARNING):
        rows = read_trace([{"time": bad}, {"time": 2}], on_invalid="skip")
    assert [row.time for row in rows] == [2.0]


def test_a_field_of_digits_past_a_float_stays_text():
    """`1e400` reads as infinity in a float, so the field stays text, as the time
    column treats it."""
    assert plain_value("1e400") == "1e400"
    assert plain_value("1" + "0" * 400) == "1" + "0" * 400
    assert plain_value("1e300") == 1e300
