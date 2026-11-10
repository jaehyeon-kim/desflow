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
