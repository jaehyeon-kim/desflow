"""read_trace: the sources, units and checks a replayed arrival relies on."""

from datetime import datetime

import pytest

from desflow import read_trace


def test_parquet_and_timestamps_map_from_the_logical_start(tmp_path):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    start = datetime(2026, 10, 18, 9, 0, 0)
    path = tmp_path / "orders.parquet"
    pq.write_table(
        pa.table(
            {
                "at": [
                    datetime(2026, 10, 18, 9, 0, 30),
                    datetime(2026, 10, 18, 9, 2, 0),
                ],
                "order": ["o1", "o2"],
            }
        ),
        path,
    )
    rows = read_trace(path, time="at", unit="timestamp", fields=["order"], start=start)
    assert [(row.time, row.fields) for row in rows] == [
        (30.0, {"order": "o1"}),
        (120.0, {"order": "o2"}),
    ]


def test_units_and_the_initial_time():
    rows = read_trace([{"time": 1.5}, {"time": 2}], unit="min", initial_time=100)
    assert [row.time for row in rows] == [190.0, 220.0]


def test_rows_before_the_start_and_unknown_settings_are_refused():
    start = datetime(2026, 10, 18, 9, 0, 0)
    with pytest.raises(ValueError, match="before the start of the run"):
        read_trace([{"time": "2026-10-18T08:59:00"}], unit="timestamp", start=start)
    with pytest.raises(ValueError, match="unit must be one of"):
        read_trace([], unit="days")
    with pytest.raises(ValueError, match="needs the run's logical_start_time"):
        read_trace([], unit="timestamp")
