"""read_trace: the sources, units and checks a replayed arrival relies on."""

from datetime import UTC, datetime

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


def test_a_replayed_timestamp_arrives_when_records_carry_it():
    """With an initial time of 100, a row stamped 110 s after logical_start_time
    arrives at simulation time 110, and its record carries the row's time."""
    from datetime import datetime

    from desflow import SimulationContext

    start = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
    records = []

    class Keep:
        async def run(self, egress_queue):
            import asyncio
            import queue

            while True:
                try:
                    records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)

    app = SimulationContext(
        "Shop", factor=0, logical_start_time=start, initial_time=100
    ).add_egress(Keep())
    app.add_arrival_replay(
        "orders", [{"at": "2026-10-01T09:01:50+00:00"}], time="at", unit="timestamp"
    )

    @app.task()
    def take(task_id):
        return {"event": "arrived"}

    @app.arrival_loop("orders")
    def loop(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(take(context.next_instance("take"), entity=entity))

    app.run(until=200)
    [record] = [r for r in records if r["stream_type"] == "event"]
    assert record["sim_ts"] == 110.0
    assert record["timestamp"] == "2026-10-01T09:01:50.000+00:00"


def test_replay_refuses_unsorted_and_invalid_rows():
    """A row out of time order is refused, naming it; so is a row with no valid
    time when on_invalid is error."""

    from desflow import read_trace
    from tests.conformance.harness import BLUEPRINTS, run_yaml
    from tests.conformance.test_s3_replay import ORDERS, _builder

    with pytest.raises(ValueError, match="row 3 is at 5 s, before the row above it"):
        read_trace(BLUEPRINTS / "s3_unsorted.csv")
    with pytest.raises(ValueError, match="row 3 is at 5 s"):
        _builder(BLUEPRINTS / "s3_unsorted.csv")
    with pytest.raises(ValueError, match="row 3 is at 5 s"):
        run_yaml("s3_unsorted.yaml")
    with pytest.raises(ValueError, match="row 4 has no valid time: 'bad'"):
        read_trace(ORDERS)


def test_replay_stops_at_the_end_of_the_input(caplog):
    """After its last row the arrival stops, with one INFO line naming the time."""
    import logging

    from tests.conformance.test_s3_replay import _arrived, _builder

    rows = [
        {"time": 1, "order": "o1", "sku": "tea"},
        {"time": 3, "order": "o2", "sku": "mug"},
    ]
    with caplog.at_level(logging.INFO, logger="desflow.core.context"):
        records = _builder(rows)
    assert _arrived(records) == [(1.0, "o1", "tea"), (3.0, "o2", "mug")]
    lines = [
        r.getMessage()
        for r in caplog.records
        if "replayed its last row" in r.getMessage()
    ]
    assert lines == ["Arrival 'orders' replayed its last row at 3 s; it stops."]
