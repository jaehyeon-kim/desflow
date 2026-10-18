"""S3 local replay: arrivals at the times a local file records.

A plain SimPy loop over the same rows sets the arrival times. Rows must be in time
order, and the first that is not is named; rows at equal times arrive at that time
in file order; a row with no valid time is refused or skipped; rows at or after
`until` never arrive; and after the last row the arrival stops with one log line.
"""

import logging

import pytest
import simpy

from desflow import read_trace
from desflow.core.context import SimulationContext
from tests.conformance.harness import BLUEPRINTS, run_builder, run_low_level, run_yaml

ORDERS = BLUEPRINTS / "s3_orders.csv"
EXPECTED = [
    (2.0, "o1", "tea"),
    (5.0, "o2", "mug"),
    (5.0, "o3", "cup"),
    (9.0, "o5", "pot"),
]


def _arrived(records):
    return [
        (r["sim_ts"], r["value"]["order"], r["value"]["sku"])
        for r in records
        if r["stream_type"] == "event"
    ]


def _reference():
    env = simpy.Environment()
    arrived = []
    rows = [
        (2, "o1", "tea"),
        (5, "o2", "mug"),
        (5, "o3", "cup"),
        (9, "o5", "pot"),
        (12, "o6", "lid"),
    ]

    def source():
        for at, order, sku in rows:
            yield env.timeout(at - env.now)
            arrived.append((float(env.now), order, sku))

    env.process(source())
    env.run(until=12)
    return arrived


def _low_level():
    def start(env):
        rows = read_trace(ORDERS, fields=["order", "sku"], on_invalid="skip")

        def source():
            for n, row in enumerate(rows, start=1):
                yield env.timeout(row.time - env.now)
                env.publish_event(
                    f"orders-{n}",
                    {"event": "arrived", **row.fields},
                    task="take",
                    instance=n,
                )

        env.process(source())

    return run_low_level(None, start, 12)[0]


def _builder(source=ORDERS, **options):
    app = SimulationContext("S3", factor=0.0)
    app.add_arrival_replay(
        "orders", source, fields=["order", "sku"], on_invalid="skip", **options
    )

    @app.task()
    def take(task_id, entity):
        return {"event": "arrived", **entity.attributes}

    @app.arrival_loop("orders")
    def orders(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(take(context.next_instance("take"), entity=entity))

    return run_builder(app, 12)


def test_s3_replay_equal_times_invalid_rows_and_the_horizon():
    """o2 and o3 arrive together at 5 in file order; o4, with no valid time, is
    skipped; o6, at 12, the end of the run, never arrives."""
    assert _reference() == EXPECTED
    for records in (_low_level(), _builder(), run_yaml("s3_replay.yaml")[0]):
        assert _arrived(records) == EXPECTED


def test_s3_replay_refuses_unsorted_and_invalid_rows():
    """A row out of time order is refused, naming it; so is a row with no valid
    time when on_invalid is error."""
    with pytest.raises(ValueError, match="row 3 is at 5 s, before the row above it"):
        read_trace(BLUEPRINTS / "s3_unsorted.csv")
    with pytest.raises(ValueError, match="row 3 is at 5 s"):
        _builder(BLUEPRINTS / "s3_unsorted.csv")
    with pytest.raises(ValueError, match="row 3 is at 5 s"):
        run_yaml("s3_unsorted.yaml")
    with pytest.raises(ValueError, match="row 4 has no valid time: 'bad'"):
        read_trace(ORDERS)


def test_s3_replay_stops_at_the_end_of_the_input(caplog):
    """After its last row the arrival stops, with one INFO line naming the time."""
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
