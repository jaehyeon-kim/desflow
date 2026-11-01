"""S3 local replay: arrivals at the times a local file records.

A plain SimPy loop over the same rows sets the arrival times. Rows must be in time
order, and the first that is not is named; rows at equal times arrive at that time
in file order; a row with no valid time is refused or skipped; rows at or after
`until` never arrive; and after the last row the arrival stops with one log line.
"""

import pytest
import simpy

from desflow import read_trace
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
    BLUEPRINTS,
    run_builder,
    run_low_level,
    run_yaml,
    run_yaml_text,
)

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


def _tied_reference(n):
    """Replay `a` has `n` rows at 5 and replay `b` has one row at 5. Each is a process
    made in this order. At 5 the process of `a` starts all its workers in its step,
    and then the process of `b` starts its worker."""
    env = simpy.Environment()
    seen = []

    def worker(name):
        seen.append((env.now, name, "started"))
        yield env.timeout(1)

    def replay(prefix, rows):
        yield env.timeout(5)
        for i in range(1, rows + 1):
            env.process(worker(f"{prefix}-{i}"))

    env.process(replay("a", n))
    env.process(replay("b", 1))
    env.run(until=30)
    return seen


@pytest.mark.parametrize("n", [1, 3, 10])
def test_tied_replay_rows_start_in_one_step(tmp_path, n):
    """The rows of one time of a replay arrival start their entities in one step, in
    file order, as a SimPy loop that starts them one after another does. Another
    replay arrival due at that time starts its entities after all of them, not
    between two of them (GD-RS6 and P40 shape)."""
    expected = _tied_reference(n)
    assert [k for _t, k, _s in expected] == [f"a-{i}" for i in range(1, n + 1)] + [
        "b-1"
    ]

    text = (
        "simulation: {sim_id: S3, factor: 0}\n"
        "services: {one: {dist: constant, value: 1}}\n"
        "arrivals:\n"
        "  a: {replay: {path: a.csv}, spawn: A}\n"
        "  b: {replay: {path: b.csv}, spawn: B}\n"
        "tasks:\n"
        "  A: {service: one, payload: {event: x}}\n"
        "  B: {service: one, payload: {event: x}}\n"
        "run: {until: 30}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [5] * n, "b": [5]})
    assert outcome.error is None
    got = [
        r["key"]
        for r in outcome.records
        if r["stream_type"] == "event" and r["value"].get("status") == "started"
    ]
    assert got == [k for _t, k, _s in expected]
