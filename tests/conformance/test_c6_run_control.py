"""C6 run control: the simulation clock starts at the chosen initial time.

A plain SimPy reference and the three desflow interfaces run the same
deterministic arrival and service model with the clock started at 100. Every
event time is relative to that start, matching SimPy.
"""

from pathlib import Path

import simpy

from desflow import DynamicRealtimeEnvironment
from desflow.core.context import SimulationContext
from tests.conformance.harness import RecordingEgress, event_trace

EXPECTED_TIMES = [110.0, 115.0]


def _reference_times():
    env = simpy.Environment(initial_time=100)
    times = []

    def entity():
        times.append(env.now)
        yield env.timeout(5)
        times.append(env.now)

    def source():
        while True:
            yield env.timeout(10)
            env.process(entity())

    env.process(source())
    env.run(until=116)
    return times


def _lowlevel_records():
    env = DynamicRealtimeEnvironment(initial_time=100, factor=0.0)
    rec = RecordingEgress()
    env.setup_egress([rec])

    def entity(n):
        env.publish_event(f"orders-{n}", {"status": "started"}, task="wait", instance=n)
        yield env.timeout(5.0)
        env.publish_event(
            f"orders-{n}", {"event_type": "waited"}, task="wait", instance=n
        )

    def source():
        n = 0
        while True:
            yield env.timeout(10.0)
            n += 1
            env.process(entity(n))

    env.process(source())
    try:
        env.run(until=116)
    finally:
        env.teardown()
    return rec.records


def _builder_records():
    rec = RecordingEgress()
    app = SimulationContext("C6", factor=0.0, initial_time=100.0)
    app.add_service("wait", dist="constant", value=5.0)
    app.add_arrival("orders", dist="constant", value=10.0)
    app.add_egress(rec)

    @app.task(service_id="wait", name="wait")
    def wait(task_id):
        return {"event_type": "waited"}

    @app.arrival_loop("orders")
    def orders(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(wait(context.next_instance("wait"), entity=entity))

    app.run(until=116)
    return rec.records


def _yaml_records():
    rec = RecordingEgress()
    path = Path(__file__).parent / "blueprints" / "initial_time.yaml"
    app = SimulationContext.from_yaml(path)
    app.add_egress(rec)
    app.run()
    return rec.records


def test_c6_initial_time():
    assert _reference_times() == EXPECTED_TIMES

    for records in (_lowlevel_records(), _builder_records(), _yaml_records()):
        assert [sim_ts for sim_ts, *_ in event_trace(records)] == EXPECTED_TIMES


# Stop at a time: an event due at exactly `until` does not fire, as SimPy's run
# stops at that instant. One entity arrives at 10 and emits an event; the next
# would arrive at 20, the horizon, so it never happens.
EXPECTED_STOP = [10.0]


def _stop_reference():
    env = simpy.Environment()
    times = []

    def source():
        while True:
            yield env.timeout(10)
            times.append(env.now)

    env.process(source())
    env.run(until=20)
    return times


def _stop_lowlevel_records():
    env = DynamicRealtimeEnvironment(factor=0.0)
    rec = RecordingEgress()
    env.setup_egress([rec])

    def source():
        n = 0
        while True:
            yield env.timeout(10.0)
            n += 1
            env.publish_event(f"p-{n}", {"event": "ping"}, task="ping", instance=n)

    env.process(source())
    try:
        env.run(until=20)
    finally:
        env.teardown()
    return rec.records


def _stop_builder_records():
    rec = RecordingEgress()
    app = SimulationContext("C6stop", factor=0.0)
    app.add_arrival("pings", dist="constant", value=10.0)
    app.add_egress(rec)

    @app.task(name="ping")
    def ping(task_id):
        return {"event": "ping"}

    @app.arrival_loop("pings")
    def pings(context):
        while True:
            entity = yield context.wait_for_arrival("pings")
            context.spawn(ping(context.next_instance("ping"), entity=entity))

    app.run(until=20)
    return rec.records


def _stop_yaml_records():
    rec = RecordingEgress()
    path = Path(__file__).parent / "blueprints" / "stop_at_time.yaml"
    app = SimulationContext.from_yaml(path)
    app.add_egress(rec)
    app.run()
    return rec.records


def test_c6_stop_at_a_time():
    assert _stop_reference() == EXPECTED_STOP

    for records in (
        _stop_lowlevel_records(),
        _stop_builder_records(),
        _stop_yaml_records(),
    ):
        assert [sim_ts for sim_ts, *_ in event_trace(records)] == EXPECTED_STOP
