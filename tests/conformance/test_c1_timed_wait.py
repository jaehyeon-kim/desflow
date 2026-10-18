"""C1 timed wait: a service delay ends at the specified simulation time.

A plain SimPy reference and the three desflow interfaces run the same
deterministic model: an entity arrives every 10 seconds and a timed task holds
it for 5. The event times must match SimPy, and each entity's finished event
must fall exactly 5 seconds after its started event.
"""

from pathlib import Path

import simpy

from desflow import DynamicRealtimeEnvironment
from desflow.core.context import SimulationContext
from tests.conformance.harness import RecordingEgress, event_trace

EXPECTED_TIMES = [10.0, 15.0, 20.0, 25.0]


def _reference_times():
    env = simpy.Environment()
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
    env.run(until=26)
    return times


def _lowlevel_records():
    env = DynamicRealtimeEnvironment(factor=0.0)
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
        env.run(until=26)
    finally:
        env.teardown()
    return rec.records


def _builder_records():
    rec = RecordingEgress()
    app = SimulationContext("C1", factor=0.0)
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

    app.run(until=26)
    return rec.records


def _yaml_records():
    rec = RecordingEgress()
    path = Path(__file__).parent / "blueprints" / "timed_service.yaml"
    app = SimulationContext.from_yaml(path)
    app.add_egress(rec)
    app.run()
    return rec.records


def test_c1_timed_wait():
    assert _reference_times() == EXPECTED_TIMES

    for records in (_lowlevel_records(), _builder_records(), _yaml_records()):
        trace = event_trace(records)
        assert [sim_ts for sim_ts, *_ in trace] == EXPECTED_TIMES
        times_by_key: dict = {}
        for sim_ts, key, _task, _status in trace:
            times_by_key.setdefault(key, []).append(sim_ts)
        for started_then_finished in times_by_key.values():
            assert started_then_finished[1] - started_then_finished[0] == 5.0
