"""C4 ordinary resource request and release: queued work is served in turn.

A capacity-one resource and a 5 second service, fed by an arrival every 2
seconds, serialise the jobs. The time each job acquires the resource (its
started event) must match a plain SimPy `Resource`, in all three interfaces.
"""

from pathlib import Path

import simpy

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import RecordingEgress, event_trace

EXPECTED_STARTS = [2.0, 7.0, 12.0]


def _started(records):
    return [
        sim_ts
        for sim_ts, _key, _task, status in event_trace(records)
        if status == "started"
    ]


def _reference_starts():
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    starts = []

    def entity():
        with machine.request() as req:
            yield req
            starts.append(env.now)
            yield env.timeout(5)

    def source():
        while True:
            yield env.timeout(2)
            env.process(entity())

    env.process(source())
    env.run(until=13)
    return starts


def _lowlevel_records():
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="C4",
            resources={"machine": CapacityConfig(current_cap=1, max_cap=1)},
        )
    )
    rec = RecordingEgress()
    env.setup_egress([rec])
    machine = DynamicResource(env, "C4", "machine")

    def entity(n):
        env.publish_event(f"j-{n}", {"status": "queued"}, task="work", instance=n)
        with machine.request() as req:
            yield req
            env.publish_event(f"j-{n}", {"status": "started"}, task="work", instance=n)
            yield env.timeout(5.0)
            env.publish_event(f"j-{n}", {"event_type": "done"}, task="work", instance=n)

    def source():
        n = 0
        while True:
            yield env.timeout(2.0)
            n += 1
            env.process(entity(n))

    env.process(source())
    try:
        env.run(until=13)
    finally:
        env.teardown()
    return rec.records


def _builder_records():
    rec = RecordingEgress()
    app = SimulationContext("C4", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("work", dist="constant", value=5.0)
    app.add_arrival("jobs", dist="constant", value=2.0)
    app.add_egress(rec)

    @app.task(service_id="work", resource_id="machine", name="work")
    def work(task_id):
        return {"event_type": "done"}

    @app.arrival_loop("jobs")
    def jobs(context):
        while True:
            entity = yield context.wait_for_arrival("jobs")
            context.spawn(work(context.next_instance("work"), entity=entity))

    app.run(until=13)
    return rec.records


def _yaml_records():
    rec = RecordingEgress()
    path = Path(__file__).parent / "blueprints" / "resource_queue.yaml"
    app = SimulationContext.from_yaml(path)
    app.add_egress(rec)
    app.run()
    return rec.records


def test_resource_queue_matches_simpy_in_all_three_apis():
    assert _reference_starts() == EXPECTED_STARTS

    for records in (_lowlevel_records(), _builder_records(), _yaml_records()):
        assert _started(records) == EXPECTED_STARTS
