"""C4 resources: ordinary and priority requests are served in SimPy's order.

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
    Entity,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
    RecordingEgress,
    event_trace,
    run_builder,
    run_low_level,
    run_yaml,
)

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


def test_c4_ordinary_request_and_release():
    assert _reference_starts() == EXPECTED_STARTS

    for records in (_lowlevel_records(), _builder_records(), _yaml_records()):
        assert _started(records) == EXPECTED_STARTS


# Priority request: a holder takes the machine at 100 for 10 seconds; at 101 four
# orders queue, a and c with priority 2, b and d with priority 1. The lowest
# priority goes first, and equal priorities first in, first out: b, d, a, c.
PRIORITIES = {"a": 2, "b": 1, "c": 2, "d": 1}
EXPECTED_PRIORITY_STARTS = [(110, "b"), (111, "d"), (112, "a"), (113, "c")]


def _reference_priority():
    env = simpy.Environment()
    machine = simpy.PriorityResource(env, capacity=1)
    starts = []

    def user(name, priority, hold):
        with machine.request(priority=priority) as req:
            yield req
            if name != "holder":
                starts.append((env.now, name))
            yield env.timeout(hold)

    def source():
        yield env.timeout(100)
        env.process(user("holder", 0, 10))
        yield env.timeout(1)
        for name, priority in PRIORITIES.items():
            env.process(user(name, priority, 1))

    env.process(source())
    env.run(until=114)
    return starts


def _priority_starts(records):
    return [
        (t, key.split("-")[0])
        for t, key, task, status in event_trace(records)
        if status == "started" and task == "work"
    ]


def _lowlevel_priority():
    def start(env):
        machine = DynamicResource(env, "C4p", "machine", queue="priority")

        def user(name, priority, hold, n):
            key = f"{name}-1"
            with machine.request(priority=priority) as req:
                yield req
                task = "hold" if name == "holder" else "work"
                env.publish_event(key, {"status": "started"}, task=task, instance=n)
                yield env.timeout(hold)

        def source():
            yield env.timeout(100.0)
            env.process(user("holder", 0, 10.0, 0))
            yield env.timeout(1.0)
            for n, (name, priority) in enumerate(PRIORITIES.items()):
                env.process(user(name, priority, 1.0, n))

        env.process(source())

    params = SimParameter(
        sim_id="C4p",
        resources={"machine": CapacityConfig(current_cap=1, max_cap=1)},
    )
    return run_low_level(params, start, 114)[0]


def _builder_priority():
    app = SimulationContext("C4p", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1, queue="priority")
    app.add_service("hold", dist="constant", value=10.0)
    app.add_service("work", dist="constant", value=1.0)

    @app.task(service_id="hold", resource_id="machine", name="hold", priority=0)
    def hold(task_id):
        return {"event": "held"}

    @app.task(service_id="work", resource_id="machine", name="work", priority="attr.p")
    def work(task_id):
        return {"event": "done"}

    def script(context):
        yield context.env.timeout(100.0)
        context.spawn(hold(0, entity=Entity("holder-1", "holder")))
        yield context.env.timeout(1.0)
        for n, (name, priority) in enumerate(PRIORITIES.items()):
            context.spawn(work(n, entity=Entity(f"{name}-1", name, {"p": priority})))

    app.add_process(script)
    return run_builder(app, 114)


def test_c4_priority_resource_request():
    assert _reference_priority() == EXPECTED_PRIORITY_STARTS
    for records in (
        _lowlevel_priority(),
        _builder_priority(),
        run_yaml("resource_priority.yaml")[0],
    ):
        assert _priority_starts(records) == EXPECTED_PRIORITY_STARTS
