"""C7 patience: reneging after a wait, and balking before joining.

Reneging: a holder takes the one machine at time 1 and keeps it; a waiter
arrives at 3 with a patience of 3, is never served, and reneges at 6, matching
a plain SimPy reneging pattern. Balking: a task whose balk condition holds
never joins the queue, so it emits a balked event and no queued event.
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
from tests.conformance.harness import RecordingEgress, event_trace

EXPECTED_RENEGE = [6.0]


def _reneged(records):
    return [t for t, _k, _task, status in event_trace(records) if status == "reneged"]


def _statuses(records):
    return [status for _t, _k, _task, status in event_trace(records)]


def _reference_renege():
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    reneged = []

    def holder():
        with machine.request() as req:
            yield req
            yield env.timeout(10)

    def waiter():
        with machine.request() as req:
            yield req | env.timeout(3)
            if not req.triggered:
                reneged.append(env.now)
                return
            yield env.timeout(1)

    def source():
        yield env.timeout(1)
        env.process(holder())
        yield env.timeout(2)
        env.process(waiter())

    env.process(source())
    env.run(until=7)
    return reneged


def _lowlevel_renege_records():
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="C7",
            resources={"machine": CapacityConfig(current_cap=1, max_cap=1)},
        )
    )
    rec = RecordingEgress()
    env.setup_egress([rec])
    machine = DynamicResource(env, "C7", "machine")

    def holder():
        with machine.request() as req:
            yield req
            yield env.timeout(10.0)

    def waiter():
        env.publish_event("w-1", {"status": "queued"}, task="waiter", instance=1)
        with machine.request() as req:
            yield req | env.timeout(3.0)
            if not req.triggered:
                env.publish_event(
                    "w-1", {"status": "reneged"}, task="waiter", instance=1
                )
                return
            env.publish_event("w-1", {"status": "started"}, task="waiter", instance=1)
            yield env.timeout(1.0)

    def source():
        yield env.timeout(1.0)
        env.process(holder())
        yield env.timeout(2.0)
        env.process(waiter())

    env.process(source())
    try:
        env.run(until=7)
    finally:
        env.teardown()
    return rec.records


def _builder_renege_records():
    rec = RecordingEgress()
    app = SimulationContext("C7", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=10.0)
    app.add_service("quick", dist="constant", value=1.0)
    app.add_egress(rec)

    @app.task(service_id="hold", resource_id="machine", name="holder")
    def holder(task_id):
        return {"event": "held"}

    @app.task(service_id="quick", resource_id="machine", name="waiter", patience=3.0)
    def waiter(task_id):
        return {"event": "served"}

    def script(context):
        yield context.env.timeout(1.0)
        context.spawn(holder(context.next_instance("holder")))
        yield context.env.timeout(2.0)
        context.spawn(waiter(context.next_instance("waiter")))

    app.add_process(script)
    app.run(until=7)
    return rec.records


def _yaml_renege_records():
    rec = RecordingEgress()
    path = Path(__file__).parent / "blueprints" / "patience_renege.yaml"
    app = SimulationContext.from_yaml(path)
    app.add_egress(rec)
    app.run()
    return rec.records


def test_reneging_matches_simpy_in_all_three_apis():
    assert _reference_renege() == EXPECTED_RENEGE

    for records in (
        _lowlevel_renege_records(),
        _builder_renege_records(),
        _yaml_renege_records(),
    ):
        assert _reneged(records) == EXPECTED_RENEGE


def _builder_balk_records():
    rec = RecordingEgress()
    app = SimulationContext("C7balk", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("quick", dist="constant", value=1.0)
    app.add_egress(rec)

    @app.task(
        service_id="quick", resource_id="machine", name="maybe", balk="attr.skip == 1"
    )
    def maybe(task_id):
        return {"event": "served"}

    def script(context):
        context.spawn(
            maybe(
                context.next_instance("maybe"), entity=Entity("s-1", "x", {"skip": 1})
            )
        )
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=5)
    return rec.records


def _yaml_balk_records():
    rec = RecordingEgress()
    path = Path(__file__).parent / "blueprints" / "balk.yaml"
    app = SimulationContext.from_yaml(path)
    app.add_egress(rec)
    app.run()
    return rec.records


def test_balking_emits_balked_and_no_queued_in_builder_and_yaml():
    for records in (_builder_balk_records(), _yaml_balk_records()):
        statuses = _statuses(records)
        assert "balked" in statuses
        assert "queued" not in statuses


def test_reneging_does_not_leak_the_in_flight_statistic():
    # A reneging task was counted as queued, so its in-flight record must be
    # cleared, or it would grow without bound in a long run that reneges.
    app = SimulationContext("C7leak", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=10.0)
    app.add_service("quick", dist="constant", value=1.0)
    app.with_stats("all")

    @app.task(service_id="hold", resource_id="machine", name="holder")
    def holder(task_id):
        return {"event": "held"}

    @app.task(service_id="quick", resource_id="machine", name="waiter", patience=3.0)
    def waiter(task_id):
        return {"event": "served"}

    def script(context):
        yield context.env.timeout(1.0)
        context.spawn(holder(context.next_instance("holder")))
        yield context.env.timeout(2.0)
        context.spawn(waiter(context.next_instance("waiter")))

    app.add_process(script)
    app.run(until=7)
    assert ("waiter", 0) not in app.env.stats._in_flight
