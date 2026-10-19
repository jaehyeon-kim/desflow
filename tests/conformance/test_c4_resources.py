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


# Cancel before and after a grant. A holder takes the machine at 10 for 10 s; a
# waiter queues at 11. Before a grant, cancelling (low-level cancel(), or an
# interrupt with abandon) leaves the queue at once, and nothing is granted to it
# later. After a grant, SimPy's cancel releases nothing; an interrupt with abandon
# gives the unit back at once, so the next waiter starts then.


def _reference_cancel():
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    seen = {}

    def holder():
        yield env.timeout(10)
        req = machine.request()
        yield req
        yield env.timeout(2)
        req.cancel()  # after the grant: releases nothing
        seen["after cancel"] = machine.count
        yield env.timeout(8)
        machine.release(req)

    def waiter():
        yield env.timeout(11)
        req = machine.request()
        yield env.timeout(2)
        req.cancel()  # before the grant: leaves the queue
        seen["queue after cancel"] = len(machine.queue)
        yield env.timeout(10)
        seen["waiter granted"] = req.triggered

    env.process(holder())
    env.process(waiter())
    env.run(until=25)
    return seen


def _low_level_cancel():
    seen = {}

    def start(env):
        machine = DynamicResource(env, "C4c", "machine")

        def holder():
            yield env.timeout(10)
            req = machine.request()
            yield req
            yield env.timeout(2)
            req.cancel()
            seen["after cancel"] = machine.in_use
            yield env.timeout(8)
            machine.release(req)

        def waiter():
            yield env.timeout(11)
            req = machine.request()
            yield env.timeout(2)
            req.cancel()
            seen["queue after cancel"] = len(machine.queue.items)
            yield env.timeout(10)
            seen["waiter granted"] = req.triggered

        env.process(holder())
        env.process(waiter())

    run_low_level(
        SimParameter(
            sim_id="C4c",
            resources={"machine": CapacityConfig(current_cap=1, max_cap=1)},
        ),
        start,
        25,
    )
    return seen


def _builder_abandon(target):
    """A holder from 10 for 10 s and a waiter from 11; `target` is interrupted with
    abandon at 13. Returns the waiter's events and the machine's use after 13."""
    app = SimulationContext("C4c", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=10)
    app.add_service("work", dist="constant", value=1)
    seen = {}

    @app.task(service_id="hold", resource_id="machine", on_interrupt="abandon")
    def holder(task_id):
        return {"event": "held"}

    @app.task(service_id="work", resource_id="machine", on_interrupt="abandon")
    def waiter(task_id):
        return {"event": "worked"}

    def script(context):
        yield context.env.timeout(10)
        context.spawn(holder(0, entity=Entity("holders-1", "holders")))
        yield context.env.timeout(1)
        context.spawn(waiter(0, entity=Entity("waiters-1", "waiters")))
        yield context.env.timeout(2)
        context.interrupt(target, cause="recall")
        yield context.env.timeout(0.5)
        machine = context.get_resource("machine")
        seen["in use"], seen["queue"] = machine.in_use, len(machine.queue.items)

    app.add_process(script)
    records = run_builder(app, 25)
    return records, seen


def _started_of(records, key):
    return [
        r["sim_ts"]
        for r in records
        if r["stream_type"] == "event"
        and r["key"] == key
        and r["value"].get("status") == "started"
    ]


def test_c4_cancel_before_a_grant():
    """The waiter's request leaves the queue at 13 and is never granted."""
    assert _reference_cancel()["queue after cancel"] == 0
    assert _reference_cancel()["waiter granted"] is False
    seen = _low_level_cancel()
    assert seen["queue after cancel"] == 0 and seen["waiter granted"] is False
    records, after = _builder_abandon("waiter")
    assert after == {"in use": 1, "queue": 0}
    assert _started_of(records, "waiters-1") == []
    yaml_records, app = run_yaml("c4_cancel_waiting.yaml")
    assert _started_of(yaml_records, "waiters-1") == []
    assert app.get_resource("machine").queue.items == []


def test_c4_cancel_after_a_grant():
    """A granted request's cancel releases nothing; an interrupt with abandon gives
    the unit back at 13, so the waiter starts at 13, not at 20."""
    assert _reference_cancel()["after cancel"] == 1
    assert _low_level_cancel()["after cancel"] == 1
    records, after = _builder_abandon("holder")
    assert after == {"in use": 1, "queue": 0}
    assert _started_of(records, "waiters-1") == [13.0]
    yaml_records, _app = run_yaml("c4_cancel_granted.yaml")
    assert _started_of(yaml_records, "waiters-1") == [13.0]


# Hold several resources: an order acquires the picker, then the bench, holding the
# picker while it waits. The bench is busy from 9 to 29, so at the horizon of 15 the
# order holds the picker and waits for the bench: a partial holding, as nested
# SimPy requests leave it.


def _reference_several():
    env = simpy.Environment()
    picker, bench = simpy.Resource(env, 1), simpy.Resource(env, 1)
    trace = []

    def bench_user():
        yield env.timeout(9)
        with bench.request() as req:
            yield req
            yield env.timeout(20)

    def order():
        yield env.timeout(10)
        with picker.request() as p:
            yield p
            trace.append((env.now, "picker"))
            with bench.request() as b:
                yield b
                trace.append((env.now, "bench"))
                yield env.timeout(5)

    env.process(bench_user())
    env.process(order())
    env.run(until=15)
    return trace, (picker.count, bench.count, len(bench.queue))


def _builder_several():
    app = SimulationContext("C4s", factor=0.0)
    app.add_resource("picker", current_cap=1, max_cap=1)
    app.add_resource("bench", current_cap=1, max_cap=1)
    app.add_service("long", dist="constant", value=20)
    app.add_service("work", dist="constant", value=5)

    @app.task(service_id="long", resource_id="bench")
    def other(task_id):
        return {"event": "other"}

    @app.task(service_id="work", resource_id=["picker", "bench"])
    def order(task_id):
        return {"event": "done"}

    def script(context):
        yield context.env.timeout(9)
        context.spawn(other(0, entity=Entity("others-1", "others")))
        yield context.env.timeout(1)
        context.spawn(order(0, entity=Entity("orders-1", "orders")))

    app.add_process(script)
    records = run_builder(app, 15)
    return records, app


def _holdings(app):
    picker, bench = app.get_resource("picker"), app.get_resource("bench")
    return (picker.in_use, bench.in_use, len(bench.queue.items))


def test_c4_hold_several_resources():
    """At 15 the order holds the picker and waits for the bench."""
    trace, holdings = _reference_several()
    assert trace == [(10, "picker")]
    assert holdings == (1, 1, 1)

    def start(env):
        picker = DynamicResource(env, "C4s", "picker")
        bench = DynamicResource(env, "C4s", "bench")
        start.parts = (picker, bench)

        def bench_user():
            yield env.timeout(9)
            with bench.request() as req:
                yield req
                yield env.timeout(20)

        def order():
            yield env.timeout(10)
            with picker.request() as p:
                yield p
                with bench.request() as b:
                    yield b
                    yield env.timeout(5)

        env.process(bench_user())
        env.process(order())

    params = SimParameter(
        sim_id="C4s",
        resources={
            "picker": CapacityConfig(current_cap=1, max_cap=1),
            "bench": CapacityConfig(current_cap=1, max_cap=1),
        },
    )
    run_low_level(params, start, 15)
    picker, bench = start.parts
    assert (picker.in_use, bench.in_use, len(bench.queue.items)) == holdings

    records, app = _builder_several()
    assert _holdings(app) == holdings
    assert _started_of(records, "orders-1") == []
    yaml_records, yaml_app = run_yaml("c4_several.yaml")
    assert _holdings(yaml_app) == holdings


# Keep: the picker carries an order to the bench and stays with it until the bench
# is free, so the next order waits for the picker as long as the bench is busy. In
# plain SimPy the picker's request is released only once the bench's is granted.

KEEP_UNTIL = 30


def _reference_keep():
    env = simpy.Environment()
    picker, bench = simpy.Resource(env, 1), simpy.Resource(env, 1)
    starts = []

    def order(n):
        p = picker.request()
        yield p
        starts.append((env.now, f"orders-{n}", "pick"))
        yield env.timeout(1)
        b = bench.request()
        yield b
        picker.release(p)
        starts.append((env.now, f"orders-{n}", "pack"))
        yield env.timeout(4)
        bench.release(b)

    def source():
        n = 0
        while True:
            yield env.timeout(2)
            n += 1
            env.process(order(n))

    env.process(source())
    env.run(until=KEEP_UNTIL)
    return sorted(starts)


def _keep_starts(records):
    return sorted(
        (t, key, task)
        for t, key, task, status in event_trace(records)
        if status == "started"
    )


def _low_level_keep():
    def start(env):
        picker = DynamicResource(env, "C4k", "picker")
        bench = DynamicResource(env, "C4k", "bench")

        def order(n):
            key = f"orders-{n}"
            p = picker.request()
            yield p
            env.publish_event(key, {"status": "started"}, task="pick", instance=n)
            yield env.timeout(1)
            b = bench.request()
            yield b
            picker.release(p)
            env.publish_event(key, {"status": "started"}, task="pack", instance=n)
            yield env.timeout(4)
            bench.release(b)

        def source():
            n = 0
            while True:
                yield env.timeout(2)
                n += 1
                env.process(order(n))

        env.process(source())

    params = SimParameter(
        sim_id="C4k",
        resources={
            "picker": CapacityConfig(current_cap=1, max_cap=1),
            "bench": CapacityConfig(current_cap=1, max_cap=1),
        },
    )
    return run_low_level(params, start, KEEP_UNTIL)[0]


def _builder_keep():
    app = SimulationContext("C4k", factor=0.0)
    app.add_resource("picker", current_cap=1, max_cap=1)
    app.add_resource("bench", current_cap=1, max_cap=1)
    app.add_service("walking", dist="constant", value=1)
    app.add_service("packing", dist="constant", value=4)
    app.add_arrival("orders", dist="constant", value=2)

    @app.task(service_id="walking", resource_id="picker", keep=["picker"], next="pack")
    def pick(task_id):
        return {"event": "picked"}

    @app.task(service_id="packing", resource_id="bench")
    def pack(task_id):
        return {"event": "packed"}

    @app.arrival_loop("orders")
    def loop(context):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pick(context.next_instance("pick"), entity=order))

    return run_builder(app, KEEP_UNTIL)


def test_c4_keep_hands_a_unit_to_the_next_task():
    """Each order's pick and pack start when SimPy's nested requests give them, and
    no pick starts while the picker waits with an order for the bench."""
    expected = _reference_keep()
    assert expected[:4] == [
        (2, "orders-1", "pick"),
        (3, "orders-1", "pack"),
        (4, "orders-2", "pick"),
        (7, "orders-2", "pack"),
    ]
    assert _keep_starts(_low_level_keep()) == expected
    assert _keep_starts(_builder_keep()) == expected
    assert _keep_starts(run_yaml("c4_keep.yaml")[0]) == expected


# Mixed preempt flags on a priority resource: SimPy's PriorityResource ranks a
# waiting request by (priority, time, not preempt), so among equal priorities and
# times the request that may preempt is served first, whatever the order they came.


def _reference_mixed():
    env = simpy.Environment()
    machine = simpy.PriorityResource(env, capacity=1)
    starts = []

    def job(name, at, priority, preempt, hold):
        yield env.timeout(at)
        with machine.request(priority=priority, preempt=preempt) as req:
            yield req
            starts.append((env.now, name))
            yield env.timeout(hold)

    env.process(job("holder", 0.001, 0, True, 10))
    env.process(job("plain", 5, 1, False, 1))
    env.process(job("eager", 5, 1, True, 1))
    env.run(until=10.5)
    return starts


def _mixed_starts(records):
    names = {"holders": "holder", "plain": "plain", "eager": "eager"}
    return [
        (t, names[key.split("-")[0]])
        for t, key, _task, status in event_trace(records)
        if status == "started"
    ]


def test_c4_priority_request_with_mixed_preempt_flags():
    """At 10.001 the request that may preempt is served before the one that may
    not, though both have priority 1 and the other came first."""
    expected = _reference_mixed()
    assert expected == [(0.001, "holder"), (10.001, "eager")]

    starts = []

    def start(env):
        machine = DynamicResource(env, "C4m", "machine", queue="priority")

        def job(name, at, priority, preempt, hold):
            yield env.timeout(at)
            with machine.request(priority=priority, preempt=preempt) as req:
                yield req
                starts.append((env.now, name))
                yield env.timeout(hold)

        env.process(job("holder", 0.001, 0, True, 10))
        env.process(job("plain", 5, 1, False, 1))
        env.process(job("eager", 5, 1, True, 1))

    params = SimParameter(
        sim_id="C4m", resources={"machine": CapacityConfig(current_cap=1, max_cap=1)}
    )
    run_low_level(params, start, 10.5)
    assert [(round(t, 3), n) for t, n in starts] == expected

    app = SimulationContext("C4m", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1, queue="priority")
    app.add_service("hold", dist="constant", value=10)
    app.add_service("work", dist="constant", value=1)
    app.add_arrival("holders", dist="constant", value=0.001)
    app.add_arrival("plain", dist="constant", value=5)
    app.add_arrival("eager", dist="constant", value=5)

    @app.task(service_id="hold", resource_id="machine", priority=0)
    def holder(task_id):
        return {"event": "held"}

    @app.task(service_id="work", resource_id="machine", priority=1, preempt=False)
    def plain_job(task_id):
        return {"event": "done"}

    @app.task(service_id="work", resource_id="machine", priority=1)
    def eager_job(task_id):
        return {"event": "done"}

    for arrival, task in (
        ("holders", holder),
        ("plain", plain_job),
        ("eager", eager_job),
    ):

        def loop(context, arrival=arrival, task=task):
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(task.__name__), entity=entity))

        app.arrival_loop(arrival)(loop)

    assert _mixed_starts(run_builder(app, 10.5)) == expected
    assert _mixed_starts(run_yaml("c4_mixed_preempt.yaml")[0]) == expected
