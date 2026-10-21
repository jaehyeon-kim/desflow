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
from tests.conformance import custom_resources as custom
from tests.conformance.b3 import Job, run_desflow, run_simpy, who
from tests.conformance.harness import (
    RecordingEgress,
    event_trace,
    run_builder,
    run_low_level,
    run_yaml,
    run_yaml_text,
    times,
)

EXPECTED_STARTS = [2.0, 7.0, 12.0]


def _started(records):
    return [
        sim_ts
        for sim_ts, _key, _task, status in event_trace(records)
        if status == "started"
    ]


def _reference_starts(capacity=1):
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=capacity)
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


def _lowlevel_records(caps=CapacityConfig(current_cap=1, max_cap=1)):
    """The jobs on a `DynamicResource`; `caps` None registers nothing for it."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(sim_id="C4", resources={} if caps is None else {"machine": caps})
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


def _builder_records(**capacity):
    rec = RecordingEgress()
    app = SimulationContext("C4", factor=0.0)
    app.add_resource("machine", **(capacity or {"current_cap": 1, "max_cap": 1}))
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


def _yaml_records(blueprint="resource_queue.yaml"):
    rec = RecordingEgress()
    path = Path(__file__).parent / "blueprints" / blueprint
    app = SimulationContext.from_yaml(path)
    app.add_egress(rec)
    app.run()
    return rec.records


def test_c4_ordinary_request_and_release():
    assert _reference_starts() == EXPECTED_STARTS

    for records in (_lowlevel_records(), _builder_records(), _yaml_records()):
        assert _started(records) == EXPECTED_STARTS


def test_c4_default_capacity():
    """A resource declared with no capacity has one unit, as `simpy.Resource(env)`:
    the jobs are served one after the other. The low-level resource registers
    itself when the registry has no path for it."""
    assert _reference_starts() == EXPECTED_STARTS

    for records in (
        _lowlevel_records(None),
        _builder_records(),
        _yaml_records("c4_default_capacity.yaml"),
    ):
        assert _started(records) == EXPECTED_STARTS

    # Only max_cap given: the capacity is one, and the largest a live raise can set
    # is max_cap
    records = _builder_records(max_cap=3)
    assert _started(records) == EXPECTED_STARTS


UNLIMITED_STARTS = [2.0, 4.0, 6.0, 8.0, 10.0, 12.0]


def test_c4_resource_with_no_capacity_limit():
    """A capacity of infinity never queues a request, as `simpy.Resource(env,
    float("inf"))`: every job starts when it arrives."""
    assert _reference_starts(float("inf")) == UNLIMITED_STARTS

    inf = float("inf")
    for records in (
        _lowlevel_records(CapacityConfig(current_cap=inf, max_cap=inf)),
        _lowlevel_records(CapacityConfig(current_cap=inf)),
        _builder_records(current_cap=inf),
        _yaml_records("c4_no_limit.yaml"),
    ):
        assert _started(records) == UNLIMITED_STARTS


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
            seen["queue after cancel"] = len(machine.queue)
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


def _reference_abandon(target):
    """Plain SimPy: a holder from 10 for 10 s and a waiter from 11, each leaving its
    `with` block when interrupted; `target` is interrupted at 13. Returns the
    waiter's start times and the machine's use after 13."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    starts, seen, processes = [], {}, {}

    def holder():
        yield env.timeout(10)
        try:
            with machine.request() as req:
                yield req
                yield env.timeout(10)
        except simpy.Interrupt:
            pass

    def waiter():
        yield env.timeout(11)
        try:
            with machine.request() as req:
                yield req
                starts.append(env.now)
                yield env.timeout(1)
        except simpy.Interrupt:
            pass

    def script():
        yield env.timeout(13)
        processes[target].interrupt("recall")
        yield env.timeout(0.5)
        seen["in use"], seen["queue"] = machine.count, len(machine.queue)

    processes["holder"] = env.process(holder())
    processes["waiter"] = env.process(waiter())
    env.process(script())
    env.run(until=25)
    return starts, seen


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
        seen["in use"], seen["queue"] = machine.in_use, len(machine.queue)

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
    starts, expected = _reference_abandon("waiter")
    records, after = _builder_abandon("waiter")
    assert after == expected
    assert _started_of(records, "waiters-1") == starts
    yaml_records, app = run_yaml("c4_cancel_waiting.yaml")
    assert _started_of(yaml_records, "waiters-1") == starts
    assert app.get_resource("machine").queue == []


def test_c4_cancel_after_a_grant():
    """A granted request's cancel releases nothing; an interrupt with abandon gives
    the unit back at 13, so the waiter starts at 13, not at 20."""
    assert _reference_cancel()["after cancel"] == 1
    assert _low_level_cancel()["after cancel"] == 1
    starts, expected = _reference_abandon("holder")
    assert starts == [13]
    records, after = _builder_abandon("holder")
    assert after == expected
    assert _started_of(records, "waiters-1") == starts
    yaml_records, _app = run_yaml("c4_cancel_granted.yaml")
    assert _started_of(yaml_records, "waiters-1") == starts


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
    return (picker.in_use, bench.in_use, len(bench.queue))


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
    assert (picker.in_use, bench.in_use, len(bench.queue)) == holdings

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


# A renege ahead of a preempting request: SimPy tries the head of the queue after
# every release, the release of a request that reneged included. A waiting request
# of priority 0 that may not preempt stands ahead of one of priority 1 that may;
# when the first reneges at 3, the second becomes the head and preempts the holder.


def _reference_renege_ahead():
    env = simpy.Environment()
    machine = simpy.PreemptiveResource(env, capacity=1)
    trace = []

    def holder():
        try:
            with machine.request(priority=2) as req:
                yield req
                trace.append((env.now, "holder", "started"))
                yield env.timeout(20)
        except simpy.Interrupt:
            trace.append((env.now, "holder", "preempted"))

    def patient():
        yield env.timeout(1)
        with machine.request(priority=0, preempt=False) as req:
            result = yield req | env.timeout(2)
            if req not in result:
                trace.append((env.now, "patient", "reneged"))
                return
            trace.append((env.now, "patient", "started"))
            yield env.timeout(1)

    def eager():
        yield env.timeout(2)
        with machine.request(priority=1) as req:
            yield req
            trace.append((env.now, "eager", "started"))
            yield env.timeout(1)

    for process in (holder, patient, eager):
        env.process(process())
    env.run(until=10)
    return trace


def _renege_trace(records):
    names = {"holders": "holder", "patients": "patient", "eagers": "eager"}
    return [
        (t, names[key.split("-")[0]], status)
        for t, key, _task, status in event_trace(records)
        if status in ("started", "reneged", "preempted")
    ]


def test_c4_renege_ahead_of_a_preempting_request():
    """At 3 the request of priority 0 reneges, and the request of priority 1 behind
    it takes the machine from the holder at once, as in SimPy."""
    expected = _reference_renege_ahead()
    assert expected == [
        (0, "holder", "started"),
        (3, "patient", "reneged"),
        (3, "holder", "preempted"),
        (3, "eager", "started"),
    ]
    trace = []

    def start(env):
        machine = DynamicResource(env, "C4r", "machine", queue="priority", preempt=True)

        def holder():
            try:
                with machine.request(priority=2) as req:
                    yield req
                    trace.append((env.now, "holder", "started"))
                    yield env.timeout(20)
            except simpy.Interrupt:
                trace.append((env.now, "holder", "preempted"))

        def patient():
            yield env.timeout(1)
            with machine.request(priority=0, preempt=False) as req:
                result = yield req | env.timeout(2)
                if req not in result:
                    trace.append((env.now, "patient", "reneged"))
                    return
                trace.append((env.now, "patient", "started"))
                yield env.timeout(1)

        def eager():
            yield env.timeout(2)
            with machine.request(priority=1) as req:
                yield req
                trace.append((env.now, "eager", "started"))
                yield env.timeout(1)

        for process in (holder, patient, eager):
            env.process(process())

    params = SimParameter(
        sim_id="C4r", resources={"machine": CapacityConfig(current_cap=1, max_cap=1)}
    )
    run_low_level(params, start, 10)
    assert trace == expected

    app = SimulationContext("C4r", factor=0.0)
    app.add_resource(
        "machine", current_cap=1, max_cap=1, queue="priority", preempt=True
    )
    app.add_service("hold", dist="constant", value=20)
    app.add_service("work", dist="constant", value=1)
    app.add_arrival_replay("holders", [{"time": 0}])
    app.add_arrival_replay("patients", [{"time": 1}])
    app.add_arrival_replay("eagers", [{"time": 2}])

    @app.task(
        service_id="hold", resource_id="machine", priority=2, on_interrupt="abandon"
    )
    def holder(task_id):
        return {"event": "done"}

    @app.task(
        service_id="work",
        resource_id="machine",
        priority=0,
        preempt=False,
        patience=2,
    )
    def patient(task_id):
        return {"event": "done"}

    @app.task(service_id="work", resource_id="machine", priority=1)
    def eager(task_id):
        return {"event": "done"}

    for arrival, task in (
        ("holders", holder),
        ("patients", patient),
        ("eagers", eager),
    ):

        def loop(context, arrival=arrival, task=task):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(task.__name__), entity=entity))

        app.arrival_loop(arrival)(loop)

    assert _renege_trace(run_builder(app, 10)) == expected
    assert _renege_trace(run_yaml("c4_renege_ahead.yaml")[0]) == expected


# ------------------------------------------------- count, queue and users


def _snapshot(machine):
    """What a monitor reads: the count, the queue length, the users and the queue."""
    return (
        machine.count,
        len(machine.queue),
        [who(request) for request in machine.users],
        [who(request) for request in machine.queue],
    )


def test_c4_count_queue_and_users(tmp_path):
    """After each event the count, the queue and the users of a resource read as
    SimPy's, the monitoring guide's pattern extended with the users: a resource of
    two units served first in, first out, and a priority resource whose requests
    have equal priorities and mixed preempt flags (`not preempt` ranks last)."""
    times = [i + 0.25 for i in range(11)]
    cases = {
        "fifo": (
            {"current_cap": 2, "max_cap": 2},
            lambda env: simpy.Resource(env, capacity=2),
            [
                Job("a", 0, 4),
                Job("b", 0.5, 4),
                Job("c", 1, 2),
                Job("d", 1.5, 2),
                Job("e", 2, 1),
            ],
        ),
        "equal priorities": (
            {"current_cap": 1, "max_cap": 1, "queue": "priority"},
            lambda env: simpy.PriorityResource(env, capacity=1),
            [
                Job("a", 0, 5, priority=0),
                Job("x", 1, 1, priority=1, preempt=False),
                Job("y", 1, 1, priority=1),
                Job("z", 1.5, 1, priority=1),
            ],
        ),
    }
    for name, (spec, make, jobs) in cases.items():
        folder = tmp_path / name.replace(" ", "_")
        folder.mkdir()
        reference = run_simpy(make, jobs, 12, observe=(times, _snapshot))
        assert reference.readings[1][1][0] == 2 or name != "fifo"
        for result in run_desflow(folder, spec, jobs, 12, observe=(times, _snapshot)):
            assert result.readings == reference.readings
            assert result.trace == reference.trace


def _reference_conditions():
    """Plain SimPy: a late customer balks when two requests wait; a watcher resumes
    when the machine has no user."""
    env = simpy.Environment()

    class Watched(simpy.Resource):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.changed = []

        def _do_get(self, event):
            # SimPy's own `_do_get`, with the watchers woken as the user leaves: before
            # the release event, whose callback grants the next request
            if event.request in self.users:
                self.users.remove(event.request)
            for waiting in self.changed:
                waiting.succeed()
            self.changed = []
            event.succeed()

    machine = Watched(env, capacity=1)
    seen = {}

    def job(at, hold):
        yield env.timeout(at)
        with machine.request() as req:
            yield req
            yield env.timeout(hold)

    def late():
        yield env.timeout(3)
        if len(machine.queue) >= 2:
            seen["balked"] = env.now
            return
        with machine.request() as req:
            yield req

    def watcher():
        yield env.timeout(0.5)
        while machine.count >= 1:
            event = env.event()
            machine.changed.append(event)
            yield event
        seen["resumed"] = env.now

    for at, hold in ((0, 4), (1, 2), (2, 2)):
        env.process(job(at, hold))
    env.process(late())
    env.process(watcher())
    env.run(until=12)
    return seen


def test_c4_users_and_queue_in_conditions():
    """`queue.<resource>` in a balk and `users.<resource>` in a wait condition read
    the queue length and the user count, in YAML and the builder."""
    expected = _reference_conditions()
    assert expected == {"balked": 3, "resumed": 4}

    def seen(records):
        return {
            "balked": times(records, "late", "balked")[0],
            "resumed": times(records, "watcher")[0],
        }

    records, app = run_yaml("c4_conditions.yaml")
    assert seen(records) == expected
    assert app.get_resource("machine").queue == []

    app = SimulationContext("C4q", factor=0.0)
    app.add_resource("machine")
    app.add_service("hold", dist="constant", value=4)
    app.add_service("short", dist="constant", value=2)

    @app.task(service_id="hold", resource_id="machine")
    def holder(task_id):
        return {"event": "held"}

    @app.task(service_id="short", resource_id="machine")
    def job(task_id):
        return {"event": "done"}

    @app.task(service_id="short", resource_id="machine", balk="queue.machine >= 2")
    def late(task_id):
        return {"event": "done"}

    @app.task(wait={"condition": "users.machine < 1"})
    def watcher(task_id):
        return {"event": "free"}

    def script(context):
        for at, task in ((0, holder), (0.5, watcher), (1, job), (2, job), (3, late)):
            yield context.env.timeout(at - context.env.now)
            context.spawn(task(context.next_instance(task.__name__)))

    app.add_process(script)
    assert seen(run_builder(app, 12)) == expected


CONDITION_MODEL = """
simulation: {sim_id: T, factor: 0}
resources:
  machine: {queue: priority, preempt: true}
services:
  long: {dist: constant, value: 5}
arrivals:
  los: {replay: {path: los.csv}, spawn: lo}
  his: {replay: {path: his.csv}, spawn: hi}
  watchers: {replay: {path: watchers.csv}, spawn: watcher}
tasks:
  lo: {service: long, resource: machine, priority: 5, payload: {event: done}}
  hi: {service: long, resource: machine, priority: 1, payload: {event: done}}
  watcher:
    wait: {condition: "%s"}
    payload: {event: seen}
run: {until: %s}
"""


def test_c4_a_condition_is_not_checked_between_the_steps_of_one_call(tmp_path):
    """A request that is granted at once never shows in `queue.<resource>`, and a
    preemption never shows an empty `users.<resource>`: a condition is checked
    after the request is made and granted, not between the two."""
    (tmp_path / "queue").mkdir()
    seen_queue = run_yaml_text(
        tmp_path / "queue",
        CONDITION_MODEL % ("queue.machine >= 1", 10),
        {"los": [1], "his": [], "watchers": [0.5]},
    )
    assert seen_queue.error is None
    assert times(seen_queue.records, "lo") == [6.0]  # the request was granted at 1
    assert times(seen_queue.records, "watcher") == []

    (tmp_path / "users").mkdir()
    seen_users = run_yaml_text(
        tmp_path / "users",
        CONDITION_MODEL % ("users.machine < 1", 3),
        {"los": [0], "his": [1], "watchers": [0.5]},
    )
    assert seen_users.error is None
    assert times(seen_users.records, "lo", "preempted") == [1.0]
    assert times(seen_users.records, "watcher") == []


# ------------------------------------------------------- the release event


def _release_scenario(env, machine, seen):
    """What a release event shows: its request, when it is triggered, its value, and
    when the next waiting request is granted. `machine` is a SimPy `Resource` or a
    `DynamicResource`."""
    holder = machine.request()
    yield holder
    waiter = machine.request()
    yield env.timeout(1)
    release = machine.release(holder)
    seen["request"] = release.request is holder
    seen["triggered"] = release.triggered
    seen["processed"] = release.processed
    seen["value"] = release.value
    seen["waiter at release"] = waiter.triggered
    yield release
    seen["released at"] = env.now
    seen["waiter after release event"] = waiter.triggered
    yield waiter
    seen["waiter granted at"] = env.now
    # A release of a request that was never granted gives back nothing
    queued = machine.request()
    never = machine.release(queued)
    seen["never granted"] = (
        never.request is queued,
        never.triggered,
        len(machine.queue),
    )
    # A second release of a request gives back nothing either
    machine.release(holder)
    yield env.timeout(0)
    seen["after the second release"] = (machine.count, len(machine.queue))


def test_c4_release_event():
    """A release is an event that holds its request, is triggered as soon as it is
    made, has the value None, and grants the next waiting request when it is
    processed, one event later: the times equal SimPy's in the low-level API and the
    builder."""
    env = simpy.Environment()
    reference = {}
    env.process(_release_scenario(env, simpy.Resource(env, 1), reference))
    env.run(until=10)
    assert reference["request"] and reference["triggered"]
    assert reference["value"] is None and not reference["waiter at release"]
    assert reference["waiter granted at"] == 1

    seen = {}

    def start(env):
        machine = DynamicResource(env, "C4", "machine")
        env.process(_release_scenario(env, machine, seen))

    run_low_level(SimParameter(sim_id="C4"), start, 10)
    assert seen == reference

    seen = {}
    app = SimulationContext("C4", factor=0.0)
    app.add_resource("machine")
    app.add_process(
        lambda context: _release_scenario(
            context.env, context.get_resource("machine"), seen
        )
    )
    run_builder(app, 10)
    assert seen == reference

    # No request names the oldest user, and holds it
    def start_none(env):
        machine = DynamicResource(env, "C4", "machine")
        request = machine.request()
        assert machine.release().request is request
        assert machine.release().request is None

    run_low_level(SimParameter(sim_id="C4"), start_none, 1)


# -------------------------------------------------------------- usage since


def _usage_scenario(env, machine, seen):
    """`usage_since` is None while a request waits and the time of its grant after:
    a grant at a release, and a grant at a release that ties with a new request."""
    first = machine.request()
    second = machine.request()
    yield first
    seen["first"] = first.usage_since
    seen["second waiting"] = second.usage_since
    yield env.timeout(3)
    machine.release(first)
    yield second
    seen["second"] = second.usage_since
    yield env.timeout(2)
    # A release and a new request at one instant: the request is queued behind the
    # release, which grants the head at the same time
    third = machine.request()
    machine.release(second)
    yield third
    seen["third"] = third.usage_since
    seen["users"] = [user.usage_since for user in machine.users]


def test_c4_usage_since():
    """A granted request holds the time of its grant in `usage_since`, as SimPy's,
    and a request still waiting holds None. After a capacity raise, every request it
    grants holds the time of the raise."""
    env = simpy.Environment()
    reference = {}
    env.process(_usage_scenario(env, simpy.Resource(env, 1), reference))
    env.run(until=20)
    assert reference == {
        "first": 0,
        "second waiting": None,
        "second": 3,
        "third": 5,
        "users": [5],
    }

    seen = {}

    def start(env):
        env.process(_usage_scenario(env, DynamicResource(env, "C4", "machine"), seen))

    run_low_level(SimParameter(sim_id="C4"), start, 20)
    assert seen == reference

    # A raise grants every waiting request at once, with the time of the raise
    raised = {}

    def start_raise(env):
        machine = DynamicResource(env, "C4", "machine")

        def user():
            requests = [machine.request() for _ in range(3)]
            yield env.timeout(4)
            env.registry.update("C4.resources.machine.max_cap", 3)
            env.registry.update("C4.resources.machine.current_cap", 3)
            yield env.timeout(1)
            raised["usage"] = [request.usage_since for request in requests]

        env.process(user())

    run_low_level(SimParameter(sim_id="C4"), start_raise, 10)
    assert raised["usage"] == [0, 4, 4]


# --------------------------------------------------------- default priority


def test_c4_default_priority(tmp_path):
    """A request that names no priority has priority 0, as SimPy's `PriorityRequest`.
    On a priority resource requests without a priority tie with priority 0 first in,
    first out and go ahead of priority 1; on a preemptive resource one takes the unit
    of a priority 1 user. A low-level `request()` and a task with no `priority` both
    rank 0."""
    ordered = [
        Job("holder", 0, 5, priority=0),
        Job("one", 1, 1, priority=1),
        Job("none_a", 1.5, 1),
        Job("zero", 2, 1, priority=0),
        Job("none_b", 2.5, 1),
    ]
    preemptive = [Job("user", 0, 5, priority=1), Job("none", 2, 1)]
    cases = {
        "queue": (
            {"current_cap": 1, "max_cap": 1, "queue": "priority"},
            lambda env: simpy.PriorityResource(env, capacity=1),
            ordered,
            ["holder", "none_a", "zero", "none_b", "one"],
        ),
        "preempt": (
            {"current_cap": 1, "max_cap": 1, "queue": "priority", "preempt": True},
            lambda env: simpy.PreemptiveResource(env, capacity=1),
            preemptive,
            ["user", "none", "user"],
        ),
    }
    for name, (spec, make, jobs, order) in cases.items():
        folder = tmp_path / name
        folder.mkdir()
        reference = run_simpy(make, jobs, 20)
        started = [n for _t, n, s in reference.trace if s in ("started", "resumed")]
        assert started == order
        for result in run_desflow(folder, spec, jobs, 20):
            assert result.trace == reference.trace


# -------------------------------------------------------- custom resources


def _custom_cases():
    """Each custom type, its SimPy twin, the jobs and what the resource keeps."""
    return {
        "monitored": (
            {"current_cap": 1, "max_cap": 1, "type": custom.MonitoredResource},
            lambda env: custom.SimPyMonitoredResource(env, capacity=1),
            [Job("a", 0, 3), Job("b", 0.5, 3), Job("c", 1, 2), Job("d", 1.5, 1)],
            "data",
        ),
        "newest user loses": (
            {
                "current_cap": 2,
                "max_cap": 2,
                "queue": "priority",
                "type": custom.NewestUserLoses,
            },
            lambda env: custom.SimPyNewestUserLoses(env, capacity=2),
            [Job("a", 0, 10, priority=3), Job("b", 1, 10, priority=2)]
            + [Job("c", 2, 3, priority=1)],
            None,
        ),
        "two waiting": (
            {
                "current_cap": 1,
                "max_cap": 1,
                "queue": "priority",
                "type": custom.TwoWaiting,
            },
            lambda env: custom.SimPyTwoWaiting(env, capacity=1),
            [
                Job("a", 0, 5, priority=1),
                Job("b", 1, 3, priority=2),
                Job("c", 2, 2, priority=0),
                Job("d", 3, 1, priority=1),
            ],
            None,
        ),
        "stamped requests": (
            {"current_cap": 1, "max_cap": 1, "type": custom.Stamping},
            lambda env: custom.SimPyStamping(env, capacity=1),
            [Job("a", 0, 3), Job("b", 1, 3), Job("c", 2, 1)],
            "stamps",
        ),
    }


def test_c4_custom_resource_type(tmp_path):
    """A subclass of `ResourceBase` behaves as the same SimPy subclass: the guide's
    `MonitoredResource`, a preemptive resource that takes the unit of the newest
    user, a priority resource whose put queue is `SortedQueue` with a `maxlen`, and
    a request class that stamps the time. The low-level API, the builder and YAML
    give SimPy's trace and what the resource kept."""
    for name, (spec, make, jobs, kept) in _custom_cases().items():
        folder = tmp_path / name.replace(" ", "_")
        folder.mkdir()
        reference = run_simpy(make, jobs, 20)
        assert reference.trace
        for result in run_desflow(folder, spec, jobs, 20):
            assert type(result.resource) is spec["type"], name
            assert result.trace == reference.trace, name
            if kept is not None:
                assert getattr(result.resource, kept) == getattr(
                    reference.resource, kept
                ), name
    # The scenarios differ from the stock classes where they are meant to
    spec, make, jobs, _kept = _custom_cases()["newest user loses"]
    stock = run_simpy(lambda env: simpy.PreemptiveResource(env, capacity=2), jobs, 20)
    assert stock.trace != run_simpy(make, jobs, 20).trace
    spec, make, jobs, _kept = _custom_cases()["two waiting"]
    assert "balked" in [s for _t, _n, s in run_simpy(make, jobs, 20).trace]
