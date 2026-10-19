"""C6 end conditions: stop at an event, stop on a condition, drain, step and peek.

A plain SimPy reference and the same model in the low-level API, the builder and
YAML. A `stop` ends the run at the event or the first moment the condition holds,
and `until` wins a tie, because SimPy schedules it ahead of other events at that
time. A drain stops the arrivals at `until` and runs on until every admitted entity
has left, or a limit passes. Stepping processes one event at a time, and `peek`
gives the time of the next, with the statistics process present.
"""

import simpy
from simpy.core import StopSimulation

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Signal,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import RecordingEgress, run_yaml


def _times(records, task):
    return [
        r["sim_ts"]
        for r in records
        if r["stream_type"] == "event"
        and r["task"] == task
        and "status" not in r["value"]
    ]


def _periodic(env, gap, start):
    while True:
        yield env.timeout(gap)
        env.process(start())


# ------------------------------------------------------- stop at an event


def _reference_signal(close_at, until):
    env = simpy.Environment()
    closing = env.event()
    pings = []

    def ping():
        pings.append(env.now)
        yield env.timeout(0)

    def close():
        yield env.timeout(close_at)
        closing.succeed()

    env.process(_periodic(env, 10, ping))
    env.process(close())
    closing.callbacks.append(StopSimulation.callback)
    env.run(until=until)
    return pings, env.now


def _builder_signal(close_at, until):
    app = SimulationContext("C6", factor=0.0)
    app.add_signal("closing")
    app.add_arrival("pings", dist="constant", value=10)
    app.add_arrival("closings", dist="constant", value=close_at)

    @app.task()
    def ping(task_id):
        return {"event": "ping"}

    @app.task(signal="closing")
    def close(task_id):
        return {"event": "closed"}

    for arrival, task, name in (("pings", ping, "ping"), ("closings", close, "close")):

        def loop(context, arrival=arrival, task=task, name=name):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)
    rec = RecordingEgress()
    app.add_egress(rec)
    app.run(until=until, stop={"signal": "closing"})
    return rec.records, app.env.now


def _low_level_signal(close_at, until):
    built = {}

    def start(env):
        closing = Signal(env, "closing")
        n = iter(range(100))

        def ping():
            i = next(n)
            env.publish_event(f"p-{i}", {"event": "ping"}, task="ping", instance=i)
            yield env.timeout(0)

        def close():
            yield env.timeout(close_at)
            closing.fire()

        env.process(_periodic(env, 10, ping))
        env.process(close())
        built["stop"] = closing.wait()

    env = DynamicRealtimeEnvironment(factor=0.0)
    rec = RecordingEgress()
    env.setup_egress([rec], lag_monitor_interval=None)
    start(env)
    built["stop"].callbacks.append(StopSimulation.callback)
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return rec.records, env.now


def test_c6_stop_at_an_event():
    """The shop closes at 25: the run ends there, after the pings of 10 and 20."""
    pings, end = _reference_signal(25, 100)
    assert (pings, end) == ([10, 20], 25)
    for records, now in (
        _low_level_signal(25, 100),
        _builder_signal(25, 100),
        (lambda r: (r[0], r[1].env.now))(run_yaml("c6_stop_signal.yaml")),
    ):
        assert (_times(records, "ping"), now) == ([10.0, 20.0], 25)


def test_c6_stop_at_an_event_at_the_same_instant_as_until():
    """The signal and until are both due at 30: until ends the run first, so the
    closing event never happens."""
    pings, end = _reference_signal(30, 30)
    assert (pings, end) == ([10, 20], 30)
    for records, now in (
        _low_level_signal(30, 30),
        _builder_signal(30, 30),
        (lambda r: (r[0], r[1].env.now))(run_yaml("c6_stop_signal_tie.yaml")),
    ):
        assert _times(records, "close") == []
        assert (_times(records, "ping"), now) == ([10.0, 20.0], 30)


# ----------------------------------------------------- explicit condition


def _reference_condition():
    env = simpy.Environment()
    stock = simpy.Container(env, capacity=20)
    full = env.event()
    pings = []

    def restock():
        yield stock.put(2)
        if stock.level >= 5 and not full.triggered:
            full.succeed()

    def ping():
        pings.append(env.now)
        yield env.timeout(0)

    env.process(_periodic(env, 3, restock))
    env.process(_periodic(env, 4, ping))
    env.run(until=full)
    return pings, env.now, stock.level


def test_c6_explicit_stop_condition():
    """The stock first holds 5 at 9 (level 6): the run ends there, after the pings
    of 4 and 8, and the ping of 12 never arrives."""
    assert _reference_condition() == ([4, 8], 9, 6)

    def start(env):
        stock = DynamicContainer(env, "C6", "stock")
        n = iter(range(100))
        full = env.event()
        start.parts = (stock, full)

        def check():
            if stock.level >= 5 and not full.triggered:
                full.succeed()
                env.unwatch_changes(check)

        env.watch_changes(check)

        def restock():
            yield stock.put(2)

        def ping():
            i = next(n)
            env.publish_event(f"p-{i}", {"event": "ping"}, task="ping", instance=i)
            yield env.timeout(0)

        env.process(_periodic(env, 3, restock))
        env.process(_periodic(env, 4, ping))

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="C6",
            containers={"stock": ContainerConfig(current_cap=20, max_cap=20)},
        )
    )
    rec = RecordingEgress()
    env.setup_egress([rec], lag_monitor_interval=None)
    start(env)
    stock, full = start.parts
    try:
        env.run(until=full)
    finally:
        env.teardown()
    assert (_times(rec.records, "ping"), env.now, stock.level) == ([4.0, 8.0], 9, 6)

    app = SimulationContext("C6", factor=0.0)
    app.add_container("stock", current_cap=20, max_cap=20)
    app.add_arrival("restocks", dist="constant", value=3)
    app.add_arrival("pings", dist="constant", value=4)

    @app.task(put={"container": "stock", "amount": 2})
    def restock(task_id):
        return {"event": "restocked"}

    @app.task()
    def ping(task_id):
        return {"event": "ping"}

    for arrival, task, name in (
        ("restocks", restock, "restock"),
        ("pings", ping, "ping"),
    ):

        def loop(context, arrival=arrival, task=task, name=name):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)
    rec = RecordingEgress()
    app.add_egress(rec)
    app.run(until=100, stop={"condition": "level.stock >= 5"})
    builder = (
        _times(rec.records, "ping"),
        app.env.now,
        app.get_container("stock").level,
    )
    records, yaml_app = run_yaml("c6_stop_condition.yaml")
    yaml = (
        _times(records, "ping"),
        yaml_app.env.now,
        yaml_app.get_container("stock").level,
    )
    assert builder == yaml == ([4.0, 8.0], 9, 6)


# ------------------------------------------------------------------ drain


def _reference_drain(limit=None):
    """When each order finishes, and where the run ends, after arrivals stop at 10."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    done, orders = [], []

    def order():
        with machine.request() as req:
            yield req
            yield env.timeout(5)
            done.append(env.now)

    def source():
        try:
            while True:
                yield env.timeout(2)
                orders.append(env.process(order()))
        except simpy.Interrupt:
            pass

    arrivals = env.process(source())
    env.run(until=10)
    arrivals.interrupt()
    end = env.all_of(orders)
    if limit is not None:
        end = end | env.timeout(limit)
    env.run(until=end)
    return done, env.now, sum(1 for o in orders if o.is_alive)


def _builder_drain(drain):
    app = SimulationContext("C6", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("work", dist="constant", value=5)
    app.add_arrival("orders", dist="constant", value=2)
    app.with_stats("basic")

    @app.task(service_id="work", resource_id="machine")
    def work(task_id):
        return {"event": "done"}

    @app.arrival_loop("orders")
    def orders(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(work(context.next_instance("work"), entity=entity))

    rec = RecordingEgress()
    app.add_egress(rec)
    app.run(until=10, drain=drain)
    return rec.records, app


def _in_system(app):
    return app.env.stats.readings()["C6.stats.flows.orders.in_system"]


def _low_level_drain(limit=None):
    def start(env):
        machine = DynamicResource(env, "C6", "machine")
        start.orders = []

        def order(n):
            key = f"orders-{n}"
            with machine.request() as req:
                yield req
                yield env.timeout(5)
                env.publish_event(key, {"event": "done"}, task="work", instance=n)

        def source():
            n = 0
            try:
                while True:
                    yield env.timeout(2)
                    n += 1
                    start.orders.append(env.process(order(n)))
            except simpy.Interrupt:
                pass

        start.arrivals = env.process(source())

    params = SimParameter(
        sim_id="C6", resources={"machine": CapacityConfig(current_cap=1, max_cap=1)}
    )
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(params)
    rec = RecordingEgress()
    env.setup_egress([rec], lag_monitor_interval=None)
    start(env)
    try:
        env.run(until=10)
        start.arrivals.interrupt()
        end = env.all_of(start.orders)
        if limit is not None:
            end = end | env.timeout(limit)
        env.run(until=end)
    finally:
        env.teardown()
    return rec.records, env.now, sum(1 for o in start.orders if o.is_alive)


def test_c6_stop_arrivals_and_drain():
    """Arrivals stop at 10, after the orders of 2, 4, 6 and 8; all four are served,
    the last at 22, and the run ends there with none left."""
    done, end, left = _reference_drain()
    assert (done, end, left) == ([7, 12, 17, 22], 22, 0)
    records, now, alive = _low_level_drain()
    assert (_times(records, "work"), now, alive) == ([7.0, 12.0, 17.0, 22.0], 22, 0)
    for records, app in (_builder_drain(True), run_yaml("c6_drain.yaml")):
        assert (_times(records, "work"), app.env.now) == ([7.0, 12.0, 17.0, 22.0], 22)
        assert _in_system(app) == 0


def test_c6_stop_arrivals_and_drain_with_a_limit():
    """A drain of at most 5 s ends at 15: two orders are served, one is being
    served and one still waits, and the counts say so."""
    done, end, left = _reference_drain(limit=5)
    assert (done, end, left) == ([7, 12], 15, 2)
    records, now, alive = _low_level_drain(limit=5)
    assert (_times(records, "work"), now, alive) == ([7.0, 12.0], 15, 2)
    for records, app in (_builder_drain({"limit": 5}), run_yaml("c6_drain_limit.yaml")):
        assert (_times(records, "work"), app.env.now) == ([7.0, 12.0], 15)
        readings = app.env.stats.readings()
        assert readings["C6.stats.flows.orders.started"] == 4
        assert readings["C6.stats.flows.orders.finished"] == 2
        assert _in_system(app) == 2
        assert readings["C6.stats.tasks.work.in_progress"] == 2


# --------------------------------------------------------- step and peek


def _stepped(peek, step, now, until):
    """(peeked time, time after the step) for every step before `until`."""
    trace = []
    while peek() < until:
        before = peek()
        step()
        trace.append((before, now()))
    return trace


def _reference_steps():
    env = simpy.Environment()

    def ping():
        yield env.timeout(0)

    def stats():
        while True:
            yield env.timeout(10)

    env.process(_periodic(env, 10, ping))
    env.process(stats())
    return _stepped(env.peek, env.step, lambda: env.now, 40)


def _times_visited(trace):
    return sorted({after for _before, after in trace})


def test_c6_step_one_event():
    """Each step processes one event and moves the clock to the time peek gave."""
    reference = _reference_steps()
    assert all(before == after for before, after in reference)
    expected_times = _times_visited(reference)
    assert expected_times == [0, 10, 20, 30]

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.setup_stats("C6", "basic", interval=10)

    def ping():
        yield env.timeout(0)

    env.process(_periodic(env, 10, ping))
    low = _stepped(env.peek, env.step, lambda: env.now, 40)
    env.teardown()

    app = SimulationContext("C6", factor=0.0)
    app.add_arrival("pings", dist="constant", value=10)
    app.with_stats("basic", interval=10)

    @app.task()
    def ping_task(task_id):
        return {"event": "ping"}

    @app.arrival_loop("pings")
    def pings(context):
        while True:
            entity = yield context.wait_for_arrival("pings")
            context.spawn(ping_task(context.next_instance("ping_task"), entity=entity))

    app.start()
    builder = _stepped(app.peek, app.step, lambda: app.env.now, 40)
    app.close()

    from tests.conformance.harness import BLUEPRINTS

    yaml_app = SimulationContext.from_yaml(BLUEPRINTS / "c6_step.yaml")
    yaml_app.start()
    yaml = _stepped(yaml_app.peek, yaml_app.step, lambda: yaml_app.env.now, 40)
    yaml_app.close()
    for trace in (low, builder, yaml):
        assert all(before == after for before, after in trace)
        assert _times_visited(trace) == expected_times


def test_c6_inspect_next_event_time():
    """peek gives the time of the next event SimPy would process, stepping the same
    model in the same way, in all three APIs; with no event left it gives
    infinity."""
    env = simpy.Environment()
    env.process(_periodic(env, 10, lambda: iter(())))
    while env.peek() < 5:
        env.step()
    expected = env.peek()
    assert simpy.Environment().peek() == float("inf")

    low = DynamicRealtimeEnvironment(factor=0.0)
    low.process(_periodic(low, 10, lambda: iter(())))
    while low.peek() < 5:
        low.step()
    assert low.peek() == expected
    low.teardown()
    empty = DynamicRealtimeEnvironment(factor=0.0)
    assert empty.peek() == float("inf")
    empty.teardown()

    from tests.conformance.harness import BLUEPRINTS

    yaml_app = SimulationContext.from_yaml(BLUEPRINTS / "c6_step.yaml")
    yaml_app.start()
    while yaml_app.peek() < 5:
        yaml_app.step()
    assert yaml_app.peek() == expected
    yaml_app.close()

    app = SimulationContext("C6", factor=0.0)
    app.add_arrival("pings", dist="constant", value=10)

    @app.task()
    def ping(task_id):
        return {}

    @app.arrival_loop("pings")
    def pings(context):
        while True:
            entity = yield context.wait_for_arrival("pings")
            context.spawn(ping(context.next_instance("ping"), entity=entity))

    app.start()
    while app.peek() < 5:
        app.step()
    assert app.peek() == expected == 10
    app.close()


def _stopped_and_resumed(env):
    """A run stopped at an event that a process waits for, then run on.

    The stop is asked for before the process waits, so the process's callback comes
    after SimPy's stop on the event: what happens to it when the run resumes is
    the version's rule (4.1.2 keeps it and runs it on resume).
    """
    trace = []
    gate = env.event()

    def opener():
        yield env.timeout(5)
        gate.succeed("open")

    def waiter():
        yield env.timeout(3)
        value = yield gate
        trace.append((env.now, "through", value))

    env.process(opener())
    env.process(waiter())
    # `run(until=gate)` adds its stop callback to the gate before the waiter does
    env.run(until=2)
    stop_value = env.run(until=gate)
    trace.append((env.now, "stopped", stop_value))
    env.run(until=10)
    return trace


def test_c6_stopped_and_resumed_run():
    """A low-level run stopped at an event and run on gives SimPy's trace: on
    SimPy 4.1.2 the waiter, whose callback came after the stop, goes through when
    the run resumes."""
    expected = _stopped_and_resumed(simpy.Environment())
    assert expected[0] == (5, "stopped", "open")
    env = DynamicRealtimeEnvironment(factor=0.0)
    assert _stopped_and_resumed(env) == expected
    env.teardown()
