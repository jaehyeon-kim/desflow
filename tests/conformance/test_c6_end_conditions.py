"""C6 end conditions: stop at an event, stop on a condition, drain, step and peek.

A plain SimPy reference and the same model in the low-level API, the builder and
YAML. A `stop` ends the run at the event or the first moment the condition holds,
and `until` wins a tie, because SimPy schedules it ahead of other events at that
time. A drain stops the arrivals at `until` and runs on until every admitted entity
has left, or a limit passes. Stepping processes one event at a time, and `peek`
gives the time of the next, counting the model's events only, with the statistics
process and the other processes of desflow's own present.
"""

from datetime import datetime, timezone

import pytest
import simpy
from simpy.core import EmptySchedule, StopSimulation

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    LocalIngress,
    Signal,
    SignalFailed,
    SimParameter,
)
from desflow.core.context import SimulationContext
from desflow.core.internal import is_internal
from tests.conformance.batch5_scripts import pinger
from tests.conformance.harness import (
    BLUEPRINTS,
    RecordingEgress,
    periodic,
    run_yaml,
    times,
)


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

    env.process(periodic(env, 10, ping))
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

        env.process(periodic(env, 10, ping))
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
        assert (times(records, "ping"), now) == ([10.0, 20.0], 25)


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
        assert times(records, "close") == []
        assert (times(records, "ping"), now) == ([10.0, 20.0], 30)


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

    env.process(periodic(env, 3, restock))
    env.process(periodic(env, 4, ping))
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

        env.process(periodic(env, 3, restock))
        env.process(periodic(env, 4, ping))

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
    assert (times(rec.records, "ping"), env.now, stock.level) == ([4.0, 8.0], 9, 6)

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
        times(rec.records, "ping"),
        app.env.now,
        app.get_container("stock").level,
    )
    records, yaml_app = run_yaml("c6_stop_condition.yaml")
    yaml = (
        times(records, "ping"),
        yaml_app.env.now,
        yaml_app.get_container("stock").level,
    )
    assert builder == yaml == ([4.0, 8.0], 9, 6)


# -------------------------------------------------------- the run's value


def _builder_stop_model(stop, until=100):
    """Pings every 2 s, a closing at 25, and a stock of 5 at 6, run with `stop`."""
    app = SimulationContext("C6", factor=0.0)
    app.add_signal("closing")
    app.add_container("stock", current_cap=20, max_cap=20)
    app.add_arrival("pings", dist="constant", value=2)
    app.add_arrival("closings", dist="constant", value=25)

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

    def fill(context):
        yield context.env.timeout(6)
        yield context.get_container("stock").put(5)

    app.add_process(fill)
    rec = RecordingEgress()
    app.add_egress(rec)
    return app, rec, app.run(until=until, stop=stop)


def test_c6_run_value():
    """run returns the value of the event that ended it, as SimPy's env.run does: a
    signal's payload, True for a condition, a function event's value, and None for
    until or a drain. A stop that fails raises the exception of the failed event
    itself, once, after the run closed and the sink received its records."""
    env = simpy.Environment()
    assert env.run(until=env.timeout(3, value="done")) == "done"
    env = simpy.Environment()
    assert env.run(until=4) is None
    error = ValueError("scratched")
    env = simpy.Environment()
    failing = env.event()
    failing.fail(error)
    with pytest.raises(ValueError) as caught:
        env.run(until=failing)
    assert caught.value is error

    # low-level: env.run is SimPy's, with the end events of desflow's own queued
    low = DynamicRealtimeEnvironment(factor=0.0)
    low.setup_egress([RecordingEgress()], lag_monitor_interval=1)
    low.setup_stats("C6", "basic", interval=10)
    closing = Signal(low, "closing")
    low.process(_fire_at(low, closing, 7, {"event": "closed"}))
    assert low.run(until=closing.wait()) == {"event": "closed"}
    assert low.run(until=low.timeout(3, value="done")) == "done"
    assert low.run(until=50) is None
    low.teardown()

    # a signal, a condition and a function event, in the builder and YAML
    closed = {"event": "closed"}
    for stop in ({"signal": "closing"}, "closing"):
        assert _builder_stop_model(stop)[2] == closed
    assert _builder_stop_model(lambda c: c.signal("closing").wait())[2] == closed
    assert _builder_stop_model({"condition": "level.stock >= 5"})[2] is True
    assert _builder_stop_model(lambda c: c.env.timeout(30, value="done"))[2] == "done"
    app, rec, value = _builder_stop_model(None, until=20)
    assert value is None
    assert times(rec.records, "ping") == [float(n) for n in range(2, 20, 2)]
    assert run_yaml("c6_stop_signal.yaml", raises=True).value == closed
    assert run_yaml("c6_stop_condition.yaml", raises=True).value is True
    assert run_yaml("c6_stop_signal_tie.yaml", raises=True).value is None
    assert run_yaml("c6_drain.yaml", raises=True).value is None

    # a failed stop: the same exception object, raised once, after close()
    stops = []

    def capture(context):
        stops.append(context.signal("closing").wait())
        return stops[0]

    app = SimulationContext("C6", factor=0.0)
    app.add_signal("closing")
    app.add_arrival("pings", dist="constant", value=2)

    @app.task()
    def ping(task_id):
        return {"event": "ping"}

    @app.arrival_loop("pings")
    def pings(context):
        while True:
            entity = yield context.wait_for_arrival("pings")
            context.spawn(ping(context.next_instance("ping"), entity=entity))

    def fail(context):
        yield context.env.timeout(5)
        context.signal("closing").fail("scratched")

    app.add_process(fail)
    rec = RecordingEgress()
    app.add_egress(rec)
    with pytest.raises(SignalFailed) as caught:
        app.run(until=100, stop=capture)
    assert caught.value is stops[0]._value
    assert caught.value.reason == "scratched"
    assert (app.env.now, app.started) == (5, False)
    assert times(rec.records, "ping") == [2.0, 4.0]

    outcome = run_yaml("c6_stop_failed.yaml", raises=True)
    assert isinstance(outcome.error, SignalFailed)
    assert outcome.error.reason == "scratched"
    assert outcome.value is None
    assert times(outcome.records, "ping") == [2.0, 4.0]
    assert outcome.app_or_env.env.now == 5


def _fire_at(env, signal, moment, value):
    yield env.timeout(moment)
    signal.fire(value)


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
    assert (times(records, "work"), now, alive) == ([7.0, 12.0, 17.0, 22.0], 22, 0)
    for records, app in (_builder_drain(True), run_yaml("c6_drain.yaml")):
        assert (times(records, "work"), app.env.now) == ([7.0, 12.0, 17.0, 22.0], 22)
        assert _in_system(app) == 0


def test_c6_stop_arrivals_and_drain_with_a_limit():
    """A drain of at most 5 s ends at 15: two orders are served, one is being
    served and one still waits, and the counts say so."""
    done, end, left = _reference_drain(limit=5)
    assert (done, end, left) == ([7, 12], 15, 2)
    records, now, alive = _low_level_drain(limit=5)
    assert (times(records, "work"), now, alive) == ([7.0, 12.0], 15, 2)
    for records, app in (_builder_drain({"limit": 5}), run_yaml("c6_drain_limit.yaml")):
        assert (times(records, "work"), app.env.now) == ([7.0, 12.0], 15)
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

    env.process(periodic(env, 10, ping))
    return _stepped(env.peek, env.step, lambda: env.now, 40)


def test_c6_step_one_event():
    """Each step processes one model event and moves the clock to the time peek gave.
    The low-level trace is SimPy's, step for step, although statistics are present.
    A builder or YAML model has three model events at each ping, the arrival and the
    task's two, so its sequence is that, and it is the same in both."""
    reference = _reference_steps()
    assert all(before == after for before, after in reference)
    assert [at for at, _ in reference] == [0] + [10] * 4 + [20] * 4 + [30] * 4

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.setup_stats("C6", "basic", interval=10)

    def ping():
        yield env.timeout(0)

    env.process(periodic(env, 10, ping))
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
    assert low == reference
    expected_models = [(0, 0)] + [(10, 10)] * 3 + [(20, 20)] * 3 + [(30, 30)] * 3
    assert builder == yaml == expected_models


def test_c6_inspect_next_event_time():
    """peek gives the time of the next event SimPy would process, stepping the same
    model in the same way, in all three APIs; with no event left it gives
    infinity."""
    env = simpy.Environment()
    env.process(periodic(env, 10, lambda: iter(())))
    while env.peek() < 5:
        env.step()
    expected = env.peek()
    assert simpy.Environment().peek() == float("inf")

    low = DynamicRealtimeEnvironment(factor=0.0)
    low.process(periodic(low, 10, lambda: iter(())))
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


def _assert_a_model_event(event):
    """Fails when `event`, which a step counts as the model's, is one of desflow's own.

    Two runs that both mark an event the same wrong way count the same steps, so the
    count alone cannot show a missing mark. These are the events desflow makes without
    a process of the model: the end of the run, its horizon, the limit timer of a
    drain and a task's end event (all plain events) and the warm-up end (a process,
    its first event and its timeout). The only plain events a model here makes are a
    fired signal or condition, which a `stop` or a `wait` waits for, so the caller
    counts plain events and compares the count with the ones it expects.

    Returns:
        bool: True when `event` is a plain event.
    """
    process = event if isinstance(event, simpy.Process) else None
    for callback in event.callbacks or []:
        process = process or getattr(callback, "__self__", None)
    generator = getattr(process, "_generator", None)
    assert getattr(generator, "__name__", "") != "_end_warmup", "a warm-up end"
    return type(event) is simpy.Event


def _model_steps(env, peek, step, until, plain_events=0):
    """(peeked time, time after, model events processed) for each public step.

    The count comes from the events the step took off the queue: each step must
    process exactly one model event, however many of desflow's own run before it.
    None of those may be an end, horizon, limit timer, task end or warm-up end of
    desflow's own, and the plain events among them are the `plain_events` the model
    makes itself, such as the signal a `stop` waits for.
    """
    processed = []
    plain = []
    original = env._step_event

    def counting():
        event = original()
        processed.append(not is_internal(event))
        if processed[-1] and _assert_a_model_event(event):
            plain.append(event)
        return event

    env._step_event = counting
    trace = []
    while peek() < until:
        before, taken = peek(), len(processed)
        step()
        trace.append((before, env.now, sum(processed[taken:])))
    assert len(plain) == plain_events
    return trace


def _everything(app, sink=True):
    """Adds what a live run has: a sink and an ingress with nothing to send."""
    if sink:
        app.add_egress(RecordingEgress())
    app.add_ingress(LocalIngress([]))
    return app


def _model_only_steps(app_factory, plain_events=0, **start):
    app = app_factory()
    app.start(egress=False, ingress=False, **start)
    trace = _model_steps(app.env, app.peek, app.step, 40, plain_events)
    app.close()
    return trace


def _full_steps(app_factory, plain_events=0, **start):
    app = _everything(app_factory())
    app.start(**start)
    trace = _model_steps(app.env, app.peek, app.step, 40, plain_events)
    app.close()
    return trace


def _yaml_steps(name):
    return lambda: SimulationContext.from_yaml(BLUEPRINTS / name)


def _builder_process_steps():
    app = SimulationContext("C6", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.with_stats("basic", interval=10)
    app.add_process(pinger)
    return app


def _reference_process_steps():
    env = simpy.Environment()
    simpy.Resource(env, capacity=1)

    def pinger_process():
        while True:
            yield env.timeout(10)

    env.process(pinger_process())
    return _stepped(env.peek, env.step, lambda: env.now, 40)


def test_c6_step_counts_model_events():
    """step and peek count the model's events only. With a sink, statistics, an
    ingress and a resource present, a low-level model gives SimPy's (peek, now)
    sequence, and a builder or YAML model gives the sequence it has without them.
    Each step processes exactly one model event."""
    reference = _reference_process_steps()
    assert reference == [(0, 0)] + [(10, 10), (20, 20), (30, 30)]

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="C6", resources={"machine": CapacityConfig(current_cap=1, max_cap=1)}
        )
    )
    rec = RecordingEgress()
    env.setup_egress([rec], lag_monitor_interval=1)
    env.setup_ingress([LocalIngress([])])
    env.setup_stats("C6", "basic", interval=10)
    DynamicResource(env, "C6", "machine")

    def pinger_process():
        while True:
            yield env.timeout(10)

    env.process(pinger_process())
    low = _model_steps(env, env.peek, env.step, 40)
    assert [(a, b) for a, b, _ in low] == reference
    assert {taken for _, _, taken in low} == {1}
    env.teardown()

    # Once the model's last event is processed only desflow's own events are left
    # (the next statistics reading, the lag monitor): there is no step to take and no
    # time to peek, and run() still processes them
    last = DynamicRealtimeEnvironment(factor=0.0)
    last.setup_egress([RecordingEgress()], lag_monitor_interval=1)
    last.setup_stats("C6", "basic", interval=10)

    def once():
        yield last.timeout(15)

    last.process(once())
    assert _stepped(last.peek, last.step, lambda: last.now, 40) == [
        (0, 0),
        (15, 15),
        (15, 15),
    ]
    assert last.peek() == float("inf")
    with pytest.raises(EmptySchedule):
        last.step()
    assert len(last._queue) > 0
    last.run(until=45)
    assert last.now == 45
    last.teardown()

    # builder add_process and YAML processes: with everything, then without it
    model_only = _model_only_steps(_builder_process_steps)
    assert [(a, b) for a, b, _ in model_only] == reference
    for factory in (_builder_process_steps, _yaml_steps("c6_step_process.yaml")):
        trace = _full_steps(factory)
        assert trace == model_only
        assert {taken for _, _, taken in trace} == {1}


def test_c6_step_counts_model_events_in_arrival_models():
    """The same holds for a model of arrivals and tasks: the sequence with a sink,
    statistics and an ingress is the one without them, in the builder and in YAML,
    through a warm-up, a stop and a drain."""

    def builder(**kwargs):
        def make():
            app = SimulationContext("C6", factor=0.0, **kwargs)
            app.add_arrival("pings", dist="constant", value=10)
            app.with_stats("basic", interval=10)

            @app.task()
            def ping(task_id):
                return {"event": "ping"}

            @app.arrival_loop("pings")
            def pings(context):
                while True:
                    entity = yield context.wait_for_arrival("pings")
                    context.spawn(ping(context.next_instance("ping"), entity=entity))

            return app

        return make

    cases = [
        (builder(), {}, 0),
        (builder(warmup=15), {}, 0),
        (builder(), {"until": 25, "drain": True}, 0),
        (_yaml_steps("c6_step.yaml"), {}, 0),
        (_yaml_steps("c6_stop_signal.yaml"), {}, 1),
        (_yaml_steps("c6_stop_condition.yaml"), {}, 1),
        (_yaml_steps("c6_drain.yaml"), {}, 0),
        (_yaml_steps("c6_drain_limit.yaml"), {}, 0),
    ]
    for factory, start, plain_events in cases:
        model_only = _model_only_steps(factory, plain_events, **start)
        trace = _full_steps(factory, plain_events, **start)
        assert trace == model_only
        assert {taken for _, _, taken in trace} == {1}


def test_c6_step_with_a_live_capacity_change():
    """A capacity change made by a model event wakes the resource's watcher. The
    watcher's events are desflow's own: peek still gives the next model event, and
    the next step runs them first and then that event, so the resource has its new
    capacity when the step returns."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="C6", resources={"machine": CapacityConfig(current_cap=1, max_cap=3)}
        )
    )
    machine = DynamicResource(env, "C6", "machine")

    def change():
        yield env.timeout(5)
        env.registry.update("C6.resources.machine.current_cap", 2)
        yield env.timeout(5)

    env.process(change())
    env.step()
    env.step()
    assert env.now == 5
    assert machine.capacity == 1
    # The watcher's events are due now, ahead of the model event at 10
    assert simpy.Environment.peek(env) == 5
    assert env.peek() == 10
    env.step()
    assert (env.now, machine.capacity) == (10, 2)
    env.teardown()


def test_c6_step_with_a_task_that_has_on_actions():
    """A task with `on:` has an `ended` event. step and peek skip it, each step
    processes one model event, and the `on:` action still runs: the watcher is told."""
    app = SimulationContext.from_yaml(BLUEPRINTS / "c1_on_fire.yaml")
    rec = RecordingEgress()
    app.add_egress(rec)
    app.add_ingress(LocalIngress([]))
    app.start()
    trace = _model_steps(app.env, app.peek, app.step, float("inf"), plain_events=1)
    assert {taken for _, _, taken in trace} == {1}
    assert app.peek() == float("inf")
    with pytest.raises(EmptySchedule):
        app.step()
    app.close()
    assert times(rec.records, "watcher") == [2.0]
    assert times(rec.records, "noted") == [2.0]
    plain = SimulationContext.from_yaml(BLUEPRINTS / "c1_on_fire.yaml")
    plain.start(egress=False, ingress=False)
    assert trace == _model_steps(
        plain.env, plain.peek, plain.step, float("inf"), plain_events=1
    )
    plain.close()


def test_c6_run_after_steps_gives_the_records_of_one_run():
    """run() after a run of step() calls ends with the records one run() gives."""

    def build():
        app = SimulationContext.from_yaml(BLUEPRINTS / "c6_step.yaml")
        rec = RecordingEgress()
        app.add_egress(rec)
        return app, rec

    whole, whole_rec = build()
    whole.run(until=35)
    stepped, stepped_rec = build()
    stepped.start(until=35)
    for _ in range(5):
        stepped.step()
    stepped.finish()
    assert times(stepped_rec.records, "ping") == times(whole_rec.records, "ping")
    assert len(stepped_rec.records) == len(whole_rec.records)
    assert times(whole_rec.records, "ping") == [10.0, 20.0, 30.0]


def test_c6_finish_after_steps_that_reached_until():
    """A run stepped until `now == until`, or past it, ends where the steps stopped,
    as `run(until=)` does: `finish()` returns `None` and closes without calling
    `env.run`, which SimPy refuses at `now`. After steps past `until`, the final
    statistics fall at the time where the steps stopped, none at `until`.
    Without a step or an `advance`, `finish()` still runs from the start."""

    def build():
        app, rec = _builder_app()

        def quiet(env):
            yield env.timeout(35)  # a model event at `until` that writes nothing

        app.start(until=35)
        app.env.process(quiet(app.env))
        return app, rec

    whole, whole_rec = build()
    assert whole.finish() is None

    stepped, stepped_rec = build()
    while stepped.env.now < 35:
        stepped.step()
    assert stepped.env.now == 35
    assert stepped.finish() is None
    assert not stepped.started
    assert stepped_rec.records == whole_rec.records
    assert [r["sim_ts"] for r in whole_rec.records if r["stream_type"] == "event"] == [
        10.0,
        20.0,
        30.0,
    ]

    # advance(until), then step past it: the run has ended, so finish() returns None
    past, past_rec = build()
    past.advance(35)
    while past.env.now <= 35:
        past.step()
    assert past.env.now > 35
    assert past.finish() is None
    assert not past.started
    # the run closes where the steps stopped: the final statistics fall at that
    # time and none at `until`
    stats_ts = {r["sim_ts"] for r in past_rec.records if r["stream_type"] != "event"}
    assert past.env.now in stats_ts
    assert 35.0 not in stats_ts


def test_c6_finish_after_steps_to_until_with_statistics_due_there():
    """`step()` processes the events at `until` that `run(until=)` leaves in the
    queue, as SimPy's `step()` does. With a statistics reading due at `until` that
    `step()` processed, the records are one run's records (which end with the final
    statistics that `close()` publishes), plus the records of those events, so the
    reading appears twice at `until`."""

    def build():
        app, rec = _builder_app()
        app.start(until=30)
        return app, rec

    whole, whole_rec = build()
    assert whole.finish() is None

    stepped, stepped_rec = build()
    while stepped.env.now < 30:
        stepped.step()
    assert stepped.finish() is None

    def at_until(records):
        return [r for r in records if r["sim_ts"] == 30.0]

    reading = [r for r in at_until(whole_rec.records) if r["stream_type"] != "event"]
    assert len(reading) == 4
    assert len(whole_rec.records) == 10
    assert len(stepped_rec.records) == 14
    # one run's records come first and unchanged; the rest is the final statistics
    assert stepped_rec.records[:10] == whole_rec.records
    extra = stepped_rec.records[10:]
    assert [r["stream_type"] for r in extra] == [r["stream_type"] for r in reading]
    assert [(r["sim_ts"], r["value"]) for r in extra] == [
        (r["sim_ts"], r["value"]) for r in reading
    ]


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


# ------------------------------------------------------ run in segments

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _idle(env):
    yield env.timeout(0)


def _yaml_app(name):
    """A blueprint with a sink and a fixed start, so two runs write equal records."""
    app = SimulationContext.from_yaml(BLUEPRINTS / name)
    app.logical_start_time = START
    rec = RecordingEgress()
    app.add_egress(rec)
    return app, rec


def _builder_app():
    app = SimulationContext("C6", factor=0.0, logical_start_time=START)
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

    rec = RecordingEgress()
    app.add_egress(rec)
    return app, rec


def _segmented(factory, targets, **start):
    """The records, the values `advance` returned and the end time of a run in
    segments, then the value of `finish()`."""
    app, rec = factory()
    app.start(**start)
    values = [app.advance(target) for target in targets]
    now = app.env.now
    finished = app.finish()
    return rec.records, values, now, finished, app


def _whole(factory, **start):
    app, rec = factory()
    value = app.run(**start)
    return rec.records, value, app


def test_c6_run_in_segments():
    """`advance` to 30, 60 and 100 writes the records one run to 100 writes, with
    the telemetry and the statistics, in the builder and in YAML, and the pings
    fall at SimPy's times when it runs on in the same segments."""
    env = simpy.Environment()
    pings = []

    def ping():
        pings.append(env.now)
        yield env.timeout(0)

    env.process(periodic(env, 10, ping))
    for target in (30, 60, 100):
        env.run(until=target)
        assert env.now == target
    assert pings == [10, 20, 30, 40, 50, 60, 70, 80, 90]

    for factory in (_builder_app, lambda: _yaml_app("c6_step.yaml")):
        whole, value, _ = _whole(factory, until=100)
        records, values, now, finished, app = _segmented(
            factory, [30, 60, 100], until=100
        )
        assert values == [None, None, None]
        assert now == 100
        assert finished is None and value is None
        assert records == whole
        assert [r["sim_ts"] for r in records if r["stream_type"] == "event"] == [
            float(t) for t in pings
        ]
        assert any(r["stream_type"] != "event" for r in records)  # statistics
        assert not app.started  # finish() closed the run

    # the low-level form is repeated env.run(until=), SimPy's own
    def low(segments):
        env = DynamicRealtimeEnvironment(factor=0.0, logical_start_time=START)
        rec = RecordingEgress()
        env.setup_egress([rec], lag_monitor_interval=None)
        env.setup_stats("C6", "basic", interval=10)
        env.process(periodic(env, 10, lambda: _idle(env)))
        for target in segments:
            env.run(until=target)
        env.teardown()
        return rec.records

    assert low([30, 60, 100]) == low([100])


def test_c6_advance_refuses_what_a_segment_cannot_do():
    """`advance` before `start()`, after `close()`, to a time that is not after
    `now`, and after the end raise; past the run's `until` it stops at `until`."""
    app, _ = _yaml_app("c6_step.yaml")
    with pytest.raises(RuntimeError, match="needs a run that start"):
        app.advance(10)
    app.start(until=50)
    app.advance(20)
    with pytest.raises(
        ValueError,
        match=r"until \(20\) must be greater than the current simulation time$",
    ):
        app.advance(20)
    with pytest.raises(
        ValueError,
        match=r"until \(5\) must be greater than the current simulation time$",
    ):
        app.advance(5)
    env = simpy.Environment()
    env.run(until=20)
    with pytest.raises(
        ValueError,
        match=r"until \(20\) must be greater than the current simulation time$",
    ):
        env.run(until=20)  # SimPy's own message
    assert app.advance("30 s") is None  # a duration
    assert app.env.now == 30
    assert app.advance(80) is None  # past until: stops at until
    assert app.env.now == 50
    with pytest.raises(RuntimeError, match="ended at 50"):
        app.advance(60)
    assert app.finish() is None  # returns at once, then closes
    with pytest.raises(RuntimeError, match="needs a run that start"):
        app.advance(60)

    app.start(until=50)  # a new run
    app.close()
    with pytest.raises(RuntimeError, match="needs a run that start"):
        app.advance(10)


def test_c6_finish_raises_a_failed_stop_that_advance_reached():
    """`advance` that reaches a failed `stop` raises its exception. `finish()` then
    raises it again: `env.run(until=event)` returns the value of an event that was
    processed already, which here is the exception object."""
    app, rec = _yaml_app("c6_stop_failed.yaml")
    app.start()
    with pytest.raises(SignalFailed) as advanced:
        app.advance(50)
    assert advanced.value.reason == "scratched"
    with pytest.raises(SignalFailed) as finished:
        app.finish()
    assert finished.value is advanced.value
    assert not app.started  # finish() closed the run
    assert times(rec.records, "ping") == [2.0, 4.0]


def test_c6_advance_to_events_adds_one_internal_event_to_the_queue():
    """Each `advance(event)` in a run with an `until` and no stop or drain stops at
    the end through one internal event, made once. After two segments and a step past
    the event that ended the second, `peek()` is the next model event, not `until`,
    and the queue holds one event at `until`."""
    app = SimulationContext("C6", factor=0.0, logical_start_time=START)
    app.add_signal("gate")
    app.add_egress(RecordingEgress())

    def opener(context):
        env = context.env
        yield env.timeout(3)
        context.signal("gate").fire()
        yield env.timeout(1)
        context.signal("gate").fire()
        yield env.timeout(30)

    app.add_process(opener)
    app.start(until=20)
    assert app.advance(until=app.signal("gate").wait()) is None
    assert app.advance(until=app.signal("gate").wait()) is None
    assert app.env.now == 4
    assert app.peek() == 4  # the event that ended the segment, as in SimPy
    app.step()
    assert app.peek() == 34
    at_until = [event for t, _p, _i, event in app.env._queue if t == 20]
    assert at_until == [app._limit_event]
    app.close()


def test_c6_segments_with_stop_and_drain_give_the_records_of_one_run():
    """A stop or a drain that ends inside a segment, and one that ends on a segment
    boundary, end the run as they do in one run: the same records, the same end,
    the same value, and `advance` after the end is refused."""
    cases = (
        ("c6_stop_signal.yaml", [20, 30], 25),  # the stop is at 25, inside (20, 30]
        ("c6_stop_signal.yaml", [20, 25], 25),  # on the boundary
        ("c6_drain.yaml", [6, 15, 18, 20, 40], 22.0),  # the drain ends at 22
        ("c6_drain.yaml", [10, 22], 22.0),  # begins on a boundary, ends on one
    )
    for name, targets, end in cases:
        app, rec = _yaml_app(name)
        whole_value = app.run()
        whole = rec.records
        assert app.env.now == end

        app, rec = _yaml_app(name)
        app.start()
        values = []
        for target in targets:
            values.append(app.advance(target))
            if app.env.now == end:
                break
        assert app.env.now == end, (name, targets)
        if app.env.now == end and targets[-1] != end:
            with pytest.raises(RuntimeError, match=f"ended at {end:g}"):
                app.advance(end + 1)
        assert app.finish() == whole_value
        assert rec.records == whole, (name, targets)


def test_c6_step_after_a_segment_processes_its_event_as_simpy_does():
    """The event that ends a segment stays in the queue, as SimPy leaves its `until`
    event: `peek()` reads the segment's time and the next `step()` processes it."""
    env = simpy.Environment()
    env.process(periodic(env, 10, lambda: _idle(env)))
    env.run(until=15)
    expected = (env.peek(), (env.step(), env.now)[1], env.peek())
    assert expected == (15, 15, 20)

    for factory in (_builder_app, lambda: _yaml_app("c6_step.yaml")):
        app, _ = factory()
        app.start(until=100)
        app.advance(15)
        assert (app.peek(), (app.step(), app.env.now)[1], app.peek()) == expected
        app.close()


def test_c6_advance_to_an_event_and_on():
    """`advance(until=event)` stops where the event fires and returns its value; run
    on, the waiter whose callback came after the stop goes through, as in SimPy
    (`test_c6_stopped_and_resumed_run`), in the builder and in YAML."""
    expected = _stopped_and_resumed(simpy.Environment())

    def drive(app):
        trace = []
        app.start()
        env = app.env
        gate = app.signal("gate")

        def opener():
            yield env.timeout(5)
            gate.fire("open")

        def waiter():
            yield env.timeout(3)
            value = yield gate.wait()
            trace.append((env.now, "through", value))

        env.process(opener())
        env.process(waiter())
        app.advance(2)
        stop_value = app.advance(until=gate.wait())
        trace.append((env.now, "stopped", stop_value))
        app.advance(10)
        app.close()
        return trace

    yaml_app, _ = _yaml_app("c6_gate.yaml")
    assert drive(yaml_app) == expected

    builder, _ = _builder_app()
    builder.add_signal("gate")
    assert drive(builder) == expected
