"""C6 run control: the simulation clock starts at the chosen initial time.

A plain SimPy reference and the three desflow interfaces run the same
deterministic arrival and service model with the clock started at 100. Every
event time is relative to that start, matching SimPy.
"""

from pathlib import Path

import pytest
import simpy

from desflow import DynamicRealtimeEnvironment, Entity
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
    RecordingEgress,
    event_trace,
    run_builder,
    run_yaml,
    times,
)

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


def _reference_current_time():
    env = simpy.Environment(initial_time=100)
    seen = []

    def order(n):
        seen.append((env.now, "early" if env.now < 110 else "late"))
        yield env.timeout(0)

    def source():
        yield env.timeout(5)
        env.process(order(0))
        yield env.timeout(5)
        env.process(order(1))

    env.process(source())
    env.run()
    return seen


def _builder_current_time():
    app = SimulationContext("C6", factor=0.0, initial_time=100)

    @app.task(
        next={"when": [{"condition": "t < 110", "task": "early"}], "default": "late"},
        name="order",
    )
    def order(task_id):
        return {"event": "ordered"}

    @app.task(name="early")
    def early(task_id):
        return {"event": "early"}

    @app.task(name="late")
    def late(task_id):
        return {"event": "late"}

    def script(context):
        yield context.env.timeout(5)
        context.spawn(order(0))
        yield context.env.timeout(5)
        context.spawn(order(1))

    app.add_process(script)
    return run_builder(app, 120)


def test_c6_current_time():
    """`t` is the simulation clock, so a route condition on it sees the current time.
    With the clock at 100, the order at 105 routes to early and the one at 110, where
    `t < 110` is false, routes to late, as the SimPy reference on `env.now` does."""
    assert _reference_current_time() == [(105, "early"), (110, "late")]
    for records in (_builder_current_time(), run_yaml("c6_current_time.yaml")[0]):
        assert times(records, "early") == [105.0]
        assert times(records, "late") == [110.0]


def _reference_step_count(entities, length):
    """SimPy: `entities` processes start at 0, and each takes the one unit of a
    resource `length` times in a row, serving 1 each time. The count of `env.step()`
    calls until the queue is empty."""
    env = simpy.Environment()
    unit = simpy.Resource(env, capacity=1)

    def entity():
        for _ in range(length):
            with unit.request() as request:
                yield request
                yield env.timeout(1)

    def script():
        for _ in range(entities):
            env.process(entity())
        yield env.timeout(0)

    env.process(script())
    steps = 0
    while env.peek() != float("inf"):
        env.step()
        steps += 1
    return steps, env.now


def _builder_step_count(entities, length):
    """The same model as a chain of `length` tasks, each taking the unit and serving 1,
    each handing on to the next; the count of `app.step()` calls."""
    app = SimulationContext("C6", factor=0.0)
    app.add_resource("unit", current_cap=1, max_cap=1)
    app.add_service("serve", dist="constant", value=1)
    names = [f"t{index}" for index in range(length)]
    functions = {}
    for index, name in enumerate(names):
        following = names[index + 1] if index + 1 < length else None

        @app.task(service_id="serve", resource_id="unit", next=following, name=name)
        def step(task_id):
            return {"event": "served"}

        functions[name] = step

    def script(context):
        for number in range(entities):
            context.spawn(functions[names[0]](0, entity=Entity(f"e-{number}", "e", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    app.start()
    steps = 0
    while app.peek() != float("inf"):
        app.step()
        steps += 1
    now = app.env.now
    app.close()
    return steps, now


@pytest.mark.parametrize("length", [2, 5, 14])
@pytest.mark.parametrize("entities", [1, 3, 10])
def test_step_count_equals_simpy(entities, length):
    """`step()` counts the events of the model only, so a model that loops through
    tasks takes as many steps as SimPy's program does, and ends at the same time (T8).
    """
    assert _builder_step_count(entities, length) == _reference_step_count(
        entities, length
    )
