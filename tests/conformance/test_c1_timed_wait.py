"""C1 timed wait: a service delay ends at the specified simulation time.

A plain SimPy reference and the three desflow interfaces run the same
deterministic model: an entity arrives every 10 seconds and a timed task holds
it for 5. The event times must match SimPy, and each entity's finished event
must fall exactly 5 seconds after its started event.
"""

from pathlib import Path

import pytest
import simpy

from desflow import DynamicRealtimeEnvironment, Entity
from desflow.builder.context import SimulationContext
from tests.conformance.harness import (
    RecordingEgress,
    event_trace,
    run_builder,
    run_yaml_text,
)

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


def _reference_zero(n):
    """SimPy: n processes start at 3 and each yields `timeout(0)`; the order of the
    events they publish."""
    env = simpy.Environment()
    trace = []

    def entity(i):
        trace.append((env.now, i, "started"))
        yield env.timeout(0)
        trace.append((env.now, i, "done"))

    def source():
        yield env.timeout(3)
        for i in range(1, n + 1):
            env.process(entity(i))
        yield env.event()

    env.process(source())
    env.run(until=10)
    return trace


def _zero_trace(records):
    return [
        (
            t,
            int(key.rsplit("-", 1)[1]),
            "started" if status == "started" else "done",
        )
        for t, key, _task, status in event_trace(records)
    ]


@pytest.mark.parametrize("n", [1, 3, 10])
def test_delay_of_zero_stays_zero(tmp_path, n):
    """A service of constant 0 takes no time, as `timeout(0)` takes none: the task
    starts and ends at one instant, and n tasks end in the order they started (T5;
    register case P12)."""
    expected = _reference_zero(n)
    assert expected[0] == (3, 1, "started") and expected[-1] == (3, n, "done")

    app = SimulationContext("C1", factor=0.0)
    app.add_service("zero", dist="constant", value=0)

    @app.task(service_id="zero", name="wait")
    def wait(task_id):
        return {"event": "waited"}

    def script(context):
        yield context.env.timeout(3)
        for i in range(1, n + 1):
            context.spawn(wait(i, entity=Entity(f"a-{i}", "a", {})))
        yield context.env.event()

    app.add_process(script)
    assert _zero_trace(run_builder(app, 10)) == expected

    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "services: {zero: {dist: constant, value: 0}}\n"
        "arrivals: {a: {replay: {path: a.csv}, spawn: wait}}\n"
        "tasks: {wait: {service: zero, payload: {event: waited}}}\n"
        "run: {until: 10}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [3] * n})
    assert outcome.error is None
    trace = _zero_trace(outcome.records)
    assert sorted(trace) == sorted(expected)
    assert {t for t, _i, _s in trace} == {3}


def test_a_gap_of_zero_keeps_an_arrival_at_one_instant():
    """An arrival with a constant gap of 0 stays at one instant, as a SimPy source
    that yields `timeout(0)` does: the clock does not move, and each step spawns."""
    env = simpy.Environment()
    spawned = []

    def entity():
        spawned.append(env.now)
        yield env.timeout(1)

    def source():
        while True:
            yield env.timeout(0)
            env.process(entity())

    env.process(source())
    for _ in range(20):
        env.step()
    assert env.now == 0 and spawned

    app = SimulationContext("C1", factor=0.0)
    app.add_service("wait", dist="constant", value=1)
    app.add_arrival("orders", dist="constant", value=0)

    @app.task(service_id="wait", name="wait")
    def wait(task_id):
        return {"event": "waited"}

    @app.arrival_loop("orders")
    def orders(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(wait(context.next_instance("wait"), entity=entity))

    app.start()
    for _ in range(20):
        app.step()
    assert app.env.now == 0
    app.close()


def test_integer_literals_stay_integers(tmp_path):
    """A `timeout`, a service and an `after` of whole seconds leave `env.now` an
    integer, as SimPy's do, and a number over a container keeps its type (MF2)."""

    def reference():
        env = simpy.Environment()

        def entity():
            yield env.timeout(2)
            yield env.timeout(3)

        env.process(entity())
        seen = []
        while env.peek() != float("inf"):
            env.step()
            seen.append(env.now)
        return seen

    def drive(app):
        app.start()
        seen = []
        while app.peek() != float("inf"):
            app.step()
            seen.append(app.env.now)
        app.close()
        return seen

    expected = reference()
    assert {type(now) for now in expected} == {int}

    builder = SimulationContext("C1", factor=0.0)
    builder.add_service("three", dist="constant", value=3)

    @builder.task(wait={"timeout": 2}, service_id="three", name="wait")
    def wait(task_id):
        return {"event": "waited"}

    def start(context):
        context.spawn(wait(0))
        yield context.env.event()

    builder.add_process(start)
    seen = drive(builder)
    assert {type(now) for now in seen} == {int} and seen[-1] == 5

    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "services: {three: {dist: constant, value: 3}}\n"
        "containers: {tank: {current_cap: 10, max_cap: 10, level: 1}}\n"
        "tasks:\n"
        "  wait: {wait: {timeout: 2}, service: three, put: {container: tank, amount: 2},"
        " payload: {event: waited}}\n"
        "processes: [!python tests.conformance.batch2_scripts.start_wait]\n"
    )
    folder = tmp_path
    (folder / "model.yaml").write_text(text)
    app = SimulationContext.from_yaml(folder / "model.yaml")
    seen = drive(app)
    assert {type(now) for now in seen} == {int} and seen[-1] == 5
    assert app.get_container("tank").level == 3
    assert type(app.get_container("tank").level) is int


def test_a_constant_arrival_keeps_integer_times():
    """An arrival of `dist: constant` and `value: 2` spawns at 2, 4 and 6 and the
    clock reads an `int` at every step, as SimPy's does for a source that yields
    `timeout(2)` (MF2)."""
    env = simpy.Environment()

    def source():
        while True:
            yield env.timeout(2)

    env.process(source())
    expected = []
    while env.peek() <= 6:
        env.step()
        expected.append(env.now)
    assert {type(now) for now in expected} == {int}

    app = SimulationContext("C1", factor=0.0)
    app.add_service("one", dist="constant", value=1)
    app.add_arrival("orders", dist="constant", value=2)

    @app.task(service_id="one", name="work")
    def work(task_id):
        return {"event": "worked"}

    @app.arrival_loop("orders")
    def orders(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(work(context.next_instance("work"), entity=entity))

    app.start()
    seen = []
    while app.peek() <= 6:
        app.step()
        seen.append(app.env.now)
    app.close()
    assert {type(now) for now in seen} == {int}
    assert seen[-1] == 6
