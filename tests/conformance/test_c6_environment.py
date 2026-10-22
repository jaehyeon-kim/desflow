"""C6 scheduling and the environment: `schedule` with a priority, `fire` with `urgent`
and `after`, and an environment subclass.

SimPy orders events by time, then priority (`URGENT` before `NORMAL`), then the order
they were scheduled. `env.schedule(event, priority, delay)` is the low-level form of
that. A `fire` action with `urgent` or `after` is the builder and YAML form, for a
one-shot signal. An environment subclass is built by `SimulationContext(environment=)`
and `simulation.environment`.
"""

from datetime import datetime, timezone

import pytest
import simpy
from simpy.events import NORMAL, URGENT

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)
from desflow.blueprint.loader import BlueprintError
from desflow.core.context import SimulationContext
from desflow.core.internal import is_internal
from tests.conformance.batch5_scripts import TracingEnvironment, start_go
from tests.conformance.harness import (
    BLUEPRINTS,
    RecordingEgress,
    run_low_level,
    run_yaml,
    run_yaml_text,
)

START = datetime(2026, 1, 1, tzinfo=timezone.utc)

# ------------------------------------------------- schedule with a priority


def _arm(env, event, priority, delay):
    """Triggers `event` and schedules it as `env.schedule(event, priority, delay)`."""
    event._ok, event._value = True, None
    env.schedule(event, priority, delay)


def _priority_model(env, resource, trace):
    """Events at one time in both priorities, delayed and at once, around a
    resource that is held until 10."""

    def user():
        with resource.request() as req:
            yield req
            trace.append((env.now, "user"))
            yield env.timeout(10)
        trace.append((env.now, "released"))

    def waiter(event, label):
        yield event
        trace.append((env.now, label))

    def scheduler():
        yield env.timeout(5)
        for label, priority, delay in (
            ("later_n", NORMAL, 5),
            ("later_u", URGENT, 5),
            ("now_n", NORMAL, 0),
            ("now_u", URGENT, 0),
        ):
            event = env.event()
            env.process(waiter(event, label))
            _arm(env, event, priority, delay)

    env.process(user())
    env.process(scheduler())


EXPECTED_PRIORITY = [
    (0, "user"),
    (5, "now_u"),
    (5, "now_n"),
    (10, "later_u"),
    (10, "released"),
    (10, "later_n"),
]


def test_c6_schedule_with_a_priority():
    """At one time an urgent event goes first even though it was scheduled last, a
    normal one follows in the order scheduled, and a delay moves the time. The
    low-level trace is SimPy's, with a sink, statistics and a resource present."""
    env = simpy.Environment()
    expected = []
    _priority_model(env, simpy.Resource(env, 1), expected)
    env.run(until=20)
    assert expected == EXPECTED_PRIORITY

    params = SimParameter(
        sim_id="C6", resources={"machine": CapacityConfig(current_cap=1, max_cap=1)}
    )
    trace = []

    def start(low):
        _priority_model(low, DynamicResource(low, "C6", "machine"), trace)

    records, _ = run_low_level(params, start, until=20, stats="basic")
    assert trace == expected
    assert records  # the sink and the statistics ran


# --------------------------------------------- fire with urgent and after


def _observers(trace, signals):
    """Processes that wait on each named signal and tick at 10. `signals(context)`
    returns the events by name."""

    def waiter(context, label):
        yield signals(context)[label]
        trace.append((context.env.now, label))

    def tick(context):
        yield context.env.timeout(10)
        trace.append((context.env.now, "tick"))

    def build(label):
        return lambda context: waiter(context, label)

    return [build(label) for label in ("now_n", "now_u", "late_n", "late_u")] + [tick]


EXPECTED_FIRING = [
    (5, "now_u"),
    (5, "now_n"),
    (10, "late_u"),
    (10, "tick"),
    (10, "late_n"),
]


def _firing_reference():
    """The same firings with SimPy events and `env.schedule`."""
    env = simpy.Environment()
    trace = []
    events = {name: env.event() for name in ("now_n", "now_u", "late_n", "late_u")}
    go = env.event()

    def waiter(label):
        yield events[label]
        trace.append((env.now, label))

    def tick():
        yield env.timeout(10)
        trace.append((env.now, "tick"))

    def on_go(_event):
        env.schedule(events["now_n"], NORMAL, 0)
        env.schedule(events["now_u"], URGENT, 0)
        env.schedule(events["late_n"], NORMAL, 5)
        env.schedule(events["late_u"], URGENT, 5)

    def starter():
        yield env.timeout(5)
        go.succeed()

    for event in events.values():
        event._ok, event._value = True, None
    go.callbacks.append(on_go)
    for label in events:
        env.process(waiter(label))
    env.process(tick())
    env.process(starter())
    env.run(until=20)
    return trace


def _builder_firing():
    app = SimulationContext("C6", factor=0.0)
    app.add_signal(
        "go",
        once=True,
        on=[
            {"fire": {"signal": "now_n"}},
            {"fire": {"signal": "now_u", "urgent": True}},
            {"fire": {"signal": "late_n", "after": "5 s"}},
            {"fire": {"signal": "late_u", "urgent": True, "after": 5}},
        ],
    )
    for name in ("now_n", "now_u", "late_n", "late_u"):
        app.add_signal(name, once=True)
    app.add_process(start_go)
    trace = []
    for func in _observers(
        trace, lambda c: {n: c.signal(n).wait() for n in c._signals}
    ):
        app.add_process(func)
    app.run(until=20)
    return trace


def _yaml_firing():
    trace = []

    def prepare(app):
        for func in _observers(
            trace, lambda c: {n: c.signal(n).wait() for n in c._signals}
        ):
            app.add_process(func)

    run_yaml("c6_fire_timing.yaml", prepare)
    return trace


def test_c6_fire_with_urgent_and_after():
    """`urgent` puts the firing ahead of the other events at its time and `after`
    moves it later, as `env.schedule(event, priority, delay)` does. The builder and
    YAML give the order SimPy gives for the same events."""
    expected = _firing_reference()
    assert expected == EXPECTED_FIRING
    assert _builder_firing() == expected
    assert _yaml_firing() == expected


_DRAIN = """
simulation: {{sim_id: C6, factor: 0}}
signals:
  report:
    once: true
    on:
      - fire: {{signal: echo, value: 1}}
  echo: {{once: true}}
arrivals:
  orders: {{replay: {{path: orders.csv}}, spawn: work}}
tasks:
  work:
    payload: {{event: done}}
    on:
      - fire: {{signal: report{timing}}}
run:
  until: 5
  drain: true
"""


def test_c6_fire_with_after_is_not_counted_by_a_drain(tmp_path):
    """A firing with `after` is not counted against the entity, so a drain ends at
    once and the signal's `on:` actions never run; without `after` the drain waits
    for them."""
    counts = {}
    for timing in ("", ", after: 7 s"):
        text = _DRAIN.format(timing=timing)
        folder = tmp_path / ("after" if timing else "now")
        folder.mkdir()
        outcome = run_yaml_text(folder, text, {"orders": [4]})
        assert outcome.error is None
        app = outcome.app_or_env
        counts[timing] = (app.signal("report").count, app.signal("echo").count)
        assert app.env.now == 5
    assert counts[""] == (1, 1)
    assert counts[", after: 7 s"] == (1, 0)


def test_c6_fire_twice_with_after_is_refused_as_a_one_shot_signal():
    """A one-shot signal fired twice is refused, with `after` too: the first firing
    counts at once, as SimPy's `succeed()` does. Under `stop` the second raises; under
    `continue` it is dropped and the first still happens."""
    for policy in ("stop", "continue"):
        app = SimulationContext("C6", factor=0.0)
        app.add_signal("late", once=True)
        app.start(until=20, on_unhandled_failure=policy)
        late = app.signal("late")
        late.fire("first", after=3)
        assert late.count == 1
        if policy == "stop":
            with pytest.raises(RuntimeError, match="one-shot"):
                late.fire("second", after=4)
        else:
            late.fire("second", after=4)
        assert late.count == 1
        app.advance(5)
        assert late.wait().processed
        assert late.value == "first"
        app.close()


_REARMING_BUILDER = (
    r"`urgent` and `after` need a one-shot signal \(`once=True`\); "
    "a re-arming signal fires now"
)
_REARMING_YAML = (
    r"`urgent` and `after` need a one-shot signal \(`once: true`\); "
    "a re-arming signal fires now"
)


def test_c6_fire_urgent_and_after_need_a_one_shot_signal(tmp_path):
    """A signal that re-arms fires now; asking for `urgent` or `after` is refused
    when the run is built, in the builder and in YAML, and a bad `after` is refused."""
    app = SimulationContext("C6", factor=0.0)
    app.add_signal("go", once=True, on=[{"fire": {"signal": "again", "urgent": True}}])
    app.add_signal("again")
    with pytest.raises(ValueError, match=_REARMING_BUILDER):
        app.start(until=10)
    rearming = SimulationContext("C6", factor=0.0)
    rearming.add_signal("again")
    rearming.start(until=10)
    for timing in ({"urgent": True}, {"after": 2}):
        with pytest.raises(ValueError, match=_REARMING_BUILDER):
            rearming.signal("again").fire(1, **timing)
    rearming.close()
    with pytest.raises(ValueError, match="after is more than 0"):
        SimulationContext("C6").add_signal(
            "go", once=True, on=[{"fire": {"signal": "x", "after": 0}}]
        )
    text = _DRAIN.format(timing=", urgent: true").replace(
        "report:\n    once: true", "report:\n    once: false"
    )
    with pytest.raises(BlueprintError, match=_REARMING_YAML):
        run_yaml_text(tmp_path, text, {"orders": [4]})


# ----------------------------------------------------- environment subclass


def _reference_trace():
    env = _SimpyTracing()

    def pinger():
        while True:
            yield env.timeout(10)

    env.process(pinger())
    for _ in range(5):
        env.step()
    return env.trace


class _SimpyTracing(simpy.Environment):
    def __init__(self):
        super().__init__()
        self.trace = []

    def step(self):
        super().step()
        self.trace.append((self.now, self.peek()))


def _stepped(env_or_app, steps=5):
    for _ in range(steps):
        env_or_app.step()
    return env_or_app


def test_c6_environment_subclass():
    """A subclass that records `(now, peek())` after `super().step()` records SimPy's
    trace, stepped outside `run()`, in the low-level API, the builder and YAML, with a
    sink, statistics and a resource present. The builder and YAML build the subclass
    themselves."""
    expected = _reference_trace()
    assert expected == [(0, 10), (10, 20), (20, 30), (30, 40), (40, 50)]

    env = TracingEnvironment(factor=0.0)
    env.setup_egress([RecordingEgress()], lag_monitor_interval=None)
    env.setup_stats("C6", "basic", interval=10)

    def pinger():
        while True:
            yield env.timeout(10)

    env.process(pinger())
    _stepped(env)
    env.teardown()
    assert env.trace == expected

    from tests.conformance.batch5_scripts import pinger as script

    app = SimulationContext("C6", factor=0.0, environment=TracingEnvironment)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.with_stats("basic", interval=10)
    app.add_egress(RecordingEgress())
    app.add_process(script)
    app.start(until=40)
    _stepped(app)
    assert isinstance(app.env, TracingEnvironment)
    assert app.env.trace == expected
    app.close()

    yaml_app = SimulationContext.from_yaml(BLUEPRINTS / "c6_environment.yaml")
    yaml_app.add_egress(RecordingEgress())
    yaml_app.start()
    _stepped(yaml_app)
    assert isinstance(yaml_app.env, TracingEnvironment)
    assert yaml_app.env.trace == expected
    yaml_app.close()


def test_c6_environment_subclass_in_run():
    """Inside `run()` an override of `step` runs once for every event in the queue,
    desflow's own included: at factor 0 with a sink and statistics, the run to 30
    makes 10 calls, 4 for model events (the process start, the timeouts at 10 and 20,
    and the end at 30) and 6 for desflow's own. When the run is paced, an override of
    `_step_event` runs for the same events, more of them because the sink's flush
    timers run. The records are the plain environment's."""

    def counting(method):
        class Counting(DynamicRealtimeEnvironment):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.calls = 0
                self.internal = 0

        def count(self):
            self.calls += 1
            self.internal += is_internal(self._queue[0][3])
            return getattr(super(Counting, self), method)()

        setattr(Counting, method, count)
        return Counting

    def run(cls, **kwargs):
        env = cls(logical_start_time=START, **kwargs)
        rec = RecordingEgress()
        env.setup_egress([rec], lag_monitor_interval=None)
        env.setup_stats("C6", "basic", interval=5)

        def pinger():
            while True:
                yield env.timeout(10)
                env.publish_event("p", {"event": "ping"})

        env.process(pinger())
        env.run(until=30)
        env.teardown()
        return env, rec.records

    _, plain = run(DynamicRealtimeEnvironment, factor=0.0)
    step_env, by_step = run(counting("step"), factor=0.0)
    assert by_step == plain
    assert step_env.calls == 10
    # 4 for the model: the process start, the timeouts at 10 and 20, the end at 30
    assert step_env.calls - step_env.internal == 4
    # 6 for desflow: the statistics process start and its timeouts at 5 to 25
    assert step_env.internal == 6

    paced = {"factor": 1e-9, "strict": False}
    _, plain_paced = run(DynamicRealtimeEnvironment, **paced)
    paced_step, by_step = run(counting("step"), **paced)
    paced_event, by_event = run(counting("_step_event"), **paced)
    assert by_step == by_event == plain_paced
    assert paced_event.calls == paced_step.calls > 10  # the flush timers add events

    # the environment must be a subclass, checked when the context is built
    with pytest.raises(ValueError, match="subclass of DynamicRealtimeEnvironment"):
        SimulationContext("C6", environment=simpy.Environment)
    with pytest.raises(ValueError, match="subclass of DynamicRealtimeEnvironment"):
        SimulationContext("C6", environment=TracingEnvironment())
