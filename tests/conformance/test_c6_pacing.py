"""R1 real-time factor: desflow paces a run as SimPy's RealtimeEnvironment does.

The fake clock drives `simpy.rt`'s `monotonic` and `sleep`, so a paced run advances
in fake seconds with no real waiting. The test compares the fake wall offset at each
model event between desflow and plain SimPy, at two factors.
"""

from datetime import datetime, timedelta, timezone

import pytest
import simpy
from simpy.rt import RealtimeEnvironment

from desflow import DynamicRealtimeEnvironment, SimulationContext
from tests.conformance.harness import BLUEPRINTS, FakeClock, RecordingEgress


def _offsets(env):
    """The (simulation time, fake wall offset from the run's start) at each event, for
    events at +2, +5, +5 (a tie) and +8 from the start."""
    seen = []

    def process():
        for step in (2.0, 3.0, 0.0, 3.0):
            yield env.timeout(step)
            import simpy.rt as rt

            seen.append((round(env.now, 3), round(rt.monotonic() - env.real_start, 6)))

    env.process(process())
    env.run()
    return seen


def test_c6_real_time_factor(monkeypatch):
    """At factor 0.5 and 2, each event's wall offset is the factor times the
    simulation time since the start, and two events at one instant add no wall time,
    exactly as SimPy's RealtimeEnvironment paces them."""
    for factor in (0.5, 2.0):
        FakeClock().install(monkeypatch)
        reference = simpy.rt.RealtimeEnvironment(
            factor=factor, strict=False, initial_time=100
        )
        expected = _offsets(reference)
        assert expected[0] == (102.0, round(factor * 2, 6))
        assert expected[1] == expected[2]  # the tie adds no wall time

        FakeClock().install(monkeypatch)
        env = DynamicRealtimeEnvironment(factor=factor, strict=False, initial_time=100)
        assert _offsets(env) == expected


# ------------------------------------------------ resume_pacing, strict and sync


class _Run:
    """One run in one of the three forms, driven the same way: `go(until)` runs on
    to a time, `close()` ends it. `env` is the environment under it."""

    def __init__(self, env, go, close):
        self.env, self.go, self.close = env, go, close


def _low_level(strict=True):
    env = DynamicRealtimeEnvironment(factor=1.0, strict=strict)
    return _Run(env, lambda until: env.run(until=until), env.teardown)


def _builder(strict=True):
    app = SimulationContext("C6", factor=1.0, strict=strict)
    app.add_egress(RecordingEgress())
    app.start(until=100)
    return _Run(app.env, lambda until: app.advance(until), app.close)


def _yaml(strict=True):
    app = SimulationContext.from_yaml(BLUEPRINTS / "c6_strict.yaml")
    assert app.strict is True
    app.add_egress(RecordingEgress())
    app.start(until=100)
    return _Run(app.env, lambda until: app.advance(until), app.close)


FORMS = (_low_level, _builder, _yaml)


def test_c6_resume_pacing(monkeypatch):
    """After a pause the wall clock is ahead of the run. `resume_pacing()` makes the
    next event wait `factor` seconds from the resume, as SimPy does with
    `env_start = now` and `sync()`, in the low-level API, the builder and YAML."""
    clock = FakeClock().install(monkeypatch)
    reference = RealtimeEnvironment(factor=1.0, strict=False)
    seen = []

    def event(env):
        yield env.timeout(31)
        seen.append(clock.monotonic())

    reference.process(event(reference))
    reference.run(until=30)
    clock.advance(5)
    reference.env_start = reference.now
    reference.sync()
    resumed_at = clock.monotonic()
    reference.run(until=40)
    expected = seen[0] - resumed_at
    assert expected == pytest.approx(1.0)

    for form in FORMS:
        clock = FakeClock().install(monkeypatch)
        run = form(strict=False) if form is _low_level else form()
        seen.clear()
        run.env.process(event(run.env))
        run.go(30)
        clock.advance(5)
        run.env.resume_pacing()
        resumed_at = clock.monotonic()
        run.go(40)
        assert seen[0] - resumed_at == pytest.approx(expected)
        run.close()

    # without it the event, due 31 s after the start, finds the clock at 35 and runs
    # at once, so the wall clock shows no wait
    clock = FakeClock().install(monkeypatch)
    run = _low_level(strict=False)
    seen.clear()
    run.env.process(event(run.env))
    run.go(30)
    clock.advance(5)
    paused_at = clock.monotonic()
    run.go(40)
    assert seen[0] == paused_at
    run.close()


def _slow_then_late(env, clock, seen):
    def slow():
        yield env.timeout(1)
        seen.append(("first", env.now))
        clock.advance(2.5)  # 1.5 s behind the pace at factor 1
        yield env.timeout(1)
        seen.append(("second", env.now))  # not reached under strict

    env.process(slow())


def test_c6_strict_raises_when_behind(monkeypatch):
    """A step more than `factor` seconds behind raises `RuntimeError` under `strict`,
    with SimPy's text and at SimPy's simulation time; without `strict` the run goes on."""
    clock = FakeClock().install(monkeypatch)
    reference = RealtimeEnvironment(factor=1.0, strict=True)
    seen = []
    _slow_then_late(reference, clock, seen)
    with pytest.raises(RuntimeError) as error:
        reference.run(until=10)
    expected = (str(error.value), reference.now, list(seen))
    assert expected == ("Simulation too slow for real time (1.500s).", 1, seen)

    for form in FORMS:
        clock = FakeClock().install(monkeypatch)
        run = form()
        seen.clear()
        _slow_then_late(run.env, clock, seen)
        with pytest.raises(RuntimeError) as error:
            run.go(10)
        assert (str(error.value), run.env.now, seen) == expected
        run.close()

    # strict=False runs on, behind the pace
    clock = FakeClock().install(monkeypatch)
    run = _low_level(strict=False)
    seen.clear()
    _slow_then_late(run.env, clock, seen)
    run.go(10)
    assert seen == [("first", 1), ("second", 2)]
    run.close()


def test_c6_sync_catches_up(monkeypatch):
    """With the wall clock 5 s ahead of the run, the first step raises under
    `strict`; after `sync()` the event at 1 runs at clock 6, as in SimPy, in the
    low-level API, the builder and YAML."""

    def at_one(env, ran):
        yield env.timeout(1)
        ran.append(clock.monotonic())

    clock = FakeClock().install(monkeypatch)
    reference = RealtimeEnvironment(factor=1.0, strict=True)
    ran = []
    reference.process(at_one(reference, ran))
    clock.advance(5)
    with pytest.raises(RuntimeError, match=r"too slow for real time \(5.000s\)"):
        reference.run(until=10)
    clock = FakeClock().install(monkeypatch)
    reference = RealtimeEnvironment(factor=1.0, strict=True)
    ran = []
    reference.process(at_one(reference, ran))
    clock.advance(5)
    reference.sync()
    reference.run(until=10)
    assert ran == [6.0]

    for form in FORMS:
        clock = FakeClock().install(monkeypatch)
        run = form()
        ran = []
        run.env.process(at_one(run.env, ran))
        clock.advance(5)
        with pytest.raises(RuntimeError, match=r"too slow for real time \(5.000s\)"):
            run.go(10)
        run.close()

        clock = FakeClock().install(monkeypatch)
        run = form()
        ran = []
        run.env.process(at_one(run.env, ran))
        clock.advance(5)
        run.env.sync()
        run.go(10)
        assert ran == [6.0]
        run.close()


def test_c6_strict_default(monkeypatch):
    """The low-level default is SimPy's, True; the builder and YAML default to False
    and take the setting; and `strict` is never checked at factor 0, where SimPy's
    check would raise as soon as the wall clock moves (owner decision D-E1). The
    go-live is reached under strict at factor 1 with a sink, in the low-level API and
    the builder, and nothing raises."""
    assert RealtimeEnvironment().strict is True
    assert DynamicRealtimeEnvironment().strict is True
    assert SimulationContext("C6").strict is False
    app = SimulationContext.from_yaml(BLUEPRINTS / "c6_step.yaml")
    assert app.strict is False
    assert SimulationContext("C6", strict=True).strict is True

    def idle(env):
        for _ in range(5):
            yield env.timeout(10)

    clock = FakeClock().install(monkeypatch)
    reference = RealtimeEnvironment(factor=0.0, strict=True)
    reference.process(idle(reference))
    clock.advance(100)
    with pytest.raises(RuntimeError, match="too slow"):
        reference.run()

    # desflow at factor 0: the clock jumping does not raise, also before go_live_at
    for go_live in (
        None,
        datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=1),
    ):
        clock = FakeClock().install(monkeypatch)
        env = DynamicRealtimeEnvironment(
            factor=0.0,
            strict=True,
            logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            go_live_at=go_live,
        )
        env.process(idle(env))
        clock.advance(100)
        env.run(until=40)
        assert env.now == 40
        env.teardown()

    # the go-live is reached under strict at factor 1: the wall clock keeps pace with
    # the run, so nothing is behind and nothing raises, with a sink present
    go_live = datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    FakeClock().install(monkeypatch)
    env = DynamicRealtimeEnvironment(
        factor=1.0, strict=True, logical_start_time=start, go_live_at=go_live
    )
    env.setup_egress([RecordingEgress()], lag_monitor_interval=1)

    def ticker():
        while True:
            yield env.timeout(1)
            env.publish_event("ticker", {"event": "tick"})

    env.process(ticker())
    env.run(until=15)
    assert env.now == 15
    env.teardown()

    FakeClock().install(monkeypatch)
    app = SimulationContext(
        "C6", factor=1.0, strict=True, logical_start_time=start, go_live_at=go_live
    )
    app.add_egress(RecordingEgress())
    app.add_arrival("pings", dist="constant", value=1)

    @app.task()
    def ping_task(task_id):
        return {"event": "ping"}

    @app.arrival_loop("pings")
    def pings(context):
        while True:
            entity = yield context.wait_for_arrival("pings")
            context.spawn(ping_task(context.next_instance("ping_task"), entity=entity))

    app.run(until=15)
