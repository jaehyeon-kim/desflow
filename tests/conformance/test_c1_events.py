"""C1 events: one-shot signals (V3), callbacks and triggering from another event
(V4 to V6), a failure nobody handles (V7), and the timeout value (V11).

Each test runs a plain SimPy reference, then the same model in the low-level API, the
builder and YAML, and compares the event times and values. The low-level models use
SimPy's `Event`, its `callbacks`, `trigger`, `defused` and `env.timeout(delay, value)`;
the builder and YAML use one-shot signals, `on:` actions, `on_unhandled_failure` and
the timeout `value`.
"""

import pytest
import simpy

from desflow import Signal, SignalFailed
from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder, run_low_level, run_yaml, times


def _failed(records, task):
    """The times a task published a failed status."""
    return [
        r["sim_ts"]
        for r in records
        if r["task"] == task
        and isinstance(r["value"], dict)
        and r["value"].get("status") == "failed"
    ]


# ---------------------------------------------------------- one-shot event (V3)


def _reference_one_shot():
    env = simpy.Environment()
    closing = env.event()
    out = []

    def early():
        yield env.timeout(1)
        yield closing
        out.append((env.now, "early"))

    def close():
        yield env.timeout(2)
        closing.succeed({"event": "closed"})

    def late():
        yield env.timeout(5)
        yield closing  # already triggered: resumes at once
        out.append((env.now, "late"))

    env.process(early())
    env.process(close())
    env.process(late())
    env.run(until=8)
    return out


def _builder_one_shot():
    app = SimulationContext("C1", factor=0.0)
    app.add_signal("closing", once=True)

    @app.task(signal="closing", name="close")
    def close(task_id):
        return {"event": "closed"}

    @app.task(wait={"signal": "closing"}, name="early")
    def early(task_id):
        return {"event": "early"}

    @app.task(wait={"signal": "closing"}, name="late")
    def late(task_id):
        return {"event": "late"}

    def script(context):
        context.spawn(early(0))
        yield context.env.timeout(2)
        context.spawn(close(0))
        yield context.env.timeout(3)
        context.spawn(late(0))
        yield context.env.event()

    app.add_process(script)
    return run_builder(app, 8)


def test_c1_one_shot_event():
    """A one-shot signal does not re-arm: early waits before the firing and resumes
    at 2, late starts after it and resumes at once at 5, as SimPy's `Event`."""
    assert _reference_one_shot() == [(2, "early"), (5, "late")]
    for records in (_builder_one_shot(), run_yaml("c1_one_shot.yaml")[0]):
        assert times(records, "early") == [2.0]
        assert times(records, "late") == [5.0]


def test_c1_one_shot_event_fired_twice():
    """A one-shot signal fired a second time raises SimPy's RuntimeError under
    `stop`, and is dropped under `continue`."""
    # SimPy's Event raises on a second succeed
    env = simpy.Environment()
    event = env.event()
    event.succeed(1)
    with pytest.raises(RuntimeError):
        event.succeed(2)

    def start(env):
        def fire():
            closing = Signal(env, "closing", once=True)
            closing.fire({"n": 1})
            closing.fire({"n": 2})  # raises under stop
            yield env.timeout(0)

        env.process(fire())

    with pytest.raises(RuntimeError, match="one-shot"):
        run_low_level(None, start, 2, on_unhandled_failure="stop")

    # under continue the second firing is dropped and the run goes on
    def start_continue(env):
        def fire():
            closing = Signal(env, "closing", once=True)
            closing.fire({"n": 1})
            closing.fire({"n": 2})
            env.publish_event("s-0", {"event": "ok"}, task="done", instance=0)
            yield env.timeout(0)

        env.process(fire())

    records, _ = run_low_level(None, start_continue, 2, on_unhandled_failure="continue")
    assert times(records, "done") == [0.0]


# --------------------------------------------------- callbacks and trigger (V4 to V6)


def test_c1_callbacks():
    """A callback added to an event runs when it fires, before its waiters, as a
    signal's `on:` action runs before a `wait` on it. The builder fires a second
    signal from the first's `on:`, and a waiter on the second resumes at the firing
    instant, 2."""
    env = simpy.Environment()
    order = []
    event = env.event()
    event.callbacks.append(lambda e: order.append("callback"))

    def waiter():
        yield event
        order.append("waiter")

    def fire():
        yield env.timeout(2)
        event.succeed()

    env.process(waiter())
    env.process(fire())
    env.run()
    assert order == ["callback", "waiter"]

    app = SimulationContext("C1", factor=0.0)
    app.add_signal("first", on=[{"fire": {"signal": "second"}}])
    app.add_signal("second")

    @app.task(signal="first", name="ring")
    def ring(task_id):
        return {"event": "rung"}

    @app.task(wait={"signal": "second"}, name="hears")
    def hears(task_id):
        return {"event": "heard"}

    def script(context):
        context.spawn(hears(0))
        yield context.env.timeout(2)
        context.spawn(ring(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 5)
    assert times(records, "hears") == [2.0]


def test_c1_trigger_from_another_event():
    """A `fire ... from` a task copies its finished payload onto the signal, as
    SimPy's `trigger` copies another event's value; the watcher reads it and routes
    to noted at 2."""
    env = simpy.Environment()
    source = env.event()
    target = env.event()
    source.callbacks.append(lambda e: target.trigger(source))
    seen = []

    def watcher():
        value = yield target
        seen.append((env.now, value))

    def fire():
        yield env.timeout(2)
        source.succeed({"grade": "A"})

    env.process(watcher())
    env.process(fire())
    env.run()
    assert seen == [(2, {"grade": "A"})]

    assert times(run_yaml("c1_on_fire.yaml")[0], "noted") == [2.0]


def test_c1_trigger_from_another_event_failure():
    """A `fire ... from` a failed task fails the target with the same reason, as
    SimPy's `trigger` copies a failure."""
    app = SimulationContext("C1", factor=0.0)
    app.add_signal("ready")

    @app.task(
        fail="t >= 0",
        fail_reason="scratched",
        on=[{"fire": {"signal": "ready", "from": "measure"}}],
        name="measure",
    )
    def measure(task_id):
        return {"grade": "A"}

    @app.task(wait={"signal": "ready"}, name="watcher")
    def watcher(task_id):
        return {"event": "seen"}

    def script(context):
        context.spawn(watcher(0))
        context.spawn(measure(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 5)
    # measure fails at 0, fires ready as a failure, and the watcher fails with it
    failed = [
        (r["sim_ts"], r["value"].get("reason"))
        for r in records
        if r["task"] == "watcher"
        and isinstance(r["value"], dict)
        and r["value"].get("status") == "failed"
    ]
    assert failed == [(0.0, "scratched")]


def test_c1_defused_failure():
    """A task failure that `on_fail` handles does not stop a run set to `stop`."""
    app = SimulationContext("C1", factor=0.0)

    @app.task(fail="t >= 0", fail_reason="scratched", on_fail="repair", name="work")
    def work(task_id):
        return {"event": "done"}

    @app.task(name="repair")
    def repair(task_id):
        return {"event": "repaired"}

    def script(context):
        context.spawn(work(0))
        yield context.env.event()

    app.add_process(script)
    from tests.conformance.harness import RecordingEgress

    out = RecordingEgress()
    app.add_egress(out)
    app.run(until=3, on_unhandled_failure="stop")  # no error: on_fail handles it
    assert times(out.records, "repair") == [0.0]


# ---------------------------------------------------- failure nobody handles (V7)


def test_c1_failure_nobody_handles():
    """Under `stop` a root task failing with no on_fail raises at the fail time, as
    SimPy's unhandled failure; under `continue` the flow ends and the run goes on."""

    # SimPy: an unhandled process error stops the run at 2
    def start(env):
        def work():
            yield env.timeout(2)
            raise RuntimeError("scratched")

        env.process(work())

    outcome = run_low_level(None, start, 5, raises=True)
    assert isinstance(outcome.error, RuntimeError)
    assert outcome.app_or_env.now == 2

    def build():
        app = SimulationContext("C1", factor=0.0)
        app.add_service("working", dist="constant", value=2)

        @app.task(
            service_id="working", fail="t >= 0", fail_reason="scratched", name="work"
        )
        def work(task_id):
            return {"event": "done"}

        def script(context):
            context.spawn(work(0))
            yield context.env.event()

        app.add_process(script)
        return app

    from tests.conformance.harness import RecordingEgress

    stop_app = build()
    stop_app.add_egress(RecordingEgress())
    with pytest.raises(Exception, match="scratched"):
        stop_app.run(until=5, on_unhandled_failure="stop")

    cont = build()
    out = RecordingEgress()
    cont.add_egress(out)
    cont.run(until=5, on_unhandled_failure="continue")  # reaches until
    assert _failed(out.records, "work") == [2.0]


def test_c1_failure_nobody_handles_continue():
    """Under `continue` a one-shot signal failed with nobody waiting is defused, so
    the run reaches until; under `stop` it raises `SignalFailed`."""

    def start(env):
        def go():
            scratched = Signal(env, "scratched", once=True)
            env.publish_event("s-0", {"event": "running"}, task="work", instance=0)
            scratched.fail("broken")  # nobody waits
            yield env.timeout(0)

        env.process(go())

    records, _ = run_low_level(None, start, 14, on_unhandled_failure="continue")
    assert times(records, "work") == [0.0]

    with pytest.raises(SignalFailed):
        run_low_level(None, start, 14, on_unhandled_failure="stop")

    # YAML under continue reaches until with the failed record
    records = run_yaml("c1_failure_continue.yaml")[0]
    assert _failed(records, "work") == [2.0]


def test_c1_failure_nobody_handles_losing_child_flow():
    """A losing child flow that fails after its branch lost does not stop a
    `continue` run, as SimPy leaves a defused failure alone."""
    app = SimulationContext("C1", factor=0.0)
    app.add_service("quick", dist="constant", value=1)
    app.add_service("slow", dist="constant", value=5)

    @app.task(
        wait={"any": {"fast": {"task": "fast"}, "slow": {"task": "scratch"}}},
        name="order",
    )
    def order(task_id):
        return {"event": "done"}

    @app.task(service_id="quick", name="fast")
    def fast(task_id):
        return {"event": "fast"}

    @app.task(service_id="slow", fail="t >= 0", fail_reason="scratched", name="scratch")
    def scratch(task_id):
        return {"event": "scratch"}

    def script(context):
        context.spawn(order(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 10)  # continue by default: no error
    # the order resumes when the fast branch wins at 1
    assert times(records, "order") == [1.0]


# ---------------------------------------------------------- timeout value (V11)


def test_c1_timeout_value():
    """A wait timeout carries a value the waiter reads, as SimPy's
    `env.timeout(delay, value)`, routing next at 2."""
    env = simpy.Environment()
    seen = []

    def waiter():
        value = yield env.timeout(2, value={"reason": "late"})
        seen.append((env.now, value))

    env.process(waiter())
    env.run()
    assert seen == [(2, {"reason": "late"})]

    # the low-level desflow environment inherits env.timeout(delay, value)
    def start(denv):
        def waiter():
            value = yield denv.timeout(2, value={"reason": "late"})
            denv.publish_event(
                "w-0", {"reason": value["reason"]}, task="hold", instance=0
            )

        denv.process(waiter())

    records, _ = run_low_level(None, start, 5)
    assert times(records, "hold") == [2.0]

    app = SimulationContext("C1", factor=0.0)

    @app.task(
        wait={"timeout": {"after": 2, "value": {"reason": "late"}}},
        name="hold",
        next={
            "when": [
                {"condition": 'wait.timeout.value.reason == "late"', "task": "gave_up"}
            ]
        },
    )
    def hold(task_id):
        return {"event": "held"}

    @app.task(name="gave_up")
    def gave_up(task_id):
        return {"event": "gave_up"}

    def script(context):
        context.spawn(hold(0))
        yield context.env.event()

    app.add_process(script)
    assert times(run_builder(app, 5), "gave_up") == [2.0]
    assert times(run_yaml("c1_timeout_value.yaml")[0], "gave_up") == [2.0]


# -------------------------------------------------------- counting out a drain (V4 to V6)


def _drain(app, until):
    """Runs a builder app with a drain into a recording egress."""
    from tests.conformance.harness import RecordingEgress

    out = RecordingEgress()
    app.add_egress(out)
    app.run(until=until, drain=True)
    return out.records


def test_c1_drain_waits_for_an_on_spawn():
    """A drain waits for an `on:` spawn of the last entity's final task: the spawned
    sweep ends at 7, well after the drain's until of 4."""
    app = SimulationContext("C1", factor=0.0)
    app.add_service("packing", dist="constant", value=2)
    app.add_service("sweeping", dist="constant", value=5)

    @app.task(service_id="packing", on=[{"spawn": {"task": "sweep"}}], name="pack")
    def pack(task_id):
        return {"event": "packed"}

    @app.task(service_id="sweeping", name="sweep")
    def sweep(task_id):
        return {"event": "swept"}

    def script(context):
        context.spawn(pack(0))
        yield context.env.event()

    app.add_process(script)
    records = _drain(app, 4)
    assert times(records, "sweep") == [7.0]


def test_c1_drain_waits_for_a_signals_on_actions():
    """A drain waits for the `on:` actions of a signal the last entity's final task
    fires, even though that task has no `on:` of its own: the sweep the signal spawns
    ends at 5, after the drain's until of 4."""
    app = SimulationContext("C1", factor=0.0)
    app.add_service("sweeping", dist="constant", value=5)
    app.add_signal("closing", once=True, on=[{"spawn": {"task": "sweep"}}])

    @app.task(signal="closing", name="close")
    def close(task_id):
        return {"event": "closed"}

    @app.task(service_id="sweeping", name="sweep")
    def sweep(task_id):
        return {"event": "swept"}

    def script(context):
        context.spawn(close(0))
        yield context.env.event()

    app.add_process(script)
    records = _drain(app, 4)
    assert times(records, "sweep") == [5.0]


def test_c1_drain_waits_through_a_balking_next():
    """A drain waits for a non-final task A's `on:` actions when its next task B balks
    at once: B balks, but A's spawned sweep still runs and ends at 8."""
    app = SimulationContext("C1", factor=0.0)
    app.add_resource("boxer", current_cap=1, max_cap=1)
    app.add_service("packing", dist="constant", value=2)
    app.add_service("boxing", dist="constant", value=1)
    app.add_service("sweeping", dist="constant", value=5)

    @app.task(
        service_id="packing", on=[{"spawn": {"task": "sweep"}}], next="box", name="pack"
    )
    def pack(task_id):
        return {"event": "packed"}

    @app.task(service_id="boxing", resource_id="boxer", balk="t >= 0", name="box")
    def box(task_id):
        return {"event": "boxed"}

    @app.task(service_id="sweeping", name="sweep")
    def sweep(task_id):
        return {"event": "swept"}

    def script(context):
        context.spawn(pack(0))
        yield context.env.event()

    app.add_process(script)
    records = _drain(app, 4)
    assert _failed(records, "box") == []  # box balked, it did not fail
    assert times(records, "sweep") == [7.0]


def test_c1_drain_waits_through_a_chain_of_firings():
    """A drain waits for a chain of firings: a task fires `first`, whose `on:` fires
    `second`, whose `on:` spawns a sweep that ends at 5, after the drain's until."""
    app = SimulationContext("C1", factor=0.0)
    app.add_service("sweeping", dist="constant", value=5)
    app.add_signal("first", once=True, on=[{"fire": {"signal": "second"}}])
    app.add_signal("second", once=True, on=[{"spawn": {"task": "sweep"}}])

    @app.task(signal="first", name="ring")
    def ring(task_id):
        return {"event": "rung"}

    @app.task(service_id="sweeping", name="sweep")
    def sweep(task_id):
        return {"event": "swept"}

    def script(context):
        context.spawn(ring(0))
        yield context.env.event()

    app.add_process(script)
    records = _drain(app, 4)
    assert times(records, "sweep") == [5.0]


def test_c1_process_signal_fire_is_not_counted():
    """A signal fired from a process has no entity, so its `on:` actions are not
    counted against one and a drain does not wait for them: the drain ends at its
    until of 4, before the sweep the signal spawns could end at 9."""
    app = SimulationContext("C1", factor=0.0)
    app.add_service("sweeping", dist="constant", value=5)
    app.add_signal("bell", once=True, on=[{"spawn": {"task": "sweep"}}])

    @app.task(service_id="sweeping", name="sweep")
    def sweep(task_id):
        return {"event": "swept"}

    def script(context):
        yield context.env.timeout(4)
        context.signal("bell").fire()  # fired from a process: no entity to count
        yield context.env.event()

    app.add_process(script)
    # No entity is ever in the model, so the drain ends at until; the sweep the bell
    # spawns at 4 would end at 9, but the run does not wait for it
    records = _drain(app, 4)
    assert times(records, "sweep") == []


def test_c1_on_spawn_upstream_after_each_end():
    """An `on:` spawn's upstream carries the finished payload after a finish and the
    status value after a failure, so the spawned task reads the end it followed."""
    seen = {}

    def build(fails):
        app = SimulationContext("C1", factor=0.0)
        app.add_service("packing", dist="constant", value=1)

        @app.task(
            service_id="packing",
            on=[{"spawn": {"task": "echo"}}],
            fail="t >= 0" if fails else None,
            fail_reason="scratched" if fails else None,
            name="pack",
        )
        def pack(task_id):
            return {"event": "packed"}

        @app.task(name="echo")
        def echo(task_id, upstream):
            seen["upstream"] = upstream.payload
            return {"event": "echoed"}

        def script(context):
            context.spawn(pack(0))
            yield context.env.event()

        app.add_process(script)
        return app

    run_builder(build(fails=False), 5)
    assert seen["upstream"] == {"event": "packed"}  # the finished payload
    run_builder(build(fails=True), 5)
    assert seen["upstream"]["status"] == "failed"  # the failed status after a failure
    assert seen["upstream"]["reason"] == "scratched"


def test_c1_losing_child_flow_fire_after_left_is_not_counted():
    """A losing child flow that fires a signal after its entity has left does not
    count the entity out a second time: a worker still in the model keeps the drain
    running to 5, rather than a double count ending it early at 2."""
    app = SimulationContext("C1", factor=0.0)
    app.add_service("lingering", dist="constant", value=2)
    app.add_service("working", dist="constant", value=5)
    app.add_variable("flag", 0)
    app.add_signal("done", on=[{"path": "C1.variables.flag", "value": 1}])

    @app.task(
        wait={"any": {"quick": {"timeout": 1}, "slow": {"task": "linger"}}},
        name="order",
    )
    def order(task_id):
        return {"event": "ordered"}

    @app.task(service_id="lingering", signal="done", name="linger")
    def linger(task_id):
        return {"event": "lingered"}

    @app.task(service_id="working", name="worker")
    def worker(task_id):
        return {"event": "worked"}

    def script(context):
        context.spawn(order(0))
        context.spawn(worker(0))
        yield context.env.event()

    app.add_process(script)
    # order's quick branch wins at 1 and its entity leaves; the losing linger child
    # fires done at 2 after that, which is not counted; the worker keeps the drain
    # running until it finishes at 5
    records = _drain(app, 1)
    assert times(records, "worker") == [5.0]
