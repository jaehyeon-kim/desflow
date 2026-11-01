"""C1 events: one-shot signals (V3), callbacks and triggering from another event
(V4 to V6), a failure nobody handles (V7), the timeout value (V11), and the step in
which a task's `on:` actions run (S3).

Each test runs a plain SimPy reference, then the same model in the low-level API, the
builder and YAML, and compares the event times and values. The low-level models use
SimPy's `Event`, its `callbacks`, `trigger`, `defused` and `env.timeout(delay, value)`;
the builder and YAML use one-shot signals, `on:` actions, `on_unhandled_failure` and
the timeout `value`.
"""

import pytest
import simpy
from simpy.events import NORMAL, URGENT

from desflow import Entity, Signal, SignalFailed
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
    event_trace,
    run_builder,
    run_low_level,
    run_yaml,
    run_yaml_text,
    times,
)


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
    records = run_builder(app, 5, on_unhandled_failure="continue")
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
    records = run_builder(app, 10, on_unhandled_failure="continue")  # no error
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
    run_builder(build(fails=True), 5, on_unhandled_failure="continue")
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


# ------------------------------------------- `on:` in the step the task ends (S3)

# A task's `on:` actions run in the step the task ends, before its next task starts,
# as a statement at the end of a SimPy process's step does. Each test runs a plain
# SimPy process that does the work in that statement, then the same model as tasks.
# Two arrivals set the order of the entities at one instant, in both orders.

ONE = "services: {one: {dist: constant, value: 1}, three: {dist: constant, value: 3}}"


def _arrivals(names, n, order):
    """The arrivals block and the replay files for `names` in the creation order
    `order` ("ab" or "ba"), `n` entities of each at time 0. Each arrival starts its
    entities in one step, so they start in the order a, a, b, b."""
    names = names if order == "ab" else names[::-1]
    block = "".join(
        f"  {name}: {{replay: {{path: {name}.csv}}, spawn: {name}_start}}\n"
        for name in names
    )
    return "arrivals:\n" + block, {name: [0] * n for name in names}


def _spawn_all(context, n, entries):
    """A builder process that starts `n` entities of each `(letter, task function)`
    in `entries`, all of one letter before the next, as the YAML arrivals do."""
    for letter, task in entries:
        for i in range(n):
            context.spawn(task(i, entity=Entity(f"{letter}-{i}", letter, {})))
    yield context.env.event()


def _flag_reference(n, order):
    env = simpy.Environment()
    state = {"flag": False}
    seen = []

    def setter():
        yield env.timeout(1)
        state["flag"] = True

    def reader():
        yield env.timeout(1)
        seen.append("seen" if state["flag"] else "unseen")

    for letter in order:
        for _ in range(n):
            env.process(setter() if letter == "a" else reader())
    env.run(until=10)
    return seen


def _routes(records):
    return [
        r["task"]
        for r in records
        if r["stream_type"] == "event" and r["task"] in ("seen", "unseen")
    ]


FLAG_NEXT = (
    "next: {when: [{condition: 'registry[\"C1.variables.flag\"] == 1', task: seen}],"
    " default: unseen}"
)


@pytest.mark.parametrize("order", ["ab", "ba"])
@pytest.mark.parametrize("n", [1, 3])
def test_c1_on_sets_a_flag_the_next_task_of_another_entity_reads_in_that_step(
    tmp_path, n, order
):
    """An entity sets a flag in `on:` at 1, and another entity reads it at 1 in the
    task after its service. The reader sees the flag when the setter was created
    first, as a SimPy statement at the end of the step shows it."""
    expected = _flag_reference(n, order)
    assert expected[0] == ("seen" if order == "ab" else "unseen")
    arrivals, files = _arrivals(["a", "b"], n, order)
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "variables: {flag: 0}\n"
        f"{ONE}\n"
        f"{arrivals}"
        "tasks:\n"
        "  a_start: {service: one, payload: {e: a}, "
        "on: [{path: C1.variables.flag, value: 1}]}\n"
        f"  b_start: {{service: one, payload: {{e: b}}, {FLAG_NEXT}}}\n"
        "  seen: {payload: {e: seen}}\n"
        "  unseen: {payload: {e: unseen}}\n"
        "run: {until: 10}\n"
    )
    outcome = run_yaml_text(tmp_path, text, files)
    assert outcome.error is None
    assert _routes(outcome.records) == expected

    app = SimulationContext("C1", factor=0.0)
    app.add_variable("flag", 0)
    app.add_service("one", dist="constant", value=1)

    @app.task(
        service_id="one", on=[{"path": "C1.variables.flag", "value": 1}], name="a_start"
    )
    def a_start(task_id):
        return {"e": "a"}

    @app.task(
        service_id="one",
        next={
            "when": [
                {"condition": 'registry["C1.variables.flag"] == 1', "task": "seen"}
            ],
            "default": "unseen",
        },
        name="b_start",
    )
    def b_start(task_id):
        return {"e": "b"}

    @app.task(name="seen")
    def seen(task_id):
        return {"e": "seen"}

    @app.task(name="unseen")
    def unseen(task_id):
        return {"e": "unseen"}

    app.add_process(
        lambda context: _spawn_all(
            context, n, [(x, {"a": a_start, "b": b_start}[x]) for x in order]
        )
    )
    assert _routes(run_builder(app, 10)) == expected


def _use_reference(n, order, spawner, user):
    """`spawner` entities spawn a child that uses a unit; `user` entities use it
    themselves, both after one second; the unit is held for 3."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    grants = []

    def hold(label):
        with unit.request() as req:
            yield req
            grants.append((env.now, label))
            yield env.timeout(3)

    def spawns():
        yield env.timeout(1)
        env.process(hold("cuse"))

    def uses():
        yield env.timeout(1)
        yield from hold("use")

    for letter in order:
        for _ in range(n):
            env.process(spawns() if letter == spawner else uses())
    env.run(until=60)
    return grants


def _grants(records, tasks):
    return [
        (t, task)
        for t, _key, task, status in event_trace(records)
        if status == "started" and task in tasks
    ]


@pytest.mark.parametrize("order", ["ab", "ba"])
@pytest.mark.parametrize("n", [1, 3])
def test_c1_an_on_spawn_asks_for_the_unit_in_the_step_its_task_ends(tmp_path, n, order):
    """A task spawns a child in `on:` at 1 and another entity hands on to a task that
    asks for the unit at 1. The child asks before the other entity when its spawner
    was created first, as a SimPy process started in the step does."""
    expected = _use_reference(n, order, "a", "b")
    assert expected[0][1] == ("cuse" if order == "ab" else "use")
    arrivals, files = _arrivals(["a", "b"], n, order)
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "resources: {r: {current_cap: 1}}\n"
        f"{ONE}\n"
        f"{arrivals}"
        "tasks:\n"
        "  a_start: {service: one, payload: {e: a}, on: [{spawn: {task: cuse}}]}\n"
        "  cuse: {resource: r, service: three, payload: {e: left}}\n"
        "  b_start: {service: one, payload: {e: b}, next: use}\n"
        "  use: {resource: r, service: three, payload: {e: left}}\n"
        "run: {until: 60}\n"
    )
    outcome = run_yaml_text(tmp_path, text, files)
    assert outcome.error is None
    assert _grants(outcome.records, ("cuse", "use")) == expected

    app = SimulationContext("C1", factor=0.0)
    app.add_resource("r", current_cap=1, max_cap=1)
    app.add_service("one", dist="constant", value=1)
    app.add_service("three", dist="constant", value=3)

    @app.task(service_id="one", on=[{"spawn": {"task": "cuse"}}], name="a_start")
    def a_start(task_id):
        return {"e": "a"}

    @app.task(service_id="three", resource_id="r", name="cuse")
    def cuse(task_id):
        return {"e": "left"}

    @app.task(service_id="one", next="use", name="b_start")
    def b_start(task_id):
        return {"e": "b"}

    @app.task(service_id="three", resource_id="r", name="use")
    def use(task_id):
        return {"e": "left"}

    app.add_process(
        lambda context: _spawn_all(
            context, n, [(x, {"a": a_start, "b": b_start}[x]) for x in order]
        )
    )
    assert _grants(run_builder(app, 60), ("cuse", "use")) == expected


@pytest.mark.parametrize("n", [1, 3, 10])
def test_c1_two_on_spawns_then_a_running_join_wait_for_both_children(tmp_path, n):
    """A task spawns two children in `on:`, and the next task waits for both with
    `running`. The join waits for the children, as a SimPy process that starts two
    processes and then yields both does, and ends at 2."""
    env = simpy.Environment()
    joined = []

    def child(delay):
        yield env.timeout(delay)

    def parent():
        first, second = env.process(child(1)), env.process(child(2))
        yield first & second
        joined.append(env.now)

    for _ in range(n):
        env.process(parent())
    env.run(until=10)
    assert joined == [2] * n
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "services: {one: {dist: constant, value: 1}, two: {dist: constant, value: 2}}\n"
        "arrivals: {a: {replay: {path: a.csv}, spawn: split}}\n"
        "tasks:\n"
        "  split: {payload: {e: s}, "
        "on: [{spawn: {task: c1}}, {spawn: {task: c2}}], next: join}\n"
        "  join: {wait: {all: {r1: {running: {task: c1}}, r2: {running: {task: c2}}}}, "
        "payload: {e: joined}}\n"
        "  c1: {service: one, payload: {e: c1}}\n"
        "  c2: {service: two, payload: {e: c2}}\n"
        "run: {until: 10}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0] * n})
    assert outcome.error is None
    assert times(outcome.records, "join") == [float(t) for t in joined]

    app = SimulationContext("C1", factor=0.0)
    app.add_service("one", dist="constant", value=1)
    app.add_service("two", dist="constant", value=2)

    @app.task(
        on=[{"spawn": {"task": "c1"}}, {"spawn": {"task": "c2"}}],
        next="join",
        name="split",
    )
    def split(task_id):
        return {"e": "s"}

    @app.task(
        wait={
            "all": {
                "r1": {"running": {"task": "c1"}},
                "r2": {"running": {"task": "c2"}},
            }
        },
        name="join",
    )
    def join(task_id):
        return {"e": "joined"}

    @app.task(service_id="one", name="c1")
    def c1(task_id):
        return {"e": "c1"}

    @app.task(service_id="two", name="c2")
    def c2(task_id):
        return {"e": "c2"}

    app.add_process(lambda context: _spawn_all(context, n, [("a", split)]))
    assert times(run_builder(app, 10), "join") == [float(t) for t in joined]


def _fire_from_reference(n):
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    state = {"done": env.event()}
    grants = []

    def hold(label):
        with unit.request() as req:
            yield req
            grants.append((env.now, label))
            yield env.timeout(3)

    def closer():
        yield env.timeout(1)
        # A signal that re-arms: the waiting event is replaced as it is triggered
        done, state["done"] = state["done"], env.event()
        done.succeed({"v": 1})

    def waiter():
        yield state["done"]
        yield from hold("waiter")

    def rival():
        yield env.timeout(1)
        yield env.timeout(0)
        yield from hold("rival")

    for _ in range(n):
        env.process(closer())
        env.process(waiter())
        env.process(rival())
    env.run(until=100)
    return grants


FIRE_TASKS = (
    "  closer: {service: one, payload: {v: 1}, "
    "on: [{fire: {signal: done, from: closer}}]}\n"
    "  waiter: {wait: {signal: done}, resource: r, service: three, "
    "payload: {e: w}}\n"
    "  rival: {service: one, payload: {e: r}, next: use}\n"
    "  use: {wait: {timeout: 0}, resource: r, service: three, payload: {e: r}}\n"
)


@pytest.mark.parametrize("n", [1, 3, 10])
def test_c1_fire_from_the_ending_task_wakes_a_waiter_in_the_step_the_task_ends(
    tmp_path, n
):
    """A task fires a signal in `on:` with the outcome it copies from itself. A waiter
    resumes in the step after that firing, ahead of a rival that asks for the unit
    one zero-delay step after the task ends, as `done.succeed(...)` in a SimPy
    process does."""
    expected = _fire_from_reference(n)
    assert expected[0][1] == "waiter" and "rival" in [label for _t, label in expected]
    wanted = [(t, "use" if label == "rival" else "waiter") for t, label in expected]
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "resources: {r: {current_cap: 1}}\n"
        f"{ONE}\n"
        "signals: {done: {once: false}}\n"
        "arrivals:\n"
        "  closer: {replay: {path: closer.csv}, spawn: closer}\n"
        "  waiter: {replay: {path: waiter.csv}, spawn: waiter}\n"
        "  rival: {replay: {path: rival.csv}, spawn: rival}\n"
        f"tasks:\n{FIRE_TASKS}"
        "run: {until: 100}\n"
    )
    outcome = run_yaml_text(
        tmp_path, text, {"closer": [0] * n, "waiter": [0] * n, "rival": [0] * n}
    )
    assert outcome.error is None
    assert _grants(outcome.records, ("waiter", "use")) == wanted

    app = SimulationContext("C1", factor=0.0)
    app.add_resource("r", current_cap=1, max_cap=1)
    app.add_service("one", dist="constant", value=1)
    app.add_service("three", dist="constant", value=3)
    app.add_signal("done", once=False)

    @app.task(
        service_id="one",
        on=[{"fire": {"signal": "done", "from": "closer"}}],
        name="closer",
    )
    def closer(task_id):
        return {"v": 1}

    @app.task(
        wait={"signal": "done"}, resource_id="r", service_id="three", name="waiter"
    )
    def waiter(task_id):
        return {"e": "w"}

    @app.task(service_id="one", next="use", name="rival")
    def rival(task_id):
        return {"e": "r"}

    @app.task(wait={"timeout": 0}, resource_id="r", service_id="three", name="use")
    def use(task_id):
        return {"e": "r"}

    app.add_process(
        lambda context: _spawn_all(
            context, n, [("c", closer), ("w", waiter), ("r", rival)]
        )
    )
    assert _grants(run_builder(app, 100), ("waiter", "use")) == wanted


def _interrupt_reference(n):
    """`n` closers, each interrupting every victim that is still waiting."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    grants = []
    victims = []

    def hold(label):
        with unit.request() as req:
            yield req
            grants.append((env.now, label))
            yield env.timeout(3)

    def victim(state):
        try:
            yield env.timeout(10)
        except simpy.Interrupt:
            pass
        state["waiting"] = False
        yield from hold("after")

    def closer():
        yield env.timeout(1)
        for state, process in victims:
            if state["waiting"]:
                process.interrupt("closing")

    def rival():
        yield env.timeout(1)
        yield from hold("use")

    for _ in range(n):
        state = {"waiting": True}
        victims.append((state, env.process(victim(state))))
        env.process(closer())
        env.process(rival())
    env.run(until=100)
    return grants


INTERRUPT_TASKS = (
    "  long: {service: ten, on_interrupt: abandon, on_interrupt_next: after, "
    "payload: {e: long}}\n"
    "  after: {resource: r, service: three, payload: {e: after}}\n"
    "  closer: {service: one, payload: {e: c}, "
    "on: [{interrupt: {task: long, cause: closing}}]}\n"
    "  rival: {service: one, payload: {e: r}, next: use}\n"
    "  use: {resource: r, service: three, payload: {e: use}}\n"
)


@pytest.mark.parametrize("n", [1, 3, 10])
def test_c1_an_on_interrupt_reaches_its_target_in_the_step_its_task_ends(tmp_path, n):
    """A task interrupts running tasks in `on:` at 1. The interrupted tasks ask for
    the unit before a rival that hands on to a task asking at 1, as a SimPy
    `target.interrupt()` in the closing process does."""
    expected = _interrupt_reference(n)
    assert expected[0][1] == "after" and "use" in [label for _t, label in expected]
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "resources: {r: {current_cap: 1}}\n"
        "services: {one: {dist: constant, value: 1}, three: {dist: constant, value: 3},"
        " ten: {dist: constant, value: 10}}\n"
        "arrivals:\n"
        "  victim: {replay: {path: victim.csv}, spawn: long}\n"
        "  closer: {replay: {path: closer.csv}, spawn: closer}\n"
        "  rival: {replay: {path: rival.csv}, spawn: rival}\n"
        f"tasks:\n{INTERRUPT_TASKS}"
        "run: {until: 100}\n"
    )
    outcome = run_yaml_text(
        tmp_path, text, {"victim": [0] * n, "closer": [0] * n, "rival": [0] * n}
    )
    assert outcome.error is None
    assert _grants(outcome.records, ("after", "use")) == expected

    app = SimulationContext("C1", factor=0.0)
    app.add_resource("r", current_cap=1, max_cap=1)
    app.add_service("one", dist="constant", value=1)
    app.add_service("three", dist="constant", value=3)
    app.add_service("ten", dist="constant", value=10)

    @app.task(
        service_id="ten",
        on_interrupt="abandon",
        on_interrupt_next="after",
        name="long",
    )
    def long(task_id):
        return {"e": "long"}

    @app.task(resource_id="r", service_id="three", name="after")
    def after(task_id):
        return {"e": "after"}

    @app.task(
        service_id="one",
        on=[{"interrupt": {"task": "long", "cause": "closing"}}],
        name="closer",
    )
    def closer(task_id):
        return {"e": "c"}

    @app.task(service_id="one", next="use", name="rival")
    def rival(task_id):
        return {"e": "r"}

    @app.task(resource_id="r", service_id="three", name="use")
    def use(task_id):
        return {"e": "use"}

    app.add_process(
        lambda context: _spawn_all(
            context, n, [("v", long), ("c", closer), ("r", rival)]
        )
    )
    assert _grants(run_builder(app, 100), ("after", "use")) == expected


def _set_source_reference(gap, service, new_gap):
    env = simpy.Environment()
    state = {"gap": gap}
    arrivals = []

    def job():
        yield env.timeout(service)
        state["gap"] = new_gap

    def source():
        while True:
            yield env.timeout(state["gap"])
            arrivals.append(env.now)
            env.process(job())

    env.process(source())
    env.run(until=14)
    return arrivals


def test_c1_an_on_path_sets_an_arrival_gap_in_the_step_its_task_ends(tmp_path):
    """A task sets the gap of an arrival in `on:` when its service ends at 4. The
    arrival at 6 then waits the new gap, as a SimPy source that reads a shared gap
    does, so the arrivals come at 2, 4, 6, 9 and 12."""
    expected = _set_source_reference(2, 4, 3)
    assert expected == [2, 4, 6, 9, 12]
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "services: {s: {dist: constant, value: 4}}\n"
        "arrivals: {o: {dist: constant, value: 2, spawn: k}}\n"
        "tasks:\n"
        "  k: {payload: {e: 1}, next: w}\n"
        "  w: {service: s, payload: {w: 1}, "
        "on: [{path: C1.arrival.o.value, value: 3}]}\n"
        "run: {until: 14}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    assert times(outcome.records, "k") == [float(t) for t in expected]

    app = SimulationContext("C1", factor=0.0)
    app.add_arrival("o", dist="constant", value=2, spawn="k")
    app.add_service("s", dist="constant", value=4)

    @app.task(name="k", next="w")
    def k(task_id):
        return {"e": 1}

    @app.task(
        name="w",
        service_id="s",
        on=[{"path": "C1.arrival.o.value", "value": 3}],
    )
    def w(task_id):
        return {"w": 1}

    assert times(run_builder(app, 14), "k") == [float(t) for t in expected]


def _signal_reference(urgent, after):
    """A closer ends a step at 5 and schedules a one-shot event; a rival asks for
    the unit at the time the event is processed, in a step of its own."""
    env = simpy.Environment()
    machine = simpy.Resource(env, 1)
    ready = env.event()
    grants = []

    def user(label):
        with machine.request() as req:
            yield req
            grants.append((env.now, label))
            yield env.timeout(1)

    def waiter():
        yield ready
        yield from user("waiter")

    def closer():
        yield env.timeout(5)
        ready._ok, ready._value = True, None
        env.schedule(ready, URGENT if urgent else NORMAL, after)

    def rival():
        yield env.timeout(5)
        if after:
            yield env.timeout(after)
        yield from user("rival")

    env.process(waiter())
    env.process(closer())
    env.process(rival())
    env.run(until=20)
    return grants


@pytest.mark.parametrize(
    "urgent, after", [(False, 3), (True, 0)], ids=["delay_SP-E5", "urgent_SP-E6"]
)
def test_c1_an_on_fire_with_after_or_urgent_is_scheduled_in_the_step_its_task_ends(
    tmp_path, urgent, after
):
    """A task fires a one-shot signal in `on:` with `after` or `urgent` at 5. A waiter
    for the signal takes the unit before a rival that asks for it in the step the
    event is processed, as it does when a SimPy process calls `env.schedule` at the
    end of its step."""
    expected = _signal_reference(urgent, after)
    assert [label for _t, label in expected] == ["waiter", "rival"]
    fire = "urgent: true" if urgent else f"after: {after} s"
    wait = "" if urgent else f"wait: {{timeout: {after}}}, "
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "resources: {r: {current_cap: 1}}\n"
        "services: {one: {dist: constant, value: 1}}\n"
        "signals:\n"
        "  ready: {once: true}\n"
        "arrivals:\n"
        "  waiter: {replay: {path: waiter.csv}, spawn: waiter}\n"
        "  closer: {replay: {path: closer.csv}, spawn: closer}\n"
        "  rival: {replay: {path: rival.csv}, spawn: rival}\n"
        "tasks:\n"
        "  waiter: {wait: {signal: ready}, resource: r, service: one, "
        "payload: {e: w}}\n"
        "  closer: {wait: {timeout: 5}, payload: {e: c}, "
        f"on: [{{fire: {{signal: ready, {fire}}}}}]}}\n"
        "  rival: {wait: {timeout: 5}, payload: {e: r}, next: use}\n"
        f"  use: {{{wait}resource: r, service: one, payload: {{e: u}}}}\n"
        "run: {until: 20}\n"
    )
    outcome = run_yaml_text(
        tmp_path, text, {"waiter": [0], "closer": [0], "rival": [0]}
    )
    assert outcome.error is None
    assert _grants(outcome.records, ("waiter", "use")) == [
        (t, "use" if label == "rival" else "waiter") for t, label in expected
    ]


def _same_instant_reference(n):
    """Closers interrupt victims whose timeouts end at the same instant but are not
    processed yet. A closer created before its victim runs first."""
    env = simpy.Environment()
    victims = []
    outcomes = []

    def victim(state):
        try:
            yield env.timeout(1)
            outcomes.append("finished")
        except simpy.Interrupt:
            outcomes.append("interrupted")
        state["waiting"] = False

    def closer():
        yield env.timeout(1)
        for state, process in victims:
            if state["waiting"]:
                process.interrupt("closing")

    for _ in range(n):
        env.process(closer())
        state = {"waiting": True}
        victims.append((state, env.process(victim(state))))
    env.run(until=10)
    return outcomes


@pytest.mark.parametrize("n", [1, 3])
def test_c1_an_on_interrupt_reaches_a_task_that_ends_at_the_same_instant(tmp_path, n):
    """A task interrupts a running task in `on:` at 1, when that task's service also
    ends at 1. The interrupt arrives first, as `target.interrupt()` does for a
    process whose timeout is triggered but not yet processed."""
    expected = _same_instant_reference(n)
    assert expected == ["interrupted"] * n
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        f"{ONE}\n"
        "arrivals:\n"
        "  closer: {replay: {path: closer.csv}, spawn: closer}\n"
        "  victim: {replay: {path: victim.csv}, spawn: long}\n"
        "tasks:\n"
        "  closer: {service: one, payload: {e: c}, "
        "on: [{interrupt: {task: long, cause: closing}}]}\n"
        "  long: {service: one, on_interrupt: abandon, payload: {e: long}}\n"
        "run: {until: 10}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"closer": [0] * n, "victim": [0] * n})
    assert outcome.error is None
    trace = event_trace(outcome.records)
    assert [
        s for _t, _k, task, s in trace if task == "long" and s == "interrupted"
    ] == ["interrupted"] * n
    assert times(outcome.records, "long") == []


def test_c1_closing_a_task_that_is_still_serving_runs_no_on_action(tmp_path):
    """A generator closed while its task is in service is not an end of the task, so
    no `on:` action runs, as no statement after a `yield` runs in a closed SimPy
    generator."""
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "variables: {flag: 0}\n"
        "services: {long: {dist: constant, value: 100}}\n"
        "arrivals: {a: {replay: {path: a.csv}, spawn: job}}\n"
        "tasks:\n"
        "  job: {service: long, payload: {e: 1}, "
        "on: [{path: C1.variables.flag, value: 1}]}\n"
        "run: {until: 5}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0]})
    app = outcome.app_or_env
    registry = app._env.registry
    (running,) = app._running["job"]
    assert running.process.is_alive
    running.process._generator.close()
    assert registry.get("C1.variables.flag").value == 0


_END_ON = {
    "balk": "balk: 't >= 0', ",
    "renege": "patience: 2 s, ",
}


@pytest.mark.parametrize("kind", ["balk", "renege"])
def test_c1_a_task_that_balks_or_reneges_runs_its_on_actions(tmp_path, kind):
    """A task whose `on:` spawns a child runs the action in the step it balks or
    reneges, as the statement after a SimPy process's balk or renege does. The machine
    is held to 10; the visitor balks at 1, or reneges at 3 after a patience of 2."""
    env = simpy.Environment()
    machine = simpy.Resource(env, 1)
    expected = []

    def mark():
        expected.append(env.now)
        yield env.timeout(0)

    def holder():
        with machine.request() as request:
            yield request
            yield env.timeout(10)

    def visitor():
        yield env.timeout(1)
        if kind == "balk":
            env.process(mark())
            return
        request = machine.request()
        yield request | env.timeout(2)
        if not request.triggered:
            request.cancel()
            env.process(mark())

    env.process(holder())
    env.process(visitor())
    env.run(until=20)
    assert expected == [1 if kind == "balk" else 3]

    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "resources: {machine: {current_cap: 1}}\n"
        "services: {long: {dist: constant, value: 10}, one: {dist: constant, value: 1}}\n"
        "arrivals:\n"
        "  h: {replay: {path: h.csv}, spawn: hold}\n"
        "  v: {replay: {path: v.csv}, spawn: visit}\n"
        "tasks:\n"
        "  hold: {resource: machine, service: long, payload: {e: h}}\n"
        f"  visit: {{resource: machine, service: one, {_END_ON[kind]}"
        "payload: {e: v}, on: [{spawn: {task: mark}}]}\n"
        "  mark: {payload: {e: m}}\n"
        "run: {until: 20}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"h": [0], "v": [1]})
    assert outcome.error is None
    assert times(outcome.records, "mark") == expected

    app = SimulationContext("C1", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("long", dist="constant", value=10)
    app.add_service("one", dist="constant", value=1)

    @app.task(service_id="long", resource_id="machine", name="hold")
    def hold(task_id):
        return {"e": "h"}

    end = {"balk": {"balk": "t >= 0"}, "renege": {"patience": 2}}[kind]

    @app.task(
        service_id="one",
        resource_id="machine",
        on=[{"spawn": {"task": "mark"}}],
        name="visit",
        **end,
    )
    def visit(task_id):
        return {"e": "v"}

    @app.task(name="mark")
    def mark_task(task_id):
        return {"e": "m"}

    def script(context):
        context.spawn(hold(0))
        yield context.env.timeout(1)
        context.spawn(visit(0))
        yield context.env.event()

    app.add_process(script)
    assert times(run_builder(app, 20), "mark") == expected


CHILD_ERROR = {
    "losing": (
        "  parent: {wait: {any: {c: {task: kid}, t: {timeout: 1}}}, payload: {e: p}}\n"
    ),
    "waited": "  parent: {wait: {task: kid}, payload: {e: p}}\n",
}


@pytest.mark.parametrize("child", ["losing", "waited"])
def test_c1_an_on_path_that_does_not_exist_in_a_child_flow_stops_the_run(
    tmp_path, child
):
    """An `on:` action that raises in a child flow stops the run with its KeyError,
    whether the parent lost its wait for the child or waits for it, as an exception
    in a SimPy process that nothing handles stops `run()`. The stop is at the kid's
    end."""
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        "services: {one: {dist: constant, value: 1}, three: {dist: constant, value: 3}}\n"
        "arrivals: {a: {replay: {path: a.csv}, spawn: parent}}\n"
        "tasks:\n"
        f"{CHILD_ERROR[child]}"
        "  kid: {service: three, payload: {e: k}, "
        "on: [{path: C1.variables.nope, value: 1}]}\n"
        "run: {until: 10}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0]})
    assert outcome.app_or_env.env.now == 3.0
    assert isinstance(outcome.error, KeyError)
    assert "C1.variables.nope" in str(outcome.error)


def test_c1_an_on_path_that_does_not_exist_in_a_root_task_stops_the_run(tmp_path):
    """The same action in a root task stops the run with the KeyError too."""
    text = (
        "simulation: {sim_id: C1, factor: 0}\n"
        f"{ONE}\n"
        "arrivals: {a: {replay: {path: a.csv}, spawn: job}}\n"
        "tasks:\n"
        "  job: {service: one, payload: {e: 1}, "
        "on: [{path: C1.variables.nope, value: 1}]}\n"
        "run: {until: 10}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0]})
    assert isinstance(outcome.error, KeyError)
    assert "C1.variables.nope" in str(outcome.error)


NEXT_FLAG = """
simulation: {sim_id: C1, factor: 0}
variables: {flag: 0}
services: {one: {dist: constant, value: 1}}
arrivals: {a: {replay: {path: a.csv}, spawn: first}}
tasks:
  first: {service: one, payload: {e: 1}, on: [{path: C1.variables.flag, value: 1}], %s
  ok: {payload: {e: ok}}
  nope: {payload: {e: nope}}
  second: {payload: {e: 2}, %s
run: {until: 5}
"""
FLAG_ROUTE = (
    "next: {when: [{condition: 'registry[\"C1.variables.flag\"] == 1', task: ok}], "
    "default: nope}}"
)


def test_c1_the_next_task_of_the_same_entity_reads_the_flag_its_on_set(tmp_path):
    """A task sets a flag in `on:` and hands on to a second task, which routes on the
    flag. The second task reads it set, as the next statement of a SimPy process
    does."""
    state = {"flag": False}
    env = simpy.Environment()
    routes = []

    def process():
        yield env.timeout(1)
        state["flag"] = True
        routes.append("ok" if state["flag"] else "nope")

    env.process(process())
    env.run(until=5)
    assert routes == ["ok"]
    text = NEXT_FLAG % ("next: second}", FLAG_ROUTE)
    outcome = run_yaml_text(tmp_path, text, {"a": [0]})
    assert outcome.error is None
    assert [
        t for _t, _k, t, s in event_trace(outcome.records) if t in ("ok", "nope")
    ] == ["ok"]


def test_c1_the_next_condition_of_the_task_with_the_on_reads_the_old_value(tmp_path):
    """A `next` condition on the task that has the `on:` is evaluated when the next
    task is chosen, before the actions run, so it reads the old value. The next
    task reads the new one (the test above)."""
    text = NEXT_FLAG % (FLAG_ROUTE, "next: ok}")
    outcome = run_yaml_text(tmp_path, text, {"a": [0]})
    assert outcome.error is None
    assert [
        t for _t, _k, t, s in event_trace(outcome.records) if t in ("ok", "nope")
    ] == ["nope"]


# Running the actions one step later, as before 0.29.0: the task fires a signal in its
# own `on:`, and the signal's `on:` holds the actions. The `signal:` key is not the
# same, because it fires only when the task finishes or fails.
LATER_ORDERS = {
    "flag": ["unseen"],
    "spawn": [(1.0, "use"), (4.0, "use"), (7.0, "cuse"), (10.0, "cuse")],
    "interrupt": [(1.0, "use"), (4.0, "after")],
}


def _later(tmp_path, kind, actions):
    """The YAML of a model of `kind` whose a_start/closer task runs `actions` in a
    signal's `on:` that the task fires."""
    signal = "signals:\n  later: {once: false, on: " + actions + "}\n"
    fire = "on: [{fire: {signal: later}}]"
    if kind == "flag":
        text = (
            "simulation: {sim_id: C1, factor: 0}\nvariables: {flag: 0}\n"
            f"{ONE}\n{signal}"
            "arrivals:\n"
            "  a: {replay: {path: a.csv}, spawn: a_start}\n"
            "  b: {replay: {path: b.csv}, spawn: b_start}\n"
            "tasks:\n"
            f"  a_start: {{service: one, payload: {{e: a}}, {fire}}}\n"
            f"  b_start: {{service: one, payload: {{e: b}}, {FLAG_NEXT}}}\n"
            "  seen: {payload: {e: seen}}\n  unseen: {payload: {e: unseen}}\n"
            "run: {until: 10}\n"
        )
        return run_yaml_text(tmp_path, text, {"a": [0], "b": [0]})
    if kind == "spawn":
        text = (
            "simulation: {sim_id: C1, factor: 0}\nresources: {r: {current_cap: 1}}\n"
            f"{ONE}\n{signal}"
            "arrivals:\n"
            "  a: {replay: {path: a.csv}, spawn: a_start}\n"
            "  b: {replay: {path: b.csv}, spawn: b_start}\n"
            "tasks:\n"
            f"  a_start: {{service: one, payload: {{e: a}}, {fire}}}\n"
            "  cuse: {resource: r, service: three, payload: {e: left}}\n"
            "  b_start: {service: one, payload: {e: b}, next: use}\n"
            "  use: {resource: r, service: three, payload: {e: left}}\n"
            "run: {until: 60}\n"
        )
        return run_yaml_text(tmp_path, text, {"a": [0] * 2, "b": [0] * 2})
    text = (
        "simulation: {sim_id: C1, factor: 0}\nresources: {r: {current_cap: 1}}\n"
        "services: {one: {dist: constant, value: 1}, three: {dist: constant, value: 3},"
        " ten: {dist: constant, value: 10}}\n"
        f"{signal}"
        "arrivals:\n"
        "  victim: {replay: {path: victim.csv}, spawn: long}\n"
        "  closer: {replay: {path: closer.csv}, spawn: closer}\n"
        "  rival: {replay: {path: rival.csv}, spawn: rival}\n"
        f"tasks:\n{INTERRUPT_TASKS.replace('on: [{interrupt: {task: long, cause: closing}}]', fire)}"
        "run: {until: 100}\n"
    )
    return run_yaml_text(tmp_path, text, {"victim": [0], "closer": [0], "rival": [0]})


@pytest.mark.parametrize("kind", ["flag", "spawn", "interrupt"])
def test_c1_an_on_fire_to_a_signals_on_runs_the_actions_one_step_later(tmp_path, kind):
    """The way to keep the order before 0.29.0: the task's `on:` fires a signal, and
    the actions are in the signal's `on:`, which runs as a callback of the firing."""
    actions = {
        "flag": "[{path: C1.variables.flag, value: 1}]",
        "spawn": "[{spawn: {task: cuse}}]",
        "interrupt": "[{interrupt: {task: long, cause: closing}}]",
    }[kind]
    outcome = _later(tmp_path, kind, actions)
    assert outcome.error is None
    if kind == "flag":
        assert _routes(outcome.records) == LATER_ORDERS[kind]
    elif kind == "spawn":
        got = _grants(outcome.records, ("cuse", "use"))
        assert [(t, task) for t, task in got][:4] == LATER_ORDERS[kind]
    else:
        assert _grants(outcome.records, ("after", "use")) == LATER_ORDERS[kind]
