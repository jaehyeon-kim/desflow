"""C2 waits for one or all: SimPy's `AnyOf` and `AllOf` as a task's `wait`, and a
`wait` with one branch as SimPy's `yield event`.

Each test runs a plain SimPy reference and the same model in the low-level API, the
builder and YAML: a wait for either of a signal and a timeout, two signals at one
instant, and a wait for all of a signal and a child flow, which succeeds and, when
the signal fails, fails at once while the child runs on.
"""

import pytest
import simpy
from simpy.events import NORMAL, URGENT

from desflow import Entity, Signal
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
    event_trace,
    periodic,
    run_builder,
    run_low_level,
    run_yaml,
    times,
)


def _loops(app, pairs):
    """An arrival loop per (arrival, task, name), each spawning the task."""
    for arrival, task, name in pairs:

        def loop(context, arrival=arrival, task=task, name=name):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)


# ------------------------------------------------------------ wait for one


def _reference_any():
    env = simpy.Environment()
    state = {"event": env.event()}
    outcome = []

    def order():
        stock, limit = state["event"], env.timeout(3)
        done = yield stock | limit
        outcome.append((env.now, "stocked" if stock in done else "gave_up"))

    def restock():
        event, state["event"] = state["event"], env.event()
        event.succeed({"qty": 3})
        yield env.timeout(0)

    env.process(periodic(env, 3, order))
    env.process(periodic(env, 5, restock))
    env.run(until=9.5)
    return outcome


def _low_level_any():
    def start(env):
        restocked = Signal(env, "restocked")
        counter = iter(range(100))

        def order():
            n = next(counter)
            stock, limit = restocked.wait(), env.timeout(3)
            done = yield stock | limit
            task = "stocked" if stock in done else "gave_up"
            env.publish_event(f"o-{n}", {"event": task}, task=task, instance=n)

        def restock():
            restocked.fire({"qty": 3})
            yield env.timeout(0)

        env.process(periodic(env, 3, order))
        env.process(periodic(env, 5, restock))

    return run_low_level(None, start, 9.5)[0]


def _builder_any():
    app = SimulationContext("C2", factor=0.0)
    app.add_signal("restocked")
    app.add_arrival("orders", dist="constant", value=3)
    app.add_arrival("restocks", dist="constant", value=5)

    @app.task(
        wait={"any": {"stock": {"signal": "restocked"}, "limit": {"timeout": 3}}},
        next={
            "when": [{"condition": "wait.stock.done", "task": "stocked"}],
            "default": "gave_up",
        },
    )
    def order(task_id):
        return {"event": "resumed"}

    @app.task()
    def stocked(task_id):
        return {"event": "stocked"}

    @app.task()
    def gave_up(task_id):
        return {"event": "gave_up"}

    @app.task(signal="restocked")
    def restock(task_id):
        return {"qty": 3}

    _loops(app, [("orders", order, "order"), ("restocks", restock, "restock")])
    return run_builder(app, 9.5)


def test_c2_wait_for_one_event():
    """The order of 3 is restocked at 5; the order of 6 gives up at 9."""
    assert _reference_any() == [(5, "stocked"), (9, "gave_up")]
    for records in (_low_level_any(), _builder_any(), run_yaml("c2_any.yaml")[0]):
        assert times(records, "stocked") == [5.0]
        assert times(records, "gave_up") == [9.0]


def _reference_any_tie():
    env = simpy.Environment()
    boxes, labels = env.event(), env.event()
    seen = []

    def order():
        yield env.timeout(3)
        done = yield boxes | labels
        seen.append((env.now, boxes in done, labels in done))

    def deliver():
        yield env.timeout(5)
        boxes.succeed("boxes")
        labels.succeed("labels")

    env.process(order())
    env.process(deliver())
    env.run(until=5.5)
    return seen


def _builder_any_tie():
    app = SimulationContext("C2", factor=0.0)
    app.add_signal("boxes").add_signal("labels")

    @app.task(
        wait={"any": {"boxes": {"signal": "boxes"}, "labels": {"signal": "labels"}}},
        next={
            "when": [
                {"condition": "wait.boxes.done and wait.labels.done", "task": "both"}
            ]
        },
    )
    def order(task_id):
        return {"event": "resumed"}

    @app.task()
    def both(task_id):
        return {"event": "both"}

    @app.task(signal="boxes")
    def deliver_boxes(task_id):
        return {"item": "boxes"}

    @app.task(signal="labels")
    def deliver_labels(task_id):
        return {"item": "labels"}

    def script(context):
        yield context.env.timeout(3)
        context.spawn(order(0))
        yield context.env.timeout(2)
        context.spawn(deliver_boxes(0))
        context.spawn(deliver_labels(0))

    app.add_process(script)
    return run_builder(app, 5.5)


def _low_level_any_tie():
    def start(env):
        boxes, labels = Signal(env, "boxes"), Signal(env, "labels")

        def order():
            yield env.timeout(3)
            first, second = boxes.wait(), labels.wait()
            done = yield first | second
            env.publish_event("o-1", {"event": "resumed"}, task="order", instance=0)
            if first in done and second in done:
                env.publish_event("o-1", {"event": "both"}, task="both", instance=0)

        def deliver():
            yield env.timeout(5)
            boxes.fire("boxes")
            labels.fire("labels")

        env.process(order())
        env.process(deliver())

    return run_low_level(None, start, 5.5)[0]


def test_c2_wait_for_one_event_two_at_one_instant():
    """Both signals fire at 5, so both branches are done when the wait ends, as an
    `AnyOf` value holds every event processed before it."""
    assert _reference_any_tie() == [(5, True, True)]
    for records in (
        _low_level_any_tie(),
        _builder_any_tie(),
        run_yaml("c2_any_tie.yaml")[0],
    ):
        assert times(records, "both") == [5.0]


# ------------------------------------------------------------ wait for all


def _reference_all(broken):
    env = simpy.Environment()
    stock = env.event()
    outcome = []

    def inspect():
        yield env.timeout(4)
        outcome.append((env.now, "inspected"))
        return {"grade": "A"}

    def order():
        yield env.timeout(10)
        try:
            yield stock & env.process(inspect())
            outcome.append((env.now, "resumed"))
        except RuntimeError as error:
            outcome.append((env.now, f"failed: {error}"))

    def restock():
        yield env.timeout(12 if broken else 15)
        if broken:
            stock.fail(RuntimeError("broken delivery"))
        else:
            stock.succeed({"qty": 3})

    env.process(order())
    env.process(restock())
    env.run(until=16)
    return outcome


def _low_level_all(broken):
    def start(env):
        restocked = Signal(env, "restocked")

        def inspect():
            yield env.timeout(4)
            env.publish_event("o-1", {"grade": "A"}, task="inspect", instance=0)
            return {"grade": "A"}

        def order():
            yield env.timeout(10)
            try:
                yield restocked.wait() & env.process(inspect())
                env.publish_event("o-1", {"event": "resumed"}, task="order", instance=0)
            except Exception as error:  # SignalFailed
                value = {"status": "failed", "reason": error.reason}
                env.publish_event("o-1", value, task="order", instance=0)

        def restock():
            yield env.timeout(12 if broken else 15)
            if broken:
                restocked.fail("broken delivery")
            else:
                restocked.fire({"qty": 3})

        env.process(order())
        env.process(restock())

    return run_low_level(None, start, 16)[0]


def _builder_all(broken):
    from desflow import Entity

    app = SimulationContext("C2", factor=0.0)
    app.add_signal("restocked")
    app.add_service("inspecting", dist="constant", value=4)

    @app.task(
        wait={"all": {"stock": {"signal": "restocked"}, "inspect": {"task": "inspect"}}}
    )
    def order(task_id):
        return {"event": "resumed"}

    @app.task(service_id="inspecting")
    def inspect(task_id):
        return {"grade": "A"}

    @app.task(
        signal="restocked", fail="attr.broken == 1", fail_reason="broken delivery"
    )
    def restock(task_id):
        return {"qty": 3}

    def script(context):
        yield context.env.timeout(10)
        context.spawn(order(0))
        yield context.env.timeout(2 if broken else 5)
        entity = Entity("restocks-1", "restocks", {"broken": 1 if broken else 0})
        context.spawn(restock(0, entity=entity))

    app.add_process(script)
    return run_builder(app, 16)


def test_c2_wait_for_all_events():
    """The inspection ends at 14 and the restock at 15: the order resumes at 15."""
    assert _reference_all(broken=False) == [(14, "inspected"), (15, "resumed")]
    for records in (
        _low_level_all(False),
        _builder_all(False),
        run_yaml("c2_all.yaml")[0],
    ):
        assert times(records, "inspect") == [14.0]
        assert times(records, "order") == [15.0]


def test_c2_wait_for_all_events_with_a_failing_branch():
    """The restock fails at 12, so the order fails at once; its inspection, still
    running, is not stopped and ends at 14, as SimPy leaves the process running."""
    assert _reference_all(broken=True) == [
        (12, "failed: broken delivery"),
        (14, "inspected"),
    ]
    for records in (
        _low_level_all(True),
        _builder_all(True),
        run_yaml("c2_all_failure.yaml")[0],
    ):
        assert times(records, "order", "failed") == [12.0]
        assert times(records, "order") == []
        assert times(records, "inspect") == [14.0]


def _reference_empty(mode):
    env = simpy.Environment()
    seen = []

    def order():
        timer = env.timeout(0)  # the patience timer, built first, as desflow builds it
        cond = env.all_of([]) if mode == "all" else env.any_of([])
        yield cond | timer
        seen.append((env.now, "went_on" if cond.processed else "reneged"))

    env.process(order())
    env.run()
    return seen


def _low_level_empty(mode):
    def start(env):
        def order():
            timer = env.timeout(0)
            cond = env.all_of([]) if mode == "all" else env.any_of([])
            yield cond | timer
            went = "went_on" if cond.processed else "reneged"
            env.publish_event("o-0", {"event": went}, task="order", instance=0)

        env.process(order())

    return run_low_level(None, start, 5)[0]


def _builder_empty(mode):
    app = SimulationContext("C2", factor=0.0)

    @app.task(wait={mode: {}}, patience=0, name="order")
    def order(task_id):
        return {"event": "went_on"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(0)

    app.add_process(script)
    return run_builder(app, 5)


def test_c2_condition_over_no_events():
    """An empty `any` or `all` succeeds at once. With patience 0 the wait wins the
    tie: desflow builds the patience timer first, so the empty condition is scheduled
    after it and is processed at resume, and the task goes on rather than reneging."""
    for mode, blueprint in (("any", "c2_empty_any.yaml"), ("all", "c2_empty_all.yaml")):
        assert _reference_empty(mode) == [(0, "went_on")]
        for records in (
            _low_level_empty(mode),
            _builder_empty(mode),
            run_yaml(blueprint)[0],
        ):
            assert times(records, "order")  # the order went on (a payload event)
            assert not times(records, "order", "reneged")


# ------------------------------------------- a wait with one branch (S1)

# A waiter and a rival each wait for an event that happens at one instant, then ask for
# the same unit, held for 1. SimPy gives the unit to the one whose event is processed
# first, so the order of the grants shows whether a single-branch wait goes on in the
# step its event is processed, as `yield event` does.
LEAVES = {
    "timeout": {"timeout": 2},
    "timeout_value": {"timeout": {"after": 2, "value": "v"}},
    "timeout_zero": {"timeout": 0},
    "signal": {"signal": "s"},
    "child": {"task": "child"},
    "running": {"running": {"task": "charge"}},
}


def _sleep(env, delay):
    yield env.timeout(delay)


def _leaf_event(env, kind, shared):
    """The event a SimPy process yields for a `wait` branch of this kind."""
    if kind == "timeout":
        return env.timeout(2)
    if kind == "timeout_value":
        return env.timeout(2, "v")
    if kind == "timeout_zero":
        return env.timeout(0)
    if kind == "signal":
        return shared["s"]
    if kind == "child":
        return env.process(_sleep(env, 2))
    return env.all_of([env.process(_sleep(env, 2))])


def _leaf_reference(kind, n, waiters_first):
    env = simpy.Environment()
    machine = simpy.Resource(env, 1)
    shared = {"s": env.event()}
    grants = []

    def user(label):
        yield _leaf_event(env, kind, shared)
        with machine.request() as req:
            yield req
            grants.append((env.now, label))
            yield env.timeout(1)

    def fire():
        yield env.timeout(2)
        shared["s"].succeed()

    env.process(fire())
    for i in range(n):
        for who in ("w", "r") if waiters_first else ("r", "w"):
            env.process(user((who, i)))
    env.run(until=40)
    return grants


def _leaf_builder(kind, n, waiters_first):
    app = SimulationContext("C2", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=1.0)
    app.add_service("work", dist="constant", value=2.0)
    app.add_signal("s", once=True)

    @app.task(
        wait=LEAVES[kind], service_id="hold", resource_id="machine", name="waiter"
    )
    def waiter(task_id):
        return {"event": "held"}

    @app.task(service_id="work", name="child")
    def child(task_id):
        return {"event": "child"}

    @app.task(service_id="work", name="charge")
    def charge(task_id):
        return {"event": "charge"}

    # A `running` branch waits for the instances the entity spawned, so its waiter
    # first spawns a charge, then waits for it
    @app.task(spawn={"task": "charge"}, next="waiter", name="launch")
    def launch(task_id):
        return {"event": "launched"}

    grants = []

    def rival(context, i, shared):
        env = context.env
        yield _leaf_event(env, kind, shared)
        with context.get_resource("machine").request() as req:
            yield req
            grants.append((env.now, ("r", i)))
            yield env.timeout(1)

    def script(context):
        env = context.env
        shared = {"s": context.signal("s").wait()}

        def fire():
            yield env.timeout(2)
            context.signal("s").fire()

        env.process(fire())
        for i in range(n):
            for who in ("w", "r") if waiters_first else ("r", "w"):
                if who == "w":
                    first = launch if kind == "running" else waiter
                    context.spawn(first(i, entity=Entity(f"w{i}", "w")))
                else:
                    env.process(rival(context, i, shared))
        yield env.event()

    app.add_process(script)
    records = run_builder(app, 40)
    started = [
        (t, ("w", int(key[1:])))
        for t, key, task, status in event_trace(records)
        if task == "waiter" and status == "started"
    ]
    return sorted(started + grants, key=lambda grant: grant[0])


@pytest.mark.parametrize("waiters_first", [True, False])
@pytest.mark.parametrize("n", [1, 3, 10])
@pytest.mark.parametrize("kind", list(LEAVES))
def test_c2_single_branch_wait_goes_on_in_the_step_its_event_is_processed(
    kind, n, waiters_first
):
    """A waiter and a rival ask for one unit when the same event is processed. The
    grants come in the order SimPy gives, for a timeout with and without a value, a
    timeout of 0, a signal, a child flow and a `running` branch."""
    expected = _leaf_reference(kind, n, waiters_first)
    assert len(expected) == 2 * n
    assert _leaf_builder(kind, n, waiters_first) == expected


# ------------------- a signal scheduled with a delay or a priority (SP-E5, SP-E6)


def _scheduled_reference(urgent, after):
    """A process waits for a one-shot event that `env.schedule` arms at 5; a rival
    asks for the same unit at the time the event is processed."""
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

    def starter():
        yield env.timeout(5)
        early = env.timeout(0) if urgent else None
        ready._ok, ready._value = True, None
        env.schedule(ready, URGENT if urgent else NORMAL, after)
        yield early if urgent else env.timeout(after)
        env.process(user("rival"))

    env.process(waiter())
    env.process(starter())
    env.run(until=20)
    return grants


def _scheduled_builder(urgent, after):
    app = SimulationContext("C2", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=1.0)
    app.add_signal("ready", once=True)

    @app.task(
        wait={"signal": "ready"},
        service_id="hold",
        resource_id="machine",
        name="waiter",
    )
    def waiter(task_id):
        return {"event": "held"}

    def rival(context):
        with context.get_resource("machine").request() as req:
            yield req
            grants.append((context.env.now, "rival"))
            yield context.env.timeout(1)

    grants = []

    def script(context):
        env = context.env
        context.spawn(waiter(0, entity=Entity("waiter", "w")))
        yield env.timeout(5)
        early = env.timeout(0) if urgent else None
        context.signal("ready").fire(urgent=urgent, after=after)
        yield early if urgent else env.timeout(after)
        env.process(rival(context))
        yield env.event()

    app.add_process(script)
    records = run_builder(app, 20)
    started = [
        (t, "waiter")
        for t, _key, task, status in event_trace(records)
        if task == "waiter" and status == "started"
    ]
    return sorted(started + grants, key=lambda grant: grant[0])


@pytest.mark.parametrize(
    "urgent, after", [(False, 3), (True, 0)], ids=["delay_SP-E5", "urgent_SP-E6"]
)
def test_c2_signal_scheduled_with_a_delay_or_priority_wakes_a_waiter_in_its_step(
    urgent, after
):
    """`fire` with `after` or `urgent` arms the signal as `env.schedule` does, and a
    `wait: {signal}` waiter goes on in the step the signal is processed, so it takes
    the unit before a rival that asks for it in a later event of that instant."""
    expected = _scheduled_reference(urgent, after)
    assert [label for _t, label in expected] == ["waiter", "rival"]
    assert _scheduled_builder(urgent, after) == expected


# ------------------------------------------------ the target of a wait (SP-P5)


def test_c2_target_during_a_single_branch_wait_is_the_event_waited_on():
    """`proc.target` is the event the process yields: the timeout or the signal's
    event itself, not a condition around it."""
    env = simpy.Environment()
    ready = env.event()
    seen = {}

    def worker(event):
        yield event

    def probe(timed, signalled):
        yield env.timeout(1)
        seen["timeout"] = isinstance(timed.target, simpy.events.Timeout)
        seen["signal"] = signalled.target is ready

    timed = env.process(worker(env.timeout(2)))
    signalled = env.process(worker(ready))
    env.process(probe(timed, signalled))
    env.run(until=3)
    assert seen == {"timeout": True, "signal": True}

    app = SimulationContext("C2", factor=0.0)
    app.add_signal("ready", once=True)

    @app.task(wait={"timeout": 2}, name="timed")
    def timed_task(task_id):
        return {"event": "went_on"}

    @app.task(wait={"signal": "ready"}, name="signalled")
    def signalled_task(task_id):
        return {"event": "went_on"}

    got = {}

    def script(context):
        timed = context.spawn(timed_task(0))
        signalled = context.spawn(signalled_task(0))
        yield context.env.timeout(1)
        got["timeout"] = isinstance(timed.target, simpy.events.Timeout)
        got["signal"] = signalled.target is context.signal("ready").wait()
        yield context.env.event()

    app.add_process(script)
    run_builder(app, 3)
    assert got == seen
