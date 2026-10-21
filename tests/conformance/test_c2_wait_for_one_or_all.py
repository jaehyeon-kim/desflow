"""C2 waits for one or all: SimPy's `AnyOf` and `AllOf` as a task's `wait`.

Each test runs a plain SimPy reference and the same model in the low-level API, the
builder and YAML: a wait for either of a signal and a timeout, two signals at one
instant, and a wait for all of a signal and a child flow, which succeeds and, when
the signal fails, fails at once while the child runs on.
"""

import simpy

from desflow import Signal
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
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
