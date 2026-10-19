"""C1 process interaction: signals, child flows, return values, failures,
inspection and waits on a shared condition.

Each test runs a plain SimPy reference, then the same model in the low-level API,
the builder and YAML, and compares when each waiter resumed and with what. The
low-level models use SimPy itself, with desflow's `Signal` for a named event; the
builder and YAML use a task's `wait`, `signal`, `fail` and `on_fail`.
"""

import simpy

from desflow import (
    ContainerConfig,
    DynamicContainer,
    Signal,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
    run_builder,
    run_low_level,
    run_yaml,
)


def _events(records, task, status=None):
    """(time, value) of every event of `task`, finished ones when status is None."""
    found = []
    for r in records:
        if r["stream_type"] != "event" or r["task"] != task:
            continue
        value = r["value"]
        is_status = isinstance(value, dict) and "status" in value
        if (
            status is None
            and not is_status
            or status is not None
            and is_status
            and value["status"] == status
        ):
            found.append((r["sim_ts"], value))
    return found


def _times(records, task, status=None):
    return [t for t, _value in _events(records, task, status)]


def _periodic(env, gap, start):
    """Starts `start()` as a process every `gap` seconds, from `gap`."""
    while True:
        yield env.timeout(gap)
        env.process(start())


# ---------------------------------------------------------------- signals

SIGNAL_RESUMED = [5.0, 10.0, 10.0]


def _reference_signals():
    env = simpy.Environment()
    state = {"event": env.event()}
    resumed = []

    def waiter():
        value = yield state["event"]
        resumed.append((env.now, value["qty"]))

    def restock():
        event, state["event"] = state["event"], env.event()
        event.succeed({"qty": 3})
        yield env.timeout(0)

    env.process(_periodic(env, 3, waiter))
    env.process(_periodic(env, 5, restock))
    env.run(until=11)
    return resumed


def _low_level_signals():
    def start(env):
        restocked = Signal(env, "restocked")
        counter = iter(range(100))

        def waiter():
            n = next(counter)
            value = yield restocked.wait()
            env.publish_event(f"w-{n}", {"event": "resumed"}, task="waiter", instance=n)
            if value["qty"] == 3:
                env.publish_event(
                    f"w-{n}", {"event": "noted"}, task="noted", instance=n
                )

        def restock():
            restocked.fire({"qty": 3})
            yield env.timeout(0)

        env.process(_periodic(env, 3, waiter))
        env.process(_periodic(env, 5, restock))

    return run_low_level(None, start, 11)[0]


def _builder_signals():
    app = SimulationContext("C1", factor=0.0)
    app.add_signal("restocked")
    app.add_arrival("waiters", dist="constant", value=3)
    app.add_arrival("restocks", dist="constant", value=5)

    @app.task(
        wait={"signal": "restocked"},
        next={
            "when": [{"condition": "wait.restocked.value.qty == 3", "task": "noted"}]
        },
    )
    def waiter(task_id):
        return {"event": "resumed"}

    @app.task()
    def noted(task_id):
        return {"event": "noted"}

    @app.task(signal="restocked")
    def restock(task_id):
        return {"qty": 3}

    for arrival, task, name in (
        ("waiters", waiter, "waiter"),
        ("restocks", restock, "restock"),
    ):

        def loop(context, arrival=arrival, task=task, name=name):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)
    return run_builder(app, 11)


def test_c1_signal_event_success_and_value():
    """Every waiter of a firing resumes with its value: the waiter of 3 at 5, and
    the waiters of 6 and 9, which start after that firing, at the next, at 10."""
    assert _reference_signals() == [(5, 3), (10, 3), (10, 3)]
    for records in (
        _low_level_signals(),
        _builder_signals(),
        run_yaml("c1_signals.yaml")[0],
    ):
        assert _times(records, "waiter") == SIGNAL_RESUMED
        assert _times(records, "noted") == SIGNAL_RESUMED  # the value was {qty: 3}


def _reference_signal_failure():
    env = simpy.Environment()
    state = {"event": env.event()}
    failed = []

    def waiter():
        try:
            yield state["event"]
        except RuntimeError as error:
            failed.append((env.now, str(error)))

    def restock():
        yield env.timeout(5)
        event, state["event"] = state["event"], env.event()
        event.fail(RuntimeError("broken delivery"))

    def start():
        yield env.timeout(3)
        env.process(waiter())

    env.process(start())
    env.process(restock())
    env.run(until=6)
    return failed


def _low_level_signal_failure():
    def start(env):
        restocked = Signal(env, "restocked")

        def waiter():
            yield env.timeout(3)
            try:
                yield restocked.wait()
            except Exception as error:  # SignalFailed
                env.publish_event(
                    "w-0",
                    {"status": "failed", "reason": error.reason},
                    task="waiter",
                    instance=0,
                )

        def restock():
            yield env.timeout(5)
            restocked.fail("broken delivery")

        env.process(waiter())
        env.process(restock())

    return run_low_level(None, start, 6)[0]


def _builder_signal_failure():
    app = SimulationContext("C1", factor=0.0)
    app.add_signal("restocked")

    @app.task(wait={"signal": "restocked"})
    def waiter(task_id):
        return {"event": "resumed"}

    @app.task(signal="restocked", fail="t >= 0", fail_reason="broken delivery")
    def restock(task_id):
        return {"qty": 3}

    def script(context):
        yield context.env.timeout(3)
        context.spawn(waiter(0))
        yield context.env.timeout(2)
        context.spawn(restock(0))

    app.add_process(script)
    return run_builder(app, 6)


def test_c1_signal_event_failure():
    """A failed signal reaches its waiter as a failure with the reason, at 5."""
    assert _reference_signal_failure() == [(5, "broken delivery")]
    for records in (
        _low_level_signal_failure(),
        _builder_signal_failure(),
        run_yaml("c1_signal_failure.yaml")[0],
    ):
        failed = _events(records, "waiter", "failed")
        assert [(t, v["reason"]) for t, v in failed] == [(5.0, "broken delivery")]


# ------------------------------------------------------------- child flows


def _reference_child(fails=False, handled=False):
    """The parent's outcome: when it resumed, and the child's value or failure."""
    env = simpy.Environment()
    outcome = []

    def child():
        yield env.timeout(4)
        if fails:
            if handled:
                return {"event": "repaired"}
            raise RuntimeError("scratched")
        return {"grade": "A"}

    def parent():
        yield env.timeout(10)
        try:
            value = yield env.process(child())
            outcome.append((env.now, "resumed", value))
        except RuntimeError as error:
            outcome.append((env.now, "failed", str(error)))

    env.process(parent())
    env.run(until=15)
    return outcome


def _low_level_child(fails=False, handled=False):
    def start(env):
        def child():
            yield env.timeout(4)
            if fails:
                if handled:
                    return {"event": "repaired"}
                raise RuntimeError("scratched")
            return {"grade": "A"}

        def parent():
            yield env.timeout(10)
            try:
                value = yield env.process(child())
                env.publish_event(
                    "o-1", {"event": "inspected"}, task="order", instance=0
                )
                if value.get("grade") == "A":
                    env.publish_event(
                        "o-1", {"event": "shipped"}, task="ship", instance=0
                    )
            except RuntimeError as error:
                env.publish_event(
                    "o-1",
                    {"status": "failed", "reason": str(error)},
                    task="order",
                    instance=0,
                )

        env.process(parent())

    return run_low_level(None, start, 15)[0]


def _builder_child(fails=False, handled=False):
    app = SimulationContext("C1", factor=0.0)
    app.add_service("inspecting", dist="constant", value=4)
    app.add_arrival("orders", dist="constant", value=10)

    @app.task(
        wait={"task": "inspect"},
        next={
            "when": [{"condition": 'wait.inspect.value.grade == "A"', "task": "ship"}],
            "default": "rework",
        }
        if not fails
        else None,
    )
    def order(task_id):
        return {"event": "inspected"}

    failing = dict(fail="t >= 0", fail_reason="scratched") if fails else {}
    if handled:
        failing["on_fail"] = "repair"

    @app.task(service_id="inspecting", next=None if fails else "label", **failing)
    def inspect(task_id):
        return {"grade": "B"}

    @app.task()
    def label(task_id):
        return {"grade": "A"}

    @app.task()
    def ship(task_id):
        return {"event": "shipped"}

    @app.task()
    def rework(task_id):
        return {"event": "reworked"}

    @app.task()
    def repair(task_id):
        return {"event": "repaired"}

    @app.arrival_loop("orders")
    def orders(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(order(context.next_instance("order"), entity=entity))

    return run_builder(app, 15)


def test_c1_start_and_wait_for_a_process():
    """The order waits from 10 for its child flow, which ends at 14."""
    assert [t for t, *_ in _reference_child()] == [14]
    for records in (_low_level_child(), _builder_child(), run_yaml("c1_child.yaml")[0]):
        assert _times(records, "order") == [14.0]


def test_c1_process_return_value():
    """The child flow's value is its last payload, {grade: A}, so the order ships."""
    assert _reference_child() == [(14, "resumed", {"grade": "A"})]
    for records in (_low_level_child(), _builder_child(), run_yaml("c1_child.yaml")[0]):
        assert _times(records, "ship") == [14.0]
        assert _times(records, "rework") == []


def test_c1_process_failure():
    """The child fails at 14 and nothing handles it, so the order fails with its
    reason."""
    assert _reference_child(fails=True) == [(14, "failed", "scratched")]
    for records in (
        _low_level_child(fails=True),
        _builder_child(fails=True),
        run_yaml("c1_child_failure.yaml")[0],
    ):
        failed = _events(records, "order", "failed")
        assert [(t, v["reason"]) for t, v in failed] == [(14.0, "scratched")]


def test_c1_process_failure_handled_by_on_fail():
    """The child fails at 14, on_fail sends it to repair, and the order resumes
    with repair's payload."""
    assert _reference_child(fails=True, handled=True) == [
        (14, "resumed", {"event": "repaired"})
    ]
    for records in (
        _builder_child(fails=True, handled=True),
        run_yaml("c1_child_on_fail.yaml")[0],
    ):
        assert _times(records, "inspect", "failed") == [14.0]
        assert _times(records, "repair") == [14.0]
        assert _times(records, "order") == [14.0]


# -------------------------------------------------------------- inspection


def test_c1_event_inspection():
    """A signal's pending event is untriggered before it fires; after, it is
    triggered, processed, ok and holds the value, and the signal counts it."""
    env = simpy.Environment()
    reference = env.event()
    before = (reference.triggered, reference.processed)
    reference.succeed({"qty": 3})
    env.run()
    after = (reference.triggered, reference.processed, reference.ok, reference.value)
    assert before == (False, False)
    assert after == (True, True, True, {"qty": 3})

    seen = {}

    def start(env):
        restocked = Signal(env, "restocked")
        event = restocked.wait()

        def inspect():
            seen["before"] = (event.triggered, event.processed, restocked.count)
            restocked.fire({"qty": 3})
            yield env.timeout(1)
            seen["after"] = (
                event.triggered,
                event.processed,
                event.ok,
                event.value,
                restocked.count,
                restocked.ok,
                restocked.value,
            )

        env.process(inspect())

    run_low_level(None, start, 2)
    assert seen["before"] == (False, False, 0)
    assert seen["after"] == (True, True, True, {"qty": 3}, 1, True, {"qty": 3})

    # The builder reaches the same signal through the context, and a task reads it
    # as signal.restocked.count, .ok and .value
    app = SimulationContext("C1", factor=0.0)
    app.add_signal("restocked")

    @app.task(signal="restocked")
    def restock(task_id):
        return {"qty": 3}

    @app.task(
        next={
            "when": [
                {
                    "condition": "signal.restocked.count == 1 and signal.restocked.ok "
                    "and signal.restocked.value.qty == 3",
                    "task": "noted",
                }
            ]
        }
    )
    def check(task_id):
        return {"event": "checked"}

    @app.task()
    def noted(task_id):
        return {"event": "noted"}

    def script(context):
        event = context.signal("restocked").wait()
        seen["builder before"] = event.triggered
        context.spawn(restock(0))
        yield context.env.timeout(1)
        seen["builder after"] = (event.processed, event.value)
        context.spawn(check(0))

    app.add_process(script)
    records = run_builder(app, 3)
    assert seen["builder before"] is False
    assert seen["builder after"] == (True, {"qty": 3})
    assert _times(records, "noted") == [1.0]

    # YAML reads the same signal as signal.restocked.count, .ok and .value
    assert _times(run_yaml("c1_event_inspection.yaml")[0], "noted") == [1.0]


def test_c1_process_inspection():
    """A child flow's process is alive until it ends, then holds its value."""
    env = simpy.Environment()

    def child():
        yield env.timeout(4)
        return {"grade": "A"}

    proc = env.process(child())
    env.run(until=1)
    reference = (proc.is_alive, proc.triggered)
    env.run(until=5)
    assert reference == (True, False)
    assert (proc.is_alive, proc.ok, proc.value) == (False, True, {"grade": "A"})

    low = {}

    def start(env):
        def child():
            yield env.timeout(4)
            return {"grade": "A"}

        def watch():
            process = env.process(child())
            yield env.timeout(1)
            low["during"] = (process.is_alive, process.triggered)
            yield process
            low["after"] = (process.is_alive, process.ok, process.value)

        env.process(watch())

    run_low_level(None, start, 6)
    assert low == {"during": reference, "after": (False, True, {"grade": "A"})}

    app = SimulationContext("C1", factor=0.0)
    app.add_service("inspecting", dist="constant", value=4)
    seen = {}

    @app.task(service_id="inspecting")
    def inspect(task_id):
        return {"grade": "A"}

    def script(context):
        flow = context.call("inspect")
        spawned = context.spawn(inspect(99))
        yield context.env.timeout(1)
        seen["during"] = (flow.is_alive, flow.triggered, spawned.is_alive)
        yield flow
        seen["after"] = (flow.is_alive, flow.ok, flow.value, spawned.is_alive)

    app.add_process(script)
    run_builder(app, 6)
    assert seen["during"] == (True, False, True)
    # A spawned task returns nothing; a called one returns its flow's last payload
    assert seen["after"] == (False, True, {"grade": "A"}, False)

    # YAML reads a child's state after the wait as wait.inspect.done and .value
    records = run_yaml("c1_child.yaml")[0]
    assert _times(records, "ship") == [14.0]


# -------------------------------------------------------- shared condition


def _reference_condition():
    env = simpy.Environment()
    stock = simpy.Container(env, capacity=20, init=0)
    state = {"changed": env.event()}
    resumed = []

    def waiter():
        while stock.level < 5:
            yield state["changed"]
        resumed.append(env.now)

    def restock():
        yield stock.put(2)
        event, state["changed"] = state["changed"], env.event()
        event.succeed()

    env.process(_periodic(env, 4, waiter))
    env.process(_periodic(env, 3, restock))
    env.run(until=10)
    return resumed


def _low_level_condition():
    def start(env):
        stock = DynamicContainer(env, "C1", "stock")
        counter = iter(range(100))

        def waiter():
            n = next(counter)
            resumed = env.event()

            def check():
                if not resumed.triggered and stock.level >= 5:
                    resumed.succeed()
                    env.unwatch_changes(check)

            check()
            if not resumed.triggered:
                env.watch_changes(check)
            yield resumed
            env.publish_event(f"w-{n}", {"event": "resumed"}, task="waiter", instance=n)

        def restock():
            yield stock.put(2)

        env.process(_periodic(env, 4, waiter))
        env.process(_periodic(env, 3, restock))

    params = SimParameter(
        sim_id="C1",
        containers={"stock": ContainerConfig(current_cap=20, max_cap=20, level=0)},
    )
    return run_low_level(params, start, 10)[0]


def _builder_condition():
    app = SimulationContext("C1", factor=0.0)
    app.add_container("stock", current_cap=20, max_cap=20)
    app.add_arrival("waiters", dist="constant", value=4)
    app.add_arrival("restocks", dist="constant", value=3)

    @app.task(wait={"condition": "level.stock >= 5"})
    def waiter(task_id):
        return {"event": "resumed"}

    @app.task(put={"container": "stock", "amount": 2})
    def restock(task_id):
        return {"event": "restocked"}

    for arrival, task, name in (
        ("waiters", waiter, "waiter"),
        ("restocks", restock, "restock"),
    ):

        def loop(context, arrival=arrival, task=task, name=name):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)
    return run_builder(app, 10)


def test_c1_wait_for_a_shared_condition():
    """Waiters from 4 and 8 wait until the stock holds 5; the restock of 9 brings
    it to 6, and both resume at 9."""
    assert _reference_condition() == [9, 9]
    for records in (
        _low_level_condition(),
        _builder_condition(),
        run_yaml("c1_condition.yaml")[0],
    ):
        assert _times(records, "waiter") == [9.0, 9.0]
