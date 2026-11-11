"""C1 process interaction: signals, child flows, return values, failures,
inspection and waits on a shared condition.

Each test runs a plain SimPy reference, then the same model in the low-level API,
the builder and YAML, and compares when each waiter resumed and with what. The
low-level models use SimPy itself, with desflow's `Signal` for a named event; the
builder and YAML use a task's `wait`, `signal`, `fail` and `on_fail`.
"""

import gc

import pytest
import simpy
import simpy.util

from desflow import (
    ContainerConfig,
    DynamicContainer,
    Entity,
    Signal,
    SimParameter,
)
from desflow.builder.context import SimulationContext
from desflow.builder.task import TaskRun
from tests.conformance.harness import (
    event_trace,
    periodic,
    run_builder,
    run_low_level,
    run_yaml,
    run_yaml_text,
    times,
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

    env.process(periodic(env, 3, waiter))
    env.process(periodic(env, 5, restock))
    env.run(until=11)
    return resumed


def _low_level_signals():
    def start(env):
        restocked = Signal(env, "restocked", once=False)
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

        env.process(periodic(env, 3, waiter))
        env.process(periodic(env, 5, restock))

    return run_low_level(None, start, 11)[0]


def _builder_signals():
    app = SimulationContext("C1", factor=0.0)
    app.add_signal("restocked", once=False)
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
        assert times(records, "waiter") == SIGNAL_RESUMED
        assert times(records, "noted") == SIGNAL_RESUMED  # the value was {qty: 3}


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
    return run_builder(app, 6, on_unhandled_failure="continue")


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

    return run_builder(app, 15, on_unhandled_failure="continue")


def test_c1_start_and_wait_for_a_process():
    """The order waits from 10 for its child flow, which ends at 14."""
    assert [t for t, *_ in _reference_child()] == [14]
    for records in (_low_level_child(), _builder_child(), run_yaml("c1_child.yaml")[0]):
        assert times(records, "order") == [14.0]


def test_c1_process_return_value():
    """The child flow's value is its last payload, {grade: A}, so the order ships."""
    assert _reference_child() == [(14, "resumed", {"grade": "A"})]
    for records in (_low_level_child(), _builder_child(), run_yaml("c1_child.yaml")[0]):
        assert times(records, "ship") == [14.0]
        assert times(records, "rework") == []


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
        assert times(records, "inspect", "failed") == [14.0]
        assert times(records, "repair") == [14.0]
        assert times(records, "order") == [14.0]


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
    assert times(records, "noted") == [1.0]

    # YAML reads the same signal as signal.restocked.count, .ok and .value
    assert times(run_yaml("c1_event_inspection.yaml")[0], "noted") == [1.0]


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
    # A task's process ends with its finished payload; a called flow returns its
    # last payload
    assert seen["after"] == (False, True, {"grade": "A"}, False)

    # YAML reads a child's state after the wait as wait.inspect.done and .value
    records = run_yaml("c1_child.yaml")[0]
    assert times(records, "ship") == [14.0]


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

    env.process(periodic(env, 4, waiter))
    env.process(periodic(env, 3, restock))
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

        env.process(periodic(env, 4, waiter))
        env.process(periodic(env, 3, restock))

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
        assert times(records, "waiter") == [9.0, 9.0]


# -------------------------------------------------- active process (E3, P5, P6)


def test_c1_active_process():
    """`active_process` is the running process during a resume, named after the task,
    and None between events, as SimPy's `active_process`."""
    env = simpy.Environment()
    names = []

    def worker():
        names.append(env.active_process.name)
        yield env.timeout(1)

    env.process(worker())
    assert env.active_process is None  # None outside a step
    env.run()
    assert names == ["worker"]

    # a desflow low-level process reads its own name from active_process
    seen = {}

    def start(env):
        def step():
            seen["name"] = env.active_process.name
            yield env.timeout(1)

        env.process(step())

    run_low_level(None, start, 2)
    assert seen["name"] == "step"


def _builder_process_name(seen=None):
    app = SimulationContext("C1", factor=0.0)
    app.add_service("working", dist="constant", value=1)

    @app.task(
        service_id="working",
        name="step",
        next={
            "when": [{"condition": 'process.name == "step"', "task": "same"}],
            "default": "other",
        },
    )
    def step(task_id):
        _note(app, seen)
        return {"n": task_id}

    @app.task(name="same")
    def same(task_id):
        _note(app, seen)
        return {"event": "same"}

    @app.task(name="other")
    def other(task_id):
        return {"event": "other"}

    def script(context):
        context.spawn(step(0))
        yield context.env.event()

    app.add_process(script)
    return run_builder(app, 3)


def _note(app, seen):
    """What the running process reports: its name and its generator's `__name__`."""
    if seen is not None:
        process = app.env.active_process
        seen.append((process.name, process._generator.__name__))


def test_c1_process_name():
    """`process.name` reads the running task's name in a condition, routing next to
    same at 1. The process is named after its generator's `__name__` as SimPy's is,
    so that name is set to the task that runs now, also after a hand-on."""
    for records in (_builder_process_name(), run_yaml("c1_process.yaml")[0]):
        assert times(records, "same") == [1.0]
        assert times(records, "other") == []
    seen = []
    _builder_process_name(seen)
    assert seen == [("step", "step"), ("same", "same")]


def test_c1_process_target():
    """`process.target` reads what the process waits on. In SimPy it is the event
    being yielded; in desflow it is the kind, `service` during a service, and a
    service task's next reads it."""
    env = simpy.Environment()
    captured = {}

    def worker():
        yield env.timeout(2)

    def probe(proc):
        yield env.timeout(1)  # worker is waiting on its timeout now
        captured["target"] = proc.target

    proc = env.process(worker())
    env.process(probe(proc))
    env.run()
    assert isinstance(captured["target"], simpy.events.Timeout)  # the event yielded
    assert proc.target is None  # None once the process ended

    # a desflow service task reads process.target as "service" in its next rule
    app = SimulationContext("C1", factor=0.0)
    app.add_service("working", dist="constant", value=1)

    @app.task(
        service_id="working",
        name="step",
        next={
            "when": [{"condition": 'process.target == "service"', "task": "served"}],
            "default": "idle",
        },
    )
    def step(task_id):
        return {"n": task_id}

    @app.task(name="served")
    def served(task_id):
        return {"event": "served"}

    @app.task(name="idle")
    def idle(task_id):
        return {"event": "idle"}

    def script(context):
        context.spawn(step(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 3)
    assert times(records, "served") == [1.0]
    assert times(records, "idle") == []


# --------------------------------------- following tasks in one process (S2)

# A task hands on to the next with `next`, `on_fail` or `on_interrupt_next`. The
# next task runs in the same process and the same step, as the next statement of a
# SimPy process does. Each test runs a plain SimPy process that carries on, then the
# same model as tasks.

HAND_ON = """
simulation: {sim_id: C1, factor: 0}
resources: {r: {current_cap: 1}}
services: {one: {dist: constant, value: 1}, three: {dist: constant, value: 3}}
arrivals:
  starts: {replay: {path: starts.csv}, spawn: start}
tasks:
  start: {service: one, payload: {e: s}, spawn: {task: cuse}, next: use}
  cuse: {resource: r, service: three, payload: {e: left}}
  use: {resource: r, service: three, payload: {e: left}}
run: {until: 60}
"""


def _reference_child_and_parent(n, delay):
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    grants = []

    def hold(label):
        with unit.request() as req:
            yield req
            grants.append((env.now, label))
            yield env.timeout(3)

    def parent():
        yield env.timeout(1)
        env.process(hold("cuse"))
        if delay:
            yield env.timeout(0)
        yield from hold("use")

    for _ in range(n):
        env.process(parent())
    env.run(until=60)
    return grants


def _builder_child_and_parent(n, delay):
    app = SimulationContext("C1", factor=0.0)
    app.add_resource("r", current_cap=1, max_cap=1)
    app.add_service("one", dist="constant", value=1)
    app.add_service("three", dist="constant", value=3)

    @app.task(service_id="one", spawn={"task": "cuse"}, next="use", name="start")
    def start(task_id):
        return {"e": "s"}

    @app.task(service_id="three", resource_id="r", name="cuse")
    def cuse(task_id):
        return {"e": "left"}

    @app.task(
        service_id="three",
        resource_id="r",
        wait={"timeout": 0} if delay else None,
        name="use",
    )
    def use(task_id):
        return {"e": "left"}

    def script(context):
        for i in range(n):
            context.spawn(start(i, entity=Entity(f"s-{i}", "s", {})))
        yield context.env.event()

    app.add_process(script)
    return run_builder(app, 60)


def _grants(records):
    return [
        (t, task)
        for t, _key, task, status in event_trace(records)
        if status == "started" and task in ("use", "cuse")
    ]


@pytest.mark.parametrize("delay", [False, True], ids=["next", "next_after_timeout_0"])
@pytest.mark.parametrize("n", [1, 3, 10])
def test_c1_next_asks_for_the_unit_before_a_child_spawned_in_that_step(
    tmp_path, n, delay
):
    """A task spawns a child that asks for the unit, then hands on to a task that asks
    for it too. The process carries on, so the next task asks first, as the SimPy
    parent does; a `wait: {timeout: 0}` in the next task lets the child ask first."""
    expected = _reference_child_and_parent(n, delay)
    assert expected[0][1] == ("cuse" if delay else "use")
    assert _grants(_builder_child_and_parent(n, delay)) == expected
    text = HAND_ON.replace(
        "  use: {resource",
        "  use: {wait: {timeout: 0}, resource" if delay else "  use: {resource",
    )
    outcome = run_yaml_text(tmp_path, text, {"starts": [0] * n})
    assert outcome.error is None
    assert _grants(outcome.records) == expected


def _reference_interrupt_cause():
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    out = []

    def worker():
        try:
            yield env.timeout(10)
        except simpy.Interrupt as interrupt:
            cause = interrupt.cause
        with unit.request() as req:
            yield req
            out.append((env.now, "after", cause))
            yield env.timeout(2)

    def rival():
        with unit.request() as req:
            yield req
            out.append((env.now, "rival", None))
            yield env.timeout(2)

    def closer(target):
        yield env.timeout(3)
        target.interrupt("closing")
        env.process(rival())

    target = env.process(worker())
    env.process(closer(target))
    env.run(until=20)
    return out


def _builder_interrupt_cause():
    app = SimulationContext("C1", factor=0.0)
    app.add_resource("r", current_cap=1, max_cap=1)
    app.add_service("long", dist="constant", value=10)
    app.add_service("two", dist="constant", value=2)
    out = []

    @app.task(
        service_id="long",
        on_interrupt="abandon",
        on_interrupt_next="after",
        name="work",
    )
    def work(task_id):
        return {"e": "worked"}

    @app.task(service_id="two", resource_id="r", name="after")
    def after(task_id, upstream):
        out.append((app.env.now, "after", upstream.payload["cause"]))
        return {"e": "after"}

    @app.task(service_id="two", resource_id="r", name="rival")
    def rival(task_id):
        out.append((app.env.now, "rival", None))
        return {"e": "rival"}

    def script(context):
        process = context.spawn(work(0, entity=Entity("w-1", "w", {})))
        yield context.env.timeout(3)
        process.interrupt("closing")
        context.spawn(rival(0, entity=Entity("r-1", "r", {})))
        yield context.env.event()

    app.add_process(script)
    run_builder(app, 20)
    return out


def test_c1_an_interrupt_cause_is_read_in_the_next_task():
    """A worker interrupted at 3 goes on to the next task in the same step, reads the
    cause there, and asks for the unit before a rival that is started after the
    interrupt, as the SimPy worker does."""
    expected = _reference_interrupt_cause()
    assert [label for _t, label, _c in expected] == ["after", "rival"]
    assert expected[0] == (3, "after", "closing")
    assert _builder_interrupt_cause() == expected


def _reference_chain(n):
    env = simpy.Environment()
    ticks = []

    def flat():
        count = 0
        while count < n:
            count += 1
            ticks.append(env.now)
            yield env.timeout(0)

    env.process(flat())
    env.run()
    return ticks


CHAIN = """
simulation: {sim_id: C1, factor: 0}
signals: {tick: {once: false}}
arrivals:
  starts: {replay: {path: starts.csv}, spawn: tick}
tasks:
  tick:
    signal: tick
    payload: {e: t}
    next: {when: [{condition: "signal.tick.count < 5000", task: tick}]}
run: {until: 10}
"""


def test_c1_a_next_chain_of_5000_tasks_runs_without_nesting_generators(tmp_path):
    """5000 tasks that take no time, each handing on to the next, run to the end with
    no error. They run in a loop in one process, as a SimPy loop of 5000 steps does;
    nested generators would exhaust Python's recursion limit."""
    assert len(_reference_chain(5000)) == 5000
    outcome = run_yaml_text(tmp_path, CHAIN, {"starts": [0]})
    assert outcome.error is None
    assert times(outcome.records, "tick") == [0.0] * 5000


def test_c1_a_long_next_loop_keeps_no_finished_instances():
    """A flow that loops 1000 times holds no more instances than one that loops 100
    times, as a SimPy loop holds no earlier steps."""
    app = SimulationContext("C1", factor=0.0)
    app.add_service("one", dist="constant", value=1)
    counts = {}

    @app.task(service_id="one", next="tick", name="tick")
    def tick(task):
        return {}

    def sampler(ctx):
        ctx.spawn(tick(0, entity=Entity("w", "w", {})))
        for at in (100.5, 1000.5):
            yield ctx.env.timeout(at - ctx.env.now)
            gc.collect()
            counts[at] = sum(isinstance(o, TaskRun) for o in gc.get_objects())

    app.add_process(sampler)
    run_builder(app, 1001)
    assert counts[1000.5] - counts[100.5] <= 2


def _reference_stages():
    env = simpy.Environment()
    seen = {}

    def stages():
        yield env.timeout(1)
        yield env.timeout(2)

    def probe(proc):
        yield env.timeout(1.5)
        seen["during"] = (
            proc.is_alive,
            isinstance(proc.target, simpy.events.Timeout),
            proc.triggered,
        )
        yield env.timeout(2)
        seen["after"] = (proc.is_alive, proc.target, proc.triggered)

    proc = env.process(stages())
    env.process(probe(proc))
    env.run(until=5)
    return seen


def _builder_stages():
    app = SimulationContext("C1", factor=0.0)
    app.add_service("one", dist="constant", value=1)
    app.add_service("two", dist="constant", value=2)
    seen = {}

    @app.task(service_id="one", next="second", name="first")
    def first(task_id):
        return {"e": "first"}

    @app.task(service_id="two", name="second")
    def second(task_id):
        return {"e": "second"}

    def script(context):
        env = context.env
        proc = context.spawn(first(0, entity=Entity("f-1", "f", {})))
        yield env.timeout(1.5)
        seen["during"] = (
            proc.is_alive,
            isinstance(proc.target, simpy.events.Timeout),
            proc.triggered,
        )
        yield env.timeout(2)
        seen["after"] = (proc.is_alive, proc.target, proc.triggered)
        yield env.event()

    app.add_process(script)
    run_builder(app, 5)
    return seen


def test_c1_process_is_alive_across_a_next_chain():
    """Read from outside after the first task has handed on, the process is alive,
    and it ends when the last task ends, as a SimPy process does that runs its stages
    one after the other."""
    expected = _reference_stages()
    assert expected["during"][0] and not expected["during"][2]
    assert (expected["after"][0], expected["after"][2]) == (False, True)
    got = _builder_stages()
    assert (got["during"][0], got["during"][2]) == (True, False)
    assert (got["after"][0], got["after"][2]) == (False, True)


def test_c1_process_target_across_a_next_chain():
    """Read from outside after the first task has handed on, the process waits on the
    next task's timeout, and on nothing once the last task has ended, as the SimPy
    process does."""
    expected = _reference_stages()
    assert expected["during"][1] is True
    assert expected["after"][1] is None
    got = _builder_stages()
    assert got["during"][1] is True
    assert got["after"][1] is None


def test_c1_a_process_that_waits_for_a_flow_wakes_when_the_flow_ends():
    """A process that yields a root task's process wakes when the last task of the
    flow ends, at 3, with the first task's payload, as it does for a SimPy process
    that runs its stages one after the other."""
    env = simpy.Environment()
    woke = []

    def stages():
        yield env.timeout(1)
        yield env.timeout(2)
        return "first"

    def waiter(proc):
        value = yield proc
        woke.append((env.now, value))

    env.process(waiter(env.process(stages())))
    env.run(until=5)
    assert woke == [(3, "first")]

    app = SimulationContext("C1", factor=0.0)
    app.add_service("one", dist="constant", value=1)
    app.add_service("two", dist="constant", value=2)
    got = []

    @app.task(service_id="one", next="second", name="first")
    def first(task_id):
        return "first"

    @app.task(service_id="two", name="second")
    def second(task_id):
        return "second"

    def script(context):
        proc = context.spawn(first(0, entity=Entity("f-1", "f", {})))
        value = yield proc
        got.append((context.env.now, value))

    app.add_process(script)
    run_builder(app, 5)
    assert got == woke


def test_c1_active_process_is_the_same_process_across_a_next_chain():
    """`active_process` is one process for the whole flow, as in a SimPy process that
    carries on; its name is the task that runs now."""
    env = simpy.Environment()
    seen = []

    def stages():
        seen.append(env.active_process)
        yield env.timeout(1)
        seen.append(env.active_process)

    env.process(stages())
    env.run()
    assert seen[0] is seen[1]

    app = SimulationContext("C1", factor=0.0)
    app.add_service("one", dist="constant", value=1)
    got = []

    @app.task(service_id="one", next="second", name="first")
    def first(task_id):
        got.append((app.env.active_process, app.env.active_process.name))
        return {"e": "first"}

    @app.task(name="second")
    def second(task_id):
        got.append((app.env.active_process, app.env.active_process.name))
        return {"e": "second"}

    def script(context):
        context.spawn(first(0, entity=Entity("f-1", "f", {})))
        yield context.env.event()

    app.add_process(script)
    run_builder(app, 3)
    assert got[0][0] is got[1][0]
    assert [name for _p, name in got] == ["first", "second"]


def _reference_subscribe_across_a_stage():
    env = simpy.Environment()
    out = []

    def charger():
        yield env.timeout(3)
        return {"event": "charged"}

    def driver():
        proc = env.process(charger())
        simpy.util.subscribe_at(proc)
        yield env.timeout(1)
        try:
            yield env.timeout(20)
        except simpy.Interrupt:
            out.append(env.now)

    env.process(driver())
    env.run(until=30)
    return out


SUBSCRIBE_ACROSS = """
simulation: {sim_id: C1, factor: 0}
services:
  charging: {dist: constant, value: 3}
  short: {dist: constant, value: 1}
  driving: {dist: constant, value: 20}
arrivals:
  starts: {replay: {path: starts.csv}, spawn: driver}
tasks:
  driver:
    service: short
    subscribe: {task: charger}
    next: road
    payload: {event: left}
  road:
    service: driving
    on_interrupt: abandon
    payload: {event: drove}
  charger:
    service: charging
    payload: {event: charged}
run: {until: 30}
"""


def test_c1_subscribe_interrupts_the_next_task_after_a_hand_on(tmp_path):
    """A task subscribes to a charger that ends at 3, and hands on at 1 to a task of
    20. The process is still alive, so the interrupt reaches the next task at 3, as
    it reaches the SimPy driver's second stage."""
    assert _reference_subscribe_across_a_stage() == [3]
    outcome = run_yaml_text(tmp_path, SUBSCRIBE_ACROSS, {"starts": [0]})
    assert outcome.error is None
    interrupted = [
        (r["sim_ts"], r["task"])
        for r in outcome.records
        if isinstance(r["value"], dict) and r["value"].get("status") == "interrupted"
    ]
    assert interrupted == [(3.0, "road")]


def _reference_kept_unit_preempted():
    env = simpy.Environment()
    picker = simpy.PreemptiveResource(env, 1)
    bench = simpy.Resource(env, 1)
    trace = []

    def blocker():
        with bench.request() as req:
            yield req
            yield env.timeout(10)

    def order():
        with picker.request(priority=1) as kept:
            yield kept
            yield env.timeout(1)
            try:
                with bench.request() as req:
                    yield req
            except simpy.Interrupt:
                trace.append((env.now, "order", "preempted"))

    def vip():
        yield env.timeout(3)
        with picker.request(priority=0) as req:
            yield req
            trace.append((env.now, "vip", "started"))
            yield env.timeout(1)

    env.process(blocker())
    env.process(order())
    env.process(vip())
    env.run(until=20)
    return trace


def _builder_kept_unit_preempted():
    app = SimulationContext("C1", factor=0.0)
    app.add_resource("picker", current_cap=1, max_cap=1, queue="priority", preempt=True)
    app.add_resource("bench", current_cap=1, max_cap=1)
    app.add_service("one", dist="constant", value=1)
    app.add_service("ten", dist="constant", value=10)

    @app.task(
        service_id="one",
        resource_id="picker",
        keep=["picker"],
        priority="attr.p",
        next="pack",
        name="pick",
    )
    def pick(task_id):
        return {"e": "picked"}

    @app.task(service_id="one", resource_id="bench", name="pack")
    def pack(task_id):
        return {"e": "packed"}

    @app.task(service_id="ten", resource_id="bench", name="blocker")
    def blocker(task_id):
        return {"e": "blocked"}

    @app.task(service_id="one", resource_id="picker", priority="attr.p", name="vip")
    def vip(task_id):
        return {"e": "vip"}

    def script(context):
        context.spawn(blocker(0, entity=Entity("blocker-1", "blocker", {"p": 0})))
        context.spawn(pick(0, entity=Entity("order-1", "order", {"p": 1})))
        yield context.env.timeout(3)
        context.spawn(vip(0, entity=Entity("vip-1", "vip", {"p": 0})))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 20)
    return [
        (t, key.split("-")[0], status)
        for t, key, _task, status in event_trace(records)
        if (key.startswith("order") and status == "preempted")
        or (key.startswith("vip") and status == "started")
    ]


def test_c1_a_preempted_kept_unit_interrupts_the_next_task_while_it_queues():
    """A task keeps the picker for the next task, which queues for a busy bench. A
    higher priority task takes the picker at 3, so the process is interrupted while
    it queues, as the SimPy process is that holds the picker through its wait."""
    expected = _reference_kept_unit_preempted()
    assert expected == [(3, "order", "preempted"), (3, "vip", "started")]
    assert _builder_kept_unit_preempted() == expected
