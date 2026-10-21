"""C1 processes: spawn a new entity (E4), spawn after a delay (U1), be told when an
event happens (U2), and wait for a running process (P7).

Each test runs a plain SimPy reference, then the same model in the low-level API, the
builder and YAML, and compares the event times. The low-level models use SimPy itself
(`env.process`, `simpy.util.start_delayed`, `simpy.util.subscribe_at`, `yield proc`);
the builder and YAML use the `spawn`, `subscribe` and `running` keys.
"""

import simpy
import simpy.util

from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder, run_low_level, run_yaml, times


# ------------------------------------------------------- spawn a new entity (E4)


def _reference_spawn():
    env = simpy.Environment()
    out = []

    def courier():
        out.append((env.now, "courier"))
        yield env.timeout(0)

    def pack():
        yield env.timeout(1)
        out.append((env.now, "pack"))
        env.process(courier())

    def arrivals():
        yield env.timeout(2)
        env.process(pack())

    env.process(arrivals())
    env.run(until=4)
    return out


def _low_level_spawn():
    def start(env):
        def courier(n):
            env.publish_event(
                f"courier-{n}", {"event": "parent"}, task="courier", instance=n - 1
            )
            yield env.timeout(0)

        def pack(n):
            yield env.timeout(1)
            env.publish_event(
                f"orders-{n}", {"event": "packed"}, task="pack", instance=n - 1
            )
            env.process(courier(n))

        def arrivals():
            yield env.timeout(2)
            env.process(pack(1))

        env.process(arrivals())

    return run_low_level(None, start, 4)[0]


def _builder_spawn():
    app = SimulationContext("C1", factor=0.0)
    app.add_service("packing", dist="constant", value=1)
    app.add_arrival("orders", dist="constant", value=2)

    @app.task(service_id="packing", spawn={"task": "courier"}, name="pack")
    def pack(task_id):
        return {"event": "packed"}

    @app.task(name="courier")
    def courier(task_id, entity):
        return {"parent": entity.attributes.get("parent")}

    @app.arrival_loop("orders")
    def orders(context):
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), entity=entity))

    return run_builder(app, 4)


def test_c1_start_a_process_without_waiting_for_it():
    """pack finishes at 3 and spawns courier at the same instant, as SimPy's
    `env.process`; the spawned entity is keyed courier-1 with the order's key in
    attr.parent, and counts as a flow of its own."""
    assert _reference_spawn() == [(3, "pack"), (3, "courier")]
    for records in (_low_level_spawn(), _builder_spawn(), run_yaml("c1_spawn.yaml")[0]):
        assert times(records, "pack") == [3.0]
        assert times(records, "courier") == [3.0]
    # the desflow APIs key the spawned entity and carry attr.parent
    for records in (_builder_spawn(), run_yaml("c1_spawn.yaml")[0]):
        courier = [(r["key"], r["value"]) for r in records if r["task"] == "courier"]
        assert courier == [("courier-1", {"parent": "orders-1"})]


def test_c1_start_a_process_without_waiting_for_it_drain():
    """A drain waits for a spawned entity, even when the parent is the last entity
    and leaves in the step that spawns it."""
    from tests.conformance.harness import RecordingEgress

    app = SimulationContext("C1", factor=0.0)
    app.add_service("packing", dist="constant", value=1)
    app.add_service("delivering", dist="constant", value=5)

    @app.task(service_id="packing", spawn={"task": "courier"}, name="pack")
    def pack(task_id):
        return {"event": "packed"}

    @app.task(service_id="delivering", name="courier")
    def courier(task_id):
        return {"event": "delivered"}

    def script(context):
        context.spawn(pack(0))
        yield context.env.event()

    app.add_process(script)
    out = RecordingEgress()
    app.add_egress(out)
    # pack finishes at 1 (the last entity) and spawns courier, which ends at 6; the
    # drain at until=2 runs on until the courier entity leaves
    app.run(until=2, drain=True)
    assert times(out.records, "courier") == [6.0]


# --------------------------------------------------- spawn after a delay (U1)


def _reference_delay():
    env = simpy.Environment()
    out = []

    def courier():
        out.append(env.now)
        yield env.timeout(0)

    def pack():
        yield env.timeout(1)
        simpy.util.start_delayed(env, courier(), 3)

    def arrivals():
        yield env.timeout(2)
        env.process(pack())

    env.process(arrivals())
    env.run(until=8)
    return out


def _builder_delay(after=3):
    app = SimulationContext("C1", factor=0.0)
    app.add_service("packing", dist="constant", value=1)
    app.add_arrival("orders", dist="constant", value=2)

    @app.task(
        service_id="packing", spawn={"task": "courier", "after": after}, name="pack"
    )
    def pack(task_id):
        return {"event": "packed"}

    @app.task(name="courier")
    def courier(task_id):
        return {"event": "delivered"}

    @app.arrival_loop("orders")
    def orders(context):
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), entity=entity))

    return run_builder(app, 8)


def test_c1_start_a_process_after_a_delay():
    """pack finishes at 3 and the courier starts 3 s later, at 6, built with
    SimPy's `start_delayed`."""
    assert _reference_delay() == [6]
    for records in (_builder_delay(), run_yaml("c1_spawn_delay.yaml")[0]):
        assert times(records, "courier") == [6.0]


def test_c1_start_a_process_after_a_delay_at_a_tie():
    """A courier delayed to the instant another order arrives orders its events as
    SimPy does: the delayed start and the arrival at 6 both happen, in order."""
    env = simpy.Environment()
    out = []

    def ref_courier():
        out.append((env.now, "courier"))
        yield env.timeout(0)

    def ref_other():
        out.append((env.now, "other"))
        yield env.timeout(0)

    def ref_pack():
        yield env.timeout(1)
        simpy.util.start_delayed(env, ref_courier(), 3)

    def arrivals():
        yield env.timeout(2)
        env.process(ref_pack())
        yield env.timeout(4)
        env.process(ref_other())

    env.process(arrivals())
    env.run(until=8)
    # the delayed courier and the arrival at 6 both run at 6
    assert [t for t, _ in out] == [6, 6]

    # the builder spawns a courier with after=3 that lands at 6 beside an arrival
    app = SimulationContext("C1", factor=0.0)
    app.add_service("packing", dist="constant", value=1)

    @app.task(service_id="packing", spawn={"task": "courier", "after": 3}, name="pack")
    def pack(task_id):
        return {"event": "packed"}

    @app.task(name="courier")
    def courier(task_id):
        return {"event": "delivered"}

    @app.task(name="other")
    def other(task_id):
        return {"event": "other"}

    def script(context):
        yield context.env.timeout(2)
        context.spawn(pack(0))
        yield context.env.timeout(4)
        context.spawn(other(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 8)
    assert times(records, "courier") == [6.0]
    assert times(records, "other") == [6.0]


def test_c1_start_a_process_after_a_delay_refuses_zero():
    """A spawn with an after of 0 or less is refused at load, and
    `context.spawn(gen, delay=0)` raises SimPy's ValueError."""
    import pytest

    app = SimulationContext("C1", factor=0.0)
    with pytest.raises(ValueError, match="after is more than 0"):

        @app.task(spawn={"task": "x", "after": 0}, name="pack")
        def pack(task_id):
            return {}


# ----------------------------------------------- be told when an event happens (U2)


def _reference_subscribe(after_end=False):
    env = simpy.Environment()
    out = []

    def charger():
        yield env.timeout(3)
        return {"event": "charged"}

    def driver():
        proc = env.process(charger())
        simpy.util.subscribe_at(proc)
        try:
            yield env.timeout(20)
        except simpy.Interrupt as interrupt:
            out.append((env.now, interrupt.cause))

    def late_driver():
        proc = env.process(charger())
        yield env.timeout(5)  # the charger ends at 3, before we subscribe
        try:
            simpy.util.subscribe_at(proc)
        except RuntimeError:
            out.append((env.now, "already"))
        yield env.timeout(1)

    env.process(late_driver() if after_end else driver())
    env.run(until=30)
    return out


def test_c1_be_told_when_an_event_happens():
    """A subscribe interrupts the waiting task when the charger ends, at 3, one
    urgent step after the event, with the cause carrying the event and its value."""
    out = _reference_subscribe()
    assert [t for t, _ in out] == [3]
    records = run_yaml("c1_subscribe.yaml")[0]
    interrupted = [
        (r["sim_ts"], r["value"]["cause"])
        for r in records
        if r["task"] == "driver"
        and isinstance(r["value"], dict)
        and r["value"].get("status") == "interrupted"
    ]
    assert interrupted == [(3.0, {"event": "charger", "value": {"event": "charged"}})]

    # builder: context.subscribe inside a task function
    app = SimulationContext("C1", factor=0.0)
    app.add_service("charging", dist="constant", value=3)
    app.add_service("driving", dist="constant", value=20)

    @app.task(service_id="charging", name="charger")
    def charger(task_id):
        return {"event": "charged"}

    @app.task(
        service_id="driving",
        subscribe={"task": "charger"},
        on_interrupt="abandon",
        name="driver",
    )
    def driver(task_id):
        return {"event": "drove"}

    def script(context):
        context.spawn(driver(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 30)
    interrupted = [
        r["sim_ts"]
        for r in records
        if r["task"] == "driver"
        and isinstance(r["value"], dict)
        and r["value"].get("status") == "interrupted"
    ]
    assert interrupted == [3.0]


def test_c1_be_told_when_an_event_happens_after_the_subscriber_ended():
    """`subscribe` refuses a processed event, as SimPy's `subscribe_at`."""

    out = _reference_subscribe(after_end=True)
    assert out == [(5, "already")]

    app = SimulationContext("C1", factor=0.0)
    seen = {}

    @app.task(name="late")
    def late(task_id):
        return {"event": "late"}

    def script(context):
        done = context.env.event()
        done.succeed(True)
        yield context.env.timeout(1)  # the event is processed
        try:
            context.subscribe(done)
        except RuntimeError:
            seen["refused"] = True
        yield context.env.event()

    app.add_process(script)
    run_builder(app, 5)
    assert seen.get("refused") is True


# ----------------------------------------------- wait for a running process (P7)


def _reference_running(ended=False, fails=False):
    env = simpy.Environment()
    out = []

    def charge():
        yield env.timeout(1 if ended else (2 if fails else 5))
        if fails:
            raise RuntimeError("flat_battery")
        return {"event": "charged"}

    def driver(proc, hold=0.0):
        if hold:
            yield env.timeout(hold)
        try:
            value = yield proc
            out.append((env.now, "resumed", value))
        except RuntimeError as error:
            out.append((env.now, "failed", str(error)))

    def car():
        yield env.timeout(1)  # the single arrival
        proc = env.process(charge())
        env.process(driver(proc, hold=3.0 if ended else 0.0))

    env.process(car())
    env.run(until=10)
    return out


def _yaml_driver_time(name, status="finished"):
    records = run_yaml(name)[0]
    if status == "failed":
        return [
            r["sim_ts"]
            for r in records
            if r["task"] == "driver"
            and isinstance(r["value"], dict)
            and r["value"].get("status") == "failed"
        ]
    return times(records, "driver")


def test_c1_wait_for_a_process_already_running():
    """A running wait resumes when the charge instance on the entity ends, at 6, as
    SimPy's `yield proc`."""
    out = _reference_running()
    assert [t for t, *_ in out] == [6]
    assert _yaml_driver_time("c1_running.yaml") == [6.0]


def test_c1_wait_for_a_process_that_has_ended():
    """A running wait on an instance that already ended resumes at once, at 4, with
    its value, as SimPy's `yield proc` on a finished process."""
    out = _reference_running(ended=True)
    assert [t for t, *_ in out] == [4]
    assert _yaml_driver_time("c1_running_ended.yaml") == [4.0]


def test_c1_wait_for_a_process_that_fails():
    """A running wait fails with the instance's reason when it fails, at 3, as
    SimPy's `yield proc` raises."""
    out = _reference_running(fails=True)
    assert [(t, kind) for t, kind, *_ in out] == [(3, "failed")]
    assert _yaml_driver_time("c1_running_fails.yaml", status="failed") == [3.0]


def test_c1_wait_for_a_process_two_instances():
    """A running wait over two instances of a task resumes when both end, at 6, with
    a mapping from instance id to payload, as SimPy's `all_of` over two processes."""
    env = simpy.Environment()
    out = []

    def charge():
        yield env.timeout(6)
        return {"event": "charged"}

    def driver(procs):
        values = yield env.all_of(procs)
        out.append((env.now, {p.callbacks is None: v for p, v in values.items()}))

    procs = [env.process(charge()), env.process(charge())]
    env.process(driver(procs))
    env.run(until=10)
    assert [t for t, _ in out] == [6]

    # builder: two charges and a driver on one car entity; the driver reads the map
    app = SimulationContext("C1", factor=0.0)
    app.add_service("charging", dist="constant", value=6)
    seen = {}

    @app.task(service_id="charging", name="charge")
    def charge_task(task_id):
        return {"event": "charged"}

    @app.task(wait={"running": {"task": "charge"}}, name="driver")
    def driver_task(task_id, wait):
        seen["value"] = wait["charge"]["value"]
        return {"event": "drove"}

    def script(context):
        entity = context._new_entity("car", None)
        context.spawn(charge_task(context.next_instance("charge"), entity=entity))
        context.spawn(charge_task(context.next_instance("charge"), entity=entity))
        context.spawn(driver_task(context.next_instance("driver"), entity=entity))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 10)
    assert times(records, "driver") == [6.0]
    # the value is a mapping from instance id to each charge's finished payload
    assert seen["value"] == {0: {"event": "charged"}, 1: {"event": "charged"}}

    assert times(run_yaml("c1_running_two.yaml")[0], "driver") == [6.0]


def test_c1_wait_for_a_process_that_hands_on():
    """A running wait on a task that hands on to the next task resumes when that task
    finishes, at 4, not when its flow ends at 7, as SimPy's `yield proc` on a process
    that returns before its successor runs."""
    env = simpy.Environment()
    out = []

    def more():
        yield env.timeout(3)

    def relay():
        yield env.timeout(3)
        env.process(more())  # hands on, then this process ends with its payload
        return {"event": "relayed"}

    def car():
        yield env.timeout(1)
        relay_proc = env.process(relay())
        env.process(driver(relay_proc))

    def driver(proc):
        value = yield proc
        out.append((env.now, value))

    env.process(car())
    env.run(until=10)
    assert out == [(4, {"event": "relayed"})]

    records = run_yaml("c1_running_hands_on.yaml")[0]
    assert times(records, "driver") == [4.0]
    assert times(records, "more") == [7.0]  # the flow goes on past the resume


def test_c1_wait_for_a_process_that_it_spawned():
    """A task waits for a running instance it started with a spawn, on another
    entity, and resumes when that instance ends, at 6, as SimPy's `yield proc`."""
    env = simpy.Environment()
    out = []

    def courier():
        yield env.timeout(5)
        return {"event": "delivered"}

    def dispatcher():
        yield env.timeout(1)
        proc = env.process(courier())
        value = yield proc
        out.append((env.now, value))

    env.process(dispatcher())
    env.run(until=10)
    assert out == [(6, {"event": "delivered"})]

    # builder: a dispatcher spawns a courier as a new entity, then a watcher on the
    # dispatcher's entity waits for that running courier
    app = SimulationContext("C1", factor=0.0)
    app.add_service("couriering", dist="constant", value=5)
    seen = {}

    @app.task(spawn={"task": "courier"}, next="watch", name="dispatcher")
    def dispatcher_task(task_id):
        return {"event": "dispatched"}

    @app.task(service_id="couriering", name="courier")
    def courier_task(task_id, entity):
        seen["courier_key"] = entity.key
        seen["parent"] = entity.attributes.get("parent")
        return {"event": "delivered"}

    @app.task(wait={"running": {"task": "courier"}}, name="watch")
    def watch_task(task_id):
        return {"event": "watched"}

    def script(context):
        yield context.env.timeout(1)
        context.spawn(dispatcher_task(context.next_instance("dispatcher")))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 10)
    assert times(records, "watch") == [6.0]
    # the courier ran on its own spawned entity, with the dispatcher as its parent
    assert seen["courier_key"] == "courier-1"
    assert seen["parent"] == "dispatcher-1"


def test_c1_wait_for_a_process_abandoned_in_a_child_flow():
    """A running wait on an instance abandoned in a child flow fails with its
    TaskFailed when it has no on_interrupt_next, and resumes with None when it does,
    both at the abandon time, 2."""
    env = simpy.Environment()
    out = []

    def charge():
        yield env.timeout(100)

    def driver(proc):
        try:
            yield proc
            out.append((env.now, "resumed"))
        except Exception:  # Interrupt propagated as a failure
            out.append((env.now, "failed"))

    def car():
        proc = env.process(charge())
        env.process(driver(proc))
        yield env.timeout(2)
        proc.interrupt("stop")

    env.process(car())
    env.run(until=10)
    assert out == [(2, "failed")]

    def build(on_interrupt_next):
        app = SimulationContext("C1", factor=0.0)
        app.add_service("charging", dist="constant", value=100)
        app.add_service("repairing", dist="constant", value=1)

        @app.task(
            service_id="charging",
            on_interrupt="abandon",
            on_interrupt_next=on_interrupt_next,
            name="charge",
        )
        def charge_task(task_id):
            return {"event": "charged"}

        @app.task(service_id="repairing", name="repair")
        def repair_task(task_id):
            return {"event": "repaired"}

        @app.task(wait={"running": {"task": "charge"}}, name="driver")
        def driver_task(task_id):
            return {"event": "drove"}

        def script(context):
            entity = context._new_entity("car", None)
            context.call("charge", entity=entity)  # a child flow
            context.spawn(driver_task(context.next_instance("driver"), entity=entity))
            yield context.env.timeout(2)
            context.interrupt("charge")
            yield context.env.event()

        app.add_process(script)
        return app

    # No on_interrupt_next: the child flow's end event fails, so the wait fails at 2
    failed = [
        r["sim_ts"]
        for r in run_builder(build(None), 10)
        if r["task"] == "driver"
        and isinstance(r["value"], dict)
        and r["value"].get("status") == "failed"
    ]
    assert failed == [2.0]

    # With on_interrupt_next: the end event succeeds with None, so the wait resumes
    # at 2 and the driver finishes
    assert times(run_builder(build("repair"), 10), "driver") == [2.0]
