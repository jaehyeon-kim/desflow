"""Runtime checks for the Batch 2 features: spawned entities, subscribe, running
waits, `on:` actions, one-shot signals, process expressions and the timeout value.

These exercise the builder, which runs the same runtime as YAML. The SimPy-versus-
desflow comparisons live in the conformance suite; here each feature is checked for
the behaviour the plan settles.
"""

import numpy as np
import pytest

from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder, times


# -------------------------------------------------------------------- spawn (E4)


def test_spawn_key_starts_a_new_entity_keyed_after_the_task():
    """A `spawn:` key starts the task as a new entity keyed `<task>-n`, with the
    spawning entity's key in `attr.parent`, counted as a flow of its own."""
    app = SimulationContext("S", factor=0.0)
    app.add_service("svc", dist="constant", value=1)
    app.add_arrival("orders", dist="constant", value=2)

    @app.task(service_id="svc", spawn={"task": "courier"}, name="pack")
    def pack(task_id):
        return {"packed": task_id}

    @app.task(name="courier")
    def courier(task_id, entity):
        return {"parent": entity.attributes.get("parent")}

    @app.arrival_loop("orders")
    def loop(context):
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), entity=entity))
        yield context.env.event()

    records = run_builder(app, 6)
    # the first arrival is at 2, pack finishes at 3, courier spawns as its own
    # entity keyed courier-1 and emits at once with attr.parent the order's key
    courier_events = [
        (r["sim_ts"], r["key"], r["value"]) for r in records if r["task"] == "courier"
    ]
    assert courier_events == [(3.0, "courier-1", {"parent": "orders-1"})]


def test_spawn_after_delays_the_new_entity():
    """`spawn` with `after` starts the new entity that long after the task finishes."""
    app = SimulationContext("S", factor=0.0)
    app.add_service("svc", dist="constant", value=1)
    app.add_arrival("orders", dist="constant", value=2)

    @app.task(service_id="svc", spawn={"task": "courier", "after": 3}, name="pack")
    def pack(task_id):
        return {"packed": task_id}

    @app.task(name="courier")
    def courier(task_id, entity):
        return {"out": task_id}

    @app.arrival_loop("orders")
    def loop(context):
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), entity=entity))
        yield context.env.event()

    records = run_builder(app, 10)
    # first arrival at 2, pack finishes at 3, courier starts 3 later, at 6
    assert times(records, "courier") == [6.0]


@pytest.mark.parametrize("count", [lambda entity: np.array(2), "attr.n"])
def test_a_spawn_count_takes_any_value_with_an_index(count):
    """A spawn count is read with `operator.index`, so a 0-d NumPy integer array,
    from a function or from an attribute, starts that many entities."""
    app = SimulationContext("S", factor=0.0, random_seed=1)
    app.add_arrival("a", "constant", value=5, first_at=0, attributes={"n": np.array(2)})

    @app.task(name="parent", spawn={"task": "clone", "count": count})
    def parent(task_id):
        return {"event_type": "parent"}

    @app.task(name="clone")
    def clone(task_id):
        return {"event_type": "clone"}

    @app.arrival_loop("a")
    def loop(context):
        entity = yield context.wait_for_arrival("a")
        context.spawn(parent(context.next_instance("parent"), entity=entity))

    records = run_builder(app, 1)
    assert times(records, "clone") == [0, 0]


# ---------------------------------------------------------------- on: (V4 to V6)


def test_on_fire_from_a_task_copies_its_finished_payload():
    """An `on:` `fire ... from` a task copies that task's finished payload onto the
    signal, so a later waiter receives it."""
    app = SimulationContext("S", factor=0.0)
    app.add_signal("ready")
    app.add_service("svc", dist="constant", value=1)

    @app.task(
        service_id="svc",
        on=[{"fire": {"signal": "ready", "from": "measure"}}],
        name="measure",
    )
    def measure(task_id):
        return {"grade": "A"}

    @app.task(
        wait={"signal": "ready"},
        name="watcher",
        next={
            "when": [{"condition": 'wait.ready.value.grade == "A"', "task": "noted"}]
        },
    )
    def watcher(task_id):
        return {"seen": task_id}

    @app.task(name="noted")
    def noted(task_id):
        return {"noted": task_id}

    def script(context):
        context.spawn(watcher(0))
        context.spawn(measure(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 5)
    # measure finishes at 1 and fires ready with its payload; watcher resumes and
    # routes to noted because the copied grade is A
    assert times(records, "noted") == [1.0]


def test_on_fire_with_nothing_to_copy_fires_nothing():
    """`fire ... from` a signal that has not fired yet fires nothing."""
    app = SimulationContext("S", factor=0.0)
    app.add_signal("ready")
    app.add_signal("empty")
    app.add_service("svc", dist="constant", value=1)

    @app.task(
        service_id="svc",
        on=[{"fire": {"signal": "ready", "from": "empty"}}],
        name="measure",
    )
    def measure(task_id):
        return {"grade": "A"}

    @app.task(wait={"signal": "ready"}, name="watcher")
    def watcher(task_id):
        return {"seen": task_id}

    def script(context):
        context.spawn(watcher(0))
        context.spawn(measure(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 5)
    # `empty` never fired, so `ready` is not fired and the watcher never resumes
    assert times(records, "watcher") == []


# ---------------------------------------------------------------- one-shot (V3)


def test_one_shot_signal_resumes_a_later_waiter_at_once():
    """A one-shot signal does not re-arm: a waiter that starts after the firing
    resumes at once."""
    app = SimulationContext("S", factor=0.0)
    app.add_signal("closing", once=True)
    app.add_service("svc", dist="constant", value=2)

    @app.task(service_id="svc", signal="closing", name="shut")
    def shut(task_id):
        return {"closed": task_id}

    @app.task(wait={"signal": "closing"}, name="late")
    def late(task_id):
        return {"after": task_id}

    def script(context):
        context.spawn(shut(0))
        yield context.env.timeout(5)
        context.spawn(late(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 8)
    # shut fires closing at 2; late starts at 5 and resumes at once, at 5
    assert times(records, "late") == [5.0]


def test_one_shot_signal_fired_twice_is_dropped_under_continue():
    """Under `continue`, a one-shot signal fired a second time is dropped, so a
    second firing does not resume a new waiter."""
    app = SimulationContext("S", factor=0.0)
    app.add_signal("closing", once=True)
    app.add_service("svc", dist="constant", value=1)

    @app.task(service_id="svc", signal="closing", name="shut")
    def shut(task_id):
        return {"n": task_id}

    def script(context):
        context.spawn(shut(0))
        yield context.env.timeout(3)
        context.spawn(shut(1))  # second firing: dropped
        yield context.env.event()

    app.add_process(script)
    # no waiter; the run reaches until with no error
    records = run_builder(app, 6, on_unhandled_failure="continue")
    assert times(records, "shut") == [1.0, 4.0]


# -------------------------------------------------------------------- running (P7)


def test_running_wait_resumes_when_the_named_instances_end():
    """A `running` wait on another task resumes when that task's instances on the
    same entity end, with their finished payload."""
    app = SimulationContext("S", factor=0.0)
    app.add_service("slow", dist="constant", value=5)

    @app.task(service_id="slow", name="charge")
    def charge(task_id):
        return {"charged": task_id}

    @app.task(wait={"running": {"task": "charge"}}, name="driver")
    def driver(task_id):
        return {"left": task_id}

    def script(context):
        entity = context._run.arrivals.new_entity("car", None)
        # start charge on the entity, then a driver that waits for it
        context.spawn(charge(context.next_instance("charge"), entity=entity))
        context.spawn(driver(context.next_instance("driver"), entity=entity))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 10)
    # charge finishes at 5; the driver's running wait resumes then
    assert times(records, "driver") == [5.0]


# -------------------------------------------------------------- subscribe (U2)


def test_subscribe_interrupts_when_the_event_happens():
    """A `subscribe:` key interrupts the instance when the subscribed task ends."""
    app = SimulationContext("S", factor=0.0)
    app.add_service("slow", dist="constant", value=3)
    app.add_service("long", dist="constant", value=20)

    @app.task(service_id="slow", name="charger")
    def charger(task_id):
        return {"charged": task_id}

    @app.task(
        service_id="long",
        subscribe={"task": "charger"},
        on_interrupt="abandon",
        name="waiting",
    )
    def waiting(task_id):
        return {"done": task_id}

    def script(context):
        entity = context._run.arrivals.new_entity("car", None)
        context.spawn(waiting(context.next_instance("waiting"), entity=entity))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 30)
    # the charger child flow ends at 3; the waiting task is interrupted then
    interrupted = [
        r["sim_ts"]
        for r in records
        if r["task"] == "waiting"
        and isinstance(r["value"], dict)
        and r["value"].get("status") == "interrupted"
    ]
    assert interrupted == [3.0]


# --------------------------------------------------------- process expression (E3)


def test_process_name_and_target_in_an_expression():
    """`process.name` and `process.target` read the running task and what it waits
    on, routing `next`."""
    app = SimulationContext("S", factor=0.0)
    app.add_service("svc", dist="constant", value=1)

    @app.task(
        service_id="svc",
        name="step",
        next={
            "when": [{"condition": 'process.name == "step"', "task": "same"}],
            "default": "other",
        },
    )
    def step(task_id):
        return {"n": task_id}

    @app.task(name="same")
    def same(task_id):
        return {"same": task_id}

    @app.task(name="other")
    def other(task_id):
        return {"other": task_id}

    def script(context):
        context.spawn(step(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 5)
    assert times(records, "same") == [1.0]
    assert times(records, "other") == []


def test_process_target_of_a_task_waiting_for_its_put_names_the_store():
    """While a task waits for room for its own put, `process.target` reads
    `put.<store>`, as a wait branch's put does, so an interrupt can pick it."""
    app = SimulationContext("S", factor=0.0)
    app.add_store("shelf", current_cap=1, max_cap=1, items=["full"])

    @app.task(put={"store": "shelf"}, on_interrupt="abandon", name="pack")
    def pack(task_id):
        return {"sku": "box"}

    @app.task(
        interrupt={"task": "pack", "where": 'process.target == "put.shelf"'},
        name="check",
    )
    def check(task_id):
        return {"checked": task_id}

    def script(context):
        context.spawn(pack(0))
        yield context.env.timeout(2)
        context.spawn(check(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 5)
    assert times(records, "pack", "interrupted") == [2.0]


# ----------------------------------------------------------- timeout value (V11)


def test_timeout_value_reaches_the_waiter():
    """A `{timeout: {after, value}}` branch carries a value the waiter reads."""
    app = SimulationContext("S", factor=0.0)

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
        return {"held": task_id}

    @app.task(name="gave_up")
    def gave_up(task_id):
        return {"gaveup": task_id}

    def script(context):
        context.spawn(hold(0))
        yield context.env.event()

    app.add_process(script)
    records = run_builder(app, 5)
    assert times(records, "gave_up") == [2.0]


# -------------------------------------------------------- unhandled failure (V7)


def test_stop_policy_raises_a_root_task_failure():
    """Under `on_unhandled_failure='stop'`, a root task failing with no on_fail
    raises, so the run stops; under `continue` the run goes on."""
    import pytest

    from tests.conformance.harness import RecordingEgress

    def build():
        app = SimulationContext("S", factor=0.0)

        @app.task(fail="t >= 0", fail_reason="scratched", name="broken")
        def broken(task_id):
            return {"n": task_id}

        def script(context):
            context.spawn(broken(0))
            yield context.env.event()

        app.add_process(script)
        return app

    stop_app = build()
    stop_app.add_egress(RecordingEgress())
    with pytest.raises(Exception, match="scratched"):
        stop_app.run(until=5, on_unhandled_failure="stop")

    cont_app = build()
    cont_app.add_egress(RecordingEgress())
    cont_app.run(until=5, on_unhandled_failure="continue")  # no error

    default_app = build()
    default_app.add_egress(RecordingEgress())
    with pytest.raises(Exception, match="scratched"):
        default_app.run(until=5)  # the default is stop


def test_a_signal_is_one_shot_unless_told_otherwise():
    """`add_signal` with no `once` makes a one-shot signal, and `once=False` makes a
    signal that re-arms."""
    app = SimulationContext("S", factor=0.0)
    app.add_signal("one")
    app.add_signal("again", once=False)
    assert app._signals.declaration("one").once is True
    assert app._signals.declaration("again").once is False


# -------------------------------------------------- on: fire-from load checks (V4-V6)


def _yaml_error(tmp_path, text):
    """Loads a blueprint string and returns the BlueprintError message it raises."""
    import pytest

    from desflow.blueprint.loader import BlueprintError

    path = tmp_path / "bp.yaml"
    path.write_text(text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    return str(error.value)


def test_fire_from_a_task_on_another_flow_is_refused(tmp_path):
    """A `fire ... from` a task that never runs on the same entity is refused at
    load, in YAML, where the flow is known."""
    text = """
simulation: {sim_id: S, factor: 0}
signals: [ready]
arrivals:
  orders: {dist: constant, value: 1, spawn: pack}
  reports: {dist: constant, value: 1, spawn: measure}
tasks:
  pack:
    payload: {event_type: packed}
    on:
      - fire: {signal: ready, from: measure}
  measure:
    payload: {grade: A}
run: {until: 10}
"""
    message = _yaml_error(tmp_path, text)
    assert "never runs on the same entity" in message
    assert "name a task of the same flow" in message


def test_fire_from_the_same_flow_is_accepted(tmp_path):
    """A `fire ... from` a task on the same flow loads; a copy on one entity is
    meaningful."""
    import pytest

    from desflow.blueprint.loader import BlueprintError

    text = """
simulation: {sim_id: S, factor: 0}
signals: [ready]
arrivals:
  orders: {dist: constant, value: 1, spawn: pack}
tasks:
  pack:
    payload: {event_type: packed}
    next: measure
    on:
      - fire: {signal: ready, from: measure}
  measure:
    payload: {grade: A}
run: {until: 10}
"""
    path = tmp_path / "ok.yaml"
    path.write_text(text)
    try:
        SimulationContext.from_yaml(path)  # pack reaches measure via next: accepted
    except BlueprintError as error:  # pragma: no cover
        pytest.fail(f"a same-flow fire-from was refused: {error}")


def test_a_signal_on_action_fires_from_a_task_of_the_entity(tmp_path):
    """A signal's `on:` entry that fires from a task copies the payload of that
    task's last instance on the entity that fired the signal, so the waiter of the
    copied firing reads it."""
    path = tmp_path / "model.yaml"
    path.write_text(
        """
simulation: {sim_id: S, factor: 0, random_seed: 1}
signals:
  go: {on: [{fire: {signal: ready, from: measure}}]}
  ready: {}
arrivals:
  orders: {initial: 1, spawn: measure}
  watchers: {initial: 1, spawn: ship}
tasks:
  measure: {service_time: 1, payload: {grade: A}, next: ping}
  ping: {service_time: 1, signal: go, payload: {event_type: pinged}}
  ship:
    wait: {signal: ready}
    payload: {event_type: shipped, grade: {gen: expr, expr: wait.ready.value.grade}}
"""
    )

    class Records:
        def __init__(self):
            self.records = []

        def run(self, queue, *args, **kwargs):
            while (batch := queue.get()) is not None:
                self.records.extend(batch)

    app = SimulationContext.from_yaml(path)
    sink = Records()
    app.add_egress(sink)
    app.run(until=10)
    shipped = [
        (row["sim_ts"], row["value"])
        for row in sink.records
        if isinstance(row.get("value"), dict)
        and row["value"].get("event_type") == "shipped"
    ]
    assert shipped == [(2.0, {"event_type": "shipped", "grade": "A"})]


def test_a_fire_from_a_task_with_no_ended_instance_fires_nothing(tmp_path, caplog):
    """A `fire ... from` a task that has not ended on the entity yet has nothing to
    copy, so it fires nothing and logs a warning."""
    path = tmp_path / "model.yaml"
    path.write_text(
        """
simulation: {sim_id: S, factor: 0, random_seed: 1}
signals: [ready]
arrivals: {orders: {initial: 1, spawn: pack}}
tasks:
  pack:
    payload: {event_type: packed}
    next: measure
    on:
      - fire: {signal: ready, from: measure}
  measure:
    payload: {grade: A}
"""
    )
    app = SimulationContext.from_yaml(path)
    with caplog.at_level("WARNING", logger="desflow.core.context"):
        app.run(until=10)
    assert app.signal("ready").count == 0
    assert [r.getMessage() for r in caplog.records if "nothing" in r.getMessage()] == [
        "the on: action of task 'pack' fires 'ready' from 'measure', which has "
        "nothing to copy yet; it fires nothing"
    ]


def test_a_signal_declared_again_keeps_its_place_and_takes_the_new_settings():
    """A second `add_signal` of one name keeps one signal, with the settings of the
    second call."""
    app = SimulationContext("S", factor=0)
    app.add_signal("s")
    app.add_signal("s", once=False)
    app.start(until=1)
    assert list(app._signals.entries) == ["s"]
    assert app.signal("s").once is False


def test_fire_from_neither_signal_nor_task_is_refused(tmp_path):
    """A `fire ... from` a name that is neither a signal nor a task is refused."""
    text = """
simulation: {sim_id: S, factor: 0}
signals: [ready]
arrivals:
  orders: {dist: constant, value: 1, spawn: pack}
tasks:
  pack:
    payload: {event_type: packed}
    on:
      - fire: {signal: ready, from: nowhere}
run: {until: 10}
"""
    assert "neither a signal nor a task" in _yaml_error(tmp_path, text)


def test_fire_value_and_from_together_is_refused(tmp_path):
    """A fire action takes value or from, not both, in YAML and the builder."""
    import pytest

    text = """
simulation: {sim_id: S, factor: 0}
signals: [ready]
arrivals:
  orders: {dist: constant, value: 1, spawn: pack}
tasks:
  pack:
    payload: {event_type: packed}
    on:
      - fire: {signal: ready, value: 1, from: pack}
run: {until: 10}
"""
    assert "value or from, not both" in _yaml_error(tmp_path, text)

    app = SimulationContext("S", factor=0.0)
    with pytest.raises(ValueError, match="value or from, not both"):

        @app.task(
            on=[{"fire": {"signal": "ready", "value": 1, "from": "pack"}}], name="pack"
        )
        def pack(task_id):
            return {}


def test_subscribe_and_on_target_existence_refused_at_load(tmp_path):
    """A subscribe target, an on: fire signal and an on: interrupt task that do not
    exist are refused at load."""
    missing_subscribe = """
simulation: {sim_id: S, factor: 0}
arrivals:
  orders: {dist: constant, value: 1, spawn: pack}
tasks:
  pack:
    service: packing
    subscribe: {task: ghost}
    payload: {event_type: packed}
services: {packing: {dist: constant, value: 1}}
run: {until: 10}
"""
    assert "subscribes to task 'ghost'" in _yaml_error(tmp_path, missing_subscribe)

    missing_signal = """
simulation: {sim_id: S, factor: 0}
arrivals:
  orders: {dist: constant, value: 1, spawn: pack}
tasks:
  pack:
    payload: {event_type: packed}
    on:
      - fire: {signal: ghost}
run: {until: 10}
"""
    assert "fires signal 'ghost'" in _yaml_error(tmp_path, missing_signal)
