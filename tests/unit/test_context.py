import asyncio
import queue
import re
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from desflow import Entity, SimulationContext
from desflow.blueprint import BlueprintError
from desflow.core.expressions import Expression
from desflow.values import generator
from tests.conformance.harness import RecordingEgress, run_yaml_text


@pytest.mark.parametrize("setting", ["logical_start_time", "go_live_at"])
def test_a_number_is_refused_as_a_start_or_go_live_time(setting):
    with pytest.raises(
        ValueError,
        match=f"{setting} is 20260101 of type int, .*must be a datetime such as datetime",
    ):
        SimulationContext("S", **{setting: 20260101})


@pytest.mark.parametrize("setting", ["logical_start_time", "go_live_at"])
@pytest.mark.parametrize(
    "value,fragment",
    [
        ("numpy", "of type int64"),
        ("decimal", "of type Decimal"),
        ("hex text", "0x10"),
        ("date", "of type date"),
        ("text", "'hello'"),
    ],
)
def test_a_value_that_is_not_a_datetime_is_refused_as_a_start_or_go_live_time(
    setting, value, fragment
):
    from datetime import date
    from decimal import Decimal

    import numpy

    given = {
        "numpy": numpy.int64(20260101),
        "decimal": Decimal(5),
        "hex text": "0x10",
        "date": date(2026, 1, 1),
        "text": "hello",
    }[value]
    with pytest.raises(
        ValueError, match=f"{setting} is .*{re.escape(fragment)}.*must be a datetime"
    ):
        SimulationContext("S", **{setting: given})


def test_builder_state_management():
    """Validates that the fluent API correctly aggregates all configuration states."""
    app = (
        SimulationContext("TestSim", factor=0.0)
        .add_resource("packers", current_cap=1, max_cap=5)
        .add_container("wear_structural", current_cap=0.0, max_cap=100.0)  # NEW
        .add_variable("velocity", {"type": "abrupt", "value": 5.0})  # NEW
        .add_service("packing", dist="normal", mean=5.0, std=1.0)
        .add_arrival("standard", dist="exponential", rate=2.0)
        .with_batching(batch_size=100, flush_interval=0.5)
    )

    # Check discrete resources
    assert "packers" in app._holders.resources_config
    assert app._holders.resources_config["packers"].max_cap == 5

    # Check continuous containers (NEW)
    assert "wear_structural" in app._holders.containers_config
    assert app._holders.containers_config["wear_structural"].max_cap == 100.0

    # Check dynamic variables (NEW)
    assert "velocity" in app._variables_config
    assert app._variables_config["velocity"]["value"] == 5.0

    # Check statistical distributions
    assert "packing" in app._services_config
    assert app._services_config["packing"].mean == 5.0
    assert "standard" in app._arrivals.configs
    assert app._arrivals.configs["standard"].rate == 2.0

    # Check batching state
    assert app._sinks.batch_size == 100
    assert app._sinks.flush_interval == 0.5


def test_runtime_errors_before_build():
    """Ensures runtime helpers fail safely if called before .run()."""
    app = SimulationContext("TestSim")

    with pytest.raises(RuntimeError, match="not instantiated until context is run"):
        app.get_resource("packers")

    with pytest.raises(
        RuntimeError, match="Cannot spawn processes before context is run"
    ):
        app.spawn(MagicMock())

    with pytest.raises(
        RuntimeError, match="Cannot publish telemetry before context is run"
    ):
        app.publish("utilization", 95.0)


def test_env_property_exposes_environment():
    """Ensures the public env property guards the Builder Phase and exposes the environment after .run()."""
    app = SimulationContext("TestSim", factor=0.0)

    with pytest.raises(RuntimeError, match="not attached until context is run"):
        _ = app.env

    # Because factor=0.0, this executes instantly in wall-clock time.
    app.run(until=0.1)

    assert app.env is app._env


@patch("desflow.builder.context.DynamicRealtimeEnvironment")
def test_simulation_execution_wiring(MockEnv):
    """
    Validates the .run() orchestration logic without running a real SimPy loop.
    Ensures connectors and registry are wired correctly.
    """
    mock_env_instance = MockEnv.return_value
    mock_env_instance.now = 0  # finish() compares the clock with `until`
    # A container reads its capacities from the registry, and compares them
    mock_env_instance.registry.get.return_value.value = 100
    mock_ingress = MagicMock()
    mock_egress = MagicMock()

    app = (
        SimulationContext("TestSim", factor=0.0)
        .add_ingress(mock_ingress)
        .add_egress(mock_egress)
        .add_resource("packers", 1, 1)
        .add_container("wear", 0.0, 100.0)
    )

    app.run(until=10)

    # Validate that Engine was created and run
    MockEnv.assert_called_once_with(
        factor=0.0, strict=None, on_unhandled_failure="stop"
    )
    mock_env_instance.run.assert_called_once_with(until=10)
    mock_env_instance.teardown.assert_called_once()

    # Validate that connectors were passed to the engine
    mock_env_instance.setup_ingress.assert_called_once_with([mock_ingress])
    mock_env_instance.setup_egress.assert_called_once()


def test_go_live_at_switches_pacing_during_a_builder_run():
    """The backfill-then-live run: fast until the go-live instant, real time after it."""
    from datetime import datetime, timedelta

    start = datetime(2024, 1, 1, 12, 0, 0)
    app = SimulationContext(
        "TestSim",
        factor=0.0,
        logical_start_time=start,
        go_live_at=start + timedelta(seconds=0.5),
    )

    seen: list = []

    @app.telemetry_loop(interval=0.25)
    def watch(context):
        seen.append((round(context.env.now, 3), context.env.factor))

    app.run(until=1.0)

    before = [factor for sim_time, factor in seen if sim_time < 0.5]
    after = [factor for sim_time, factor in seen if sim_time >= 0.5]

    assert before and set(before) == {0.0}
    assert len(after) >= 2 and set(after) == {1.0}


def test_lightweight_integration():
    """
    A full fast-forward integration test to ensure decorators yield correctly,
    the sampler is exposed, and variables/containers can be fetched.
    """
    app = (
        SimulationContext("IntegrationSim", factor=0.0)
        .add_resource("packers", 1, 5)
        .add_variable("base_temp", 1200.0)
        .add_service("packing", "exponential", rate=10.0)
        .add_arrival("standard", "exponential", rate=5.0)
    )

    execution_state = {
        "tasks_completed": 0,
        "telemetry_run": False,
        "sampler_accessed": False,
    }

    @app.task(service_id="packing", resource_id="packers")
    def process_part(task_id):
        execution_state["tasks_completed"] += 1
        return {"status": "ok"}

    @app.arrival_loop("standard")
    def arrival_generator(context):
        # Verify the sampler is exposed and accessible to raw generators
        noise = context.sampler.rng.normal(0, 1)
        if noise is not None:
            execution_state["sampler_accessed"] = True

        yield context.wait_for_arrival("standard")
        context.spawn(process_part(1))

    @app.telemetry_loop(interval=1.0)
    def telemetry_generator(context):
        # Verify variables made it into the switchboard
        temp = context._env.registry.get("IntegrationSim.variables.base_temp").value
        assert temp == 1200.0
        execution_state["telemetry_run"] = True

    # Run for a short duration.
    # Because factor=0.0, this executes instantly in wall-clock time.
    app.run(until=2)

    # If the simulation ran correctly, all loops should have fired successfully
    assert execution_state["tasks_completed"] > 0
    assert execution_state["telemetry_run"] is True
    assert execution_state["sampler_accessed"] is True


def test_with_batching_defaults_the_egress_bounds():
    """A caller that ignores the new settings still gets the safe defaults."""
    app = SimulationContext("TestSim", factor=0.0).with_batching(
        batch_size=100, flush_interval=0.5
    )

    assert app._sinks.max_queued_batches == 2000
    assert app._sinks.drain_stall_seconds == 30.0


@patch("desflow.builder.context.DynamicRealtimeEnvironment")
def test_egress_bounds_reach_the_environment(MockEnv):
    """What the builder collects has to arrive at setup_egress, or it does nothing."""
    instance = MockEnv.return_value
    instance.now = 0  # finish() compares the clock with `until`
    provider = MagicMock()

    app = (
        SimulationContext("TestSim", factor=0.0)
        .add_egress(provider)
        .with_batching(
            batch_size=10,
            flush_interval=1.0,
            max_queued_batches=64,
            drain_stall_seconds=3.0,
        )
    )
    app.run(until=1)

    _, kwargs = instance.setup_egress.call_args
    assert kwargs["max_queued_batches"] == 64
    assert kwargs["drain_stall_seconds"] == 3.0


@patch("desflow.builder.context.DynamicRealtimeEnvironment")
def test_per_provider_cadence_reaches_the_environment(MockEnv):
    """A cadence given on add_egress has to arrive, aligned by position."""
    instance = MockEnv.return_value
    instance.now = 0  # finish() compares the clock with `until`

    app = (
        SimulationContext("TestSim", factor=0.0)
        .add_egress(MagicMock(), batch_size=5, flush_interval=0.25)
        .add_egress(MagicMock())
        .with_batching(batch_size=100, flush_interval=2.0)
    )
    app.run(until=1)

    _, kwargs = instance.setup_egress.call_args
    # None is the second sink saying nothing, which setup_egress reads as the default.
    assert kwargs["batch_sizes"] == [5, None]
    assert kwargs["flush_intervals"] == [0.25, None]


class _CountingEgress:
    """Records the size of every batch it is handed, so sinks can be compared."""

    def __init__(self):
        self.batch_sizes: list = []

    async def run(self, egress_queue):
        try:
            while True:
                try:
                    self.batch_sizes.append(len(egress_queue.get_nowait()))
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


def test_per_provider_cadence_takes_effect_through_the_builder():
    """The whole path, not just the call: a small sink flushes while a large one waits."""
    small = _CountingEgress()
    large = _CountingEgress()

    app = (
        SimulationContext("TestSim", factor=0.0)
        .add_egress(small, batch_size=2)
        .add_egress(large, batch_size=10**6)
        .with_batching(batch_size=10**6, flush_interval=1.0)
    )

    @app.telemetry_loop(interval=1.0)
    def emit(context):
        context._env.publish_event("task", {"n": 1})

    app.run(until=6)

    # The small sink flushed on its own size during the run; the large one held
    # everything until teardown, so it saw one batch rather than several.
    assert len(small.batch_sizes) > 1
    assert set(small.batch_sizes) == {2}
    assert len(large.batch_sizes) == 1


def test_add_arrival_std_reaches_the_registry():
    """The spread is registered, so it can be changed while the run is live."""
    app = SimulationContext("TestSim", factor=0.0).add_arrival(
        "batch", dist="lognormal", mean=4.0, std=0.5
    )
    app.run(until=1)

    assert app.env.registry.get("TestSim.arrival.batch.std").value == 0.5


def test_add_process_starts_the_generator_with_the_context_and_kwargs():
    seen = []

    def drift(context, step):
        while True:
            yield context.env.timeout(step)
            seen.append(context.env.now)

    app = SimulationContext("TestSim", factor=0.0).add_process(drift, step=2.0)
    app.run(until=7)

    assert seen == [2.0, 4.0, 6.0]


def test_a_task_without_service_or_resource_emits_its_payload_at_once():
    records = []

    class Capture:
        async def run(self, egress_queue):
            while True:
                try:
                    records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)

    app = SimulationContext("TestSim", factor=0.0).add_egress(Capture())

    @app.task()
    def tick(task_id):
        return {"tick": task_id}

    def spawner(context):
        yield context.env.timeout(3.0)
        context.spawn(tick(7))

    app.add_process(spawner)
    app.run(until=5)

    events = [r for r in records if r["stream_type"] == "event"]
    # Called without an entity, the task starts one keyed by its name and a counter
    assert [
        (r["sim_ts"], r["key"], r["task"], r["instance"], r["value"]) for r in events
    ] == [(3.0, "tick-1", "tick", 7, {"tick": 7})]


def test_a_resource_needs_a_service():
    with pytest.raises(ValueError, match="a task with a resource needs a service"):
        SimulationContext("TestSim").task(resource_id="r")


def test_add_service_refuses_an_invalid_parameter():
    """A bad builder value fails when it is added, not during the run."""
    with pytest.raises(ValueError, match="std"):
        SimulationContext("TestSim").add_service("packing", "normal", mean=1.0, std=-1)
    with pytest.raises(ValueError, match="rate"):
        SimulationContext("TestSim").add_arrival("standard", "exponential")


def test_a_store_get_rejects_a_priority():
    """A priority orders a put into a priority store, so a get must not carry one."""
    app = SimulationContext("TestSim", factor=0.0).add_store(
        "buffer", current_cap=1, max_cap=1, priority=True
    )
    with pytest.raises(ValueError, match="a get takes no priority"):
        app.task(name="drain", get={"store": "buffer", "priority": 1})


def test_initial_store_items_are_checked_when_the_store_is_added():
    app = SimulationContext("TestSim", factor=0.0)
    with pytest.raises(ValueError, match="cannot start with 2"):
        app.add_store("parcels", current_cap=1, max_cap=1, items=["a", "b"])
    with pytest.raises(ValueError, match="pairs"):
        app.add_store("express", current_cap=2, max_cap=2, priority=True, items=["a"])


def test_filters_and_patience_are_checked_before_the_run():
    """A filter needs a store without priority; patience needs something to wait
    for; item.<field> needs a store get before it."""
    app = SimulationContext("TestSim", factor=0.0)
    app.add_store("express", current_cap=2, max_cap=2, priority=True)

    @app.task(name="pick", get={"store": "express", "filter": "item.size == 1"})
    def pick(task_id):
        return {}

    with pytest.raises(ValueError, match="both ranks and filters"):
        app.run(until=1)
    with pytest.raises(ValueError, match="needs a wait, a seize, a resource or a get"):
        app.task(name="idle", patience=5)
    with pytest.raises(ValueError, match="a put takes no filter"):
        app.task(name="drop", put={"store": "express", "filter": "item.size == 1"})

    other = SimulationContext("TestSim", factor=0.0)

    @other.task(
        name="ship", next={"when": [{"condition": "item.size > 1", "task": "ship"}]}
    )
    def ship(task_id):
        return {}

    with pytest.raises(ValueError, match="no store get before it"):
        other.run(until=1)


def test_patience_on_a_get_takes_nothing_after_it_runs_out():
    """The get leaves at 2; the parcel put at 3 stays in the store."""
    app = SimulationContext("TestSim", factor=0.0)
    app.add_store("parcels", current_cap=1, max_cap=1)
    app.with_stats("basic")

    @app.task(name="collect", get={"store": "parcels"}, patience=2)
    def collect(task_id):
        return {}

    @app.task(name="drop", put={"store": "parcels"})
    def drop(task_id):
        return {"parcel": 1}

    def script(context):
        context.spawn(collect(0))
        yield context.env.timeout(3)
        context.spawn(drop(0))

    app.add_process(script)
    app.run(until=4)
    assert app.get_store("parcels").items == [{"parcel": 1}]
    assert app.env.stats.readings()["TestSim.stats.tasks.collect.reneged"] == 1


def test_waits_signals_and_failures_are_checked_before_the_run():
    """Every signal, waited task, branch read and on_fail target must exist."""

    def run(**task):
        app = SimulationContext("TestSim", factor=0.0).add_signal("ready")

        @app.task(name="t", **task)
        def t(task_id):
            return {}

        app.run(until=1)

    for task, fragment in (
        ({"signal": "missing"}, "signal 'missing'"),
        ({"wait": {"signal": "missing"}}, "signal 'missing'"),
        ({"wait": {"task": "missing"}}, "task 'missing'"),
        ({"fail": "t > 1", "on_fail": "missing"}, "task 'missing'"),
        (
            {
                "wait": {"timeout": 1},
                "next": {"when": [{"condition": "wait.x.done", "task": "t"}]},
            },
            "wait.x",
        ),
    ):
        with pytest.raises(ValueError, match=fragment):
            run(**task)
    with pytest.raises(ValueError, match="fail_reason needs a fail condition"):
        SimulationContext("TestSim").task(fail_reason="late")


def test_a_raising_function_fails_the_task_and_ends_the_flow():
    """The builder treats an error in a task's function as the task failing."""
    app = SimulationContext("TestSim", factor=0.0).with_stats("basic")

    @app.task(name="t")
    def t(task_id):
        raise KeyError("sku")

    def script(context):
        context.spawn(t(0))
        yield context.env.timeout(1)

    app.add_process(script)
    app.run(until=2, on_unhandled_failure="continue")
    readings = app.env.stats.readings()
    assert readings["TestSim.stats.tasks.t.failed"] == 1
    assert readings["TestSim.stats.flows.t.failed"] == 1


def _two_resources():
    return (
        SimulationContext("TestSim", factor=0.0)
        .add_resource("picker", current_cap=1, max_cap=1)
        .add_resource("bench", current_cap=1, max_cap=1)
        .add_service("s", "constant", value=1.0)
    )


def test_resources_acquired_in_opposite_orders_are_warned_about(caplog):
    """Two tasks that each hold one resource while waiting for the other could wait
    for ever, so the model gets a warning, also through a keep handoff, and runs as
    SimPy runs it."""
    app = _two_resources()

    @app.task(service_id="s", resource_id=["picker", "bench"], name="a")
    def a(task_id):
        return {}

    @app.task(service_id="s", resource_id=["bench", "picker"], name="b")
    def b(task_id):
        return {}

    with caplog.at_level("WARNING"):
        app.run(until=1)
    assert caplog.text.count("in one order everywhere") == 1
    assert "task 'a' acquires 'picker' before 'bench'" in caplog.text
    caplog.clear()

    handoff = _two_resources()

    @handoff.task(
        service_id="s", resource_id="bench", keep=["bench"], next="c", name="d"
    )
    def d(task_id):
        return {}

    @handoff.task(service_id="s", resource_id="picker", name="c")
    def c(task_id):
        return {}

    @handoff.task(service_id="s", resource_id=["picker", "bench"], name="e")
    def e(task_id):
        return {}

    with caplog.at_level("WARNING"):
        handoff.run(until=1)
    assert "task 'd' keeping it for 'c' acquires 'bench' before 'picker'" in caplog.text


def test_a_kept_unit_is_released_once_the_next_task_starts():
    """pick keeps the picker for pack, which releases it when it starts; a second
    order's pick waits for it until then."""
    app = _two_resources()
    starts = []

    @app.task(
        service_id="s", resource_id="picker", keep=["picker"], next="pack", name="pick"
    )
    def pick(task_id):
        starts.append(("pick", task_id, app.env.now))
        return {}

    @app.task(service_id="s", resource_id="bench", name="pack")
    def pack(task_id):
        starts.append(("pack", task_id, app.env.now))
        return {}

    def script(context):
        context.spawn(pick(0))
        context.spawn(pick(1))
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=10)
    # pick 0 runs 0-1 and keeps the picker; pack 0 starts at 1 and releases it, so
    # pick 1 starts at 1 too
    assert starts == [
        ("pick", 0, 0.0),
        ("pack", 0, 1.0),
        ("pick", 1, 1.0),
        ("pack", 1, 2.0),
    ]


def test_interrupt_reaches_only_the_running_instances_its_condition_names():
    app = SimulationContext("TestSim", factor=0.0)
    app.add_service("s", "constant", value=10.0)

    @app.task(service_id="s", name="work", on_interrupt="abandon")
    def work(task_id, entity):
        return {}

    counts = []

    def script(context):
        for n in range(3):
            context.spawn(work(n, entity=Entity(f"o-{n}", "o", {"n": n})))
        yield context.env.timeout(1)
        counts.append(context.interrupt("work", where="attr.n >= 1", cause="recall"))
        yield context.env.timeout(1)
        counts.append(context.interrupt("work"))

    app.add_process(script)
    app.run(until=20)
    assert counts == [2, 1]


def test_a_task_interrupting_its_own_name_skips_itself():
    """`interrupt` never reaches the caller's own instance, so a task that interrupts
    its own name from inside its function counts zero and runs on."""
    app = SimulationContext("TestSim", factor=0.0)
    app.add_service("s", "constant", value=1.0)
    counts = []
    reached = []

    @app.task(service_id="s", name="work", on_interrupt="abandon")
    def work(task_id, entity):
        counts.append(app.interrupt("work", cause="self"))
        reached.append(task_id)
        return {}

    def script(context):
        context.spawn(work(0, entity=Entity("o-0", "o", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=5)
    assert counts == [0]
    assert reached == [0]


def test_preemption_settings_are_checked():
    with pytest.raises(ValueError, match="on_interrupt must be one of"):
        SimulationContext("TestSim").task(on_interrupt="pause")
    with pytest.raises(ValueError, match="list only resources in its resource_id"):
        SimulationContext("TestSim").task("s", "m", keep=["bench"])


def test_stop_and_drain_are_checked_when_the_run_starts():
    app = SimulationContext("TestSim", factor=0.0)
    with pytest.raises(ValueError, match="needs until"):
        app.run(drain=True)
    with pytest.raises(
        ValueError,
        match=r"signal 'closing', which is not added; add it with "
        r"add_signal\('closing'\)",
    ):
        SimulationContext("TestSim", factor=0.0).run(
            until=1, stop={"signal": "closing"}
        )
    closing = SimulationContext("TestSim", factor=0.0).add_signal("closing")
    with pytest.raises(ValueError, match="did you mean 'closing'"):
        closing.run(until=1, stop={"signal": "closng"})
    with pytest.raises(ValueError, match="drain takes a limit, not limt; did you"):
        SimulationContext("TestSim", factor=0.0).run(until=1, drain={"limt": 5})
    for bad in ("false", 1, [5]):
        with pytest.raises(ValueError, match="drain is True, False or {'limit'"):
            SimulationContext("TestSim", factor=0.0).run(until=1, drain=bad)
    with pytest.raises(ValueError, match="stop takes a signal or a condition"):
        SimulationContext("TestSim", factor=0.0).run(until=1, stop={"event": "x"})


def test_an_event_is_not_an_until_of_run_or_start():
    """`until` is seconds or a duration; an event is refused with a message saying
    so, not with a `TypeError` from a comparison."""
    import simpy

    event = simpy.Environment().event()
    with pytest.raises(ValueError, match="until is seconds or a duration"):
        SimulationContext("TestSim", factor=0.0).start(until=event)
    with pytest.raises(ValueError, match="until is seconds or a duration"):
        SimulationContext("TestSim", factor=0.0).run(until=event)


def test_balk_with_an_unknown_level_fails_before_the_run():
    """A balk expression is checked before the clock starts, like a priority."""
    app = (
        SimulationContext("TestSim", factor=0.0)
        .add_resource("m", current_cap=1, max_cap=1)
        .add_service("s", "constant", value=1.0)
    )

    @app.task(service_id="s", resource_id="m", name="t", balk="level.nope > 0")
    def t(task_id):
        return {}

    with pytest.raises(ValueError, match="level.nope"):
        app.run(until=1)


class _Sink:
    """An egress that discards what it receives."""

    async def run(self, egress_queue):
        while True:
            try:
                egress_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.01)


def test_a_bad_until_stops_the_run_before_any_connector_starts():
    """until is read before the egress thread starts, so a bad one leaves none."""
    app = SimulationContext("Shop", factor=0).add_egress(_Sink())
    with pytest.raises(ValueError):
        app.run(until="ten minutes")
    assert app._env is None


def test_a_failed_start_tears_the_connectors_down():
    """An error after the connectors start stops their thread, as run() would."""
    app = SimulationContext("Shop", factor=0).add_egress(_Sink())
    app.add_resource("packers", current_cap=1, max_cap=1)
    with patch(
        "desflow.builder.holders.DynamicResource", side_effect=RuntimeError("broken")
    ):
        with pytest.raises(RuntimeError, match="broken"):
            app.run(until=10)
    thread = app._env._egress_thread
    thread.join(timeout=5)
    assert not thread.is_alive()


@pytest.mark.parametrize(
    "start, error, message",
    [
        ({"seed": "x"}, ValueError, "seed is a whole number of 0 or more"),
        (
            {"until": 1, "overrides": {"Shop.resources.packers.current_cap": -1}},
            ValueError,
            "cannot be set to -1",
        ),
    ],
)
def test_a_start_that_fails_before_it_makes_the_holders_leaves_none_to_get(
    start, error, message
):
    """After a good run, a start that fails before it makes the run's holders
    leaves none, and each getter raises KeyError with the name, as for a name the
    model does not have."""
    app = SimulationContext("Shop", factor=0)
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_store("shelf", current_cap=2)
    app.add_container("tank", current_cap=5)
    app.run(until=1)
    assert app.get_resource("packers") is not None
    with pytest.raises(error, match=message):
        app.start(**start)
    for get, name in [
        (app.get_resource, "packers"),
        (app.get_store, "shelf"),
        (app.get_container, "tank"),
    ]:
        with pytest.raises(KeyError) as raised:
            get(name)
        assert raised.value.args == (name,)


def test_a_context_runs_twice_with_a_drain():
    """A second run starts with no entity in the system and no drain under way, so
    a drain after a run that left orders in the shop ends when its own orders
    leave."""
    app = SimulationContext("Shop", factor=0, random_seed=1)
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("packing", dist="constant", value=2)
    app.add_arrival("orders", dist="constant", value=1)

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id):
        return {"event": "packed"}

    @app.arrival_loop("orders")
    def loop(context):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack(context.next_instance("pack"), entity=order))

    # The first run ends with orders still in the shop; the second drains them
    app.run(until=10)
    app.run(until=10, drain=True)
    assert app.env.now > 10


def test_a_second_run_keeps_no_flow_or_held_request_of_the_first(tmp_path):
    """A second run of one context starts with none of the first run's instances of
    a flow another entity waits for whole, and none of the requests a `release:
    {task}` reaches, so it gives the records of the first run."""
    path = tmp_path / "model.yaml"
    path.write_text(
        "simulation: {sim_id: Twice, factor: 0, random_seed: 42}\n"
        "resources: {m: {current_cap: 1}}\n"
        "arrivals:\n"
        "  a: {initial: 1, spawn: relay}\n"
        "  w: {initial: 1, spawn: watch}\n"
        "  holder: {initial: 1, spawn: hold}\n"
        "  third: {initial: 1, spawn: free_it}\n"
        "tasks:\n"
        "  relay: {service_time: 3, payload: {event_type: relay}, next: more}\n"
        "  more: {service_time: 3, payload: {event_type: more}}\n"
        "  watch: {wait: {running: {task: relay, whole_flow: true}},"
        " payload: {event_type: flow_done, at: {gen: expr, expr: t}}}\n"
        "  hold: {seize: m, service_time: 5, payload: {event_type: held},"
        " release: m}\n"
        "  free_it: {service_time: 1, payload: {event_type: freed},"
        " release: {resource: m, task: hold}}\n"
    )
    app = SimulationContext.from_yaml(path)
    sink = _Records()
    app.add_egress(sink)
    runs = []
    for _ in range(2):
        sink.records.clear()
        app.run(until=20)
        runs.append(
            [(row["sim_ts"], row["value"]) for row in sink.records if "value" in row]
        )
        stale = [
            request
            for listed in app._run.held_by_task.values()
            for request in listed
            if request.resource.env is not app.env
        ]
        assert stale == []
    assert runs[0] == runs[1]
    assert any(value.get("event_type") == "flow_done" for _, value in runs[1])


def test_the_requests_a_release_task_reaches_drop_those_that_reneged(tmp_path):
    """The requests a `release: {task}` reaches are listed by task, and the list drops
    those that no longer hold or wait once it reaches 64: of 100 requests that
    reneged, the 36 made after the drop are left, and the drop length stays 64."""
    path = tmp_path / "model.yaml"
    path.write_text(
        "simulation: {sim_id: Prune, factor: 0, random_seed: 1}\n"
        "resources: {m: {current_cap: 1}}\n"
        "arrivals:\n"
        "  b: {initial: 1, spawn: block}\n"
        "  h: {gap: 1, max_events: 100, spawn: hold}\n"
        "  f: {first_at: 500, max_events: 1, gap: 1000, spawn: free_it}\n"
        "tasks:\n"
        "  block: {seize: m, service_time: 1000, payload: {event_type: blocked},"
        " release: m}\n"
        "  hold: {resource: m, patience: 0.5, service_time: 1,"
        " payload: {event_type: held}}\n"
        "  free_it: {payload: {event_type: freed},"
        " release: {resource: m, task: hold}}\n"
    )
    app = SimulationContext.from_yaml(path)
    app.run(until=200)
    assert len(app._run.held_by_task["hold"]) == 36
    assert app._run.held_prune == {"hold": 64}


def test_a_variable_that_cannot_be_copied_runs_twice():
    """A run, and the start check of a model with a scenario, copy the config objects
    they write into, not a variable's value, so a builder variable that holds a lock
    runs, twice, and holds the user's own object."""
    import threading

    lock = threading.Lock()
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=1)
        .add_resource("p", current_cap=1)
        .add_service("s", dist="constant", value=1)
        .add_arrival("a", dist="constant", value=2)
        .add_variable("guard", lock)
        .add_scenario([{"at": 1, "path": "Shop.resources.p.current_cap", "value": 2}])
    )
    for _ in range(2):
        app.run(until=5)
        assert app.env.registry.get("Shop.variables.guard").value is lock


def test_a_sim_id_with_a_dot_draws_what_a_plain_one_draws(tmp_path):
    """A distribution's random sequence is named by its path with the sim id taken off
    the front, so `Shop.v2` and `Shop` draw the same times with the same seed."""

    def times(sim_id):
        path = tmp_path / f"{sim_id}.yaml"
        path.write_text(
            f"simulation: {{sim_id: {sim_id}, factor: 0, random_seed: 7}}\n"
            "resources: {desk: {current_cap: 1}}\n"
            "services: {s: {dist: exponential, rate: 1}}\n"
            "arrivals: {a: {dist: exponential, rate: 0.5, spawn: work}}\n"
            "tasks: {work: {resource: desk, service: s, payload: {e: work}}}\n"
            "run: {until: 20}\n"
        )
        app = SimulationContext.from_yaml(path)
        sink = _Records()
        app.add_egress(sink)
        app.run()
        assert app.env.registry.get_config(f"{sim_id}.service.s").stream == "service.s"
        return [
            (row["sim_ts"], row["key"], row["value"].get("status"))
            for row in sink.records
            if row["stream_type"] == "event"
        ]

    plain = times("Shop")
    assert plain and times("Shop.v2") == plain


def test_a_run_with_an_ingress_and_a_sink_ends_at_the_last_model_event():
    """With an ingress or a sink and no `until`, the run ends when no model event is
    left, as SimPy's `run()` does. At factor 0 the ingress monitor keeps scheduling
    events of desflow's own, which do not hold the run open. Teardown still writes the
    records."""
    from desflow import LocalIngress

    class _Keep:
        def __init__(self):
            self.records = []

        async def run(self, egress_queue):
            while True:
                try:
                    self.records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)

    kept = _Keep()
    app = SimulationContext("Shop", factor=0.0)
    app.add_ingress(LocalIngress([]))
    app.add_egress(kept)

    def model(context):
        yield context.env.timeout(5)
        context.env.publish_event("job-1", {"status": "done"})

    app.add_process(model)
    app.run()  # no until

    assert app.env.now == 5
    assert any(r.get("key") == "job-1" for r in kept.records)


def test_a_drain_ends_before_a_telemetry_reading_due_as_the_last_entity_leaves():
    """The drain end is urgent, so when the last entity leaves at the instant a
    periodic reading is due, the run ends before that reading; only the teardown
    reading lands at that instant."""
    app = SimulationContext("Shop", factor=0.0).add_egress(_Sink())
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("packing", dist="constant", value=2.0)
    app.with_stats("basic", interval=1.0)

    @app.task(service_id="packing", resource_id="packers", name="pack")
    def pack(task_id):
        return {"event": "packed"}

    def script(context):
        context.spawn(pack(0, entity=Entity("o-0", "o", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    env = app.start(until=0.5, drain=True)
    times = []
    original = env.stats.publish

    def record():
        times.append(round(env.now, 3))
        return original()

    env.stats.publish = record
    app.finish()

    # The entity leaves at 2.0, exactly when the third reading is due; the service
    # completion was enqueued at 0, before that reading was enqueued at 1.0, so the
    # drain end preempts it. The reading at 1.0 and the teardown reading at 2.0 remain.
    assert env.now == 2.0
    assert times == [1.0, 2.0]


def test_scenario_steps_and_warmup_count_from_the_start_of_the_run():
    """With an initial time of 100, a step at 10 applies at 110, and a warm-up of
    5 drops the records before 105."""
    records = []

    class Keep:
        async def run(self, egress_queue):
            while True:
                try:
                    records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)

    app = SimulationContext("Shop", factor=0, initial_time=100, warmup=5)
    app.add_egress(Keep())
    app.add_variable("speed", 1)
    app.add_step(10, path="Shop.variables.speed", value=2)

    def watch(context):
        while True:
            yield context.env.timeout(5)
            context.publish(
                "speed", context.env.registry.get("Shop.variables.speed").value
            )

    app.add_process(watch)
    app.run(until=130)
    speeds = [
        (r["sim_ts"], r["value"]) for r in records if r.get("path_id") == "Shop.speed"
    ]
    assert speeds[:2] == [(105.0, 1), (110.0, 2)]
    assert min(r["sim_ts"] for r in records) >= 105


def test_an_until_at_or_before_the_initial_time_is_refused():
    """SimPy refuses run(until=t) with t not after now; desflow refuses it too, with
    stop or drain as well, before anything is built."""
    for kwargs in ({}, {"drain": True}, {"stop": {"condition": "t > 1"}}):
        app = SimulationContext("Shop", factor=0, initial_time=100)
        with pytest.raises(ValueError, match="until \\(100\\) must be greater"):
            app.run(until=100, **kwargs)
        assert app._env is None


def test_a_stop_condition_is_checked_after_a_finished_task():
    """A timed step ends at 5 and changes no level or capacity; the stop condition
    `t >= 5` is checked then, so the run ends at 5, not at until."""
    app = SimulationContext("Shop", factor=0)
    app.add_service("wait", dist="constant", value=5)

    @app.task(service_id="wait")
    def step(task_id):
        return {"event": "done"}

    def script(context):
        context.spawn(step(0))
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=100, stop={"condition": "t >= 5"})
    assert app.env.now == 5


@pytest.mark.parametrize("condition", ["running(work) == 0 and t > 0", "peek() > 4"])
def test_a_stop_condition_reads_running_and_peek(tmp_path, condition):
    """A stop condition reads `running()` and `peek()` as a task's expressions do.
    The three orders finish at 5, when no `work` runs and the next event is due at
    5, so either condition ends the run at 5, before the order at 50."""
    path = tmp_path / "model.yaml"
    path.write_text(
        "simulation: {sim_id: Stop, factor: 0, random_seed: 1}\n"
        "arrivals:\n"
        "  a: {initial: 3, spawn: work}\n"
        "  later: {dist: constant, value: 50, spawn: work}\n"
        "tasks:\n"
        "  work: {service_time: 5, payload: {event_type: done}}\n"
        f'run: {{until: 100, stop: {{condition: "{condition}"}}}}\n'
    )
    app = SimulationContext.from_yaml(path)
    app.run()
    assert app.env.now == 5


def test_a_preempt_rule_reads_running(tmp_path):
    """A preempt rule reads `running()`: with two `job` instances running or
    waiting, the order of rank 1 takes the unit from the one of rank 5."""
    path = tmp_path / "model.yaml"
    path.write_text(
        "simulation: {sim_id: Rule, factor: 0, random_seed: 1}\n"
        "resources:\n"
        "  m: {current_cap: 1, queue: {rank: attr.rank},"
        ' preempt: {when: "running(job) > 1"}}\n'
        "arrivals:\n"
        "  a: {dist: constant, value: 1, spawn: job, max_events: 1, first_at: 0,"
        " attributes: {rank: 5}}\n"
        "  b: {dist: constant, value: 1, spawn: job, max_events: 1, first_at: 1,"
        " attributes: {rank: 1}}\n"
        "tasks:\n"
        "  job: {resource: m, preempt: true, service_time: 5, on_interrupt: abandon,"
        " payload: {event_type: done}}\n"
        "run: {until: 30}\n"
    )
    app = SimulationContext.from_yaml(path)
    sink = _Records()
    app.add_egress(sink)
    app.run()
    preempted = [
        row["key"]
        for row in sink.records
        if row.get("value", {}).get("status") == "preempted"
    ]
    assert preempted == ["a-1"]


def test_an_error_in_an_arrival_loop_stops_the_run():
    """Only the interrupt a drain sends ends an arrival loop quietly; any other
    error in the loop reaches the caller, as an error in a SimPy process does."""
    app = SimulationContext("Shop", factor=0)
    app.add_arrival("orders", dist="constant", value=1)

    @app.arrival_loop("orders")
    def loop(context):
        yield context.wait_for_arrival("orders")
        raise RuntimeError("the loop broke")

    with pytest.raises(RuntimeError, match="the loop broke"):
        app.run(until=10)


def test_the_builder_stop_takes_a_signal_name_or_a_function():
    """A stop given by the signal's name or by a function of the context ends the
    run when its event happens; the function is called once the run exists."""
    for stop in ("closing", lambda context: context.env.timeout(7)):
        app = SimulationContext("Shop", factor=0)
        app.add_signal("closing")

        def close_at_7(context):
            yield context.env.timeout(7)
            context.signal("closing").fire(None)

        app.add_process(close_at_7)
        app.add_arrival("orders", dist="constant", value=1)
        app.run(until=100, stop=stop)
        assert app.env.now == 7


def test_a_second_run_numbers_entities_and_instances_from_the_start():
    """Two runs of one context with the same seed give the same records: entity
    keys and instance ids start again at orders-1 and 0."""
    records = []

    class Keep:
        async def run(self, egress_queue):
            while True:
                try:
                    records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)

    app = SimulationContext("Shop", factor=0, random_seed=3).add_egress(Keep())
    app.add_arrival("orders", dist="exponential", rate=1.0)

    @app.task()
    def take(task_id):
        return {"event": "taken"}

    @app.arrival_loop("orders")
    def loop(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(take(context.next_instance("take"), entity=entity))

    runs = []
    for _ in range(2):
        records.clear()
        app.run(until=5)
        runs.append([(r["key"], r["instance"]) for r in records])
    assert runs[0] and runs[0] == runs[1]


def test_starting_again_closes_the_run_still_open():
    """start() on a run not closed closes it first, so its egress thread stops;
    close() leaves `started` False."""
    app = SimulationContext("Shop", factor=0).add_egress(_Sink())
    app.start(until=10)
    first = app._env
    app.start(until=10)
    first._egress_thread.join(timeout=5)
    assert not first._egress_thread.is_alive()
    assert app.started
    app.close()
    assert not app.started


def test_the_builder_refuses_patience_reading_the_wait():
    """patience is read before the wait starts, so wait.<branch> in it is
    refused."""
    app = SimulationContext("Shop", factor=0)

    @app.task(
        wait={"any": {"stock": {"timeout": 5}}},
        patience="1 if wait.stock.done else 5",
    )
    def order(task_id):
        return {}

    with pytest.raises(ValueError, match="reads wait.stock"):
        app.run(until=10)


def test_equal_time_scenario_steps_apply_in_order():
    """Two steps at 25 set the packers to 3, then 2: the last one given wins."""
    app = SimulationContext("S2", factor=0.0)
    app.add_resource("packers", current_cap=1, max_cap=4)
    app.add_step(25, path="S2.resources.packers.current_cap", value=3)
    app.add_step(25, path="S2.resources.packers.current_cap", value=2)
    app.run(until=30)
    assert app.env.registry.get("S2.resources.packers.current_cap").value == 2


def test_a_scenario_step_at_until_does_not_apply():
    """A step at exactly until is dropped; one just before applies."""
    for at, expected in ((40, 1), (39.9, 4)):
        app = SimulationContext("S2", factor=0.0)
        app.add_resource("packers", current_cap=1, max_cap=4)
        app.add_step(at, path="S2.resources.packers.current_cap", value=4)
        app.run(until=40)
        assert app.get_resource("packers").capacity == expected


@pytest.mark.parametrize(
    "where, refusal",
    [("attr.x ==", "interrupt where: 'attr.x ==' is not a valid"), (False, "False")],
    ids=["text that does not parse", "not text"],
)
def test_a_scenario_interrupt_whose_where_is_no_expression_is_refused_when_added(
    where, refusal
):
    """`add_step` and `add_scenario` refuse an interrupt `where` that is no
    expression, as a task's interrupt action does when it is declared, and the
    low-level `run_scenario` does too."""
    app = SimulationContext("S", factor=0.0)
    step = {"at": 3, "interrupt": {"task": "t", "where": where}}
    with pytest.raises(ValueError, match=refusal):
        app.add_step(at=3, interrupt=step["interrupt"])
    with pytest.raises(ValueError, match=refusal):
        app.add_scenario([step])
    assert app._scenario_steps == []
    from desflow.builder.scenario import run_scenario

    with pytest.raises(ValueError, match=refusal):
        next(run_scenario(None, [step]))


def test_an_interval_given_as_a_duration_is_kept_in_seconds():
    """`add_readings`, `telemetry_loop` and `with_stats` take a duration such as
    "10 s" for the interval, and the run reads it as seconds."""
    app = SimulationContext("S", factor=0.0)
    app.add_variable("v", 0)
    app.add_readings("10 s", {"v": 'registry["S.variables.v"]'})
    ticks = []
    app.telemetry_loop("5 s")(lambda context: ticks.append(context.env.now))
    app.with_stats("basic", interval="1 min")
    assert app._sinks.stats_interval == 60
    app.run(until=21)
    assert ticks == [0, 5, 10, 15, 20]


@pytest.mark.parametrize(
    "refused, message",
    [
        (
            lambda app: app.add_arrival(
                "a", "constant", value=2, first_at=-1, spawn="t"
            ),
            "first_at is a time of 0 or more",
        ),
        (
            lambda app: app.add_initial_arrival("a", 2, spawn="t", gap=-1),
            "gap",
        ),
        (
            lambda app: app.add_arrival_replay("a", [1, 2], unit="parsec", spawn="t"),
            "unit",
        ),
        (
            lambda app: app.add_arrival("a", "constant", value=2, max_events=3),
            "max_events needs a spawn",
        ),
    ],
    ids=["add_arrival", "add_initial_arrival", "add_arrival_replay", "max_events"],
)
def test_a_refused_redeclaration_of_an_arrival_keeps_the_first_declaration(
    refused, message
):
    """A refused `add_arrival`, `add_initial_arrival` or `add_arrival_replay` under a
    name already used changes nothing: the run starts the first declaration's
    arrivals."""
    started = []
    app = SimulationContext("S", factor=0.0)
    app.add_arrival("a", "constant", value=1, spawn="t")
    with pytest.raises(ValueError, match=message):
        refused(app)
    assert app._arrivals.configs["a"].value == 1
    assert app._arrivals.spawns == {"a": "t"}
    app.task(name="t")(lambda task_id: started.append(task_id) or {})
    app.run(until=4.5)
    assert len(started) == 4


def test_a_numpy_count_of_a_spawn_action_is_a_whole_number():
    """`count` of a `spawn` takes a NumPy integer, as `group` and `max_events` do."""
    import numpy

    from desflow.builder.context import _parse_spawn

    parsed = _parse_spawn({"task": "t", "count": numpy.int64(2)})
    assert parsed["count"] == 2 and type(parsed["count"]) is int


def test_sim_id_must_be_text_that_is_not_empty():
    """`sim_id` starts every registry path, so `None`, a number and empty text are
    refused when the context is made."""
    for bad in (None, "", 3):
        with pytest.raises(ValueError, match="sim_id is text that is not empty"):
            SimulationContext(bad)
    assert SimulationContext("Shop.v2").sim_id == "Shop.v2"


def test_an_empty_sim_id_in_a_blueprint_is_refused_at_its_line(tmp_path):
    """A blueprint's empty `sim_id` is refused when the file loads, with the file and
    the line, not by the context after the load."""
    path = tmp_path / "empty.yaml"
    path.write_text("simulation:\n  sim_id: ''\n  factor: 0\n", encoding="utf-8")
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:2: .*sim_id is text that is not empty",
    ):
        SimulationContext.from_yaml(path)


def test_a_scenario_step_after_until_does_not_apply_during_a_drain():
    """A drain runs past `until`, and a step due after `until` still never applies."""
    seen = []
    app = SimulationContext("S", factor=0.0)
    app.add_variable("v", 0)
    app.on_change.append(lambda change: seen.append((change.path, change.source)))
    app.add_arrival("a", "constant", value=5, spawn="job", max_events=1)
    app.add_step(at=12, path="S.variables.v", value=7)
    app.task(service_time=20, name="job")(lambda task_id: {})
    app.run(until=10, drain=True)
    assert app.env.now == 25
    assert ("S.variables.v", "scenario") not in seen


def test_a_misspelt_name_names_the_closest_or_how_to_add_it():
    """A task that names a resource, service, task or path nothing added gets the
    closest name, or the call that adds it."""
    app = SimulationContext("Shop", factor=0)
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("packing", dist="constant", value=1)

    @app.task(service_id="packing", resource_id="packer")
    def pack(task_id):
        return {}

    with pytest.raises(ValueError, match="'packer', which is not added; did you mean"):
        app.run(until=1)

    app = SimulationContext("Shop", factor=0)
    app.add_service("packing", dist="constant", value=1)

    @app.task(service_id="paking")
    def pick(task_id):
        return {}

    with pytest.raises(ValueError, match="'paking', which is not added; did you"):
        app.run(until=1)

    app = SimulationContext("Shop", factor=0)
    app.add_service("packing", dist="constant", value=1)
    app.add_step(5, path="Shop.service.packng.mean", value=2)
    with pytest.raises(ValueError, match="did you mean 'Shop.service.packing.value'"):
        app.run(until=1)
    with pytest.raises(ValueError, match="does not take pth; did you mean 'path'"):
        app.add_scenario([{"at": 1, "pth": "Shop.service.packing.value", "value": 2}])
    with pytest.raises(ValueError, match="got 'timout'; did you mean 'timeout'"):
        app.task(wait={"timout": 3})


def test_run_refuses_an_on_or_subscribe_name_nothing_added():
    """`run()` refuses, as YAML does at load, an on: fire signal, an on: interrupt
    task, a subscribe task and a subscribe signal that nothing added, on a task or a
    signal."""
    on_task = "the on: action of task 'pack' "
    on_signal = "the on: action of signal 's' "
    cases = [
        ({"on": [{"fire": {"signal": "nope"}}]}, on_task + "fires signal 'nope'"),
        (
            {"on": [{"interrupt": {"task": "nope"}}]},
            on_task + "interrupts task 'nope'",
        ),
        ({"subscribe": {"task": "nope"}}, "subscribes to task 'nope'"),
        ({"subscribe": {"signal": "nope"}}, "subscribes to signal 'nope'"),
    ]
    for options, message in cases:
        app = SimulationContext("S", factor=0.0)

        @app.task(name="pack", **options)
        def pack(task_id):
            return {}

        with pytest.raises(ValueError, match=f"{message}, which is not added"):
            app.run(until=5)

    signal_cases = [
        ([{"fire": {"signal": "nope"}}], on_signal + "fires signal 'nope'"),
        ([{"interrupt": {"task": "nope"}}], on_signal + "interrupts task 'nope'"),
    ]
    for on, message in signal_cases:
        app = SimulationContext("S", factor=0.0)
        app.add_signal("s", on=on)
        with pytest.raises(ValueError, match=f"{message}, which is not added"):
            app.run(until=5)


def test_run_refuses_an_on_action_path_the_registry_lacks():
    """`start()` refuses, as for a scenario step, an on: action whose path is no
    registry path, on a task, in its listen and on a signal, and takes a family
    member's path, which is made when the run starts."""
    missing = "S.variables.nope"
    on = [{"path": missing, "value": 1}]
    cases = [
        ({"on": on}, None, "the on: action of task 'pack' "),
        (
            {"listen": {"signal": "go", "on": on}},
            None,
            "the listen on: action of task 'pack' ",
        ),
        ({}, on, "the on: action of signal 'go' "),
    ]
    for options, signal_on, place in cases:
        app = SimulationContext("S", factor=0.0)
        app.add_variable("v", 0)
        app.add_signal("go", on=signal_on)

        @app.task(name="pack", **options)
        def pack(task_id):
            return {}

        with pytest.raises(ValueError) as error:
            app.start(until=5)
        assert str(error.value).startswith(
            f"{place}sets '{missing}', which is not a registry path"
        )

    app = SimulationContext("S", factor=0.0)
    app.add_resource("desk", current_cap=1, family=True, members=2)
    app.add_variable("v", 0)
    app.add_arrival("a", gap=1, spawn="pack", max_events=1)

    @app.task(
        name="pack",
        on=[
            {"path": "S.variables.v", "value": 1},
            {"path": "S.resources.desk[0].current_cap", "value": 1},
        ],
    )
    def pack_with_member_path(task_id):
        return {}

    app.run(until=5)


def test_a_path_on_a_family_member_is_checked_for_the_attribute_a_member_has():
    """A scenario step or an on: action on a member of a family is refused at start
    when the member has no such attribute, and is taken when it has one."""

    def model():
        app = SimulationContext("S", factor=0.0)
        app.add_resource("desk", current_cap=1, family=True, members=2)
        app.add_container("tank", current_cap=1, max_cap=5, family=True, members=2)
        app.add_store("shelf", current_cap=1, family=True, members=2)
        app.add_signal("lane", family=True, members=2)
        return app

    for path in (
        "S.resources.desk[0].current_cap",
        "S.resources.desk[1].queue_limit",
        "S.containers.tank[0].level",
        "S.stores.shelf[0].max_cap",
    ):
        app = model()
        app.add_step(at=3, path=path, value=1)
        app.run(until=10)
    for path in (
        "S.resources.desk[0].curent_cap",
        "S.resources.desk[0].current_cap.extra",
        "S.stores.shelf[0].level",
        "S.signals.lane[0].count",
    ):
        for declare in ("step", "on"):
            app = model()
            if declare == "step":
                app.add_step(at=3, path=path, value=1)
            else:
                app.add_signal("go", on=[{"path": path, "value": 1}])
            with pytest.raises(ValueError, match=f"'{re.escape(path)}'.*registry path"):
                app.start(until=10)


def test_a_path_on_a_member_the_family_can_never_have_is_refused_at_start():
    """A step or an on: action on `desk[7]` of a family with `members: 2` is refused
    at start, unless a task names the family with a `key`, which makes a member at its
    first use. A `members` read at start is not known, so it takes every key."""

    def model(members, key=None):
        app = SimulationContext("S", factor=0.0)
        app.add_resource("desk", current_cap=1, family=True, members=members)
        if key is not None:
            app.task(name="t", resource_id={"of": "desk", "key": key}, service_time=1)(
                lambda task_id: {}
            )
        return app

    def declare(app, path, how):
        if how == "step":
            app.add_step(at=3, path=path, value=1)
        else:
            app.add_signal("go", on=[{"path": path, "value": 1}])

    for how in ("step", "on"):
        for members, path in (
            (2, "S.resources.desk[7].current_cap"),
            (2, "S.resources.desk['a'].current_cap"),
            (None, "S.resources.desk[0].current_cap"),
        ):
            app = model(members)
            declare(app, path, how)
            with pytest.raises(ValueError, match=f"'{re.escape(path)}'.*registry path"):
                app.start(until=10)
        import numpy

        for members, path in (
            (numpy.int64(2), "S.resources.desk[7].current_cap"),
            (numpy.int64(2), "S.resources.desk['a'].current_cap"),
        ):
            app = model(members)
            declare(app, path, how)
            with pytest.raises(
                ValueError,
                match=f"'{re.escape(path)}'.*registry path.*"
                r"family 'desk' never makes: `members: 2` makes desk\[0\] to desk\[1\]",
            ):
                app.start(until=10)
        app = model(None)
        declare(app, "S.resources.desk[0].current_cap", how)
        with pytest.raises(ValueError, match="the family has no `members`"):
            app.start(until=10)
        for members, made in (
            (0, "`members: 0` makes none"),
            (1, "makes only desk[0]"),
        ):
            app = model(members)
            declare(app, "S.resources.desk[1].current_cap", how)
            with pytest.raises(ValueError, match=re.escape(made)):
                app.start(until=10)
        for members, key, path in (
            (2, None, "S.resources.desk[1].current_cap"),
            (2, "attr.i", "S.resources.desk[7].current_cap"),
            (2, 7, "S.resources.desk[7].current_cap"),
            (None, "attr.i", "S.resources.desk[5].current_cap"),
            ("2 + 1", None, "S.resources.desk[7].current_cap"),
        ):
            app = model(members, key)
            declare(app, path, how)
            app.start(until=10)


def test_calls_that_need_a_running_model_say_where_to_make_them():
    """call, signal and interrupt before the run name where they work."""
    app = SimulationContext("Shop", factor=0)
    with pytest.raises(RuntimeError, match="call them from a task or a process"):
        app.call("pack")
    with pytest.raises(RuntimeError, match="read them from a task or a process"):
        app.signal("closing")
    with pytest.raises(RuntimeError, match="from a task, a process or a scenario"):
        app.interrupt("pack")


def test_a_task_called_with_any_instance_id_runs_with_statistics_off():
    """A builder task whose first argument is a dict, as some projects pass their
    event, runs when statistics are off, as in 0.21.0: nothing hashes the id."""
    app = SimulationContext("Shop", factor=0)
    app.add_arrival("orders", dist="constant", value=1)
    emitted = []

    @app.task()
    def emit_event(event, context):
        emitted.append(event)
        return event

    @app.arrival_loop("orders")
    def loop(context):
        while True:
            yield context.wait_for_arrival("orders")
            context.spawn(emit_event({"event_type": "order_placed"}, context))

    app.run(until=3)
    assert len(emitted) == 2


def test_add_resource_checks_its_capacity_and_type():
    app = SimulationContext("Shop", factor=0.0)
    app.add_resource("bench")
    app.add_resource("tank", max_cap=3)
    app.add_resource("open", float("inf"))
    with pytest.raises(
        ValueError, match="needs a whole number of 0 or more for current_cap"
    ):
        app.add_resource("odd", 1.5)
    with pytest.raises(ValueError, match="max_cap 0 and no current_cap"):
        app.add_resource("none", max_cap=0)
    with pytest.raises(ValueError, match="subclass of ResourceBase"):
        app.add_resource("plain", type=dict)
    with pytest.raises(ValueError, match="queue_limit must be 0 or more"):
        app.add_resource("limited", queue_limit=-2)


def test_a_resource_in_the_wait_and_in_resource_is_refused():
    """A task holds a resource once: naming it in a wait request and in resource would
    queue the task behind its own unit for ever."""
    app = _two_resources()

    @app.task(
        wait={"request": "picker"}, service_id="s", resource_id="picker", name="a"
    )
    def a(task_id):
        return {}

    with pytest.raises(ValueError, match="holds each resource once"):
        app.run(until=1)


def test_a_wait_request_counts_in_the_acquisition_order(caplog):
    """A wait request is acquired before the task's resources, so it gets a warning
    when another task acquires the two in the opposite order, as two resource lists
    do."""
    app = _two_resources()

    @app.task(wait={"request": "picker"}, service_id="s", resource_id="bench", name="a")
    def a(task_id):
        return {}

    @app.task(service_id="s", resource_id=["bench", "picker"], name="b")
    def b(task_id):
        return {}

    with caplog.at_level("WARNING"):
        app.run(until=1)
    assert "in one order everywhere" in caplog.text
    caplog.clear()

    same = _two_resources()

    @same.task(
        wait={"request": "picker"}, service_id="s", resource_id="bench", name="a"
    )
    def a_same(task_id):
        return {}

    @same.task(service_id="s", resource_id=["picker", "bench"], name="b")
    def b_same(task_id):
        return {}

    with caplog.at_level("WARNING"):
        same.run(until=1)
    assert "in one order everywhere" not in caplog.text


def test_requests_under_one_all_are_acquired_together(caplog):
    """Two requests under an `all` are made together, and either can be held while
    the other is waited for, so a task that acquires the two in either order gets a
    warning. A group branch of an `any` can hold a unit while the `any` waits for
    its other branches, so it pairs with them too. Single request branches of an
    `any` end it when one is granted and set no order."""
    waits = (
        {"all": {"p": {"request": "picker"}, "b": {"request": "bench"}}},
        {
            "any": {
                "p": {"request": "picker"},
                "g": {"all": {"b": {"request": "bench"}, "t": {"timeout": 1}}},
            }
        },
    )
    for wait in waits:
        for listed in (["picker", "bench"], ["bench", "picker"]):
            app = _two_resources()

            @app.task(wait=wait, service_id="s", name="a")
            def a(task_id):
                return {}

            @app.task(service_id="s", resource_id=listed, name="b")
            def b(task_id):
                return {}

            caplog.clear()
            with caplog.at_level("WARNING"):
                app.run(until=1)
            assert " together in its wait" in caplog.text

    either = _two_resources()

    @either.task(
        wait={"any": {"p": {"request": "picker"}, "b": {"request": "bench"}}},
        service_id="s",
        name="a",
    )
    def a_any(task_id):
        return {}

    @either.task(
        wait={"any": {"b": {"request": "bench"}, "p": {"request": "picker"}}},
        service_id="s",
        name="b",
    )
    def b_any(task_id):
        return {}

    caplog.clear()
    with caplog.at_level("WARNING"):
        either.run(until=1)
    assert "in one order everywhere" not in caplog.text


def test_a_kept_resource_requested_in_the_next_wait_is_refused(tmp_path):
    """A task that keeps the picker for a task whose wait requests the picker would
    queue that request behind the unit kept for it, so the model is refused."""
    app = _two_resources()

    @app.task(
        service_id="s",
        resource_id="picker",
        keep=["picker"],
        next="b",
        name="a",
    )
    def a(task_id):
        return {}

    @app.task(wait={"request": "picker"}, service_id="s", name="b")
    def b(task_id):
        return {}

    with pytest.raises(ValueError, match="requests 'picker' in its wait") as refused:
        app.run(until=1)
    # The advice runs: listing the picker in the resource of 'b' instead would
    # stall, so the message does not give it
    advice = str(refused.value)
    assert "in the resource of" not in advice
    assert "Take 'picker' out of the keep of 'a'" in advice
    assert "a seize of 'picker' in 'a' and a release of 'picker' in 'b'" in advice
    # Both forms the message advises run to the end
    for pick, tidy in (
        ("resource: picker", "wait: {request: picker}"),
        ("seize: picker", "release: picker"),
    ):
        text = (
            "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
            "resources: {picker: {current_cap: 1}}\n"
            "arrivals: {a: {initial: 1, spawn: pick}}\n"
            "tasks:\n"
            f"  pick: {{{pick}, service_time: 1, payload: {{e: pick}}, next: tidy}}\n"
            f"  tidy: {{{tidy}, service_time: 1, payload: {{e: tidy}}}}\n"
            "run: {until: 50}\n"
        )
        outcome = run_yaml_text(tmp_path, text, {})
        assert outcome.error is None
        done = [
            r["value"]["e"]
            for r in outcome.records
            if r["stream_type"] == "event" and "e" in r["value"]
        ]
        assert done == ["pick", "tidy"]


def test_a_priority_needs_a_resource_or_a_wait_request_on_a_priority_queue():
    """A priority ranks the task's request in a priority queue: a wait request counts
    as a resource, and its resource has to queue by priority."""
    ranked = SimulationContext("TestSim", factor=0.0)
    ranked.add_resource("picker", current_cap=1, max_cap=1, queue="priority")
    ranked.add_service("s", "constant", value=1.0)

    @ranked.task(wait={"request": "picker"}, service_id="s", priority=2, name="a")
    def a(task_id):
        return {}

    ranked.run(until=1)

    fifo = _two_resources()

    @fifo.task(wait={"request": "picker"}, service_id="s", priority=2, name="a")
    def a_fifo(task_id):
        return {}

    with pytest.raises(ValueError, match="does not queue by priority"):
        fifo.run(until=1)

    with pytest.raises(
        ValueError, match="needs a resource, a seize or a request in its wait or open"
    ):
        SimulationContext("TestSim").task(
            "s", wait={"timeout": 1}, priority=2, name="b"
        )


class _Records:
    """Keeps every record it receives."""

    def __init__(self):
        self.records: list = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)

    def events(self, name):
        return [
            row
            for row in self.records
            if row.get("value", {}).get("event_type") == name
        ]


def _busy_hour(spawn: bool) -> list:
    """The shop of the Queues and Capacity snippet, with spawn= or its hand loop."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_service("packing", dist="constant", value=1.5)
    app.add_arrival(
        "orders", dist="exponential", rate=1.8, spawn="pack_order" if spawn else None
    )
    app.add_resource("packers", current_cap=2, max_cap=10)
    capture = _Records()
    app.add_egress(capture)

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    if not spawn:

        @app.arrival_loop("orders")
        def order_generator(context):
            task_id = 0
            while True:
                order = yield context.wait_for_arrival("orders")
                context.spawn(pack_order(task_id, entity=order))
                task_id += 1

    app.run(until=120)
    return capture.records


def test_add_arrival_spawn_starts_the_task():
    """spawn= gives the records of the arrival loop a builder model writes by hand."""
    records = _busy_hour(spawn=True)
    assert any(r["stream_type"] == "event" for r in records)
    assert records == _busy_hour(spawn=False)


def test_spawn_naming_no_task_is_refused():
    app = SimulationContext("Shop", factor=0)
    app.add_arrival("orders", dist="constant", value=1, spawn="pack_ordr")

    @app.task()
    def pack_order(task_id):
        return {}

    with pytest.raises(
        ValueError,
        match="arrival 'orders' spawns task 'pack_ordr', which is not added; "
        "did you mean 'pack_order'",
    ):
        app.run(until=1)


def test_replay_spawn():
    """A replayed arrival with spawn= starts the task for each row, at its time."""
    app = SimulationContext("Shop", factor=0)
    rows = [{"time": 1.0, "order": "a"}, {"time": 2.5, "order": "b"}]
    app.add_arrival_replay("orders", rows, spawn="pack_order")
    capture = _Records()
    app.add_egress(capture)

    @app.task()
    def pack_order(task_id):
        return {"task_id": task_id}

    app.run(until=5)
    events = [r for r in capture.records if r["stream_type"] == "event"]
    assert [(e["sim_ts"], e["key"], e["value"]) for e in events] == [
        (1.0, "orders-1", {"task_id": 0}),
        (2.5, "orders-2", {"task_id": 1}),
    ]


def _wait_for_ingress_item(env):
    """Waits, for at most 2 wall-clock seconds, until an ingress item is queued."""
    import time

    deadline = time.monotonic() + 2
    while env.ingress_queue.qsize() < 1:
        assert time.monotonic() < deadline, "the ingress item never arrived"
        time.sleep(0.005)


def _app_with_speed():
    app = SimulationContext("Shop", factor=0.0)
    app.add_variable("speed", 1)
    changes: list = []
    app.on_change.append(changes.append)
    return app, changes


def test_builder_ingress_change_carries_its_source():
    """The builder reports a connector's change as source "ingress", with its class."""
    from desflow import LocalIngress

    app, changes = _app_with_speed()
    app.add_ingress(LocalIngress([(0.0, "Shop.variables.speed", 4)]))
    env = app.start(until=1)
    _wait_for_ingress_item(env)
    app.finish()

    assert [(c.path, c.new, c.source, c.actor) for c in changes] == [
        ("Shop.variables.speed", 4, "ingress", "LocalIngress")
    ]


def test_builder_file_ingress_change_carries_its_source(tmp_path):
    """The builder reports a file's change as source "ingress", with FileIngress."""
    from desflow import FileIngress

    path = tmp_path / "params.yaml"
    path.write_text("Shop.variables.speed: 4\n")
    app, changes = _app_with_speed()
    app.add_ingress(FileIngress(path, poll=0.05))
    env = app.start(until=1)
    _wait_for_ingress_item(env)
    app.finish()

    assert [(c.path, c.new, c.source, c.actor) for c in changes] == [
        ("Shop.variables.speed", 4, "ingress", "FileIngress")
    ]


def test_a_context_run_leaves_the_end_flag_it_found_on_the_environment():
    """`_run_low_level` and a context's run both set `_end_is_set`; a context's run
    restores the value it found, as `_run_low_level` does, so it does not clear the
    flag an outer run set."""
    app, _ = _app_with_speed()
    env = app.start(until=5)
    env._end_is_set = True
    app.advance(env.timeout(2))
    assert env._end_is_set is True
    app.finish()


def test_builder_scenario_step_has_the_scenario_source():
    """A scenario step is reported as source "scenario"."""
    app, changes = _app_with_speed()
    app.add_step(2, path="Shop.variables.speed", value=3)
    app.run(until=5)

    assert [(c.old, c.new, c.source, c.at) for c in changes] == [(1, 3, "scenario", 2)]


def test_an_on_action_change_has_no_source():
    """A change from an `on:` action is the model's own, so its source is None."""
    app, changes = _app_with_speed()
    app.add_arrival("orders", dist="constant", value=1)

    @app.task(on=[{"path": "Shop.variables.speed", "value": 7}], name="courier")
    def courier(task_id):
        return {}

    @app.arrival_loop("orders")
    def loop(context):
        entity = yield context.wait_for_arrival("orders")
        context.spawn(courier(context.next_instance("courier"), entity=entity))
        yield context.env.event()

    app.run(until=3)

    assert [(c.new, c.source, c.actor) for c in changes][:1] == [(7, None, None)]


def test_an_on_action_with_a_refused_value_is_logged_and_the_run_goes_on(caplog):
    """Only a scenario step is refused; an `on:` action keeps `update`."""
    app = SimulationContext("Shop", factor=0.0)
    app.add_arrival("orders", dist="exponential", rate=1.0)

    @app.task(on=[{"path": "Shop.arrival.orders.rate", "value": 0}], name="courier")
    def courier(task_id):
        return {}

    @app.arrival_loop("orders")
    def loop(context):
        entity = yield context.wait_for_arrival("orders")
        context.spawn(courier(context.next_instance("courier"), entity=entity))
        yield context.env.event()

    with caplog.at_level("ERROR"):
        app.run(until=10)

    assert "Invalid value for 'Shop.arrival.orders.rate'" in caplog.text
    assert "Update ignored." in caplog.text
    assert app.env.now == 10


def test_a_builder_scenario_value_the_registry_refuses_stops_start():
    """`start()` refuses a refused value before the clock or any connector starts."""
    import threading

    from desflow import LocalIngress

    app = SimulationContext("Shop", factor=0.0)
    app.add_arrival("orders", dist="exponential", rate=1.0)
    app.add_ingress(LocalIngress([]))
    app.add_step(5, path="Shop.arrival.orders.rate", value=0)
    threads = threading.active_count()

    with pytest.raises(ValueError) as error:
        app.start(until=10)

    message = str(error.value)
    assert message.startswith("scenario step at 5 s sets 'Shop.arrival.orders.rate': ")
    assert "exponential rate must be greater than 0" in message
    assert threading.active_count() == threads


def test_a_builder_scenario_is_checked_with_the_overrides_of_start():
    """An override can make a later step's value invalid, and `start()` refuses it
    before the clock starts, not at the step's time."""
    app = SimulationContext("Shop", factor=0.0)
    app.add_arrival("orders", dist="uniform", low=1.0, high=5.0)
    app.add_step(5, path="Shop.arrival.orders.high", value=3.0)

    with pytest.raises(ValueError) as error:
        app.start(until=10, overrides={"Shop.arrival.orders.low": 4.0})

    message = str(error.value)
    assert message.startswith("scenario step at 5 s sets 'Shop.arrival.orders.high': ")
    assert "low 4.0, high 3.0" in message

    # Without the override the same scenario starts and runs to its end
    app.run(until=10)
    assert app.env.now == 10


def test_a_builder_scenario_check_leaves_the_starting_values():
    """The check applies the steps to a copy, so the model keeps its own values."""
    app = SimulationContext("Shop", factor=0.0)
    app.add_arrival("orders", dist="exponential", rate=1.0)
    app.add_step(5, path="Shop.arrival.orders.rate", value=3)

    app.start(until=1)
    try:
        assert app.env.registry.get("Shop.arrival.orders.rate").value == 1.0
    finally:
        app.finish()


def test_low_level_run_scenario_raises_for_a_refused_value():
    """A refused step stops a low-level run at its time with `ParamRejected`."""
    from desflow import (
        DistributionConfig,
        DynamicRealtimeEnvironment,
        ParamRejected,
        SimParameter,
    )
    from desflow.builder.scenario import run_scenario

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
        )
    )
    env.process(
        run_scenario(env, [{"at": 5, "path": "Shop.arrival.orders.rate", "value": 0}])
    )

    with pytest.raises(ParamRejected) as error:
        env.run(until=10)

    assert env.now == 5
    assert error.value.path == "Shop.arrival.orders.rate"


def test_start_describes_its_overrides():
    """`start()` gives each override's change the source, actor and reason it is given."""
    app, changes = _app_with_speed()
    app.add_variable("mode", "slow")
    app.start(
        until=1,
        overrides={"Shop.variables.speed": 5, "Shop.variables.mode": "fast"},
        source="user",
        actor="alice",
        reason="test",
    )
    app.finish()

    assert [(c.path, c.new, c.source, c.actor, c.reason, c.at) for c in changes] == [
        ("Shop.variables.speed", 5, "user", "alice", "test", 0),
        ("Shop.variables.mode", "fast", "user", "alice", "test", 0),
    ]


def test_every_run_of_a_context_calls_its_callbacks():
    """Two runs of one context call the callback in both; a callback added after
    `start()` waits for the next run."""
    app, changes = _app_with_speed()
    late: list = []

    app.start(until=1, overrides={"Shop.variables.speed": 2})
    app.on_change.append(late.append)
    app.finish()
    assert [c.new for c in changes] == [2]
    assert late == []

    app.start(until=1, overrides={"Shop.variables.speed": 3})
    app.finish()
    assert [c.new for c in changes] == [2, 3]
    assert [c.new for c in late] == [3]


def test_low_level_scenario_changes_have_the_scenario_source():
    """The low-level `run_scenario` reports its steps as source "scenario"."""
    from desflow import DynamicRealtimeEnvironment, SimParameter
    from desflow.builder.scenario import run_scenario

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(SimParameter(sim_id="Shop", variables={"x": 0}))
    changes: list = []
    env.registry.on_change.append(changes.append)
    env.process(run_scenario(env, [{"at": 2, "path": "Shop.variables.x", "value": 9}]))
    env.run(until=5)

    assert [(c.new, c.source, c.at) for c in changes] == [(9, "scenario", 2)]


def _shop_with_ingress():
    from desflow import LocalIngress

    app = SimulationContext("Shop", factor=0.0)
    app.add_resource("packers", current_cap=2, max_cap=5)
    app.add_container("tank", current_cap=10.0, max_cap=20.0)
    app.add_service("packing", dist="normal", mean=5.0, std=1.0)
    app.add_arrival("orders", dist="exponential", rate=0.5)
    app.add_variable("speed", 1)
    app.add_ingress(LocalIngress([]))
    return app


def test_registry_paths_are_the_paths_and_values_of_a_run():
    """`registry_paths()` equals what the run's registry holds, and starts no thread."""
    import threading

    app = _shop_with_ingress()
    threads = threading.active_count()

    paths = app.registry_paths()

    assert threading.active_count() == threads
    assert app.started is False
    env = app.start(until=1)
    try:
        registry = env.registry
        assert paths == {p: registry.get(p).value for p in registry.paths()}
        assert list(paths) == registry.paths()
    finally:
        app.finish()


def test_registry_paths_of_a_refused_model_raise_an_ordinary_exception():
    """A model the registry refuses raises its error, not `SystemExit`, and starts no thread."""
    import threading

    app = _shop_with_ingress()
    config = app._holders.resources_config["packers"]
    config.current_cap, config.max_cap = None, 0.5
    threads = threading.active_count()

    with pytest.raises(Exception, match="max_cap 0.5 and no current_cap") as raised:
        app.registry_paths()

    assert isinstance(raised.value, ValueError)
    assert not isinstance(raised.value, SystemExit)
    assert threading.active_count() == threads


def _entry_times(app, until, **run):
    capture = _Records()
    app.add_egress(capture)
    app.run(until=until, **run)
    return capture


def _noting(app):
    @app.task(name="note")
    def note(task_id):
        return {"event_type": "arrived"}

    return app


def test_add_arrival_takes_a_gap_expression_in_place_of_a_distribution():
    """A `gap` of `"1 + t / 4"` gives the gaps 1, 1.25, ... as a blueprint's `gap` does."""
    app = SimulationContext("G", factor=0)
    app.add_arrival("a", gap="1 + t / 4", spawn="note")
    capture = _entry_times(_noting(app), 4)
    assert [row["sim_ts"] for row in capture.events("arrived")] == [1.0, 2.25, 3.8125]


def test_add_arrival_takes_a_gap_that_is_a_number():
    app = SimulationContext("G", factor=0).add_arrival("a", gap=2, spawn="note")
    capture = _entry_times(_noting(app), 7)
    assert [row["sim_ts"] for row in capture.events("arrived")] == [2, 4, 6]


def test_add_arrival_takes_a_gap_that_is_a_duration():
    """A text gap that is a duration, as `"2s"` or `"1 min"`, is read as seconds, as
    a blueprint's `gap` and a task's `service_time` read it."""
    for gap, seconds in (("2s", 2), ("1 min", 60)):
        app = SimulationContext("G", factor=0).add_arrival("a", gap=gap, spawn="note")
        capture = _entry_times(_noting(app), 3 * seconds + 1)
        assert [row["sim_ts"] for row in capture.events("arrived")] == [
            seconds,
            2 * seconds,
            3 * seconds,
        ]


@pytest.mark.parametrize(
    "arguments, message",
    [
        ({}, "neither is given"),
        ({"dist": "constant", "value": 1, "gap": 1}, "not both"),
        ({"gap": 1, "rate": 2}, "takes no distribution parameter: rate"),
        ({"gap": -1}, "arrival 'a': gap is 0 or more"),
        ({"gap": float("nan")}, "arrival 'a': gap is 0 or more"),
        ({"gap": "-5"}, "arrival 'a': gap is 0 or more"),
        ({"gap": "-5 min"}, "arrival 'a': gap"),
        ({"gap": True}, "arrival 'a': gap is a number of seconds"),
        ({"gap": [1]}, "arrival 'a': gap is a number of seconds"),
    ],
)
def test_add_arrival_refuses_a_gap_that_does_not_fit(arguments, message):
    with pytest.raises(ValueError, match=message):
        SimulationContext("G").add_arrival("a", **arguments)


def test_add_initial_arrival_takes_a_key():
    app = SimulationContext("K", factor=0)
    app.add_initial_arrival("m", 3, spawn="note", key=lambda n: f"Machine {n - 1}")
    capture = _entry_times(_noting(app), 1)
    keys = [row["key"] for row in capture.events("arrived")]
    assert keys == ["Machine 0", "Machine 1", "Machine 2"]


def test_add_initial_arrival_with_a_gap_goes_on_arriving():
    app = SimulationContext("K", factor=0)
    app.add_initial_arrival("m", 2, spawn="note", gap=5)
    capture = _entry_times(_noting(app), 11)
    assert [row["sim_ts"] for row in capture.events("arrived")] == [0, 0, 5, 10]


def _ticking(app):
    @app.task(name="tick", service_time=1)
    def tick(task_id):
        return {"event_type": "ticked"}

    app.add_arrival("a", "constant", value=2, spawn="tick")
    return app


def _readings(capture, path):
    return [
        row["value"] for row in capture.records if row.get("stream_path") == path
    ] or [
        row["value"]
        for row in capture.records
        if row.get("path") == path or row.get("telemetry_path") == path
    ]


def test_run_segments_publish_a_reading_after_each_segment():
    app = _ticking(SimulationContext("S", factor=0))
    capture = _entry_times(
        app, 10, segments=[{"until": 3}, {"until": 6}, {}], sync=False
    )
    readings = [
        row for row in capture.records if "system.simulation.segment" in str(row)
    ]
    assert len(readings) == 3
    assert [row["value"]["index"] for row in readings] == [0, 1, 2]
    assert [row["value"]["now"] for row in readings] == [3, 6, 10]


def test_run_segments_every_divides_until():
    app = _ticking(SimulationContext("S", factor=0))
    capture = _entry_times(app, 10, segments={"every": 4})
    readings = [r for r in capture.records if "system.simulation.segment" in str(r)]
    assert [row["value"]["now"] for row in readings] == [4, 8, 10]


def test_run_steps_publish_a_reading_after_each_event():
    app = _ticking(SimulationContext("S", factor=0))
    capture = _entry_times(app, 5, steps=True)
    readings = [r for r in capture.records if "system.simulation.step" in str(r)]
    assert readings
    assert [row["value"]["step"] for row in readings] == list(
        range(1, len(readings) + 1)
    )


def test_run_without_segments_or_steps_publishes_no_reading():
    app = _ticking(SimulationContext("S", factor=0))
    capture = _entry_times(app, 5)
    assert not [r for r in capture.records if "system.simulation.s" in str(r)]


def test_run_sync_calls_the_environment_sync(monkeypatch):
    from desflow.core.environment import DynamicRealtimeEnvironment

    calls = []
    original = DynamicRealtimeEnvironment.sync
    monkeypatch.setattr(
        DynamicRealtimeEnvironment,
        "sync",
        lambda self: (calls.append(self.now), original(self))[1],
    )
    app = _ticking(SimulationContext("S", factor=0))
    app.run(until=3, sync=True)
    assert calls == [0]
    calls.clear()
    _ticking(SimulationContext("S", factor=0)).run(until=3)
    assert calls == []


@pytest.mark.parametrize(
    "arguments, message",
    [
        ({"segments": 3}, "segments is a list of segment mappings"),
        ({"segments": [{"until": 1, "stop": {"signal": "x"}}]}, "not both"),
        ({"segments": [{"until": 1}, {"untill": 1}]}, r"(?s)segments\[1\].*untill"),
        ({"segments": {"every": 0}}, "(?s)segments: .*every must be above 0"),
        ({"segments": [{"until": 1, "look": "nowhere"}]}, "look names task 'nowhere'"),
        ({"steps": True, "segments": [{"until": 1}]}, "run.steps cannot be used"),
        ({"steps": "yes"}, "steps is True or False"),
        ({"sync": 1}, "sync is True or False"),
    ],
)
def test_run_refuses_segments_steps_and_sync_that_do_not_fit(arguments, message):
    app = _ticking(SimulationContext("S", factor=0))
    with pytest.raises(ValueError, match=message):
        app.run(until=5, **arguments)


def test_run_segments_every_needs_until():
    app = _ticking(SimulationContext("S", factor=0))
    with pytest.raises(ValueError, match="needs an until"):
        app.run(segments={"every": 2})


def test_a_task_that_returns_no_record_finishes_with_no_finished_event():
    """`desflow.NO_RECORD` is the Builder form of `payload: null`: the task still runs
    its service and starts its `next`, and publishes no finished event."""
    from desflow import NO_RECORD

    app = SimulationContext("N", factor=0)
    app.add_initial_arrival("a", 1, spawn="quiet")

    @app.task(name="quiet", service_time=2, next="after")
    def quiet(task_id):
        return NO_RECORD

    @app.task(name="after")
    def after(task_id):
        return {"event_type": "after"}

    capture = _entry_times(app, 10)
    assert [row["sim_ts"] for row in capture.events("after")] == [2.0]
    assert not [
        row
        for row in capture.records
        if row.get("task") == "quiet" and "event_type" in row.get("value", {})
    ]


@pytest.mark.parametrize(
    "spec",
    [
        {"dist": "normal", "mean": "4 min", "std": "1 min"},
        {"dist": "constant", "value": "1 day"},
        {"dist": "uniform", "low": "1 min", "high": 120},
        {"dist": "triangular", "low": "1 min", "mode": "2 min", "high": "3 min"},
        {"dist": "gamma", "shape": 2, "scale": "30 s"},
        {"dist": "empirical", "values": ["1 min", 120], "weights": [1, 1]},
        {"dist": "empirical", "edges": ["1 min", "2 min", 180], "counts": [1, 2]},
    ],
)
def test_builder_reads_a_duration_in_a_distribution_as_the_blueprint_does(spec):
    """A time-valued parameter of `add_service`, `add_arrival` and a breakdown takes a
    duration, and gives the config the blueprint's `Distribution` gives."""
    from desflow.blueprint.models import Distribution

    expected = Distribution(**spec).config()
    given = {key: value for key, value in spec.items() if key != "dist"}
    app = SimulationContext("D", factor=0)
    app.add_service("s", spec["dist"], **given)
    app.add_arrival("a", spec["dist"], **given)
    assert app._services_config["s"] == expected
    assert app._arrivals.configs["a"] == expected
    app.add_breakdown(
        "r", time_to_failure=spec, repair={"dist": "constant", "value": 1}
    )
    assert app._holders.breakdowns_config["r"].time_to_failure == expected


def test_builder_takes_a_real_number_other_than_a_bool_as_a_time():
    """A NumPy number and a `Decimal` were taken as seconds before the Builder read
    durations, and still are, as the number it was given (a `Decimal` as a float)."""
    from decimal import Decimal

    import numpy

    app = SimulationContext("D", factor=0, initial_time=numpy.int64(5))
    assert app.initial_time == 5
    app = SimulationContext("D", factor=0, warmup=Decimal(7))
    assert app.warmup == 7.0
    app = SimulationContext("D", factor=0)
    app.add_service("s", "constant", value=numpy.int64(5))
    app.add_service("d", "constant", value=Decimal("2.5"))
    assert app._services_config["s"].value == 5
    assert app._services_config["d"].value == 2.5


def test_builder_refuses_a_value_that_is_not_a_duration_in_a_distribution():
    with pytest.raises(ValueError, match="soon"):
        SimulationContext("D").add_service("s", "constant", value="soon")
    with pytest.raises(ValueError, match="expected seconds or a duration"):
        SimulationContext("D").add_service("s", "constant", value=True)
    with pytest.raises(ValueError, match="expected seconds or a duration"):
        SimulationContext("D").add_breakdown(
            "r",
            time_to_failure={"dist": "constant", "value": True},
            repair={"dist": "constant", "value": 1},
        )


@pytest.mark.parametrize(
    "declare, message",
    [
        (
            lambda app: app.add_arrival(
                "a", "constant", value=1, spawn="t", max_events=2.0
            ),
            "arrival 'a': max_events is a whole number of 0 or more, an expression "
            "or a generator, got 2.0",
        ),
        (
            lambda app: app.add_arrival("a", "constant", value=1, spawn="t", group=[1]),
            "arrival 'a': group is a whole number, an expression or a generator, "
            "got [1]",
        ),
        (
            lambda app: app.add_arrival_replay("r", [], unit="weeks"),
            "unit must be one of ms, s, min, h, timestamp, got 'weeks'",
        ),
        (
            lambda app: app.add_arrival_replay("r", [], on_invalid="drop"),
            "on_invalid must be error or skip, got 'drop'",
        ),
        (
            lambda app: app.add_breakdown(
                "r", time_to_failure="x", repair={"dist": "constant", "value": 1}
            ),
            "a distribution is a mapping with a dist, got 'x'",
        ),
        (
            lambda app: app.start(until=1, stats="bogus"),
            "stats must be one of none, minimal, basic, typical, all, got 'bogus'",
        ),
        (
            lambda app: app.add_egress(MagicMock(), when="later"),
            "when takes a function, 'labels' or 'trace', got 'later'",
        ),
        (
            lambda app: app.with_stats("most"),
            "stats level must be one of none, minimal, basic, typical, all, got 'most'",
        ),
        (
            lambda app: app.with_stats("basic", interval=0),
            "stats interval must be greater than 0, got 0",
        ),
    ],
    ids=[
        "max_events a whole float",
        "group a list",
        "replay unit",
        "replay on_invalid",
        "breakdown without dist",
        "stats level",
        "egress when",
        "with_stats level",
        "with_stats interval",
    ],
)
def test_a_builder_value_its_check_refuses(declare, message):
    """Each refusal of a builder value that the earlier checks let through, with its
    exact text."""
    with pytest.raises(ValueError) as refused:
        declare(SimulationContext("S", factor=0))
    assert str(refused.value) == message


def test_stores_and_containers_are_read_only_from_a_run():
    """A store or a container is made by the run, so reading one before it is
    refused; stopping the arrivals before a run does nothing."""
    app = SimulationContext("S", factor=0)
    with pytest.raises(RuntimeError, match="Stores are not instantiated"):
        app.get_store("s")
    with pytest.raises(RuntimeError, match="Containers are not instantiated"):
        app.get_container("c")
    app.stop_arrivals()


def test_a_start_whose_environment_fails_raises_its_error():
    """A start that fails before it has an environment has no connector to stop, so
    the error of the environment is the one raised."""
    app = SimulationContext("S", factor=0)

    def broken(**settings):
        raise OSError("no environment")

    app.environment = broken
    with pytest.raises(OSError, match="no environment"):
        app.run(until=1)


def test_a_replay_with_no_row_left_logs_its_end_once(caplog):
    """A replay with no row left gives a wait that never ends, and logs that it
    stopped once, however often it is asked."""
    app = SimulationContext("S", factor=0)
    app.add_arrival_replay("r", [])

    def ask_twice(context):
        context.wait_for_arrival("r")
        context.wait_for_arrival("r")
        yield context.env.timeout(1)

    app.add_process(ask_twice)
    with caplog.at_level("INFO", logger="desflow.builder.context"):
        app.run(until=2)
    ended = [r for r in caplog.records if "replayed its last row" in r.getMessage()]
    assert len(ended) == 1


def _yaml_records(tmp_path, text, until):
    """The values of a blueprint's run that have an `n`, with their times."""
    path = tmp_path / "model.yaml"
    path.write_text(text)
    app = SimulationContext.from_yaml(path)
    sink = _Records()
    app.add_egress(sink)
    app.run(until=until)
    return app, [
        (row["sim_ts"], row["value"])
        for row in sink.records
        if isinstance(row.get("value"), dict) and "n" in row["value"]
    ]


@pytest.mark.parametrize(
    "model, message",
    [
        (
            "stores: {s: {priority: true, items: [{gen: expr, expr: '5'}]}}\n"
            "arrivals: {a: {initial: 1, spawn: t}}\n"
            "tasks: {t: {payload: {}}}\n",
            "priority store 's' starts with (priority, item) pairs; a generator in "
            "its items gave [5]",
        ),
        (
            "resources: {m: {current_cap: 1, queue: priority,"
            " preempt: {when: 't / 0 > 1'}}}\n"
            "arrivals: {a: {gap: 1, max_events: 2, spawn: t}}\n"
            "tasks: {t: {seize: m, service_time: 5, payload: {}, release: m}}\n",
            "the preempt rule of resource 'm': cannot evaluate 't / 0 > 1': "
            "division by zero",
        ),
        (
            "arrivals: {a: {gap: 't >= 0', spawn: t}}\ntasks: {t: {payload: {n: 1}}}\n",
            "arrival 'a' gap gave True, which is not a number of seconds",
        ),
    ],
    ids=["priority store items", "preempt rule", "gap not a number"],
)
def test_a_place_read_in_the_run_that_gives_a_wrong_value_stops_it(
    tmp_path, model, message
):
    """A place the run reads, a store's generated items, a preempt rule or a gap,
    that gives a value it cannot take stops the run with a message naming it."""
    with pytest.raises(Exception) as stopped:
        _yaml_records(
            tmp_path,
            "simulation: {sim_id: S, factor: 0, random_seed: 1}\n" + model,
            20,
        )
    assert message in str(stopped.value)


def test_an_arrival_with_keys_forgets_the_keys_of_entities_that_left(tmp_path):
    """An arrival with a `key` remembers which keys are in use, and once 2,048 keys
    are listed it forgets those whose entities have left, so 2,100 one-off keys
    leave 53 listed."""
    app, _ = _yaml_records(
        tmp_path,
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "arrivals: {a: {gap: 1, max_events: 2100, spawn: t,"
        " key: {gen: expr, expr: t}}}\n"
        "tasks: {t: {payload: {}}}\n",
        3000,
    )
    assert len(app._run.arrivals.live_keys["a"]) == 53


@pytest.mark.parametrize(
    "off_mode, on_at, times",
    [
        ("drop", 5.2, [1.0, 2.0, 6.0, 7.0, 8.0]),
        ("restart", 5.2, [1.0, 2.0, 6.2, 7.2, 8.2]),
        # Turned on again before the dropped arrival is due at 3
        ("restart", 2.8, [1.0, 2.0, 3.8, 4.8, 5.8, 6.8, 7.8, 8.8]),
    ],
)
def test_an_arrival_that_falls_while_its_source_is_off_draws_no_attribute(
    tmp_path, off_mode, on_at, times
):
    """An arrival due while its source is off, or dropped by a restarting source,
    starts nobody and draws no attribute, so the sequence goes on with the next
    entity that starts. A dropped arrival draws none even when the source is on
    again by the time it is due."""
    _, records = _yaml_records(
        tmp_path,
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "arrivals:\n"
        f"  a: {{gap: 1, spawn: t, off_mode: {off_mode},"
        " attributes: {n: {gen: sequence, start: 1}}}\n"
        "  stopper: {first_at: 2.5, max_events: 1, gap: 100, spawn: stop_a}\n"
        f"  starter: {{first_at: {on_at}, max_events: 1, gap: 100, spawn: start_a}}\n"
        "tasks:\n"
        "  t: {payload: {n: {gen: expr, expr: attr.n}}}\n"
        "  stop_a: {deactivate: [a], payload: {}}\n"
        "  start_a: {activate: [a], payload: {}}\n",
        9,
    )
    assert records == [(at, {"n": n}) for n, at in enumerate(times, start=1)]


def test_a_task_that_balks_gives_back_the_unit_kept_for_it(tmp_path):
    """A task that balks at a full line gives back the unit its flow's earlier task
    kept for it, so the next entity's `first` is served at 5 and done at 6."""
    _, records = _yaml_records(
        tmp_path,
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "resources:\n"
        "  r: {current_cap: 1}\n"
        "  s: {current_cap: 1, queue_limit: 0}\n"
        "arrivals:\n"
        "  a: {first_at: 0, max_events: 1, gap: 100, spawn: hold_s}\n"
        "  b: {first_at: 1, max_events: 2, gap: 4, spawn: first}\n"
        "tasks:\n"
        "  hold_s: {resource: s, service_time: 100, payload: {}}\n"
        "  first: {resource: r, service_time: 1, keep: [r], next: second,"
        " payload: {n: {gen: expr, expr: t}}}\n"
        "  second: {resource: [s, r], service_time: 1, payload: {}}\n",
        20,
    )
    assert records == [(2.0, {"n": 2}), (6.0, {"n": 6})]


def test_a_signal_resume_reads_its_limit_in_the_run_scope(tmp_path):
    """A signal's `resume` whose `limit` is an expression and that has no scope of
    its own reads the limit in the run's scope: `1 + 0` wakes one of two."""
    _, records = _yaml_records(
        tmp_path,
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "signals: {go: {on: [{resume: {task: w, limit: '1 + 0'}}]}}\n"
        "arrivals:\n"
        "  a: {initial: 2, spawn: w}\n"
        "  b: {first_at: 3, max_events: 1, gap: 100, spawn: g}\n"
        "tasks:\n"
        "  w: {wait: {suspend: true}, payload: {n: {gen: expr, expr: t}}}\n"
        "  g: {signal: go, payload: {}}\n",
        20,
    )
    assert records == [(3.0, {"n": 3})]


def test_a_preemption_made_outside_any_process_names_no_entity(tmp_path):
    """A request that preempts a task from an event callback, outside any process,
    has no entity, so the preempted record's `preempted_by` is None."""
    path = tmp_path / "model.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "resources: {m: {current_cap: 1, queue: priority, preempt: true}}\n"
        "arrivals: {a: {initial: 1, spawn: t}}\n"
        "tasks: {t: {seize: m, service_time: 10, payload: {e: done}, release: m}}\n"
    )
    app = SimulationContext.from_yaml(path)

    def preempt_from_a_callback(context):
        timer = context.env.timeout(2)
        timer.callbacks.append(
            lambda _event: context.get_resource("m").request(priority=-1, preempt=True)
        )
        yield timer

    app.add_process(preempt_from_a_callback)
    sink = _Records()
    app.add_egress(sink)
    app.run(until=20)
    preempted = [
        row["value"]
        for row in sink.records
        if isinstance(row.get("value"), dict)
        and row["value"].get("status") == "preempted"
    ]
    assert preempted == [
        {"status": "preempted", "resource": "m", "preempted_by": None, "usage_since": 0}
    ]


def _start_refusal(declare):
    """The refusal of `start()` for a model with one arrival, `a`, that spawns `t`,
    a signal `s`, and what `declare` adds."""
    app = SimulationContext("S", factor=0)
    app.add_arrival("a", "constant", value=1, spawn="t")
    app.add_signal("s")
    declare(app)
    with pytest.raises(ValueError) as refused:
        app.start(until=1)
    return str(refused.value)


def _builder_task(app, name, **settings):
    app.task(name=name, service_time=0, **settings)(lambda task_id: {})


@pytest.mark.parametrize(
    "declare, message",
    [
        (
            lambda app: _builder_task(app, "t", spawn={"task": "nosuch"}),
            "task 't' spawns task 'nosuch', which is not added; add it with "
            "@app.task()",
        ),
        (
            lambda app: (
                _builder_task(app, "t", spawn={"task": "a"}),
                _builder_task(app, "a"),
            ),
            "task 'a', which task 't' spawns, has the name of arrival 'a'; rename the "
            "task or the arrival, because their flow statistics and entity numbers "
            "would merge",
        ),
        (
            lambda app: (
                _builder_task(app, "t", on=[{"fire": {"signal": "s", "from": "x"}}]),
                app.add_signal("x"),
                _builder_task(app, "x"),
            ),
            "the on: action of task 't' fires from 'x', which is both a signal and a "
            "task; rename one",
        ),
        (
            lambda app: _builder_task(
                app, "t", on=[{"fire": {"signal": "s", "from": "nowhere"}}]
            ),
            "the on: action of task 't' fires from 'nowhere', which is neither a "
            "signal nor a task",
        ),
    ],
    ids=["spawn unknown", "spawn an arrival's name", "from both", "from neither"],
)
def test_a_builder_name_the_start_refuses(declare, message):
    """Each refusal of a `spawn` or `fire from` name at the start of a builder run,
    which a blueprint refuses at load."""
    assert _start_refusal(declare) == message


def test_an_on_action_deactivates_and_activates_an_arrival(tmp_path):
    """`deactivate` stops an arrival's events and `activate` starts them again."""
    outcome = run_yaml_text(
        tmp_path,
        """
simulation: {sim_id: S, factor: 0, random_seed: 42}
arrivals:
  a: {dist: constant, value: 1, first_at: 0, spawn: t}
  s: {initial: 1, spawn: stop}
tasks:
  t: {payload: {e: a}}
  stop: {wait: {timeout: 2.5}, payload: {e: s}, on: [{deactivate: a}], next: go}
  go: {wait: {timeout: 2}, payload: {e: g}, on: [{activate: a}]}
run: {until: 6}
""",
        {},
    )
    assert outcome.error is None
    assert [
        (round(r["sim_ts"], 2), r["value"]["e"])
        for r in outcome.records
        if r["stream_type"] == "event" and "status" not in r["value"]
    ] == [(0.0, "a"), (1.0, "a"), (2.0, "a"), (2.5, "s"), (4.5, "g"), (5.0, "a")]


# A-U3: each unknown name an expression reads is refused with its own message, and in
# one expression the first refused follows the order each check reads the kinds in
_UNKNOWN_REFERENCES = [
    (
        "task",
        "users.nope > 0",
        "task 't' reads users.nope, but no resource is called 'nope'; add it with add_resource()",
    ),
    (
        "task",
        "queue.nope > 0",
        "task 't' reads queue.nope, but no resource is called 'nope'; add it with add_resource()",
    ),
    (
        "task",
        "held(nope) > 0",
        "task 't' reads held.nope, but no resource is called 'nope'; add it with add_resource()",
    ),
    (
        "task",
        "waiting(nope) > 0",
        "task 't' reads waiting.nope, but no resource is called 'nope'; add it with add_resource()",
    ),
    (
        "task",
        "level.nope > 0",
        "task 't' reads level.nope, but no store or container is called 'nope'",
    ),
    (
        "task",
        "putters.nope > 0",
        "task 't' reads putters.nope, but no store or container is called 'nope'",
    ),
    (
        "task",
        "getters.nope > 0",
        "task 't' reads getters.nope, but no store or container is called 'nope'",
    ),
    (
        "task",
        "putting(nope) > 0",
        "task 't' reads putting(nope), but no store or container is called 'nope'",
    ),
    (
        "task",
        "getting(nope) > 0",
        "task 't' reads getting(nope), but no store or container is called 'nope'",
    ),
    (
        "task",
        "count(nope) > 0",
        "task 't' reads count(nope), but no store is called 'nope'",
    ),
    (
        "task",
        "len(items(nope)) > 0",
        "task 't' reads items(nope), but no store is called 'nope'",
    ),
    (
        "task",
        "running(nope) > 0",
        "task 't' reads running(nope), but no task is called 'nope'",
    ),
    (
        "task",
        'registry["S.nope"].value > 0',
        "task 't' reads registry[\"S.nope\"], which is not a registry path",
    ),
    (
        "task",
        'registry["S.nope"] > 0',
        "task 't' reads registry[\"S.nope\"], which is not a registry path",
    ),
    (
        "task",
        "users.nope > 0 and level.nope2 > 0",
        "task 't' reads level.nope2, but no store or container is called 'nope2'",
    ),
    (
        "task",
        "level.nope2 > 0 and users.nope > 0",
        "task 't' reads level.nope2, but no store or container is called 'nope2'",
    ),
    (
        "task",
        "running(nope) > 0 and count(nope2) > 0",
        "task 't' reads running(nope), but no task is called 'nope'",
    ),
    (
        "task",
        "count(nope2) > 0 and running(nope) > 0",
        "task 't' reads running(nope), but no task is called 'nope'",
    ),
    (
        "task",
        'registry["S.nope"] > 0 and users.nope > 0',
        "task 't' reads users.nope, but no resource is called 'nope'; add it with add_resource()",
    ),
    (
        "task",
        "users.packer > 0",
        "task 't' reads users.packer, but no resource is called 'packer'; did you mean 'packers'?",
    ),
    (
        "task",
        "level.shel > 0",
        "task 't' reads level.shel, but no store or container is called 'shel'",
    ),
    (
        "rule",
        "users.nope > 0",
        "signal 's' on entry 0 when reads users.nope, but no resource is called 'nope'",
    ),
    (
        "rule",
        "queue.nope > 0",
        "signal 's' on entry 0 when reads queue.nope, but no resource is called 'nope'",
    ),
    (
        "rule",
        "held(nope) > 0",
        "signal 's' on entry 0 when reads held.nope, but no resource is called 'nope'",
    ),
    (
        "rule",
        "waiting(nope) > 0",
        "signal 's' on entry 0 when reads waiting.nope, but no resource is called 'nope'",
    ),
    (
        "rule",
        "level.nope > 0",
        "signal 's' on entry 0 when reads level.nope, but no store or container is called 'nope'",
    ),
    (
        "rule",
        "putters.nope > 0",
        "signal 's' on entry 0 when reads putters.nope, but no store or container is called 'nope'",
    ),
    (
        "rule",
        "getters.nope > 0",
        "signal 's' on entry 0 when reads getters.nope, but no store or container is called 'nope'",
    ),
    (
        "rule",
        "putting(nope) > 0",
        "signal 's' on entry 0 when reads putting.nope, but no store or container is called 'nope'",
    ),
    (
        "rule",
        "getting(nope) > 0",
        "signal 's' on entry 0 when reads getting.nope, but no store or container is called 'nope'",
    ),
    (
        "rule",
        "count(nope) > 0",
        "signal 's' on entry 0 when reads count or items of 'nope', but no store is called 'nope'",
    ),
    (
        "rule",
        "len(items(nope)) > 0",
        "signal 's' on entry 0 when reads count or items of 'nope', but no store is called 'nope'",
    ),
    (
        "rule",
        "signal.nope.count > 0",
        "signal 's' on entry 0 when reads signal.nope, but no signal is called 'nope'",
    ),
    (
        "rule",
        "running(nope) > 0",
        "signal 's' on entry 0 when reads running(nope), but no task is called 'nope'",
    ),
    (
        "rule",
        'registry["S.nope"].value > 0',
        "signal 's' on entry 0 when reads registry[\"S.nope\"], which is not a registry path",
    ),
    (
        "rule",
        'registry["S.nope"] > 0',
        "signal 's' on entry 0 when reads registry[\"S.nope\"], which is not a registry path",
    ),
    (
        "rule",
        "users.nope > 0 and level.nope2 > 0",
        "signal 's' on entry 0 when reads users.nope, but no resource is called 'nope'",
    ),
    (
        "rule",
        "level.nope2 > 0 and users.nope > 0",
        "signal 's' on entry 0 when reads users.nope, but no resource is called 'nope'",
    ),
    (
        "rule",
        "running(nope) > 0 and count(nope2) > 0",
        "signal 's' on entry 0 when reads count or items of 'nope2', but no store is called 'nope2'",
    ),
    (
        "rule",
        "count(nope2) > 0 and running(nope) > 0",
        "signal 's' on entry 0 when reads count or items of 'nope2', but no store is called 'nope2'",
    ),
    (
        "rule",
        'registry["S.nope"] > 0 and users.nope > 0',
        "signal 's' on entry 0 when reads users.nope, but no resource is called 'nope'",
    ),
    (
        "rule",
        "users.packer > 0",
        "signal 's' on entry 0 when reads users.packer, but no resource is called 'packer'; did you mean 'packers'?",
    ),
    (
        "rule",
        "level.shel > 0",
        "signal 's' on entry 0 when reads level.shel, but no store or container is called 'shel'; did you mean 'shelf'?",
    ),
]


def _reading_model(kind, form):
    """A model whose task balk, or whose signal on entry, reads `form`."""
    app = SimulationContext("S", factor=0, random_seed=1)
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_store("shelf", current_cap=5, max_cap=5)
    app.add_arrival_replay("a", [{"time": 0}], spawn="t")
    if kind == "task":
        app.add_service("s", "constant", value=1.0)
        app.task(service_id="s", resource_id="packers", name="t", balk=form)(
            lambda task_id: {}
        )
    else:
        app.add_signal("s", on=[{"when": form, "fire": {"signal": "s"}}])
        app.task(name="t", wait={"timeout": 1})(lambda task_id: {"e": "t"})
    return app


@pytest.mark.parametrize("kind,form,message", _UNKNOWN_REFERENCES)
def test_an_unknown_name_an_expression_reads_is_refused_with_its_message(
    kind, form, message
):
    with pytest.raises(ValueError) as refused:
        _reading_model(kind, form).run(until=1)
    assert str(refused.value).startswith(message)


@pytest.mark.parametrize(
    "form,message",
    [
        ("users.nope", "reads users.nope, but no resource is called 'nope'"),
        ("level.nope", "reads level.nope, but no store or container is called"),
        (
            "signal.nope.count",
            "signal 'nope', which is not added|reads signal.nope, but no signal",
        ),
        ("running(nope)", "reads running(nope), but no task is called 'nope'"),
        (
            'registry["S.nope"]',
            'reads registry["S.nope"], which is not a registry path',
        ),
    ],
)
def test_a_name_an_expr_generator_reads_is_refused_wherever_the_generator_is(
    form, message
):
    """A generator `expr` is checked as the same text written as an expression: in a
    task's vars and service_time, an arrival's attributes and gap, and a scenario
    step's value."""
    expr = generator({"gen": "expr", "expr": form})

    def model():
        app = SimulationContext("S", factor=0, random_seed=1)
        app.add_variable("v", 0)
        app.add_arrival_replay("a", [{"time": 0}], spawn="t")
        return app

    def add_task(app, **options):
        app.task(name="t", **options)(lambda task_id: {})

    cases = [
        lambda app: add_task(app, service_time=1, vars={"x": expr}),
        lambda app: add_task(app, service_time=expr),
        lambda app: (
            add_task(app)
            or app.add_arrival("b", gap=1, spawn="t", attributes={"x": expr})
        ),
        lambda app: add_task(app) or app.add_arrival("b", gap=expr, spawn="t"),
        lambda app: (
            add_task(app) or app.add_step(at=1, path="S.variables.v", value=expr)
        ),
    ]
    for declare in cases:
        app = model()
        declare(app)
        with pytest.raises(ValueError) as refused:
            app.start(until=1)
        assert re.search(
            re.escape(message) if "|" not in message else message, str(refused.value)
        )


@pytest.mark.parametrize(
    "steps,message",
    [
        (
            [{"at": 1, "interrupt": {"task": "nope"}}],
            "scenario step at 1 s interrupts task 'nope', which is not added; "
            "add it with @app.task()",
        ),
        (
            [
                {"at": 2, "interrupt": {"task": "nope"}},
                {"at": 1, "path": "S.nope", "value": 1},
            ],
            "scenario step at 1 s sets 'S.nope', which is not a registry path",
        ),
        (
            [
                {"at": 1, "interrupt": {"task": "nope"}},
                {"at": 2, "path": "S.nope", "value": 1},
            ],
            "scenario step at 1 s interrupts task 'nope', which is not added; "
            "add it with @app.task()",
        ),
        (
            [
                {"at": 1, "path": "S.resources.packers.max_cap", "value": 3},
                {"at": 1, "path": "S.nope", "value": 1},
            ],
            "scenario step at 1 s sets 'S.nope', which is not a registry path",
        ),
    ],
)
def test_the_scenario_check_refuses_the_first_step_in_time_order(steps, message):
    """A-U4: the steps are checked in the order the run applies them, and steps at
    one time in the order they are written."""
    app = SimulationContext("S", factor=0, random_seed=1)
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("s", "constant", value=1.0)
    app.add_arrival_replay("a", [{"time": 0}], spawn="t")
    app.task(service_id="s", resource_id="packers", name="t")(lambda task_id: {})
    app.add_scenario(steps)
    with pytest.raises(ValueError) as refused:
        app.run(until=1)
    assert str(refused.value) == message


@pytest.mark.parametrize("form", [str, Expression], ids=["text", "an Expression"])
def test_the_registry_paths_a_scenario_interrupt_where_reads_are_checked(form):
    """A scenario interrupt's `where` is checked before the run, as text and as an
    `Expression`, as a YAML scenario's is; the attributes it reads are not."""

    def model(where):
        app = SimulationContext("S", factor=0, random_seed=1)
        app.add_variable("v", 0)
        app.add_arrival_replay("a", [{"time": 0}], spawn="t")
        app.task(name="t", service_time=5)(lambda task_id: {})
        app.add_scenario([{"at": 1, "interrupt": {"task": "t", "where": form(where)}}])
        return app

    with pytest.raises(ValueError) as refused:
        model("registry['S.variables.missing'] == 1").start(until=2)
    assert str(refused.value).startswith(
        "scenario step at 1 s interrupt where reads "
        'registry["S.variables.missing"], which is not a registry path'
    ), refused.value
    model("registry['S.variables.v'] == 0 or attr.n == 1").run(until=2)


_TIMER_ORDERS = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
arrivals:
  a: {replay: {path: a.csv}, spawn: t}
signals:
  go: {}
tasks:
  t:
    wait: {any: {timer: {timeout: 2}, go: {signal: go}}}
    service_time: 10
    on_interrupt: abandon
    payload: {e: t}
scenario:
  - at: 5
    interrupt: {task: t, where: "WHERE", cause: late}
"""


def _timer_orders(form):
    """The builder twin of `_TIMER_ORDERS`, with the where given in `form`."""

    def model(where):
        app = SimulationContext("S", factor=0, random_seed=42)
        app.add_signal("go")
        app.add_arrival_replay("a", [{"time": 0}, {"time": 1}, {"time": 4}], spawn="t")
        app.task(
            name="t",
            wait={"any": {"timer": {"timeout": 2}, "go": {"signal": "go"}}},
            service_time=10,
            on_interrupt="abandon",
        )(lambda task_id: {"e": "t"})
        app.add_scenario(
            [
                {
                    "at": 5,
                    "interrupt": {"task": "t", "where": form(where), "cause": "late"},
                }
            ]
        )
        return app

    return model


def _interrupted(records):
    return [
        (r["sim_ts"], r["key"])
        for r in records
        if r["stream_type"] == "event" and r["value"].get("status") == "interrupted"
    ]


@pytest.mark.parametrize("form", [str, Expression], ids=["text", "an Expression"])
def test_a_scenario_interrupt_where_reads_the_wait_of_the_task_it_interrupts(
    tmp_path, form
):
    """A scenario interrupt's `where` is read in each instance's scope, so it reads
    the interrupted task's `wait` branches, in YAML and in the builder. At 5 s two
    orders have passed their timer and are interrupted. The third is still in its
    wait, reads `wait.timer.done` as false, and runs to the end."""
    outcome = run_yaml_text(
        tmp_path, _TIMER_ORDERS.replace("WHERE", "wait.timer.done"), {"a": [0, 1, 4]}
    )
    assert outcome.error is None
    assert _interrupted(outcome.records) == [(5.0, "a-1"), (5.0, "a-2")]
    rec = RecordingEgress()
    app = _timer_orders(form)("wait.timer.done")
    app.add_egress(rec)
    app.run(until=30)
    assert _interrupted(rec.records) == [(5.0, "a-1"), (5.0, "a-2")]
    assert [r["sim_ts"] for r in rec.records if r["value"].get("e") == "t"] == [16.0]


@pytest.mark.parametrize("form", [str, Expression], ids=["text", "an Expression"])
def test_a_scenario_interrupt_where_that_names_no_branch_of_the_task_is_refused(
    tmp_path, form
):
    """A `wait.<branch>` in a scenario interrupt's `where` names a branch of the
    interrupted task's wait, in YAML at load and in the builder before the run."""
    (tmp_path / "a.csv").write_text("time\n0\n")
    (tmp_path / "model.yaml").write_text(
        _TIMER_ORDERS.replace("WHERE", "wait.timr.done")
    )
    with pytest.raises(BlueprintError) as refused:
        SimulationContext.from_yaml(tmp_path / "model.yaml")
    assert str(refused.value).endswith(
        ":15: wait.timr is not a branch of the wait of task 't', which the step "
        "interrupts; did you mean 'timer'?"
    ), refused.value
    with pytest.raises(ValueError) as refused:
        _timer_orders(form)("wait.timr.done").start(until=2)
    assert str(refused.value) == (
        "scenario step at 5 s interrupt where reads wait.timr, which is not a branch "
        "of the wait of task 't'; did you mean 'timer'?"
    )


def test_the_scenario_walk_yields_each_step_with_its_problem():
    """A-U4: the walk the scenario checks share takes the steps in time order, skips a
    step with no path, a member's path or a generated value, and ends at the first
    problem the registry's own check finds."""
    from desflow.builder.context import scratch_registry
    from desflow.builder.scenario import walk_steps
    from desflow.values.models import Expr

    app = SimulationContext("S", factor=0)
    app.add_resource("m", current_cap=1, max_cap=2, queue_limit=3)

    def walk(steps):
        member = lambda path: path.startswith("S.resources.desk[")  # noqa: E731
        return list(walk_steps(steps, scratch_registry(app), member))

    cap = "S.resources.m.current_cap"
    refusal = "resource 'm' needs a whole number of 0 or more for current_cap, got 'x'"
    assert walk([(1, cap, "x")]) == [(0, "value", refusal)]
    assert walk(
        [(2, "S.nope", 1), (1, None, None), (1, "S.resources.desk[3].current_cap", 1)]
    ) == [(1, None, None), (2, None, None), (0, "path", None)]
    assert walk([(1, cap, Expr(gen="expr", expr="1"))]) == [(0, None, None)]
    assert walk([(1, cap, 2.0), (1, cap, 2)]) == [(0, None, None), (1, None, None)]
    assert walk([(1, "S.resources.m.queue_limit", "x")])[0][:2] == (0, "value")
