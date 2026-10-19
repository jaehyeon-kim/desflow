import asyncio
import queue
from unittest.mock import MagicMock, patch

import pytest

from desflow import Entity, SimulationContext


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
    assert "packers" in app._resources_config
    assert app._resources_config["packers"].max_cap == 5

    # Check continuous containers (NEW)
    assert "wear_structural" in app._containers_config
    assert app._containers_config["wear_structural"].max_cap == 100.0

    # Check dynamic variables (NEW)
    assert "velocity" in app._variables_config
    assert app._variables_config["velocity"]["value"] == 5.0

    # Check statistical distributions
    assert "packing" in app._services_config
    assert app._services_config["packing"].mean == 5.0
    assert "standard" in app._arrivals_config
    assert app._arrivals_config["standard"].rate == 2.0

    # Check batching state
    assert app._batch_size == 100
    assert app._flush_interval == 0.5


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


@patch("desflow.core.context.DynamicRealtimeEnvironment")
def test_simulation_execution_wiring(MockEnv):
    """
    Validates the .run() orchestration logic without running a real SimPy loop.
    Ensures connectors and registry are wired correctly.
    """
    mock_env_instance = MockEnv.return_value
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
    MockEnv.assert_called_once_with(factor=0.0)
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

    assert app._max_queued_batches == 2000
    assert app._drain_stall_seconds == 30.0


@patch("desflow.core.context.DynamicRealtimeEnvironment")
def test_egress_bounds_reach_the_environment(MockEnv):
    """What the builder collects has to arrive at setup_egress, or it does nothing."""
    instance = MockEnv.return_value
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


@patch("desflow.core.context.DynamicRealtimeEnvironment")
def test_per_provider_cadence_reaches_the_environment(MockEnv):
    """A cadence given on add_egress has to arrive, aligned by position."""
    instance = MockEnv.return_value

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
    with pytest.raises(ValueError, match="needs a wait, a resource or a get"):
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
    with pytest.raises(ValueError, match="need a fail condition"):
        SimulationContext("TestSim").task(on_fail="t")


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
    app.run(until=2)
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


def test_resources_acquired_in_opposite_orders_are_refused():
    """Two tasks that each hold one resource while waiting for the other could wait
    for ever, so the model is refused, also through a keep handoff."""
    app = _two_resources()

    @app.task(service_id="s", resource_id=["picker", "bench"], name="a")
    def a(task_id):
        return {}

    @app.task(service_id="s", resource_id=["bench", "picker"], name="b")
    def b(task_id):
        return {}

    with pytest.raises(ValueError, match="in one order everywhere"):
        app.run(until=1)

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

    with pytest.raises(ValueError, match="keeping it for 'c'"):
        handoff.run(until=1)


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


def test_preemption_settings_are_checked():
    with pytest.raises(ValueError, match="needs queue='priority'"):
        SimulationContext("TestSim").add_resource("m", 1, 1, preempt=True)
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
    with pytest.raises(ValueError, match="stop takes a signal or a condition"):
        SimulationContext("TestSim", factor=0.0).run(until=1, stop={"event": "x"})


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
        "desflow.core.context.DynamicResource", side_effect=RuntimeError("broken")
    ):
        with pytest.raises(RuntimeError, match="broken"):
            app.run(until=10)
    thread = app._env._egress_thread
    thread.join(timeout=5)
    assert not thread.is_alive()


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
