import asyncio
import queue
from unittest.mock import MagicMock, patch

import pytest

from desflow import SimulationContext


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
