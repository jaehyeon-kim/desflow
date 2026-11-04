"""A run to an event that never triggers ends with SimPy's error on the Low-level API,
unless an ingress is set up that can trigger it, and a blueprint with a `stop` that never
fires runs to its time."""

import pytest

from desflow import DynamicRealtimeEnvironment
from tests.conformance.harness import RecordingEgress


def test_run_until_an_event_with_only_internal_timers_left_raises_simpys_error():
    env = DynamicRealtimeEnvironment(factor=0.1)
    env.setup_egress([RecordingEgress()], lag_monitor_interval=None)
    try:
        with pytest.raises(RuntimeError, match='No scheduled events left but "until"'):
            env.run(until=env.event())
    finally:
        env.teardown()


class _Later:
    """An ingress that sends one change after 0.3 s of wall time."""

    async def run(self, update_queue):
        import asyncio

        await asyncio.sleep(0.3)
        update_queue.put(("S.variables.v", 2))


def test_a_run_to_an_event_goes_on_while_an_ingress_can_trigger_it():
    """No model event is queued while the process waits, but the ingress sends the
    change that ends the wait, so the run goes on until it arrives."""
    from desflow import SimParameter

    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(SimParameter(sim_id="S", variables={"v": 1}))
    env.setup_ingress([_Later()])

    def wait():
        v = env.registry.get("S.variables.v")
        yield v.wait_for_change()
        return v.value

    try:
        assert env.run(until=env.process(wait())) == 2
    finally:
        env.teardown()


def test_a_blueprint_run_with_a_stop_that_never_fires_runs_to_its_time(tmp_path):
    """A `stop` reaches `env.run` as an event, so the run has a time `until` of its own
    and only the timers of desflow's own (the statistics readings) after the last model
    event: it runs to the time, and not to SimPy's error."""
    from desflow import SimulationContext

    path = tmp_path / "model.yaml"
    path.write_text(
        """
simulation: {sim_id: S, factor: 0}
stats: {level: minimal, interval: 1}
signals: {never: {}}
resources: {r: {current_cap: 1}}
arrivals: {a: {dist: constant, value: 1, spawn: t, max_events: 2}}
tasks: {t: {resource: r, service_time: 1, payload: {e: 1}}}
run: {until: 30, stop: {signal: never}}
"""
    )
    app = SimulationContext.from_yaml(path)
    recorder = RecordingEgress()
    app.add_egress(recorder)
    app.run()
    assert app.env.now == 30
    readings = [
        r["sim_ts"] for r in recorder.records if r["stream_type"] == "telemetry"
    ]
    assert readings and readings[-1] >= 29
