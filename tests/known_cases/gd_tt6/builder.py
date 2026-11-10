"""GD-TT6: An unhandled exception stops the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", 1, spawn="boom")

    @app.task(name="boom", service_time=1, fail="True", fail_reason="boom")
    def boom(task_id):
        return {"event_type": "boom"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=5, on_unhandled_failure="stop")
