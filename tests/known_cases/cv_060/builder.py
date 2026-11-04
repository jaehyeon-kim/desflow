"""CV-060: An exception that leaves the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="boom")

    @app.task(name="boom", fail="True", fail_reason="Onoes!")
    def boom(task_id):
        return {"event_type": "boom"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(on_unhandled_failure="stop")
