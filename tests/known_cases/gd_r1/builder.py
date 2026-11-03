"""GD-R1: One time unit takes a tenth of a second.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0.1, strict=True, random_seed=42)
    app.add_initial_arrival("a", 1, spawn="one")

    @app.task(name="one", service_time=1)
    def one(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=1.5)
