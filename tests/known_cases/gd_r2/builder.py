"""GD-R2: A step slower than real time fails only in strict mode.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=1e-06, strict=True, random_seed=42)
    app.add_initial_arrival("a", size, spawn="one")

    @app.task(name="one", service_time=1)
    def one(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=2)
