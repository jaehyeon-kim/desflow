"""ST-RT5: An end time below the clock is refused.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=1.0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")

    @app.task(name="pem")
    def pem(task_id):
        return {"event_type": "tick"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=-1)
