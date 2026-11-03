"""P41: Renege on a drawn patience.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("a", size, spawn="job")

    @app.task(
        name="job",
        resource_id="m",
        patience=generator({"gen": "sample", "dist": "uniform", "low": 1, "high": 5}),
        service_time=10,
    )
    def job(task_id):
        return {"event_type": "served"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)
