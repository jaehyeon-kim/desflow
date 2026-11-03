"""ST-E02: Run in three segments.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem", attributes={"t": 5})
    app.add_initial_arrival("b", 1, spawn="pem", attributes={"t": 10})
    app.add_initial_arrival("c", 1, spawn="pem", attributes={"t": 15})

    @app.task(name="pem", service_time="attr.t")
    def pem(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model with the `run` section of `model.yaml`."""
    return app.run(segments=[{"until": 10}, {"until": 15}, {}])
