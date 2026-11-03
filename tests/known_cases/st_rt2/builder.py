"""ST-RT2: A paced run that is called twice.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0.1, strict=True, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")

    @app.task(name="pem", service_time=1, next="pem")
    def pem(task_id):
        return {"event_type": "tick"}

    return app


def run(app: SimulationContext):
    """Runs to 2 and then to 4, as the run section of `model.yaml` does."""
    return app.run(until=4, segments=[{"until": 2}, {"until": 4}])
