"""ST-E06: An end time written as text.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")

    @app.task(name="pem", next="pem_wait")
    def pem(task_id):
        return {"event_type": "now"}

    @app.task(name="pem_wait", service_time=1, next="pem")
    def pem_wait(task_id):
        return {"event_type": "tick"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=3.141592)
