"""ST-E03: Run until an event that has already been processed.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("done", once=True)
    app.add_initial_arrival("a", size, spawn="pem")

    @app.task(name="pem", service_time=1, signal="done")
    def pem(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(stop={"signal": "done"})
