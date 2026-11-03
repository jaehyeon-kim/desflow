"""ST-E04: Run until an event that never triggers.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("never", once=True)
    app.add_initial_arrival("a", size, spawn="pem")

    @app.task(name="pem")
    def pem(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(stop={"signal": "never"})
