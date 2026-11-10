"""ST-RT7: Run until an event that never triggers, on a paced environment.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0.1, random_seed=42)
    app.add_signal("never", once=True)
    app.add_initial_arrival("a", size, spawn="pem")

    @app.task(name="pem")
    def pem(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs until the signal `never` fires, which it does not."""
    return app.run(stop={"signal": "never"})
