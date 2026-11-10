"""ST-C02: And with an event that never triggers stays blocked.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("never", once=True)
    app.add_initial_arrival("a", size, spawn="pem")

    @app.task(name="pem", wait={"timeout": 1}, next="blocked")
    def pem(task_id):
        return {"event_type": "waited"}

    @app.task(
        name="blocked", wait={"all": {"t": {"timeout": 0}, "n": {"signal": "never"}}}
    )
    def blocked(task_id):
        return {"event_type": "unreachable"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
