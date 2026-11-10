"""ST-V07: Run until an or condition with an event already succeeded.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("a", once=True)
    app.add_signal("b", once=True)
    app.add_initial_arrival("m", size, spawn="maker")

    @app.task(name="maker", on=[{"fire": {"signal": "a"}}])
    def maker(task_id):
        return {"event_type": "made"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(stop={"condition": "signal.a.count >= 1 or signal.b.count >= 1"})
