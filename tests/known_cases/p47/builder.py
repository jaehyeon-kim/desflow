"""P47: Round robin.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_signal("rr", once=False)
    app.add_initial_arrival("a", size, spawn="route")

    @app.task(
        name="route",
        signal="rr",
        next={
            "when": [
                {"condition": "signal.rr.count % 3 == 1", "task": "q1"},
                {"condition": "signal.rr.count % 3 == 2", "task": "q2"},
            ],
            "default": "q3",
        },
    )
    def route(task_id):
        return {"event_type": "dealt"}

    @app.task(name="q1")
    def q1(task_id):
        return {"event_type": "q1"}

    @app.task(name="q2")
    def q2(task_id):
        return {"event_type": "q2"}

    @app.task(name="q3")
    def q3(task_id):
        return {"event_type": "q3"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
