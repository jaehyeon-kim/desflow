"""P69: Leave with a probability.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival("orders", "constant", value=1, spawn="t")

    @app.task(name="t", next={"weights": {"gone": 1, "stay": 1}})
    def t(task_id):
        return {"e": 1}

    @app.task(name="gone")
    def gone(task_id):
        return {"left": 1}

    @app.task(name="stay")
    def stay(task_id):
        return {"stay": 1}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=5)
