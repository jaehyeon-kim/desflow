"""P68: A warm-up period.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42, warmup=5)
    app.add_arrival("orders", "constant", value=2, spawn="t")

    @app.task(name="t")
    def t(task_id):
        return {"e": 1}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=9)
