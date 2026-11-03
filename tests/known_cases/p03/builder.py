"""P03: First arrival at 0, then drawn gaps.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=5, first_at=0, spawn="t")

    @app.task(name="t")
    def t(task_id):
        return {"event_type": "arrived"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=12)
