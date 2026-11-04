"""DS-A4: An arrival gap computed from the time.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="A4", factor=0, random_seed=42)
    app.add_arrival("a", gap="1 + t / 4", spawn="t")

    @app.task(name="t")
    def t(task_id):
        return {"event_type": "arrived"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=8)
