"""CV-035: Run with no end time.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=1, spawn="tick", max_events=3)

    @app.task(name="tick")
    def tick(task_id):
        return {"event_type": "tick"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
