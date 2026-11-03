"""ST-X03: A failed event that nobody defuses stops the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("ev", once=True)
    app.add_initial_arrival("a", size, spawn="fail_it")

    @app.task(
        name="fail_it", on=[{"fire": {"signal": "ev", "fail": True, "value": ""}}]
    )
    def fail_it(task_id):
        return {"event_type": "fail"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=1, on_unhandled_failure="stop")
