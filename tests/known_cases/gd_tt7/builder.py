"""GD-TT7: A run with no end time stops when no event is left.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", 1, spawn="log_now")

    @app.task(name="log_now", next="wait_one")
    def log_now(task_id):
        return {"event_type": "logged"}

    @app.task(
        name="wait_one",
        service_time=1,
        next={"when": [{"condition": "t < 2", "task": "log_now"}]},
    )
    def wait_one(task_id):
        return {"event_type": "waited"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)
