"""GD-T1: Events at one time run in the order they were scheduled.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="finish_at_one")
    app.add_initial_arrival("b", 2, spawn="finish_at_two")

    @app.task(name="finish_at_one", service_time=1)
    def finish_at_one(task_id):
        return {"event_type": "finished"}

    @app.task(name="finish_at_two", service_time=2)
    def finish_at_two(task_id):
        return {"event_type": "finished"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
