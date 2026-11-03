"""GD-RS1: Request, hold and release one unit.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("a", size, spawn="user")

    @app.task(name="user", seize="res", next="use")
    def user(task_id):
        return {"event_type": "got"}

    @app.task(name="use", service_time=1, release="res")
    def use(task_id):
        return {"event_type": "used"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
