"""ST-R02: Two users share one unit with the with form.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("users", size, spawn="pem")

    @app.task(name="pem", resource_id="res", service_time=1)
    def pem(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
