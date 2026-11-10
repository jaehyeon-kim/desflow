"""ST-R07: An exception inside the with block releases the unit.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("raising", size, spawn="raising")
    app.add_initial_arrival("plain", 1, spawn="plain")

    @app.task(name="raising", resource_id="res", service_time=1)
    def raising(task_id):
        return {"event_type": "logged"}

    @app.task(name="plain", resource_id="res", service_time=1)
    def plain(task_id):
        return {"event_type": "logged"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
