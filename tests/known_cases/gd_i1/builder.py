"""GD-I1: A car that parks and drives for ever.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Car", factor=0, random_seed=42)
    app.add_service("park", dist="constant", value=5)
    app.add_service("drive", dist="constant", value=2)
    app.add_initial_arrival("car", size, spawn="parking")

    @app.task(name="parking", service_id="park", next="driving")
    def parking(task_id):
        return {"event_type": "parked"}

    @app.task(name="driving", service_id="drive", next="parking")
    def driving(task_id):
        return {"event_type": "drove"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=15)
