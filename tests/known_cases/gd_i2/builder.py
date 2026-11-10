"""GD-I2: A car that waits for a charging child flow.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Car", factor=0, random_seed=42)
    app.add_service("charge", dist="constant", value=5)
    app.add_service("drive", dist="constant", value=2)
    app.add_initial_arrival("car", size, spawn="start_parking")

    @app.task(name="start_parking", wait={"task": "charging"}, next="driving")
    def start_parking(task_id):
        return {"event_type": "start_driving"}

    @app.task(name="charging", service_id="charge")
    def charging(task_id):
        return {"event_type": "charged"}

    @app.task(name="driving", service_id="drive", next="start_parking")
    def driving(task_id):
        return {"event_type": "parked_again"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=15)
