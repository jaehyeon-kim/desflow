"""GD-P1: A car and a battery controller that pause and wake each other.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="EV", factor=0, random_seed=42)
    app.add_signal("reactivate", once=False)
    app.add_initial_arrival("ev", size, spawn="drive")
    app.add_initial_arrival("ctrl", 1, spawn="passivate")

    @app.task(
        name="drive",
        service_time=generator({"gen": "integer", "bounds": [20, 40]}),
        signal="reactivate",
        next="park",
    )
    def drive(task_id):
        return {"event_type": "start_parking"}

    @app.task(
        name="park",
        service_time=generator({"gen": "integer", "bounds": [60, 360]}),
        next="drive",
    )
    def park(task_id):
        return {"event_type": "stop_parking"}

    @app.task(name="passivate", wait={"signal": "reactivate"}, next="charge")
    def passivate(task_id):
        return {"event_type": "reactivated"}

    @app.task(
        name="charge",
        service_time=generator({"gen": "integer", "bounds": [30, 90]}),
        next="passivate",
    )
    def charge(task_id):
        return {"event_type": "charged"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=150)
