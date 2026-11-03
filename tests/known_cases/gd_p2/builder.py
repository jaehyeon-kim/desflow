"""GD-P2: Wait for charging and parking together.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="EV", factor=0, random_seed=42)
    app.add_initial_arrival("ev", size, spawn="drive")

    @app.task(
        name="drive",
        service_time=generator({"gen": "integer", "bounds": [20, 40]}),
        next="run_ev",
    )
    def drive(task_id):
        return {"event_type": "start_parking"}

    @app.task(
        name="run_ev",
        wait={"all": {"charging": {"task": "bat_ctrl"}, "parking": {"task": "park"}}},
        next="drive",
    )
    def run_ev(task_id):
        return {"event_type": "stop_parking"}

    @app.task(
        name="bat_ctrl", service_time=generator({"gen": "integer", "bounds": [30, 90]})
    )
    def bat_ctrl(task_id):
        return {"event_type": "bat_done"}

    @app.task(
        name="park", service_time=generator({"gen": "integer", "bounds": [60, 360]})
    )
    def park(task_id):
        return {"event_type": "parked"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=310)
