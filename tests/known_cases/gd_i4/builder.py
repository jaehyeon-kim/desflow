"""GD-I4: Four cars share a charging station of two spots.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 4) -> SimulationContext:
    app = SimulationContext(sim_id="Car", factor=0, random_seed=42)
    app.add_resource("bcs", current_cap=2)
    app.add_initial_arrival(
        "cars",
        size,
        key=lambda n: "Car {}".format(n - 1),
        attributes={
            "driving_time": generator({"gen": "sequence", "start": 0, "step": 2})
        },
        spawn="drive",
    )

    @app.task(name="drive", service_time="attr.driving_time", next="charge")
    def drive(task_id):
        return {"event_type": "arrived"}

    @app.task(name="charge", resource_id="bcs", service_time=5)
    def charge(task_id):
        return {"event_type": "charged"}

    return app


def run(app: SimulationContext):
    """Runs until no event is left: the Builder source has no end of its own, as `model.yaml` has no `run` section."""
    return app.run()
