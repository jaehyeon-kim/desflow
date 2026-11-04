"""GA-2: A machine slows down as it wears, then is repaired.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext

SCHEDULE = [
    {"at": 3600, "path": "Press.service.stamp.mean", "value": 12},
    {"at": 7200, "path": "Press.service.stamp.mean", "value": 14},
    {"at": 10800, "path": "Press.service.stamp.mean", "value": 16},
    {"at": 14400, "path": "Press.service.stamp.mean", "value": 10},
]


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Press", factor=0, random_seed=42)
    app.add_resource("station", current_cap=1)
    app.add_service("stamp", dist="normal", mean=10, std=1)
    app.add_arrival("blanks", "exponential", rate=0.05, spawn="blank")
    app.add_scenario(SCHEDULE)

    @app.task(name="blank", resource_id="station", service_id="stamp")
    def blank(task_id):
        return {"event_type": "stamped"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=18000)
