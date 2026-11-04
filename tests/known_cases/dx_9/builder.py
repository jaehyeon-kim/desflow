"""DX-9: A service time computed for each car.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Station", factor=0)
    app.add_resource("pump", current_cap=1)
    for name, first, car, litres in (
        ("car1", 0, "car-1", 4),
        ("car2", 1, "car-2", 6),
        ("car3", 2, "car-3", 2),
    ):
        app.add_arrival(
            name,
            "constant",
            value=1,
            spawn="fill",
            max_events=1,
            first_at=first,
            attributes={"car": car, "litres": litres},
        )

    @app.task(name="fill", resource_id="pump", service_time="attr.litres / 2")
    def fill(task_id):
        return Template({"car": {"gen": "attr", "name": "car"}}, name="tasks.fill")

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)
