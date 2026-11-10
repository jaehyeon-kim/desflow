"""GA-6: Five sensors each drift from their previous value.

Builder form of `model.yaml`. A blueprint's `count: 5` is five arrivals here, each with
the key of its sensor.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Sensors", factor=0, random_seed=42)
    for number in range(5):
        app.add_arrival(
            f"reading{number}",
            "constant",
            value=1,
            spawn="sensor_reading",
            key=lambda n, number=number: f"sensor-{number}",
        )
    reading_payload = Template(
        {
            "sensor_id": {"gen": "entity"},
            "value": {
                "gen": "add",
                "args": [
                    {"gen": "previous", "path": "value", "default": 0},
                    {"gen": "uniform", "low": -1, "high": 1},
                ],
            },
        },
        name="tasks.sensor_reading",
    )

    @app.task(name="sensor_reading")
    def sensor_reading(task_id):
        return reading_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=100)
