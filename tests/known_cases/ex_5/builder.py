"""EX-5: Gas station with two pumps and a tank truck.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Station", factor=0, random_seed=42)
    app.add_resource("pumps", current_cap=2)
    app.add_container("tank", current_cap=200, level=200)
    app.add_initial_arrival("control", size, spawn="control")
    app.add_arrival(
        "cars",
        gap=generator({"gen": "integer", "bounds": [30, 300]}),
        key=lambda n: "Car {}".format(n - 1),
        attributes={"level": generator({"gen": "integer", "bounds": [5, 25]})},
        spawn="car",
    )
    car_payload = Template(
        {
            "event_type": "refueled",
            "litres": {"gen": "expr", "expr": "50 - attr.level"},
        },
        name="car",
    )

    @app.task(
        name="car",
        resource_id="pumps",
        get={"container": "tank", "amount": "50 - attr.level"},
        service_time="(50 - attr.level) / 2",
    )
    def car(task_id):
        return car_payload

    @app.task(
        name="control",
        next={
            "when": [
                {"condition": "level.tank / 200 * 100 < 25", "task": "call_truck"}
            ],
            "default": "pause",
        },
    )
    def control(task_id):
        return {"event_type": "checked"}

    @app.task(name="call_truck", next="await_truck")
    def call_truck(task_id):
        return {"event_type": "truck_called"}

    @app.task(name="await_truck", wait={"task": "truck"}, next="pause")
    def await_truck(task_id):
        return {"event_type": "truck_returned"}

    truck_payload = Template(
        {
            "event_type": "truck_arrived",
            "litres": {"gen": "expr", "expr": "200 - level.tank"},
        },
        name="truck",
    )

    @app.task(
        name="truck",
        service_time=300,
        put={"container": "tank", "amount": "200 - level.tank"},
    )
    def truck(task_id):
        return truck_payload

    @app.task(name="pause", service_time=10, next="control")
    def pause(task_id):
        return {"event_type": "waited"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=1000)
