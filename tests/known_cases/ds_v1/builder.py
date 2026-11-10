"""DS-V1: A service time computed for each entity.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="V1", factor=0, random_seed=42)
    app.add_resource("pump", current_cap=1)
    app.add_arrival("a", "constant", value=1, first_at=0, attributes={"fuel": 2})
    app.add_arrival("b", "constant", value=1, first_at=0, attributes={"fuel": 4})
    car_payload = Template(
        {"event_type": "done", "fuel": {"gen": "attr", "name": "fuel"}}, name="car"
    )

    @app.task(name="car", resource_id="pump", service_time="attr.fuel / 2")
    def car(task_id):
        return car_payload

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("a")
            context.spawn(car(context.next_instance("car"), entity=entity))

    @app.arrival_loop("b")
    def b_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("b")
            context.spawn(car(context.next_instance("car"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()
