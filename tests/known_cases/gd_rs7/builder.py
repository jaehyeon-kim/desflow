"""GD-RS7: Gas station with a monitor, a tanker and a container.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="R", factor=0, random_seed=42)
    app.add_resource("dispensers", current_cap=2)
    app.add_container("tank", current_cap=1000, level=100)
    app.add_initial_arrival("monitor", size, spawn="monitor")
    app.add_arrival("cars", "constant", value=5, first_at=0)

    @app.task(name="car", next="take")
    def car(task_id):
        return {"event_type": "arriving"}

    @app.task(name="take", seize="dispensers", next="fuel")
    def take(task_id):
        return {"event_type": "starts"}

    @app.task(
        name="fuel",
        get={"container": "tank", "amount": 40},
        service_time=5,
        release="dispensers",
    )
    def fuel(task_id):
        return {"event_type": "done"}

    @app.task(
        name="monitor",
        next={
            "when": [{"condition": "level.tank < 100", "task": "call"}],
            "default": "nap",
        },
    )
    def monitor(task_id):
        return {"event_type": "checked"}

    @app.task(name="call", spawn={"task": "tanker"}, next="nap")
    def call(task_id):
        return {"event_type": "calling_tanker"}

    @app.task(name="nap", service_time=15, next="monitor")
    def nap(task_id):
        return {"event_type": "napped"}

    @app.task(name="tanker", service_time=10, next="refill")
    def tanker(task_id):
        return {"event_type": "arriving"}

    @app.task(name="refill", put={"container": "tank", "amount": "1000 - level.tank"})
    def refill(task_id):
        return {"event_type": "refilled"}

    @app.arrival_loop("cars")
    def cars_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("cars")
            context.spawn(car(context.next_instance("car"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=35)
