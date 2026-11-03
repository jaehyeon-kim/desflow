"""EX-1: Bank with customers who give up waiting.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Bank", factor=0, random_seed=42)
    app.add_resource("counter", current_cap=1)
    app.add_arrival(
        "customers",
        "exponential",
        rate=0.1,
        first_at=0,
        key=lambda n: "Customer{:02d}".format(n - 1),
    )

    @app.task(
        name="customer",
        resource_id="counter",
        patience=generator({"gen": "uniform", "low": 1, "high": 3}),
        service_time=generator(
            {"gen": "sample", "dist": "exponential", "rate": 0.08333333333333333}
        ),
    )
    def customer(task_id):
        return {"event_type": "finished"}

    @app.arrival_loop("customers")
    def customers_arrivals(context):
        for _ in range(5):
            entity = yield context.wait_for_arrival("customers")
            context.spawn(customer(context.next_instance("customer"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=200)
