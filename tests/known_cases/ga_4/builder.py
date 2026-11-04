"""GA-4: Orders belong to existing customers.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.add_arrival("signups", "exponential", rate=0.1, spawn="sign_up")
    app.add_arrival("orders", "exponential", rate=1, spawn="place_order")
    sign_up_payload = Template(
        {"customer_id": {"gen": "sequence"}}, name="tasks.sign_up"
    )
    place_order_payload = Template(
        {"customer_id": {"gen": "lookup", "task": "sign_up", "path": "customer_id"}},
        name="tasks.place_order",
    )

    @app.task(name="sign_up", history=1000)
    def sign_up(task_id):
        return sign_up_payload

    @app.task(name="place_order")
    def place_order(task_id):
        return place_order_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=1000)
