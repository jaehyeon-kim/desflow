"""GA-1: Orders wait for busy packers.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.with_stats(level="minimal")
    app.add_resource("packers", current_cap=3)
    app.add_service("pack", dist="normal", mean=240, std=60)
    app.add_arrival("orders", "exponential", rate=0.015, spawn="order")

    @app.task(name="order", resource_id="packers", service_id="pack")
    def order(task_id):
        return {"event_type": "packed", "order_id": task_id}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=7200)
