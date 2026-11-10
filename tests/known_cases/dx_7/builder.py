"""DX-7: Custom readings every 30 seconds.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.add_resource("packers", current_cap=2, max_cap=5)
    app.add_service("packing", dist="gamma", shape=4, scale=0.75)
    app.add_arrival("orders", "exponential", rate=0.6, spawn="pack_order")
    app.add_readings(
        30,
        {
            "waiting_per_packer": "queue.packers / max(registry['Shop.resources.packers.current_cap'], 1)",
            "arrival_rate": "registry['Shop.arrival.orders.rate']",
        },
    )

    @app.task(name="pack_order", service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=120)
