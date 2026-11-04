"""DX-18: A sink that keeps only the finished orders.

Builder form of `model.yaml`.
"""

from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("packing", dist="constant", value=1.5)
    app.add_arrival("orders", "constant", value=2, spawn="pack_order")
    app.add_egress(
        ConsoleEgress(),
        when=lambda record: record["value"].get("event_type") == "order_packed",
    )

    @app.task(name="pack_order", service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)
