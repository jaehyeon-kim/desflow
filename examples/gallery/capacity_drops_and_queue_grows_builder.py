"""Capacity drops to one at 20 seconds, and the queue grows, builder API.

The builder version of `capacity_drops_and_queue_grows.yaml`: 3 packers, an order a
second, and a scenario step at 20 s that sets the capacity to 1.

Needs: nothing
Run it with: python examples/gallery/capacity_drops_and_queue_grows_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext

UNTIL = 60


def build() -> SimulationContext:
    """Builds the shop."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.with_stats(level="minimal", interval=1)
    app.add_resource("packers", current_cap=3)
    app.add_service("pack", dist="normal", mean=2.5, std=0.3)
    app.add_arrival("orders", "exponential", rate=1, spawn="order")
    app.add_scenario(
        [{"at": 20, "path": "Shop.resources.packers.current_cap", "value": 1}]
    )

    @app.task(name="order", resource_id="packers", service_id="pack")
    def order(task_id):
        """Packs one order."""
        return {"event_type": "packed"}

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
