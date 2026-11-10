"""Orders wait in a queue when all packers are busy, builder API.

The builder version of `orders_wait_for_busy_packers.yaml`: a shop with 3 packers,
orders about once a minute, and a packing time that is normal with a mean of 240 s.

Needs: nothing
Run it with: python examples/gallery/orders_wait_for_busy_packers_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext

UNTIL = 7200


def build() -> SimulationContext:
    """Builds the shop."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.with_stats(level="minimal")
    app.add_resource("packers", current_cap=3)
    app.add_service("pack", dist="normal", mean=240, std=60)
    app.add_arrival("orders", "exponential", rate=0.015, spawn="order")

    @app.task(name="order", resource_id="packers", service_id="pack")
    def order(task_id):
        """Packs one order."""
        return {"event_type": "packed", "order_id": task_id}

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
