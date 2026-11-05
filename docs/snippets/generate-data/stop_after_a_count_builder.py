"""Stop after a count: an arrival that ends after exactly 12 records, builder API.

The builder version of `stop_after_a_count.yaml`: an order every 2 seconds, ended by
`max_events` after 12.

Needs: nothing.
Run it with: python docs/snippets/generate-data/stop_after_a_count_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = None


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("orders", "constant", value=2, spawn="order", max_events=12)
    order_payload = Template(
        {"order_id": {"gen": "sequence", "start": 1}}, name="tasks.order"
    )

    @app.task(name="order")
    def order(task_id):
        """Gives one order record."""
        return order_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run()
