"""Skewed keys: 5 of 25 customers place 75 percent of the orders, builder API.

The builder version of `skewed_keys.yaml`: an order every second, 75 percent from the
5 busy customers of 25, with the customer as the key.

Needs: nothing.
Run it with: python docs/snippets/generate-data/skewed_keys_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 1000


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("orders", "constant", value=1, spawn="order")
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "customer_id": {
                "gen": "when",
                "condition": "busy",
                "then": {"gen": "integer", "bounds": [1, 5]},
                "else": {"gen": "integer", "bounds": [6, 25]},
            },
        },
        name="tasks.order",
        vars={"busy": {"gen": "boolean", "p": 0.75}},
    )

    @app.task(name="order", key_field="customer_id")
    def order(task_id):
        """Gives one order record."""
        return order_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
