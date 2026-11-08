"""Lookup: a value from a finished record of another task, builder API.

The builder version of `lookup.yaml`. A task that something looks up keeps its finished
rows, so the customer task sets `history`.

Run it with: python examples/generators/lookup_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 200


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Gen",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("customers", "constant", value=3600, spawn="customer", initial=3)
    app.add_arrival("orders", "constant", value=60, spawn="order")
    customer_payload = Template(
        {"customer_id": {"gen": "sequence", "start": 100}}, name="tasks.customer"
    )
    order_payload = Template(
        {"customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"}},
        name="tasks.order",
    )

    @app.task(name="customer", history=1_000_000)
    def customer(task_id):
        """Gives one customer."""
        return customer_payload

    @app.task(name="order")
    def order(task_id):
        """Gives one order."""
        return order_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
