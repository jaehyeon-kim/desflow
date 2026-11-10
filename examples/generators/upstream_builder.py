"""Upstream: a value from the record of the task that routed here, builder API.

The builder version of `upstream.yaml`. The order routes to the invoice with `next`.

Run it with: python examples/generators/upstream_builder.py
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
    app.add_arrival("orders", "constant", value=60, spawn="order")
    order_payload = Template(
        {"amount": {"gen": "integer", "bounds": [10, 20]}}, name="tasks.order"
    )
    invoice_payload = Template(
        {"amount": {"gen": "upstream", "path": "amount"}}, name="tasks.invoice"
    )

    @app.task(name="order", next="invoice")
    def order(task_id):
        """Gives one order."""
        return order_payload

    @app.task(name="invoice")
    def invoice(task_id):
        """Gives the invoice of one order."""
        return invoice_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
