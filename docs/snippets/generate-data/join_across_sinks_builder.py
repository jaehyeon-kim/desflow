"""Join across sinks: customers to one file, and the orders that name them to another.

The builder version of `join_across_sinks.yaml`: 5 customers at the start and one more
about every 10 seconds, and an order about every second whose `lookup` takes the
customer id from a customer already written. Each sink takes the records of one task,
with `when`.

Needs: the parquet extra, which holds the file writers. DEST_PATH sets the folder (data
by default).
Run it with: python docs/snippets/generate-data/join_across_sinks_builder.py
"""

import logging
import os
from datetime import datetime, timezone

from desflow import JsonlStorageEgress, SimulationContext
from desflow.values import Template

UNTIL = 60


def build() -> SimulationContext:
    """Builds the shop, with a file for each kind of record."""
    folder = os.environ.get("DEST_PATH", "data")
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    # --8<-- [start:egress]
    app.add_egress(
        JsonlStorageEgress(default_path=f"{folder}/customers.jsonl"),
        when=lambda record: record.get("task") == "customer",
    )
    app.add_egress(
        JsonlStorageEgress(default_path=f"{folder}/orders.jsonl"),
        when=lambda record: record.get("task") == "order",
    )
    # --8<-- [end:egress]
    # --8<-- [start:arrival]
    app.add_arrival("customers", "exponential", rate=0.1, spawn="customer", initial=5)
    app.add_arrival("orders", "exponential", rate=1, spawn="order")
    # --8<-- [end:arrival]
    # --8<-- [start:payload]
    customer_payload = Template(
        {
            "customer_id": {"gen": "sequence", "start": 1},
            "country": {"gen": "one_of", "choices": ["AU", "NZ", "SG"]},
        },
        name="tasks.customer",
    )
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"},
            "total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
        },
        name="tasks.order",
    )

    # A task that something looks up keeps its rows. `history` is the blueprint's
    # default, and a builder task needs it because a `Template` payload is not searched
    # for lookups.
    @app.task(name="customer", history=1_000_000)
    def customer(task_id):
        """Gives one customer record."""
        return customer_payload

    @app.task(name="order")
    def order(task_id):
        """Gives one order record."""
        return order_payload

    # --8<-- [end:payload]

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
