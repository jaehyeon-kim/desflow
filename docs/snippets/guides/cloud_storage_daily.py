"""The "Write a File for Each Day" step of the Write to Cloud Storage page, built with the
builder API.

The shop packs an order every hour for two days, and each order goes to the file of its
day. STORAGE and STORAGE_BUCKET pick the store as in cloud_storage.py. Needs
cloud_storage_stores.py beside it. Run it with `python cloud_storage_daily.py`.
"""

# --8<-- [start:shop]
import logging
from datetime import datetime, timezone

from cloud_storage_stores import bucket, filesystem

from desflow import JsonlStorageEgress, SimulationContext
from desflow.values import Template


def build_shop() -> SimulationContext:
    """The shop, writing each order to the file of its day."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0.0,
        random_seed=42,
        timezone="UTC",
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_arrival("orders", dist="constant", value=3600, spawn="pack_order")
    app.add_egress(
        # The file of a record with no __path__
        JsonlStorageEgress(
            default_path=f"{bucket}/daily/other.jsonl", filesystem=filesystem
        )
    )
    order = Template(
        {
            "order_id": {"gen": "sequence"},
            "amount": 19.99,
            # The file of the order: the day it was packed, in the simulated calendar
            "__path__": {
                "gen": "format_time",
                "format": f"{bucket}/daily/%Y-%m-%d.jsonl",
            },
        },
        name="tasks.pack_order",
    )

    @app.task(name="pack_order")
    def pack_order(task_id):
        return order

    return app


app = build_shop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    app.run(until="2 days")
# --8<-- [end:shop]
