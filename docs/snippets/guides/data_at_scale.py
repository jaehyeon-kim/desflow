"""The Data at Scale guide, built with the builder API: a month of orders for 1,000
customers, written as Parquet files, one for each day.

CUSTOMERS, DAYS, FIRST (the number of the first customer), SEED and OUT change the size,
the customers, the random seed and the folder. Run it with `python data_at_scale.py`.
"""

import logging
import os
from datetime import datetime, timezone

from desflow import ParquetStorageEgress, SimulationContext
from desflow.values import Template

CUSTOMERS = int(os.getenv("CUSTOMERS", "1000"))
DAYS = int(os.getenv("DAYS", "30"))
FIRST = int(os.getenv("FIRST", "1"))
OUT = os.getenv("OUT", "scale_output")


# --8<-- [start:span]
def new_shop() -> SimulationContext:
    """A shop that runs without waiting and starts on 1 January 2026, UTC."""
    return SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=int(os.getenv("SEED", "42")),
        timezone="UTC",
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


# --8<-- [end:span]


# --8<-- [start:volume]
def add_customers(app: SimulationContext) -> None:
    """One arrival for each customer, ordering about once a day (12 to 36 hours apart).
    A blueprint's `count` is one arrival here, each with its own key."""
    for number in range(FIRST, FIRST + CUSTOMERS):
        app.add_arrival(
            f"customer-{number}",
            "uniform",
            low=43200,
            high=129600,
            spawn="place_order",
            key=lambda n, number=number: f"customer-{number}",
        )


# --8<-- [end:volume]


# --8<-- [start:files]
def add_files(app: SimulationContext) -> None:
    """A Parquet sink with a large batch, and an order that names the file of its day."""
    # One file for each batch, so a large batch gives few, large files
    app.add_egress(
        ParquetStorageEgress(default_path=f"{OUT}/other.parquet"), batch_size=10000
    )
    order = Template(
        {
            # Numbered within the run, so the first customer of the run is part of the id
            "order_id": {"gen": "sequence", "format": f"{FIRST}-{{}}"},
            "customer": {"gen": "entity"},
            "amount": {"gen": "normal", "mean": 40, "std": 10, "decimals": 2},
            # The file of the order: the day it was placed, in the simulated calendar
            "__path__": {"gen": "format_time", "format": f"{OUT}/%Y-%m-%d.parquet"},
        },
        name="tasks.place_order",
    )

    @app.task(name="place_order")
    def place_order(task_id):
        return order


# --8<-- [end:files]


# --8<-- [start:run]
def build_shop() -> SimulationContext:
    app = new_shop()
    add_customers(app)
    add_files(app)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_shop().run(until=f"{DAYS} days")
# --8<-- [end:run]
