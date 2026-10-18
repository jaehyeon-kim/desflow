"""Fit a Distribution: the gaps between orders in an order log, fitted and loaded.

order_log.csv is an illustrative log of 300 orders placed one afternoon, with the
time each was placed. The script turns the times into the gaps between orders,
fits them, prints the table of candidates, and runs the shop for an hour with the
fitted arrivals. `write` saves the settings for a blueprint's `!include`.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path

from desflow import SimulationContext, fit_column

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)
LOG = Path(__file__).resolve().parent / "order_log.csv"


def gaps():
    """The gaps between consecutive orders, in seconds."""
    # --8<-- [start:gaps]
    with LOG.open(newline="", encoding="utf-8") as handle:
        placed = [
            datetime.fromisoformat(row["placed_at"]) for row in csv.DictReader(handle)
        ]
    # An arrival is fitted to the gaps between orders, not to the times themselves
    return [
        (later - earlier).total_seconds() for earlier, later in zip(placed, placed[1:])
    ]
    # --8<-- [end:gaps]


def fit():
    """Every family fitted to the gaps."""
    # --8<-- [start:fit]
    result = fit_column(gaps(), unit="s", lower=0)
    print(result.text())  # every candidate, its status and diagnostics
    # --8<-- [end:fit]
    return result


def histogram():
    """The empirical alternative: the gaps as a histogram of 10 bins."""
    # --8<-- [start:empirical]
    result = fit_column(gaps(), unit="s", candidates=["empirical"], bins=10)
    # --8<-- [end:empirical]
    return result


def write(result, folder):
    """Writes the best fit where a blueprint includes it."""
    # --8<-- [start:write]
    path = Path(folder) / "fitted" / "orders.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(result.yaml(source="order_log.csv, gaps between placed_at"))
    # --8<-- [end:write]
    return path


def shop(result) -> SimulationContext:
    """One packer, with the fitted arrivals."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=2)
        .add_service("packing", dist="gamma", shape=4, scale=0.375)
        # --8<-- [start:load]
        # The best fit's settings, such as dist="exponential", rate=0.4
        .add_arrival("orders", **result.settings())
        # --8<-- [end:load]
        .with_stats("basic")
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_loop(context):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


if __name__ == "__main__":
    app = shop(fit())
    app.run(until="1 h")
    finished = app.env.stats.readings()["Shop.stats.flows.orders.finished"]
    print(f"orders packed in an hour: {finished}")
