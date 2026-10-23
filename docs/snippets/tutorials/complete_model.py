"""The Complete Model tutorial, builder: which shop design should be recommended?

Fits the gaps between orders and the picking and packing times of 300 observed
orders, builds the shop's busy hour from the fits, compares three warm-ups, and runs
the experiment that compares the designs, checks them and writes the report. Explain
and verify the model with:
desflow explain complete_model.py --verify --until "1 h" --replications 10
"""

import csv
import logging
from pathlib import Path

from desflow import SimulationContext, fit_column, replicate, run_experiment

# Replications and the experiment log only their warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

HERE = Path(__file__).resolve().parent
OBSERVED = HERE / "complete_model_observed.csv"


def fitted() -> dict:
    """Each input fitted to its column of the observed orders."""
    # --8<-- [start:fit]
    with OBSERVED.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    columns = {
        "orders": "gap_seconds",
        "picking": "picking_seconds",
        "packing": "packing_seconds",
    }
    # Every column is in seconds, and no time can be below 0
    return {
        name: fit_column([float(row[column]) for row in rows], unit="s", lower=0)
        for name, column in columns.items()
    }
    # --8<-- [end:fit]


# The best fit of each input: dist and its parameters
FITS = {name: result.settings() for name, result in fitted().items()}


def shop() -> SimulationContext:
    """A new busy hour, built from the fits."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup="2 min")
        # --8<-- [start:resources]
        # One picker and three packers, as the shop has today
        .add_resource("pickers", current_cap=1, max_cap=4)
        .add_resource("packers", current_cap=3, max_cap=10)
        # Shipping boxes: 1,000 on the shelf at the start, and room for 2,000
        .add_container("stock", current_cap=2000, max_cap=2000, level=1000)
        # --8<-- [end:resources]
        # --8<-- [start:inputs]
        # The fitted gaps between orders; each order starts with picking
        .add_arrival("orders", **FITS["orders"], spawn="pick_order")
        # A delivery of 1,000 boxes every 10 minutes
        .add_arrival("deliveries", dist="constant", value=600, spawn="restock")
        # The fitted picking and packing times
        .add_service("picking", **FITS["picking"])
        .add_service("packing", **FITS["packing"])
        # --8<-- [end:inputs]
        # Waits, utilisation and the stock's level, published at the end
        .with_stats("typical", interval=3600)
    )

    # --8<-- [start:flow]
    @app.task(service_id="picking", resource_id="pickers", next="pack_order")
    def pick_order(task_id: int):
        return {"event_type": "order_picked"}

    @app.task(service_id="packing", resource_id="packers", next="ship_order")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    # Each parcel leaves in a box from the shelf; waits while the shelf is empty
    @app.task(get={"container": "stock", "amount": 1})
    def ship_order(task_id: int):
        return {"event_type": "order_shipped"}

    @app.task(put={"container": "stock", "amount": 1000})
    def restock(task_id: int):
        return {"event_type": "stock_delivered"}

    # --8<-- [end:flow]
    return app


# The one context desflow explain complete_model.py reads
app = shop()

WAITS = ["tasks.pick_order.wait_mean", "tasks.pack_order.wait_mean"]
DESIGNS = ["current", "second-picker", "fourth-packer"]


def warmups() -> dict:
    """The mean waits of 40 runs of today's shop, after three candidate warm-ups."""
    # --8<-- [start:warmups]
    return {
        warmup: replicate(shop, 40, seed=42, until="1 h", warmup=warmup, measures=WAITS)
        for warmup in (0, "2 min", "10 min")
    }
    # --8<-- [end:warmups]


EXPERIMENT = {
    "experiment": "complete-model",
    "question": "Which design keeps both mean waits of the busy hour under 1 second?",
    # --8<-- [start:model]
    # The busy hour, with its 2-minute warm-up; 40 runs of each design
    "model": shop,
    "until": "1 h",
    "replications": 40,
    "seed": 42,
    "measures": [
        "tasks.pick_order.wait_mean",
        "tasks.pack_order.wait_mean",
        "resources.packers.utilization_avg",
        "tasks.ship_order.wait_mean",
    ],
    # --8<-- [end:model]
    # --8<-- [start:observed]
    # Values made from the model's own runs to show the method, not shop records;
    # the real shop runs the current design
    "observed": {
        "file": str(HERE / "complete_model_outputs.csv"),
        "variant": "current",
    },
    # --8<-- [end:observed]
    # --8<-- [start:designs]
    # Every design runs with the same seeds, so they are compared seed by seed
    "pairing": "common",
    "compare": [
        {"baseline": "current", "variant": "second-picker"},
        {"baseline": "current", "variant": "fourth-packer"},
    ],
    "variants": {
        "current": {},
        "second-picker": {"Shop.resources.pickers.current_cap": 2},
        "fourth-packer": {"Shop.resources.packers.current_cap": 4},
        # --8<-- [end:designs]
        # --8<-- [start:sensitivity]
        # The recommended design, with orders 10% faster than the fitted rate of 1.50719
        "fourth-packer-busier": {
            "Shop.resources.packers.current_cap": 4,
            "Shop.arrival.orders.rate": 1.65791,
        },
        # --8<-- [end:sensitivity]
    },
    # --8<-- [start:checks]
    "checks": [
        # The recommended design: the upper end of each wait's interval is under 1 s
        {"measure": "tasks.pick_order.wait_mean", "variant": "fourth-packer", "max": 1},
        {"measure": "tasks.pack_order.wait_mean", "variant": "fourth-packer", "max": 1},
        # No order waits for a shipping box, in each of the three designs
        {"measure": "tasks.ship_order.wait_mean", "variant": "current", "max": 0},
        {"measure": "tasks.ship_order.wait_mean", "variant": "second-picker", "max": 0},
        {"measure": "tasks.ship_order.wait_mean", "variant": "fourth-packer", "max": 0},
        # --8<-- [end:checks]
        # --8<-- [start:sensitivity-check]
        # With orders 10% faster, the fourth packer keeps both waits under 1 s
        {
            "measure": "tasks.pick_order.wait_mean",
            "variant": "fourth-packer-busier",
            "max": 1,
        },
        {
            "measure": "tasks.pack_order.wait_mean",
            "variant": "fourth-packer-busier",
            "max": 1,
        },
        # --8<-- [end:sensitivity-check]
    ],
    # --8<-- [start:report]
    "assumptions": [
        "Orders arrive at random, at a steady rate, through the busy hour.",
        "One picker and three packers work the whole hour; nobody takes a break.",
        "Every order is one parcel in one shipping box.",
    ],
    "inputs": {
        "Shop.arrival.orders": "gaps between 300 orders observed in the shop, "
        "fitted by fit_column",
        "Shop.service.picking": "picking times of the same 300 orders, "
        "fitted by fit_column",
        "Shop.service.packing": "packing times of the same 300 orders, "
        "fitted by fit_column",
        "Shop.arrival.deliveries": "the supplier's schedule, a delivery every 10 "
        "minutes",
    },
    "limits": [
        "One busy hour only; the rest of the day is not modelled.",
        "The observed values were made from the model's own runs, so comparing "
        "with them shows the method only.",
    ],
    "output": "results/complete-model",
    # --8<-- [end:report]
}


def experiment(output=None):
    """Runs the designs; raises CheckFailed when a check fails, after writing every file."""
    return run_experiment(EXPERIMENT, output=output)


def recommended(result) -> list:
    """The designs whose two mean waits both have an upper end under 1 second; the
    sensitivity variant is not a design."""
    upper: dict = {}
    for row in result.summary:
        if row["design"] in DESIGNS and row["statistic"] in WAITS:
            high = row["mean"] + row["half_width"]
            upper[row["design"]] = max(upper.get(row["design"], 0), high)
    return [design for design, high in upper.items() if high < 1]


if __name__ == "__main__":
    for name, result in fitted().items():
        print(f"{name}:\n{result.text()}\n")
    for warmup, result in warmups().items():
        for row in result.summary:
            print(
                f"warm-up {warmup}: {row['measure']} {row['mean']:.3f} ± {row['half_width']:.3f}"
            )
    print(f"recommended: {recommended(experiment())}")
