"""Fitted inputs, builder API: the shop's packing time, fitted from 40 measured times.

`fit_column` fits the 40 packing times in ../packing_times.csv to every family, and
the script prints the table of candidates. `add_service` takes the best one's
settings, the same numbers `desflow fit` writes for the YAML version. Two packers,
and an order every 1.25 seconds on average (48 a minute, rate 0.8). The run prints
every event and the statistics, and ends after one simulation hour.

Needs no containers, and the fit extra: pip install "desflow[fit]".
"""

import logging
from pathlib import Path

from desflow import ConsoleEgress, SimulationContext, fit_column
from desflow.analysis.fit import read_column

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
DATA = Path(__file__).resolve().parent.parent / "packing_times.csv"


def fitted():
    """Fits the measured packing times; none is below 0 seconds."""
    # --8<-- [start:fit]
    times = read_column(DATA, "packing_seconds")
    result = fit_column(times, unit="s", lower=0)
    print(result.text())  # every candidate, its status and diagnostics
    # --8<-- [end:fit]
    return result


def shop() -> SimulationContext:
    """The shop, with the fitted packing time."""
    result = fitted()
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_egress(ConsoleEgress())
        .add_resource("packers", current_cap=2, max_cap=4)
        # --8<-- [start:service]
        # The best candidate's settings, such as dist="triangular", low=0.546332
        .add_service("packing", **result.settings())
        # --8<-- [end:service]
        # An order every 1.25 s on average (rate 0.8)
        .add_arrival("orders", dist="exponential", rate=0.8)
        .with_stats("typical", interval=600)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id, context):
        """Packs one order."""
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_loop(context):
        """Spawns a pack task for every order."""
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack(task_id, context, entity=entity))
            task_id += 1

    return app


def run():
    """Runs one simulation hour."""
    shop().run(until="1 h")


if __name__ == "__main__":
    run()
