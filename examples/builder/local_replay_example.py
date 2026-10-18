"""Local replay: the orders of a recorded morning, at their recorded times, builder API.

Each row of ../local_replay_orders.csv is one order, arriving at its time in seconds
from the start of the run and carrying its order_id and items as attributes. The
last row is at 73 seconds; after it the arrival stops, and the run ends at 120.

Needs no containers. Runs unpaced and ends after 120 simulation seconds.
"""

import logging
from pathlib import Path

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

TRACE = Path(__file__).resolve().parent.parent / "local_replay_orders.csv"

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=1, max_cap=2)
    .add_service("packing", dist="triangular", low=1, mode=2, high=3)
    # --8<-- [start:replay]
    # A CSV or Parquet file, or a list of dicts
    .add_arrival_replay(
        "orders", TRACE, time="time", unit="s", fields=["order_id", "items"]
    )
    # --8<-- [end:replay]
)


@app.task(service_id="packing", resource_id="packers")
def pack(task_id, context, entity):
    # The recorded fields, carried by each order
    return {
        "event_type": "order_packed",
        "order_id": entity.attributes["order_id"],
        "items": entity.attributes["items"],
    }


@app.arrival_loop("orders")
def order_loop(context):
    while True:
        # The wait for the next row, as for any other arrival
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), context, entity=entity))


def run():
    """Runs 120 simulation seconds."""
    app.run(until=120)


if __name__ == "__main__":
    run()
