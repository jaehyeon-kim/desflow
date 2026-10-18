"""End conditions: a drain after closing time, and a stop when the boxes run out, builder API.

Orders arrive until the shop closes at 120 seconds; then the run drains, packing the
orders already in the shop for at most 60 seconds more. Each order takes a box, and
the run stops at once if the boxes run out first. The statistics count the orders
that started, finished and are still in the shop.

Needs no containers. Runs unpaced and ends on its own.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=1, max_cap=4)
    .add_container("boxes", current_cap=100, max_cap=100, level=60)
    .add_service("packing", dist="triangular", low=1, mode=2, high=4)
    .add_arrival("orders", dist="exponential", rate=0.45)
    # Every 30 seconds and at the end: flows.orders.started, finished and in_system
    .with_stats("basic", interval=30.0)
)


@app.task(
    service_id="packing",
    resource_id="packers",
    get={"container": "boxes", "amount": 1},
)
def pack(task_id, context):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_loop(context):
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), context, entity=entity))


def run():
    # --8<-- [start:run]
    app.run(
        # No new orders after closing time
        until=120,
        # Then pack the orders in the shop, for at most 60 seconds
        drain={"limit": 60},
        # Stop at once when the last box is taken
        stop={"condition": "level.boxes == 0"},
    )
    # --8<-- [end:run]


if __name__ == "__main__":
    run()
