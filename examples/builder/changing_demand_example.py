"""Changing demand: a rush of orders and a second packer for it, on a schedule, builder API.

Orders arrive at 0.4 a second, and at 0.8 during a rush from 120 to 180 seconds; a
second packer works during the rush. Each change applies before every other event at
its time, so the arrival gap drawn at 120 already uses the rush rate.

Needs no containers. Runs unpaced and ends after 300 simulation seconds.
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
    .add_resource("packers", current_cap=1, max_cap=2)
    .add_service("packing", dist="triangular", low=1, mode=2, high=3)
    .add_arrival("orders", dist="exponential", rate=0.4)
    # Every 60 seconds: the packers' readings and the mean wait so far
    .with_stats("typical", interval=60.0)
)


@app.task(service_id="packing", resource_id="packers")
def pack(task_id, context):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_loop(context):
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), context, entity=entity))


# --8<-- [start:schedule]
# The rush: twice the orders, and a second packer, from 120 to 180 seconds
app.add_scenario(
    [
        {"at": 120, "path": "Shop.arrival.orders.rate", "value": 0.8},
        {"at": 120, "path": "Shop.resources.packers.current_cap", "value": 2},
        {"at": 180, "path": "Shop.arrival.orders.rate", "value": 0.4},
        {"at": 180, "path": "Shop.resources.packers.current_cap", "value": 1},
    ]
)
# --8<-- [end:schedule]


def run():
    """Runs 300 simulation seconds."""
    app.run(until=300)


if __name__ == "__main__":
    run()
