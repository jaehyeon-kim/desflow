"""Patience: orders that leave the queue, and orders that refuse to join, builder API.

More orders arrive than one packer can pack. An online order waits at most 20
seconds for the packer, and a walk-in order at most 10, each from its own `patience`
attribute. An order that waits longer leaves the queue and publishes reneged. From
100 seconds the courier has made its last collection, so new orders balk: they
publish balked and never join the queue.

Needs no containers. Runs unpaced and ends after 120 simulation seconds.
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
    # An expert's estimate: at least 1 second, usually 2, at most 4
    .add_service("packing", dist="triangular", low=1, mode=2, high=4)
    .add_arrival("orders", dist="exponential", rate=0.5, attributes={"patience": 20})
    .add_arrival("walk_ins", dist="exponential", rate=0.1, attributes={"patience": 10})
    # Every 30 seconds, how many orders finished, reneged and balked so far
    .with_stats("basic", interval=30.0)
)


# --8<-- [start:pack]
@app.task(
    service_id="packing",
    resource_id="packers",
    # Each order's own patience: it leaves the queue when it runs out
    patience="attr.patience",
    # No new orders once the courier has made its last collection
    balk="t >= 100",
)
def pack(task_id, context):
    """Packs one order."""
    return {"event_type": "order_packed"}


# --8<-- [end:pack]
def order_loop(arrival):
    """Spawns a pack task for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            # One counter per task, shared by both arrivals
            context.spawn(pack(context.next_instance("pack"), context, entity=entity))

    return loop


app.arrival_loop("orders")(order_loop("orders"))
app.arrival_loop("walk_ins")(order_loop("walk_ins"))


def run():
    """Runs 120 simulation seconds."""
    app.run(until=120)


if __name__ == "__main__":
    run()
