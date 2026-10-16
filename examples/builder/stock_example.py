"""Stock, parcels and express orders, builder API.

Packing an order takes one box from the `stock` container and puts the packed parcel
in the `parcels` store, where a courier collects it. Express orders have priority 1
and standard orders 2, and the packers, whose queue order is `priority`, serve the
lowest priority first. The stock runs out, so packing waits until a restock at 40
seconds sets the level to 30.

Needs no containers. Runs unpaced and ends after 60 simulation seconds.
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
    # Waiting orders are served by priority, the oldest first among equal priorities
    .add_resource("packers", current_cap=2, max_cap=4, queue="priority")
    # Packed parcels wait here for a courier; packing waits when it is full
    .add_store("parcels", current_cap=10, max_cap=10)
    # current_cap is the most it holds; level is what it holds now
    .add_container("stock", current_cap=50, max_cap=50, level=15)
    # An expert's estimate: at least 1 second, usually 2, at most 4
    .add_service("packing", dist="triangular", low=1, mode=2, high=4)
    .add_service("collecting", dist="uniform", low=1, high=3)
    .add_arrival("orders", dist="exponential", rate=0.5, attributes={"priority": 2})
    .add_arrival("express", dist="exponential", rate=0.1, attributes={"priority": 1})
    .add_arrival("couriers", dist="constant", value=2)
    # The packers', parcels' and stock's readings every 10 seconds
    .with_stats("minimal", interval=10.0)
)


# --8<-- [start:pack]
@app.task(
    service_id="packing",
    resource_id="packers",
    # Each order's own attribute, read when it joins the queue
    priority="attr.priority",
    # Takes one box once it has a packer, and waits while the stock is empty
    get={"container": "stock", "amount": 1},
    # Puts the finished payload in the store as a parcel
    put={"store": "parcels"},
)
def pack(task_id, context, entity):
    """Packs one order."""
    return {"event_type": "order_packed", "priority": entity.attributes["priority"]}


# --8<-- [end:pack]
@app.task(service_id="collecting", get={"store": "parcels"})
def collect(task_id, context):
    """A courier waits for a parcel, then takes 1 to 3 seconds to load it."""
    return {"event_type": "parcel_collected"}


def order_loop(arrival):
    """Spawns a pack task for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            # One counter per task, shared by both order arrivals
            context.spawn(pack(context.next_instance("pack"), context, entity=entity))

    return loop


app.arrival_loop("orders")(order_loop("orders"))
app.arrival_loop("express")(order_loop("express"))


@app.arrival_loop("couriers")
def courier_loop(context):
    """Spawns a collect task for every courier."""
    while True:
        entity = yield context.wait_for_arrival("couriers")
        context.spawn(collect(context.next_instance("collect"), context, entity=entity))


def restock(context):
    """A restock at 40 seconds: the level path sets what the container holds."""
    yield context.env.timeout(40)
    context.env.registry.update("Shop.containers.stock.level", 30)


app.add_process(restock)


def run():
    """Runs 60 simulation seconds."""
    app.run(until=60)


if __name__ == "__main__":
    run()
