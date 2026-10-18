"""Filtered inventory: couriers take only parcels for their own area, builder API.

Packed orders go on one shelf as parcels, each with the area it is going to. A
courier for the north or the south takes the oldest parcel for its own area, with a
filter, and leaves without one if none comes within 20 seconds.

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
    .add_resource("packers", current_cap=2, max_cap=4)
    # Parcels for every area on one shelf, each a mapping with its area
    .add_store("parcels", current_cap=20, max_cap=20)
    .add_service("packing", dist="triangular", low=1, mode=2, high=4)
    .add_arrival(
        "north_orders", dist="exponential", rate=0.3, attributes={"area": "north"}
    )
    .add_arrival(
        "south_orders", dist="exponential", rate=0.1, attributes={"area": "south"}
    )
    .add_arrival(
        "north_couriers", dist="constant", value=4, attributes={"area": "north"}
    )
    .add_arrival(
        "south_couriers", dist="constant", value=6, attributes={"area": "south"}
    )
    # Every 30 seconds, the shelf's readings and how many couriers left empty-handed
    .with_stats("basic", interval=30.0)
)


@app.task(service_id="packing", resource_id="packers", put={"store": "parcels"})
def pack(task_id, context, entity):
    """The parcel goes on the shelf as the payload, with its area."""
    return {"event_type": "order_packed", "area": entity.attributes["area"]}


# --8<-- [start:collect]
@app.task(
    # The oldest parcel for the courier's own area
    get={"store": "parcels", "filter": "item.area == attr.area"},
    # A courier leaves without a parcel after 20 seconds
    patience=20,
)
def collect(task_id, context, item):
    """`item` is the parcel the courier took."""
    return {"event_type": "parcel_collected", "parcel": item}


# --8<-- [end:collect]
def spawn_loop(arrival, task, name):
    """Spawns `task` for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(name), context, entity=entity))

    return loop


for _arrival in ("north_orders", "south_orders"):
    app.arrival_loop(_arrival)(spawn_loop(_arrival, pack, "pack"))
for _arrival in ("north_couriers", "south_couriers"):
    app.arrival_loop(_arrival)(spawn_loop(_arrival, collect, "collect"))


def run():
    """Runs 120 simulation seconds."""
    app.run(until=120)


if __name__ == "__main__":
    run()
