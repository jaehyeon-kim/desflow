"""The A Flow of Tasks tutorial: orders are picked, packed and shipped, builder.

Two pickers, two packers and a shelf of boxes restocked every 100 seconds. An order
picked while the shelf is empty goes to backorder for a minute and is picked again,
and one parcel in 20 is packed again. Every record is printed to the terminal, and
the finished count of each task is printed at the end.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_resource("pickers", current_cap=2, max_cap=4)
    .add_resource("packers", current_cap=2, max_cap=4)
    # Boxes on the shelf, restocked with 53 every 100 seconds
    .add_container("stock", current_cap=200, max_cap=200, level=50)
    .add_service("picking", dist="uniform", low=1, high=3)
    .add_service("packing", dist="constant", value=1.5)
    # The supplier takes a minute to send an item that is out of stock
    .add_service("supplying", dist="constant", value=60)
    .add_arrival("deliveries", dist="constant", value=100)
    # Finished counts of every task and flow, at the end
    .with_stats("basic", interval=3600)
    .add_egress(ConsoleEgress())
)
# --8<-- [start:attributes]
# Each order starts with the attributes given here, read as attr.<name>
app.add_arrival("orders", dist="exponential", rate=0.4, attributes={"express": 0})
app.add_arrival("express", dist="exponential", rate=0.1, attributes={"express": 1})
# --8<-- [end:attributes]


# --8<-- [start:chain]
# The first rule that holds wins; default catches the rest
@app.task(
    service_id="picking",
    resource_id="pickers",
    next={
        "when": [{"condition": "level.stock == 0", "task": "backorder"}],
        "default": "pack_order",
    },
)
def pick_order(task_id: int, context: SimulationContext):
    return {"event_type": "order_picked"}


# --8<-- [end:chain]


# --8<-- [start:backorder]
# A timed step: a service and no resource, so it starts at once. `set` is applied
# to the order's attributes when the step finishes.
@app.task(service_id="supplying", set={"backordered": 1}, next="pick_order")
def backorder(task_id: int, context: SimulationContext):
    return {"event_type": "order_backordered"}


# --8<-- [end:backorder]


# --8<-- [start:rework]
# One parcel in 20 fails its check and is packed again
@app.task(
    service_id="packing",
    resource_id="packers",
    get={"container": "stock", "amount": 1},
    next={"weights": {"ship_order": 95, "pack_order": 5}},
)
def pack_order(task_id: int, context: SimulationContext):
    return {"event_type": "order_packed"}


# --8<-- [end:rework]


@app.task()
def ship_order(task_id: int, context: SimulationContext, entity):
    # A payload includes attributes only when it asks for them
    return {
        "event_type": "order_shipped",
        "express": entity.attributes.get("express"),
        "backordered": entity.attributes.get("backordered"),
    }


@app.task(put={"container": "stock", "amount": 53})
def restock(task_id: int, context: SimulationContext):
    return {"event_type": "stock_delivered"}


def spawn(arrival, task, name):
    def loop(context: SimulationContext):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            # One counter per task, shared by both order arrivals and by next
            context.spawn(task(context.next_instance(name), context, entity=entity))

    return loop


app.arrival_loop("orders")(spawn("orders", pick_order, "pick_order"))
app.arrival_loop("express")(spawn("express", pick_order, "pick_order"))
app.arrival_loop("deliveries")(spawn("deliveries", restock, "restock"))

if __name__ == "__main__":
    app.run(until="1 h")
    readings = app.env.stats.readings()
    for name in ("pick_order", "backorder", "pack_order", "ship_order"):
        print(f"{name:<10}: {readings[f'Shop.stats.tasks.{name}.finished']} finished")
