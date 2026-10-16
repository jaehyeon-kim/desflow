"""A flow of tasks, builder API: pick, wrap gifts, pack, then ship.

Each order is an entity with a key, such as `orders-1` or `gifts-1`, that every
event of every task it passes through carries. Gift orders carry the attribute
`gift: 1` and are wrapped after picking; the others go straight to packing. One
parcel in ten fails its check and is packed again, and wrapping and shipping are
timed steps, which use no resource.

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
    .add_resource("pickers", current_cap=2, max_cap=4)
    .add_resource("packers", current_cap=2, max_cap=4)
    # Two exponential steps in a row (Erlang): a mean of 2 seconds, less variable
    .add_service("picking", dist="gamma", shape=2, scale=1)
    .add_service("wrapping", dist="constant", value=3)
    .add_service("packing", dist="lognormal", mean=2.5, std=1)
    # Shipping times taken from data: 6 in 10 take 5 seconds
    .add_service("shipping", dist="empirical", values=[5, 10, 30], weights=[6, 3, 1])
    # Each entity starts with these attributes, read as attr.<name>
    .add_arrival("orders", dist="exponential", rate=0.4, attributes={"gift": 0})
    .add_arrival("gifts", dist="exponential", rate=0.1, attributes={"gift": 1})
)


# --8<-- [start:routes]
@app.task(
    service_id="picking",
    resource_id="pickers",
    # The first rule that holds wins; default catches the rest
    next={"when": [{"condition": "attr.gift == 1", "task": "wrap"}], "default": "pack"},
)
def pick(task_id, context):
    """Picks the order's items."""
    return {"event_type": "order_picked"}


# A timed step: a service and no resource, so no queued event and no wait.
# `set` is applied to the entity's attributes when the task finishes.
@app.task(service_id="wrapping", set={"wrapped": 1}, next="pack")
def wrap(task_id, context):
    """Wraps a gift."""
    return {"event_type": "gift_wrapped"}


# Drawn from this task's own random sequence: one parcel in ten is packed again
@app.task(
    service_id="packing",
    resource_id="packers",
    next={"weights": {"ship": 9, "pack": 1}},
)
def pack(task_id, context):
    """Packs the order."""
    return {"event_type": "order_packed"}


# --8<-- [end:routes]
@app.task(service_id="shipping")
def ship(task_id, context, entity):
    """Ships the order. A payload includes attributes only when it asks for them."""
    return {
        "event_type": "order_shipped",
        "gift": entity.attributes.get("gift"),
        "wrapped": entity.attributes.get("wrapped"),
    }


def order_loop(arrival):
    """Spawns a pick task for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            # One counter per task, shared by both arrivals and by next
            context.spawn(pick(context.next_instance("pick"), context, entity=entity))

    return loop


app.arrival_loop("orders")(order_loop("orders"))
app.arrival_loop("gifts")(order_loop("gifts"))


def run():
    """Runs 120 simulation seconds."""
    app.run(until=120)


if __name__ == "__main__":
    run()
