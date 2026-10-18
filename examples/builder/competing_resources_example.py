"""Competing resources: tasks that hold several resources, and a handoff, builder API.

A picker collects an order and carries it to a packing bench; the order keeps the
picker until it is on the bench, then the picker is free for the next order. A gift
needs the picker and the bench together for its wrapping, acquired in that order,
the same order every task uses, so no two orders can wait for each other.

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
    .add_resource("picker", current_cap=1, max_cap=2)
    .add_resource("bench", current_cap=1, max_cap=2)
    .add_service("picking", dist="uniform", low=1, high=2)
    .add_service("packing", dist="triangular", low=1, mode=2, high=4)
    .add_service("wrapping", dist="constant", value=3)
    .add_arrival("orders", dist="exponential", rate=0.25)
    .add_arrival("gifts", dist="exponential", rate=0.05)
    # Every 30 seconds, each resource's readings and the tasks finished so far
    .with_stats("basic", interval=30.0)
)


# --8<-- [start:tasks]
# The picker stays with the order until pack has started
@app.task(service_id="picking", resource_id="picker", keep=["picker"], next="pack")
def pick(task_id, context):
    return {"event_type": "order_picked"}


@app.task(service_id="packing", resource_id="bench")
def pack(task_id, context):
    return {"event_type": "order_packed"}


# Holds the picker, then the bench too: acquired in this order
@app.task(service_id="wrapping", resource_id=["picker", "bench"])
def wrap(task_id, context):
    return {"event_type": "gift_wrapped"}


# --8<-- [end:tasks]
def spawn_loop(arrival, task, name):
    """Spawns `task` for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(name), context, entity=entity))

    return loop


app.arrival_loop("orders")(spawn_loop("orders", pick, "pick"))
app.arrival_loop("gifts")(spawn_loop("gifts", wrap, "wrap"))


def run():
    """Runs 120 simulation seconds."""
    app.run(until=120)


if __name__ == "__main__":
    run()
