"""Interruptions: express orders preempt regular ones, and a fire drill interrupts packing, builder API.

One packer serves express orders (priority 0) before regular ones (priority 1), and
an express order takes the packer from a regular order being packed. The regular
order is preempted, queues again and resumes with the packing time it had left. At
60 seconds a fire drill interrupts the order being packed, which resumes when it
gets the packer back.

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
    # --8<-- [start:packers]
    # A lower priority value takes the packer from a higher one
    .add_resource("packers", current_cap=1, max_cap=4, queue="priority", preempt=True)
    # --8<-- [end:packers]
    .add_service("packing", dist="triangular", low=1, mode=2, high=4)
    .add_arrival("orders", dist="exponential", rate=0.3, attributes={"priority": 1})
    .add_arrival("express", dist="exponential", rate=0.1, attributes={"priority": 0})
    # Every 30 seconds, how many orders were preempted, interrupted and resumed
    .with_stats("basic", interval=30.0)
)


# --8<-- [start:pack]
@app.task(
    service_id="packing",
    resource_id="packers",
    priority="attr.priority",
    # A preempted or interrupted order queues again and serves what it had left
    on_interrupt="resume",
)
def pack(task_id, context, entity):
    return {"event_type": "order_packed", "priority": entity.attributes["priority"]}


# --8<-- [end:pack]
def order_loop(arrival):
    """Spawns a pack task for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(pack(context.next_instance("pack"), context, entity=entity))

    return loop


app.arrival_loop("orders")(order_loop("orders"))
app.arrival_loop("express")(order_loop("express"))


# --8<-- [start:drill]
def fire_drill(context):
    """Interrupts every order being packed; each follows its on_interrupt."""
    yield context.env.timeout(60)
    context.interrupt("pack", cause="fire drill")


app.add_process(fire_drill)
# --8<-- [end:drill]


def run():
    """Runs 120 simulation seconds."""
    app.run(until=120)


if __name__ == "__main__":
    run()
