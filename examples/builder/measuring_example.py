"""Measuring a run, builder API: built-in statistics, a warm-up and ground truth.

The shop publishes every statistic of its packers and its pack task every 5 minutes.
The first 3 minutes are a warm-up: no sink receives records from them, and the
statistics start again when it ends, so they measure the busy shop, not the empty
one it started as. At 12 minutes packing slows down, and a ground-truth record says
so, on a sink of its own that receives nothing else.

Needs no containers. Runs unpaced and ends after 20 simulation minutes.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup="3 min")
    # Events and statistics from the end of the warm-up on
    .add_egress(ConsoleEgress())
    # Ground-truth records only, for a scorer that checks what a detector found
    .add_egress(ConsoleEgress(), when="labels")
    .add_resource("packers", current_cap=2, max_cap=4)
    # A mean of 4 seconds (shape times scale)
    .add_service("packing", dist="gamma", shape=4, scale=1)
    .add_arrival("orders", dist="exponential", rate=0.4)
    # Every level, published every 5 minutes, such as Shop.stats.tasks.pack.wait_p95
    .with_stats("all", interval=300)
)


@app.task(service_id="packing", resource_id="packers")
def pack(task_id, context):
    """Packs one order."""
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_loop(context):
    """Spawns a pack task for every order."""
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(task_id, context, entity=entity))
        task_id += 1


# --8<-- [start:fault]
def slow_packing(context):
    """Packing slows from 4 to 4.8 seconds on average at 12 minutes, with its label."""
    yield context.env.timeout(720)
    path = "Shop.service.packing.scale"
    registry = context.env.registry
    old = registry.get(path).value
    registry.update(path, 1.2)
    context.env.publish_label(
        {"fault": "slow_packing", "cause": "new packing material"},
        path=path,
        old=old,
        new=registry.get(path).value,
    )


app.add_process(slow_packing)


# --8<-- [end:fault]
def run():
    """Runs 20 simulation minutes."""
    app.run(until="20 min")


if __name__ == "__main__":
    run()
