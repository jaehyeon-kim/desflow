"""Local simulation, builder API, no containers.

The smallest complete example. An online shop packs orders: `Shop` is built with
`SimulationContext` and writes to `ConsoleEgress`, so events and telemetry are printed
to the terminal and nothing external is involved. A scenario adds packers at 10
seconds and removes one at 20 seconds, and the telemetry shows the effect.

Start here. It needs no broker, no database and no object store, and it ends on its own
after 30 simulation seconds.
"""

import logging

from dynamic_des import ConsoleEgress, SimulationContext

# Logging is configured here rather than in a wrapper, because this script is run
# directly. Without it the run produces no output at all.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

# ==========================================
# 1. Infrastructure Builder
# ==========================================
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=1, max_cap=5)
    .add_service("packing", dist="normal", mean=3.0, std=0.5)
    .add_arrival("orders", dist="exponential", rate=1.0)
)


# ==========================================
# 2. Simulation Logic (Decorators)
# ==========================================
@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, context):
    """Packs one order and returns the payload of its finished event."""
    return {"event_type": "order_packed", "shipping": "standard", "order_id": task_id}


# --8<-- [start:arrival-loop]
@app.arrival_loop("orders")
def arrival_generator(context):
    """Spawns a pack_order task for every order the 'orders' distribution draws."""
    task_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, context))
        task_id += 1
        # --8<-- [end:arrival-loop]


@app.telemetry_loop(interval=2.0)
def telemetry_generator(context):
    """Samples the packers every 2 simulation seconds."""
    res = context.get_resource("packers")
    util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0

    context.publish("packers.capacity", res.capacity)
    context.publish("packers.in_use", res.in_use)
    context.publish("packers.queue_length", len(res.queue.items))
    context.publish("packers.utilization", util)


def capacity_changes(context):
    """Three packers from 10 seconds, then two from 20 seconds, on simulation time."""
    path = "Shop.resources.packers.current_cap"
    yield context.env.timeout(10)
    context.env.registry.update(path, 3)
    yield context.env.timeout(10)
    context.env.registry.update(path, 2)


app.add_process(capacity_changes)


# ==========================================
# 3. Execution
# ==========================================
def run():
    """Starts the local simulation for a fixed duration."""
    logger.info("Starting Builder Local Example. Running for 30 seconds...")
    app.run(until=30)


if __name__ == "__main__":
    run()
