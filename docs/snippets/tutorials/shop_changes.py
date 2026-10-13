"""The First Simulation tutorial, step 2, with the SimulationContext builder."""

import logging

from dynamic_des import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# --8<-- [start:random]
app = (
    # Same draws every run
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=5)  # room for up to 5 packers
    # Packing takes 3 seconds on average, give or take 0.5
    .add_service("packing", dist="normal", mean=3.0, std=0.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(ConsoleEgress())
)
# --8<-- [end:random]


@app.task(service_id="packing", resource_id="packers")
def pack_order(order_id: int):
    return {"order_id": order_id}


@app.arrival_loop("orders")
def order_generator(context: SimulationContext):
    order_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(pack_order(order_id))
        order_id += 1


@app.telemetry_loop(interval=5.0)
def report(context: SimulationContext):
    context.publish("packer_capacity", context.get_resource("packers").capacity)


# --8<-- [start:changes]
def capacity_plan(context: SimulationContext):
    """Three packers from t=10, two from t=20, on the simulation clock."""
    path = "Shop.resources.packers.current_cap"
    yield context.env.timeout(10.0)
    context.env.registry.update(path, 3)
    yield context.env.timeout(10.0)
    context.env.registry.update(path, 2)


app.add_process(capacity_plan)
# --8<-- [end:changes]

if __name__ == "__main__":
    app.run(until=30.0)
