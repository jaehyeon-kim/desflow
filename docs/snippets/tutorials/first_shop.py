"""The First Simulation tutorial: the shop, with the SimulationContext builder.

One packer, orders arriving about every 2 seconds, and 1.5 seconds of packing per
order. Every record is printed to the terminal.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# --8<-- [start:shop]
app = (
    # 1 simulated second = 1 real second; a seed gives the same arrivals every run
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    # Every order takes exactly 1.5 seconds to pack
    .add_service("packing", dist="constant", value=1.5)
    # One order every 2 seconds on average
    .add_arrival("orders", dist="exponential", rate=0.5)
)
# --8<-- [end:shop]


# --8<-- [start:order]
@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    # Requests a packer, waits the packing time, releases the packer.
    # The returned dictionary is the value of the finished event.
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_generator(context: SimulationContext):
    task_id = 0
    while True:
        # Waits for the next arrival and returns the new order, keyed orders-1, ...
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, entity=order))
        task_id += 1


# --8<-- [end:order]


# --8<-- [start:records]
app.add_egress(ConsoleEgress())
# --8<-- [end:records]

# --8<-- [start:run]
if __name__ == "__main__":
    app.run(until=20)
# --8<-- [end:run]
