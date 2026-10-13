"""The First Simulation tutorial: the shop, with the SimulationContext builder.

One packer, orders arriving about every 2 seconds, and 1.5 seconds of packing per
order. Every record is printed to the terminal.
"""

import logging

from dynamic_des import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# --8<-- [start:shop]
app = (
    # 1 simulated second = 1 real second
    SimulationContext(sim_id="Shop", factor=1.0)
    .add_resource("packers", current_cap=1, max_cap=1)
    # With no std, a normal distribution returns its mean every time
    .add_service("packing", dist="normal", mean=1.5)
    # One order every 2 seconds on average
    .add_arrival("orders", dist="exponential", rate=0.5)
)
# --8<-- [end:shop]


# --8<-- [start:order]
@app.task(service_id="packing", resource_id="packers")
def pack_order(order_id: int):
    # Requests a packer, waits the packing time, releases the packer.
    # The returned dictionary is the finished event's value.
    return {"order_id": order_id}


@app.arrival_loop("orders")
def order_generator(context: SimulationContext):
    order_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(pack_order(order_id))
        order_id += 1


# --8<-- [end:order]


# --8<-- [start:records]
app.add_egress(ConsoleEgress())
# --8<-- [end:records]

# --8<-- [start:run]
if __name__ == "__main__":
    app.run(until=10.0)
# --8<-- [end:run]
