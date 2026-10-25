"""The Randomness and Seeds tutorial: the busy shop, seeded, with returns, builder.

Orders arrive at 1.8 a second, and three packers pack each in 1.5 seconds on average,
with a standard deviation of 0.5 seconds. Returned parcels arrive every 5 seconds on
average and are recorded as they arrive. Every record is printed to the terminal.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

app = (
    # --8<-- [start:seed]
    # The same random numbers on every run; leave it out to draw a new seed
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    # --8<-- [end:seed]
    .add_resource("packers", current_cap=3, max_cap=10)
    # Packing varies: 1.5 seconds on average, with a standard deviation of 0.5
    .add_service("packing", dist="normal", mean=1.5, std=0.5)
    .add_arrival("orders", dist="exponential", rate=1.8)
    .add_egress(ConsoleEgress())
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_generator(context: SimulationContext):
    task_id = 0
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, entity=order))
        task_id += 1


# --8<-- [start:returns]
# Returned parcels: one every 5 seconds on average
app.add_arrival("returns", dist="exponential", rate=0.2)


@app.task()
def receive_return(task_id: int):
    # A return is recorded as it arrives; it does not use a packer
    return {"event_type": "return_received"}


@app.arrival_loop("returns")
def return_generator(context: SimulationContext):
    task_id = 0
    while True:
        parcel = yield context.wait_for_arrival("returns")
        context.spawn(receive_return(task_id, entity=parcel))
        task_id += 1


# --8<-- [end:returns]


if __name__ == "__main__":
    app.run(until="1 min")
