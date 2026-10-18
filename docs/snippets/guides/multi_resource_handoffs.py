import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)

app = (
    SimulationContext(sim_id="Shop", factor=1.0)
    .add_resource("picker", current_cap=1, max_cap=1)
    .add_resource("bench", current_cap=1, max_cap=1)
    # The picker takes 1 s to carry an order to the bench, and packing takes 4 s
    .add_service("walking", dist="constant", value=1.0)
    .add_service("packing", dist="constant", value=4.0)
    .add_arrival("orders", dist="exponential", rate=1.0)
    .add_egress(ConsoleEgress())
)


# --8<-- [start:handoff]
# The picker carries the order to the bench and stays with it until it is on the
# bench: keep hands the picker to the next task instead of releasing it
@app.task(service_id="walking", resource_id="picker", keep=["picker"], next="pack")
def pick(task_id: int):
    return {"event_type": "order_picked"}


# pack gives the picker back as soon as it publishes started
@app.task(service_id="packing", resource_id="bench")
def pack(task_id: int):
    return {"event_type": "order_packed"}


# --8<-- [end:handoff]


@app.arrival_loop("orders")
def arrival_loop(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pick(context.next_instance("pick"), entity=order))


if __name__ == "__main__":
    app.run(until=30.0)
