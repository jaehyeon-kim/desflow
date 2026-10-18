import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)

app = (
    SimulationContext(sim_id="Shop", factor=1.0)
    # --8<-- [start:printer]
    # A jam takes the printer from the order being labelled: priority 0 preempts 1
    .add_resource("printer", current_cap=1, max_cap=1, queue="priority", preempt=True)
    # --8<-- [end:printer]
    # 5 s to print and attach the shipping label of one order
    .add_service("labelling", dist="constant", value=5.0)
    # 10 s of repair after a jam
    .add_service("repair", dist="constant", value=10.0)
    .add_arrival("orders", dist="exponential", rate=0.2)
    # A jam every 12 s
    .add_arrival("jams", dist="constant", value=12.0)
    .add_egress(ConsoleEgress())
)


# --8<-- [start:tasks]
# An interrupted order gives the printer back, queues again, and labels what was
# left once it has the printer back
@app.task(
    service_id="labelling", resource_id="printer", priority=1, on_interrupt="resume"
)
def label(task_id: int):
    return {"event_type": "label_printed"}


# A jam holds the printer for the repair, so no order is labelled meanwhile
@app.task(service_id="repair", resource_id="printer", priority=0)
def jam(task_id: int):
    return {"event_type": "printer_repaired"}


# --8<-- [end:tasks]


@app.arrival_loop("orders")
def order_loop(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(label(context.next_instance("label"), entity=order))


@app.arrival_loop("jams")
def jam_loop(context: SimulationContext):
    while True:
        breakdown = yield context.wait_for_arrival("jams")
        context.spawn(jam(context.next_instance("jam"), entity=breakdown))


if __name__ == "__main__":
    app.run(until=40.0)
