import logging

from dynamic_des import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)

TRANSPORT = 1.0  # seconds the crane takes to carry a part to the mill
MILLING = 4.0  # seconds of milling per part

app = (
    SimulationContext(sim_id="Line_A", factor=1.0)
    .add_resource("crane", current_cap=1, max_cap=1)
    .add_resource("mill", current_cap=1, max_cap=1)
    .add_arrival("standard", dist="exponential", rate=1.0)
    .add_egress(ConsoleEgress())
)


def handoff_worker(context: SimulationContext, part_id: int):
    crane = context.get_resource("crane")
    mill = context.get_resource("mill")
    key = f"task-{part_id}"
    context.env.publish_event(key, {"status": "queued"})

    # The crane is released by hand below, not by a `with` block, because the
    # handoff happens in the middle of the mill's block.
    yield crane.request()
    context.env.publish_event(key, {"status": "crane_loaded"})
    yield context.env.timeout(TRANSPORT)

    # Ask for the mill while still holding the crane.
    with mill.request() as mill_req:
        yield mill_req
        context.env.publish_event(key, {"status": "mill_entered"})
        crane.release()  # the part is in the mill, so the crane is free
        yield context.env.timeout(MILLING)
        context.env.publish_event(key, {"status": "finished"})


@app.arrival_loop("standard")
def arrival_loop(context: SimulationContext):
    part_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(handoff_worker(context, part_id))
        part_id += 1


if __name__ == "__main__":
    app.run(until=30.0)
