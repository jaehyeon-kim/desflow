import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)

WALK = 1.0  # seconds the picker takes to carry an order to the packing bench
PACKING = 4.0  # seconds of packing per order

app = (
    SimulationContext(sim_id="Shop", factor=1.0)
    .add_resource("picker", current_cap=1, max_cap=1)
    .add_resource("bench", current_cap=1, max_cap=1)
    .add_arrival("orders", dist="exponential", rate=1.0)
    .add_egress(ConsoleEgress())
)


def handoff_worker(context: SimulationContext, order, task_id: int):
    picker = context.get_resource("picker")
    bench = context.get_resource("bench")

    def publish(value):
        # Keyed by the order, orders-1 and so on, with the task and its instance id
        context.env.publish_event(order.key, value, task="handoff", instance=task_id)

    publish({"status": "queued"})

    # The picker is released by hand below, not by a `with` block, because the
    # handoff happens in the middle of the bench's block.
    yield picker.request()
    publish({"status": "picked"})
    yield context.env.timeout(WALK)

    # Ask for the bench while still holding the picker.
    with bench.request() as bench_req:
        yield bench_req
        publish({"status": "at_bench"})
        picker.release()  # the order is on the bench, so the picker is free
        yield context.env.timeout(PACKING)
        publish({"status": "finished"})


@app.arrival_loop("orders")
def arrival_loop(context: SimulationContext):
    task_id = 0
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(handoff_worker(context, order, task_id))
        task_id += 1


if __name__ == "__main__":
    app.run(until=30.0)
