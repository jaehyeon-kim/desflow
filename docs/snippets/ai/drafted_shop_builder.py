"""The drafted packing shop with the SimulationContext builder.

The Builder form of drafted_shop.yaml: it gives the same records at the same seed.
Run it with: python drafted_shop_builder.py
"""

from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext

app = (
    SimulationContext(
        sim_id="Shop",
        factor=0,  # no pacing: the run ends as soon as it is done
        random_seed=42,
        # a fixed date, so two runs give the same records
        logical_start_time=datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc),
    )
    .add_resource("packers", current_cap=1, max_cap=1)  # said: one packer
    # said: 1.5 seconds; assumed: every order takes the same time
    .add_service("packing", dist="constant", value=1.5)
    # said: an order every 2 seconds on average; assumed: orders arrive at random
    .add_arrival("orders", dist="exponential", rate=0.5)
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


app.add_egress(ConsoleEgress(format="json"))

if __name__ == "__main__":
    app.run(until="1 hour")  # assumed: one hour answers the question
