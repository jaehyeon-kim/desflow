"""Order fulfilment: several distributions, a priority resource, a second resource
tasks compete for, a store of stock refilled from a container, patience and
statistics, builder API.

A regression scenario (see scripts/regression.py), not an example. Standard and
express orders wait for a picker (express first) and for a shelf item; one that runs
out of patience leaves. Restocking moves one item at a time from the warehouse to the
shelf and uses the same benches as packing.
"""

from desflow import ConsoleEgress, SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("pickers", current_cap=2, max_cap=4, queue="priority")
    .add_resource("benches", current_cap=2, max_cap=3)
    .add_container("warehouse", current_cap=300, max_cap=300, level=120)
    .add_store("shelf", current_cap=30, max_cap=30)
    .add_service("picking", dist="lognormal", mean=6, std=3)
    .add_service("packing", dist="gamma", shape=3, scale=1)
    .add_service("restocking", dist="triangular", low=1, mode=2, high=5)
    .add_arrival(
        "orders",
        dist="exponential",
        rate=0.18,
        attributes={"rank": 2, "patience": 40},
    )
    .add_arrival(
        "express",
        dist="weibull",
        shape=1.5,
        scale=30,
        attributes={"rank": 1, "patience": 25},
    )
    .add_arrival("deliveries", dist="uniform", low=4, high=8)
    .with_stats("basic", interval=50.0)
)


@app.task(
    service_id="picking",
    resource_id="pickers",
    priority="attr.rank",
    patience="attr.patience",
    get={"store": "shelf"},
    next="pack",
)
def pick(task_id, context):
    return {"event_type": "order_picked"}


@app.task(service_id="packing", resource_id="benches")
def pack(task_id, context):
    return {"event_type": "order_packed"}


@app.task(
    service_id="restocking",
    resource_id="benches",
    get={"container": "warehouse", "amount": 1},
    put={"store": "shelf"},
)
def restock(task_id, context):
    return {"event_type": "shelf_filled"}


def spawn_loop(arrival, task, name):
    """Spawns `task` for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(name), context, entity=entity))

    return loop


app.arrival_loop("orders")(spawn_loop("orders", pick, "pick"))
app.arrival_loop("express")(spawn_loop("express", pick, "pick"))
app.arrival_loop("deliveries")(spawn_loop("deliveries", restock, "restock"))


def run():
    app.run(until=600)


if __name__ == "__main__":
    run()
