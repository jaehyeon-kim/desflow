"""Long run: one simulated week with a warm-up, thousands of entities and telemetry
records, builder API.

A regression scenario (see scripts/regression.py), not an example. Orders arrive all
week, with a busy spell from day 3 to day 4. The first 6 hours are warm-up, which no
sink receives; the statistics at the `all` level are published every hour.
"""

from desflow import ConsoleEgress, SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup="6 h")
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=3, max_cap=4)
    .add_service("packing", dist="gamma", shape=4, scale=25)
    .add_service("shipping", dist="lognormal", mean=60, std=30)
    .add_arrival("orders", dist="exponential", rate=0.01)
    .add_scenario(
        [
            {
                "at": 172800,
                "path": "Shop.arrival.orders.rate",
                "value": 0.015,
                "record": {"fault": "busy_period"},
            },
            {
                "at": 259200,
                "path": "Shop.arrival.orders.rate",
                "value": 0.01,
                "record": {"fault": "busy_period_over"},
            },
        ]
    )
    .with_stats("all", interval=3600.0)
)


@app.task(service_id="packing", resource_id="packers", next="ship")
def pack(task_id, context):
    return {"event_type": "order_packed"}


@app.task(service_id="shipping")
def ship(task_id, context):
    return {"event_type": "order_shipped"}


@app.arrival_loop("orders")
def order_loop(context):
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), context, entity=entity))


def run():
    app.run(until=604800)


if __name__ == "__main__":
    run()
