"""The First Simulation tutorial, step 3: the step 2 shop on Kafka, builder version.

Needs Kafka: odctl up kafka-lite. Runs until stopped with Ctrl+C.
"""

from dynamic_des import KafkaEgress, KafkaIngress, SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=5)
    .add_service("packing", dist="normal", mean=3.0, std=0.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
)

# --8<-- [start:kafka]
app.add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092"))
app.add_egress(
    KafkaEgress(
        bootstrap_servers="localhost:9092",
        event_topic="sim-events",
        telemetry_topic="sim-telemetry",
    )
)
# --8<-- [end:kafka]


@app.task(service_id="packing", resource_id="packers")
def pack_order(order_id: int):
    return {"order_id": order_id}


@app.arrival_loop("orders")
def order_generator(context: SimulationContext):
    order_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(pack_order(order_id))
        order_id += 1


if __name__ == "__main__":
    app.run()
