"""The First Simulation tutorial, step 3: the step 2 factory on Kafka, builder version.

Needs Kafka: odctl up kafka-lite. Runs until stopped with Ctrl+C.
"""

from dynamic_des import KafkaEgress, KafkaIngress, SimulationContext

app = (
    SimulationContext(sim_id="Factory_A", factor=1.0, random_seed=42)
    .add_resource("lathe", current_cap=1, max_cap=5)
    .add_service("milling", dist="normal", mean=3.0, std=0.5)
    .add_arrival("parts", dist="exponential", rate=0.5)
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


@app.task(service_id="milling", resource_id="lathe")
def process_part(part_id: int):
    return {"part_id": part_id}


@app.arrival_loop("parts")
def parts_generator(context: SimulationContext):
    part_id = 0
    while True:
        yield context.wait_for_arrival("parts")
        context.spawn(process_part(part_id))
        part_id += 1


if __name__ == "__main__":
    app.run()
