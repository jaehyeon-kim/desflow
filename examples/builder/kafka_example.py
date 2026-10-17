"""Kafka Digital Twin, builder API.

Models a shop packing orders with `SimulationContext` and connects the simulation
to Kafka for input and output. `KafkaIngress` reads parameter updates from `sim-config`,
so the running simulation can be steered without restarting it. `KafkaEgress`
publishes lifecycle events to `sim-events` and metrics to `sim-telemetry`, with
Pydantic models giving the events a declared shape.

Needs a broker: `odctl up kafka-lite`. Runs until interrupted with Ctrl + C.
"""

import logging
import os
import time

from pydantic import BaseModel

from desflow import (
    KafkaAdminConnector,
    KafkaEgress,
    KafkaIngress,
    SimulationContext,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
)
logger = logging.getLogger("kafka_example")


# ==========================================
# 1. Define Strongly-Typed Event Payloads
# ==========================================
class TaskEvent(BaseModel):
    """
    Strongly typed event payload to guarantee schema consistency
    when shipping data over the wire to Kafka.
    """

    path_id: str
    status: str


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ==========================================
# 2. Infrastructure Builder
# ==========================================
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS))
    .add_egress(
        KafkaEgress(
            event_topic="sim-events",
            telemetry_topic="sim-telemetry",
            bootstrap_servers=BOOTSTRAP_SERVERS,
        )
    )
    .add_resource("packers", current_cap=1, max_cap=10)
    .add_service("packing", dist="normal", mean=3.0, std=0.5)
    .add_arrival("orders", dist="exponential", rate=1.0)
    # The packers' readings every 2 seconds, which kafka_dashboard.py shows
    .with_stats("minimal", interval=2.0)
)


# ==========================================
# 3. Simulation Logic
# ==========================================
@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, context):
    """
    The @task decorator automatically locks the resource and emits the
    'queued' and 'started' events. We just execute our custom logic
    and return the final payload.
    """
    logger.info(f"Task {task_id} started at sim time: {context._env.now:.2f}s")

    # Return the strongly-typed Pydantic model for the 'finished' state
    return TaskEvent(path_id="Shop.service.packing", status="finished").model_dump(
        mode="json"
    )


@app.arrival_loop("orders")
def arrival_generator(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, context, entity=entity))
        task_id += 1


# ==========================================
# 4. Execution
# ==========================================
def run():
    TOPICS_CONFIG = [
        {"name": "sim-config", "partitions": 1},
        {"name": "sim-telemetry", "partitions": 1},
        {"name": "sim-events", "partitions": 1},
    ]

    logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    try:
        admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
        admin.create_topics(topics_config=TOPICS_CONFIG)
        time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not explicitly create topics: {e}")

    print("Simulation started.")
    print("  - Listen to 'sim-telemetry' for system vitals.")
    print("  - Listen to 'sim-events' for task lifecycles.")
    print("  - Send to 'sim-config' to update parameters.")

    app.run()  # Starts the clock and orchestrates all connectors


if __name__ == "__main__":
    run()
