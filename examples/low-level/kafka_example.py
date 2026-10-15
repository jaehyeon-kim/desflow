"""Kafka Digital Twin, low-level API.

The low-level twin of `builder/kafka_example.py`, an online shop packing orders, with
the environment, registry and connectors wired by hand. Like the builder version, it
creates its topics first with `KafkaAdminConnector`.

Needs a broker: `odctl up kafka-lite`. Runs until interrupted with Ctrl + C.
"""

import logging
import os
import time

import numpy as np
from pydantic import BaseModel

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaAdminConnector,
    KafkaEgress,
    KafkaIngress,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
)
logger = logging.getLogger("kafka_example")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# ==========================================
# 1. Define Strongly-Typed Event Payloads
# ==========================================
class TaskEvent(BaseModel):
    """
    Thanks to desflow's duck-typing, we can pass this Pydantic model
    directly into env.publish_event(). KafkaEgress converts it to a dict and
    serializes it, to JSON or to Avro.
    """

    path_id: str
    status: str


# ==========================================
# 2. Initial System State
# ==========================================
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},  # 1 a second
    service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=10)},
)


# ==========================================
# 3. Simulation Logic
# ==========================================
def pack_order(env, sampler, packers, task_id):
    """Publishes a Pydantic model instead of a dict for each lifecycle event."""
    key, path_id = f"task-{task_id}", "Shop.service.packing"
    env.publish_event(key, TaskEvent(path_id=path_id, status="queued"))

    with packers.request() as req:
        yield req

        logger.info(f"Task {task_id} started at sim time: {env.now:.2f}s")
        env.publish_event(key, TaskEvent(path_id=path_id, status="started"))

        # Use the latest service config from the registry
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))

        env.publish_event(key, TaskEvent(path_id=path_id, status="finished"))


def arrival_generator(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, sampler, packers, task_id))
        task_id += 1


def telemetry_generator(env, packers):
    """Low-volume system health stream, pushed to the 'sim-telemetry' topic."""
    while True:
        env.publish_telemetry("Shop.packers.capacity", packers.capacity)
        env.publish_telemetry("Shop.packers.in_use", packers.in_use)
        env.publish_telemetry("Shop.packers.queue_length", len(packers.queue.items))

        util = (packers.in_use / packers.capacity) * 100 if packers.capacity > 0 else 0
        env.publish_telemetry("Shop.packers.utilization", util)

        yield env.timeout(2.0)


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
    admin_connector = KafkaAdminConnector(
        bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100
    )
    admin_connector.create_topics(topics_config=TOPICS_CONFIG)
    time.sleep(2)

    # (Optional: Pass **kwargs like `security_protocol="SASL_SSL"` for enterprise clusters)
    ingress = KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS)

    # By default, this uses JsonSerializer. To use Avro for enterprise environments:
    # from desflow.connectors.egress.kafka import ConfluentAvroSerializer
    # avro_serializer = ConfluentAvroSerializer(
    #     registry_url="http://127.0.0.1:8081", schema_str=AVRO_SCHEMA
    # )
    # Then pass: topic_serializers={"sim-events": avro_serializer}
    # Generate AVRO_SCHEMA from AvroBaseModel rather than a plain Pydantic model;
    # see the Avro and Pydantic guide. TaskEvent above has no .avro_schema().
    egress = KafkaEgress(
        telemetry_topic="sim-telemetry",
        event_topic="sim-events",
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_ingress([ingress])
    env.setup_egress([egress])

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(42))

    env.process(arrival_generator(env, sampler, packers))
    env.process(telemetry_generator(env, packers))

    print("Simulation started.")
    print("  - Listen to 'sim-telemetry' for system vitals.")
    print("  - Listen to 'sim-events' for task lifecycles.")
    print("  - Send to 'sim-config' to update parameters.")

    try:
        env.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
