"""Kafka Digital Twin, low-level API.

The low-level version of `builder/kafka_example.py`, a shop packing orders, with
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
# 1. Event Payload Model
# ==========================================
class TaskEvent(BaseModel):
    """The fields and types of each lifecycle event.

    env.publish_event() takes this Pydantic model in place of a dict, and turns it
    into a dict. KafkaEgress serializes that dict, to JSON or to Avro.
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
def pack_order(env, sampler, packers, key, task_id):
    """Publishes a Pydantic model instead of a dict for each lifecycle event."""
    path_id = "Shop.service.packing"
    env.publish_event(
        key,
        TaskEvent(path_id=path_id, status="queued"),
        task="pack_order",
        instance=task_id,
    )
    env.stats.task_queued("pack_order", task_id)  # counted as in progress

    with packers.request() as req:
        yield req

        logger.info(f"Task {task_id} started at sim time: {env.now:.2f}s")
        env.publish_event(
            key,
            TaskEvent(path_id=path_id, status="started"),
            task="pack_order",
            instance=task_id,
        )
        env.stats.task_started("pack_order", task_id)

        # Use the latest service config from the registry
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))

        env.publish_event(
            key,
            TaskEvent(path_id=path_id, status="finished"),
            task="pack_order",
            instance=task_id,
        )
        env.stats.task_finished("pack_order", task_id)


def arrival_generator(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
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
    admin_connector = KafkaAdminConnector(
        bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100
    )
    admin_connector.create_topics(topics_config=TOPICS_CONFIG)
    time.sleep(2)

    # A secured cluster needs more keyword arguments, such as security_protocol="SASL_SSL"
    ingress = KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS)

    # KafkaEgress uses JsonSerializer by default. To write Avro instead:
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

    env = DynamicRealtimeEnvironment(factor=1.0, strict=False)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_ingress([ingress])
    env.setup_egress([egress])
    # The packers' readings every 2 seconds, which kafka_dashboard.py shows
    env.setup_stats("Shop", "minimal", interval=2.0)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(42))

    env.process(arrival_generator(env, sampler, packers))

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
