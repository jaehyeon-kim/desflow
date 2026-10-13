"""The First Simulation tutorial, step 3: the step 2 shop on Kafka, low-level version.

Needs Kafka: odctl up kafka-lite. Runs until stopped with Ctrl+C.
"""

import numpy as np

from dynamic_des import DynamicRealtimeEnvironment, DynamicResource, Sampler
from dynamic_des import KafkaEgress, KafkaIngress
from shop_changes_low_level import PARAMS, SEED, capacity_plan, order_generator


def run():
    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(PARAMS)
    # --8<-- [start:kafka]
    env.setup_ingress(
        [KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092")]
    )
    env.setup_egress(
        [
            KafkaEgress(
                bootstrap_servers="localhost:9092",
                event_topic="sim-events",
                telemetry_topic="sim-telemetry",
            )
        ]
    )
    # --8<-- [end:kafka]
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, Sampler(rng=np.random.default_rng(SEED)), packers))
    env.process(capacity_plan(env))
    try:
        env.run()
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
