"""The Route Records to Topics guide, low-level API: a shop whose records go to three
Kafka topics.

It needs a Kafka broker on 127.0.0.1:9092: start one with `odctl up kafka-lite`. Run it
with `python complex_routing_low_level.py`, beside complex_routing_logic.py; it runs
for one minute.
"""

import itertools
import logging

from complex_routing_logic import create_topics, custom_kafka_router, packed
from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaEgress,
    Sampler,
    SimParameter,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


# --8<-- [start:egress]
def kafka_egress():
    """Sends each record to the topic the router picks."""
    return KafkaEgress(
        bootstrap_servers="127.0.0.1:9092",
        topic_router=custom_kafka_router,
    )


# --8<-- [end:egress]


# --8<-- [start:shop]
def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    stats = env.stats
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    stats.task_queued("pack_order", task_id)
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, packed(task_id), "pack_order", task_id)
        stats.task_finished("pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


# --8<-- [end:shop]


def run(until=60.0, factor=1.0, egress=None):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(egress or [kafka_egress()])
    env.setup_stats("Shop", "minimal", interval=10)  # readings for shop-telemetry
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    try:
        env.run(until=until)
    finally:
        env.teardown()


# --8<-- [start:run]
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    create_topics()  # with a router, KafkaEgress creates no topics
    run()
# --8<-- [end:run]
