"""The Route Records to Topics guide, low-level API: a shop whose failed orders go to
their own topic.

It needs a Kafka broker on 127.0.0.1:9092: start one with `odctl up kafka-lite`. Run it
with `python complex_routing_low_level.py`, beside complex_routing_logic.py; it runs
for one minute. With ROUTING=router in the environment, it sends the records through a
function instead, which wins over the topic in the payload.
"""

import itertools
import logging
import os

from complex_routing_logic import custom_kafka_router
from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaEgress,
    Sampler,
    SimParameter,
)

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


# --8<-- [start:egress]
def payload_egress() -> KafkaEgress:
    """Sends each record to the topic its payload names, and creates the topics."""
    return KafkaEgress(
        bootstrap_servers=BOOTSTRAP,
        event_topic="shop-events",
        telemetry_topic="shop-telemetry",
        topic_policy="create",
        topics={"system-alarms": {}},
    )


# --8<-- [end:egress]


# --8<-- [start:router]
def router_egress() -> KafkaEgress:
    """Sends each record to the topic a function picks."""
    return KafkaEgress(
        bootstrap_servers=BOOTSTRAP,
        topic_router=custom_kafka_router,
        topics={"shop-events": {}, "shop-telemetry": {}, "system-alarms": {}},
    )


# --8<-- [end:router]


# --8<-- [start:shop]
def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    stats = env.stats
    queued = {"path_id": path_id, "sim_id": "Shop", "service": "packing"}
    env.publish_event(key, {**queued, "status": "queued"}, "pack_order", task_id)
    stats.task_queued("pack_order", task_id)
    with packers.request() as unit:
        yield unit
        env.publish_event(key, {**queued, "status": "started"}, "pack_order", task_id)
        stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        failed = task_id % 10 == 9  # every tenth order
        payload = {
            "status": "packing_failed" if failed else "packed",
            "__topic__": "system-alarms" if failed else "shop-events",
        }
        env.publish_event(key, payload, "pack_order", task_id)
        stats.task_finished("pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


# --8<-- [end:shop]


def run(until=60.0, egress=None):
    """Runs the shop unpaced into `egress`, which is `payload_egress()` unless given."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([egress or payload_egress()])
    env.setup_stats("Shop", "minimal", interval=10)  # readings for shop-telemetry
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(router_egress() if os.environ.get("ROUTING") == "router" else payload_egress())
