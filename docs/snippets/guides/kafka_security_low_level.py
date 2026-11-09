"""The Connect to Secure Kafka guide, low-level API: a shop that publishes its events
through the egress of kafka_security_connections.py, which must be beside this script.

Set KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET, then run
`python kafka_security_low_level.py`. It runs for one minute and publishes to a cluster
that uses SASL. Set KAFKA_CONNECTION to mtls or msk to publish to a cluster of that kind
instead, as kafka_security_connections.py sets out.
"""

import itertools
import logging

from kafka_security_connections import msk_egress, mtls_egress, selected_egress

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


# --8<-- [start:shop]
def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    env.publish_event(
        key,
        {
            "path_id": path_id,
            "sim_id": "Shop",
            "service": "packing",
            "status": "queued",
        },
        "pack_order",
        task_id,
    )
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key,
            {
                "path_id": path_id,
                "sim_id": "Shop",
                "service": "packing",
                "status": "started",
            },
            "pack_order",
            task_id,
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def shop(sinks: list, factor: float = 0.0) -> DynamicRealtimeEnvironment:
    """The shop, publishing to the sinks given."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(sinks)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    env = shop([selected_egress()])  # KAFKA_CONNECTION picks the cluster
    try:
        env.run(until=60)
    finally:
        env.teardown()
# --8<-- [end:shop]


# --8<-- [start:mtls]
def mtls_shop() -> DynamicRealtimeEnvironment:
    """The shop with the egress of a cluster that requires mutual TLS."""
    return shop([mtls_egress()])


# --8<-- [end:mtls]


# --8<-- [start:msk]
def msk_shop() -> DynamicRealtimeEnvironment:
    """The shop with the egress of an Amazon MSK cluster."""
    return shop([msk_egress()])


# --8<-- [end:msk]
