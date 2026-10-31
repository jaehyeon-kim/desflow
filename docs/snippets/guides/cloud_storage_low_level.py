"""The Write to Cloud Storage guide, low-level API: the shop writes an hour of orders as
Parquet and JSON Lines files, as fast as the machine allows.

The store and the sinks are those of cloud_storage_stores.py, which must be beside this script.
STORAGE picks the store (local, the default, s3, gcs or azure) and STORAGE_BUCKET the
bucket, container or local folder. Run it with `python cloud_storage_low_level.py`.
"""

import itertools
import logging

from cloud_storage_stores import sinks

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
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        # The finished event is the order record
        order = {"order_id": task_id, "amount": 19.99}
        env.publish_event(key, order, "pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def shop(sinks: list, factor: float = 0.0) -> DynamicRealtimeEnvironment:
    """The shop, writing to the sinks given, as fast as the machine allows."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(sinks)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env


# --8<-- [end:shop]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    env = shop(sinks)  # Parquet and JSON Lines files in the store STORAGE names
    try:
        env.run(until=3600)
    finally:
        env.teardown()
