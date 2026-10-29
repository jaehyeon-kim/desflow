"""The Write to Iceberg Tables guide, low-level API: the shop appends an hour of orders
to the Iceberg table shop.events, as fast as the machine allows.

The catalog and the sink are those of iceberg_tables.py, which must be beside this
script. ICEBERG_CATALOG picks the catalog: sqlite (the default), rest, postgres or glue.
Run it with `python iceberg_tables_low_level.py`.
"""

import itertools
import logging

from iceberg_tables import BATCH_SIZE, sink

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


def shop(sink, factor: float = 0.0) -> DynamicRealtimeEnvironment:
    """The shop, appending to the table of the sink given, as fast as it can."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    # One commit per flush, so the sink gets a large batch of its own
    env.setup_egress([sink], batch_sizes=[BATCH_SIZE])
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env


# --8<-- [end:shop]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    env = shop(sink)  # rows of shop.events in the catalog ICEBERG_CATALOG names
    try:
        env.run(until=3600)
    finally:
        env.teardown()
