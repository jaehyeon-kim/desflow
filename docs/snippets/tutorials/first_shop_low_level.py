"""The First Simulation tutorial: the shop, on the low-level API.

One packer, orders arriving about every 2 seconds, and 1.5 seconds of packing per
order. Every record is printed to the terminal.
"""

import logging

import numpy as np

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# --8<-- [start:shop]
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="normal", mean=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)
# --8<-- [end:shop]


# --8<-- [start:order]
def pack_order(env, sampler, packers, order_id):
    """One order: wait for a packer, pack it, release the packer."""
    key = f"task-{order_id}"
    path_id = "Shop.service.packing"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})

    with packers.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        service = env.registry.get_config(path_id)
        yield env.timeout(sampler.sample(service))
        env.publish_event(key, {"order_id": order_id})


def order_generator(env, sampler, packers):
    """Starts one pack_order for every arrival."""
    arrival = env.registry.get_config("Shop.arrival.orders")
    order_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, order_id))
        order_id += 1


# --8<-- [end:order]


# --8<-- [start:records]
SINKS = [ConsoleEgress()]  # passed to env.setup_egress in run below
# --8<-- [end:records]


# --8<-- [start:run]
def run(until=10.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(SINKS)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng())

    env.process(order_generator(env, sampler, packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    print("Starting simulation...")
    run()
# --8<-- [end:run]
