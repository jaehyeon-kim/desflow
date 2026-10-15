"""The First Simulation tutorial, step 2, on the low-level API."""

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

# --8<-- [start:random]
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    # Packing takes 3 seconds on average, give or take 0.5
    service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=5)},  # up to 5 packers
)
SEED = 42  # Sampler(rng=np.random.default_rng(SEED)) in run: same draws every run
# --8<-- [end:random]


def pack_order(env, sampler, packers, order_id):
    key, path_id = f"task-{order_id}", "Shop.service.packing"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})
    with packers.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"order_id": order_id})


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    order_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, order_id))
        order_id += 1


# --8<-- [start:changes]
def capacity_plan(env):
    """Three packers from t=10, two from t=20, on the simulation clock."""
    path = "Shop.resources.packers.current_cap"
    yield env.timeout(10.0)
    env.registry.update(path, 3)
    yield env.timeout(10.0)
    env.registry.update(path, 2)


# --8<-- [end:changes]


def run(until=30.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(SEED))
    env.process(order_generator(env, sampler, packers))
    env.process(capacity_plan(env))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return packers


if __name__ == "__main__":
    run()
