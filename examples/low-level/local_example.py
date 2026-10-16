"""Local simulation, low-level API, no containers.

The low-level twin of `builder/local_example.py`, built on `DynamicRealtimeEnvironment`
directly. An online shop packs orders and writes to `ConsoleEgress`. A scenario
process adds packers at 10 seconds and removes one at 20 seconds, and the built-in
statistics show the effect.

Needs no containers. Ends on its own after 30 simulation seconds.
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
logger = logging.getLogger("local_example")

# The shop starts with 1 packer and has room for 5.
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
    service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=5)},
)


def pack_order(env, sampler, packers, key, task_id):
    """Models the task lifecycle: queued, started, then finished with its payload.

    Every event carries the order's key, and the task's name and instance id.
    """
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    with packers.request() as req:
        yield req
        publish({"path_id": path_id, "status": "started"})
        # Late binding: the service time is read when the work starts
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish(
            {"event_type": "order_packed", "shipping": "standard", "order_id": task_id}
        )


def arrival_generator(env, sampler, packers):
    """Spawns a pack_order task for every order the arrival distribution draws."""
    # A reference: the config follows the registry when the rate changes
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        # Each arrival is a new order, keyed orders-1, orders-2 and so on
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def capacity_changes(env):
    """Three packers from 10 seconds, then two from 20 seconds, on simulation time."""
    path = "Shop.resources.packers.current_cap"
    yield env.timeout(10)
    env.registry.update(path, 3)
    yield env.timeout(10)
    env.registry.update(path, 2)


def run():
    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # The packers' readings every 2 simulation seconds
    env.setup_stats("Shop", "minimal", interval=2.0)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(42))

    env.process(arrival_generator(env, sampler, packers))
    env.process(capacity_changes(env))

    logger.info("Simulation started. Watch the capacity change at 10 s and 20 s...")
    try:
        env.run(until=30)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
