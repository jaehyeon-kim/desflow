"""The Randomness and Seeds tutorial: the busy shop, seeded, with returns, low-level.

Orders arrive at 1.8 a second, and three packers pack each in 1.5 seconds on average,
with a standard deviation of 0.5 seconds. Returned parcels arrive every 5 seconds on
average and are recorded as they arrive. Every record is printed to the terminal.
"""

import logging

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

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
    # Packing varies: 1.5 seconds on average, with a standard deviation of 0.5
    service={"packing": DistributionConfig(dist="normal", mean=1.5, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
)
# --8<-- [start:seed]
# The same random numbers on every run, passed to Sampler(seed=SEED) in run below
SEED = 42
# --8<-- [end:seed]


def pack_order(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


# --8<-- [start:returns]
# Returned parcels: one every 5 seconds on average
PARAMS.arrival["returns"] = DistributionConfig(dist="exponential", rate=0.2)


def return_generator(env, sampler):
    """Records each return as it arrives; it does not use a packer."""
    # Once registered, the config draws from sampler.generator("arrival.returns")
    arrival = env.registry.get_config("Shop.arrival.returns")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        key = f"returns-{task_id + 1}"
        value = {"event_type": "return_received"}
        env.publish_event(key, value, task="receive_return", instance=task_id)
        task_id += 1


# --8<-- [end:returns]


def run(until=60, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=SEED)
    env.process(order_generator(env, sampler, packers))
    env.process(return_generator(env, sampler))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


if __name__ == "__main__":
    run()
