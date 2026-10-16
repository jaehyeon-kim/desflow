"""The First Simulation tutorial: the shop, on the low-level API.

One packer, orders arriving about every 2 seconds, and 1.5 seconds of packing per
order. Every record is printed to the terminal.
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

# --8<-- [start:shop]
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)
SEED = 42  # the same arrival times on every run
# --8<-- [end:shop]


# --8<-- [start:order]
def pack_order(env, sampler, packers, key, task_id):
    """One order: wait for a packer, pack it, release the packer."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        service = env.registry.get_config(path_id)
        yield env.timeout(sampler.sample(service))
        publish({"event_type": "order_packed"})


def order_generator(env, sampler, packers):
    """Starts one pack_order for every arrival, keyed orders-1, orders-2, ..."""
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        key = f"orders-{task_id + 1}"
        env.process(pack_order(env, sampler, packers, key, task_id))
        task_id += 1


# --8<-- [end:order]


# --8<-- [start:records]
SINKS = [ConsoleEgress()]  # passed to env.setup_egress in run below
# --8<-- [end:records]


# --8<-- [start:run]
def run(until=20, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(SINKS)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=SEED)

    env.process(order_generator(env, sampler, packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


if __name__ == "__main__":
    run()
# --8<-- [end:run]
