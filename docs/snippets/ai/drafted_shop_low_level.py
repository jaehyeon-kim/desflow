"""The drafted packing shop on the low-level API.

The Low-level form of drafted_shop.yaml: it gives the same records at the same seed.
Times are in seconds. Run it with: python drafted_shop_low_level.py
"""

from datetime import datetime, timezone

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

PARAMS = SimParameter(
    sim_id="Shop",
    # said: an order every 2 seconds on average; assumed: orders arrive at random
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    # said: 1.5 seconds; assumed: every order takes the same time
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},  # said: one packer
)
SEED = 42
# a fixed date, so two runs give the same records
START = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)


def pack_order(env, sampler, packers, key, task_id):
    """One order: wait for a packer, pack it, release the packer."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish(
        {"path_id": path_id, "sim_id": "Shop", "service": "packing", "status": "queued"}
    )
    with packers.request() as request:
        yield request
        publish(
            {
                "path_id": path_id,
                "sim_id": "Shop",
                "service": "packing",
                "status": "started",
            }
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})


def order_generator(env, sampler, packers):
    """Starts one pack_order for every arrival, keyed orders-1, orders-2, ..."""
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def run(until=3600):  # assumed: one hour answers the question
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=START)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress(format="json")])
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
