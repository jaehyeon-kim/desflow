"""Measuring a run, low-level API: built-in statistics, a warm-up and ground truth.

The low-level twin of `builder/measuring_example.py`. `env.setup_stats` publishes
every statistic of the packers and the pack task every 5 minutes. The pack task
reports when each order is queued, started and finished to `env.stats`, which is
what the builder's tasks do for you. `setup_egress` ends a 3-minute warm-up: no sink
receives records from it, and the statistics start again when it ends. At 12
minutes packing slows down, and `env.publish_label` sends a ground-truth record to
the labels sink only.

Needs no containers. Runs unpaced and ends after 20 simulation minutes.
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
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={"packing": DistributionConfig(dist="gamma", shape=4, scale=1)},
    resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
)
WARMUP = 180.0  # seconds


def pack(env, sampler, packers, key, task_id):
    """The task lifecycle, reported to the statistics at each step."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    with packers.request() as req:
        yield req
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack", task_id)
    # The order leaves the shop: one more finished flow for its arrival
    env.stats.flow_finished("orders")


def order_generator(env, sampler, packers):
    """Spawns a pack task for every order, keyed orders-1, orders-2 and so on."""
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def slow_packing(env):
    """Packing slows from 4 to 4.8 seconds on average at 12 minutes, with its label."""
    yield env.timeout(720)
    path = "Shop.service.packing.scale"
    old = env.registry.get(path).value
    env.registry.update(path, 1.2)
    env.publish_label(
        {"fault": "slow_packing", "cause": "new packing material"},
        path=path,
        old=old,
        new=env.registry.get(path).value,
    )


def build(env):
    """Builds the packers."""
    return DynamicResource(env, "Shop", "packers")


def processes(env, sampler, packers):
    """The orders and the fault."""
    return [order_generator(env, sampler, packers), slow_packing(env)]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    # --8<-- [start:measure]
    # Every level, published every 5 minutes, such as Shop.stats.tasks.pack.wait_p95
    env.setup_stats("Shop", "all", interval=300)
    # The second sink receives the ground-truth records, and nothing else
    env.setup_egress(
        [ConsoleEgress(), ConsoleEgress()], labels=[False, True], warmup=WARMUP
    )
    # --8<-- [end:measure]
    sampler = Sampler(seed=42)
    packers = build(env)
    for process in processes(env, sampler, packers):
        env.process(process)
    try:
        env.run(until=1200)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
