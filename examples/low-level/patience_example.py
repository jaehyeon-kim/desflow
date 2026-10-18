"""Patience: orders that leave the queue, and orders that refuse to join, low-level API.

The low-level twin of `builder/patience_example.py`. One packer serves more orders
than it can pack. An online order waits at most 20 seconds for the packer and a
walk-in customer 10; one that waits longer leaves the queue and publishes reneged.
From 100 seconds the courier has made its last collection, so new orders balk: they
publish balked and never join the queue.

Needs no containers. Runs unpaced and ends after 120 simulation seconds.
"""

import itertools
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
    arrival={
        "orders": DistributionConfig(dist="exponential", rate=0.5),
        "walk_ins": DistributionConfig(dist="exponential", rate=0.1),
    },
    service={
        "packing": DistributionConfig(dist="triangular", low=1, mode=2, high=4),
    },
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
)
PATIENCE = {"orders": 20, "walk_ins": 10}


# --8<-- [start:pack]
def pack(env, sampler, packers, key, patience, task_id):
    """Balks after 100 s; otherwise waits at most `patience` for a packer."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    # No new orders once the courier has made its last collection
    if env.now >= 100:
        publish({"path_id": path_id, "status": "balked"})
        env.stats.task_balked("pack", task_id)
        return
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    with packers.request() as req:
        # Each order's own patience: it leaves the queue when it runs out
        yield req | env.timeout(patience)
        if not req.triggered:
            publish({"path_id": path_id, "status": "reneged"})
            env.stats.task_reneged("pack", task_id)
            return
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
    env.stats.task_finished("pack", task_id)
    env.stats.flow_finished(key.rsplit("-", 1)[0])


# --8<-- [end:pack]
def order_generator(env, sampler, packers, arrival, pack_ids):
    """Spawns a pack task for every arrival, keyed orders-1, walk_ins-1 and so on."""
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        key = f"{arrival}-{number}"
        env.process(pack(env, sampler, packers, key, PATIENCE[arrival], next(pack_ids)))


def build(env):
    """Builds the packers."""
    return DynamicResource(env, "Shop", "packers")


def processes(env, sampler, packers):
    """The two arrivals, in the builder's order."""
    pack_ids = itertools.count()
    return [
        order_generator(env, sampler, packers, "orders", pack_ids),
        order_generator(env, sampler, packers, "walk_ins", pack_ids),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Every 30 seconds, how many orders finished, reneged and balked so far
    env.setup_stats("Shop", "basic", interval=30.0)
    sampler = Sampler(seed=42)
    packers = build(env)
    for process in processes(env, sampler, packers):
        env.process(process)
    try:
        env.run(until=120)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
