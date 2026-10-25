"""Replications, low-level API: one hour of the shop, run 20 times with intervals.

The low-level version of `builder/replications_example.py`. `model` builds the shop in
an environment and with a sampler it is given; `LowLevelModel` names it with its sim
id, and `replicate` calls it once per replication, unpaced with no sinks, after a
3-minute warm-up, with a seed spawned from 42. The pack task reports each step to
`env.stats`, as the builder's tasks do for you, and the script prints the mean and
the 95% half-width of every statistic up to the typical level. A low-level model has
no sinks of its own, so nothing receives them as telemetry.

Needs no containers.
"""

import logging

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicResource,
    LowLevelModel,
    SimParameter,
    replicate,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
# Each replication would log its build and teardown
logging.getLogger("desflow.core").setLevel(logging.WARNING)

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

    env.stats.flow_started("orders")  # the order enters the shop
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    with packers.request() as req:
        yield req
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack", task_id)
    env.stats.flow_finished("orders")


def order_generator(env, sampler, packers):
    """Spawns a pack task for every order, keyed orders-1, orders-2 and so on."""
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def build(env):
    """Builds the packers."""
    return DynamicResource(env, "Shop", "packers")


def processes(env, sampler, packers):
    """The orders."""
    return [order_generator(env, sampler, packers)]


def model(env, sampler):
    """The whole shop in a given environment: replicate sets up the statistics."""
    env.registry.register_sim_parameter(PARAMS)
    for process in processes(env, sampler, build(env)):
        env.process(process)


def run():
    """Runs 20 replications of one hour and prints their summary."""
    # --8<-- [start:replicate]
    # 20 seeds spawned from 42; each replication runs unpaced, with no sinks
    shop = LowLevelModel(model, sim_id="Shop")
    result = replicate(shop, 20, seed=42, until=3600, warmup=WARMUP)
    print(result.summary_text())
    # Any replication repeats on its own: call model with Sampler(seed=...)
    print("replication seeds:", result.seeds)
    # --8<-- [end:replicate]


if __name__ == "__main__":
    run()
