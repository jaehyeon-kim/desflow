"""Changing demand: a rush of orders and a second packer for it, on a schedule, low-level API.

The low-level twin of `builder/changing_demand_example.py`. Orders arrive at 0.4 a
second, and at 0.8 during a rush from 120 to 180 seconds; a second packer works
during the rush. `run_scenario` applies each change before every other event at its
time, so the arrival gap drawn at 120 already uses the rush rate.

Needs no containers. Runs unpaced and ends after 300 simulation seconds.
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
    run_scenario,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={"packing": DistributionConfig(dist="triangular", low=1, mode=2, high=3)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=2)},
)

# --8<-- [start:schedule]
# The rush: twice the orders, and a second packer, from 120 to 180 seconds
SCHEDULE = [
    {"at": 120, "path": "Shop.arrival.orders.rate", "value": 0.8},
    {"at": 120, "path": "Shop.resources.packers.current_cap", "value": 2},
    {"at": 180, "path": "Shop.arrival.orders.rate", "value": 0.4},
    {"at": 180, "path": "Shop.resources.packers.current_cap", "value": 1},
]
# --8<-- [end:schedule]


def pack(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    env.stats.flow_started("orders")
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack", task_id)
    env.stats.flow_finished("orders")


def order_generator(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        # The gap is drawn from the rate in force now
        yield env.timeout(sampler.sample(gap))
        env.process(pack(env, sampler, packers, f"orders-{number}", number - 1))


def build(env):
    return DynamicResource(env, "Shop", "packers")


def processes(env, sampler, packers):
    """The arrivals and the schedule, in the builder's order."""
    return [
        order_generator(env, sampler, packers),
        run_scenario(env, SCHEDULE, until=300),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Every 60 seconds: the packers' readings and the mean wait so far
    env.setup_stats("Shop", "typical", interval=60.0)
    sampler = Sampler(seed=42)
    packers = build(env)
    for process in processes(env, sampler, packers):
        env.process(process)
    try:
        env.run(until=300)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
