"""Fitted inputs, low-level API: the shop's packing time, fitted from 40 measured times.

The low-level twin of `builder/fitting_example.py`. `fit_column` fits the 40 packing
times in ../packing_times.csv to every family, and the script prints the table of
candidates. The best one's settings make the packing `DistributionConfig`, the same
numbers `desflow fit` writes for the YAML version. Two packers and orders at 0.8 a
second. The pack task reports each step to `env.stats`, as the builder's tasks do
for you. The run prints every event and the statistics, and ends after one
simulation hour.

Needs no containers, and the fit extra: pip install "desflow[fit]".
"""

import logging
from pathlib import Path

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    fit_column,
)
from desflow.analysis.fit import read_column

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
DATA = Path(__file__).resolve().parent.parent / "packing_times.csv"

# --8<-- [start:fit]
result = fit_column(read_column(DATA, "packing_seconds"), unit="s", lower=0)
print(result.text())  # every candidate, its status and diagnostics
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.8)},
    # The best candidate's settings, such as dist="triangular", low=0.546332
    service={"packing": DistributionConfig(**result.settings())},
    resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
)
# --8<-- [end:fit]


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


def run():
    """Runs one simulation hour."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_stats("Shop", "typical", interval=600)
    env.setup_egress([ConsoleEgress()])
    sampler = Sampler(seed=42)
    for process in processes(env, sampler, build(env)):
        env.process(process)
    try:
        env.run(until=3600)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
