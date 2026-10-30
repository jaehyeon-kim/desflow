"""File steering, low-level API, no containers.

The low-level version of `builder/file_example.py`, built on
`DynamicRealtimeEnvironment` directly. `FileIngress` reads ../params.yaml, whose one
line sets the packers' capacity. Open that file in an editor while the run goes,
change the 1 to 5 and save it. The packers' capacity and queue_length, which the
statistics publish every 2 seconds, show the effect within about a second.

Needs no containers. The run goes in real time and ends after 60 seconds.
"""

import logging
from pathlib import Path

import numpy as np

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    FileIngress,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS_FILE = Path(__file__).resolve().parent.parent / "params.yaml"
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
    service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=5)},
)


def pack_order(env, sampler, packers, key, task_id):
    """Models the task lifecycle: queued, started, then finished with its payload."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as req:
        yield req
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish(
            {"event_type": "order_packed", "shipping": "standard", "order_id": task_id}
        )
        env.stats.task_finished("pack_order", task_id)


def arrival_generator(env, sampler, packers):
    """Spawns a pack_order task for every order the arrival distribution draws."""
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def run():
    env = DynamicRealtimeEnvironment(factor=1.0, strict=False)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # --8<-- [start:file-ingress]
    # Applies an entry of the file when it first appears or its value changes
    env.setup_ingress([FileIngress(PARAMS_FILE)])
    # --8<-- [end:file-ingress]
    env.setup_stats("Shop", "minimal", interval=2.0)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(42))
    env.process(arrival_generator(env, sampler, packers))
    try:
        env.run(until=60)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
