"""Orders wait in a queue when all packers are busy, low-level API.

The low-level version of `orders_wait_for_busy_packers.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event` and each
reading with `env.stats`. Each order is one process that queues for a packer, packs and
publishes.

Needs: nothing
Run it with: python examples/gallery/orders_wait_for_busy_packers_low_level.py
"""

import itertools
import logging
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

UNTIL = 7200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.015)},
    service={"pack": DistributionConfig(dist="normal", mean=240, std=60)},
    resources={"packers": CapacityConfig(current_cap=3)},
)
STATS = "minimal"


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals and the orders on `env`."""
    sampler = Sampler(seed=42)
    packers = DynamicResource(env, "Shop", "packers")
    gap = env.registry.get_config("Shop.arrival.orders")
    gap.stream = "arrival.orders"  # the random sequence the blueprint's draw uses
    service = env.registry.get_config("Shop.service.pack")
    service.stream = "service.pack"

    def order(key, task_id):
        path_id = "Shop.service.pack"
        env.stats.flow_started("orders")
        env.publish_event(
            key,
            {"path_id": path_id, "status": "queued"},
            task="order",
            instance=task_id,
        )
        env.stats.task_queued("order", task_id)
        with packers.request() as request:
            yield request
            env.publish_event(
                key,
                {"path_id": path_id, "status": "started"},
                task="order",
                instance=task_id,
            )
            env.stats.task_started("order", task_id)
            yield env.timeout(sampler.sample(service))
            env.publish_event(
                key,
                {"event_type": "packed", "order_id": task_id},
                task="order",
                instance=task_id,
            )
            env.stats.task_finished("order", task_id)
        env.stats.flow_finished("orders")

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(order(f"orders-{number}", number - 1))

    env.process(source())


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    env.setup_stats("Shop", "minimal")
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
