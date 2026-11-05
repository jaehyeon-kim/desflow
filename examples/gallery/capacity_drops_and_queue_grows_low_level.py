"""Capacity drops to one at 20 seconds, and the queue grows, low-level API.

The low-level version of `capacity_drops_and_queue_grows.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event` and each
reading with `env.stats`. A process sets the capacity to 1 at 20 s.

Needs: nothing
Run it with: python examples/gallery/capacity_drops_and_queue_grows_low_level.py
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
    run_scenario,
)

UNTIL = 60
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1)},
    service={"pack": DistributionConfig(dist="normal", mean=2.5, std=0.3)},
    resources={"packers": CapacityConfig(current_cap=3)},
)
STATS = ("minimal", 1)  # the level and the interval of the readings
SCHEDULE = [{"at": 20, "path": "Shop.resources.packers.current_cap", "value": 1}]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals and the scenario on `env`."""
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
                {"event_type": "packed"},
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
    env.process(run_scenario(env, SCHEDULE, until=UNTIL))


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    env.setup_stats("Shop", "minimal", 1)
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
