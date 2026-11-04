"""GA-3: Capacity drops to one at 20 seconds, and the queue grows.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event` and each reading with `env.stats`.
"""

import itertools

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    run_scenario,
)

UNTIL = 60
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1)},
    service={"pack": DistributionConfig(dist="normal", mean=2.5, std=0.3)},
    resources={"packers": CapacityConfig(current_cap=3)},
)
STATS = ("minimal", 1)  # the level and the interval of the readings
SCHEDULE = [{"at": 20, "path": "Shop.resources.packers.current_cap", "value": 1}]


def build(env: DynamicRealtimeEnvironment) -> None:
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
                key, {"event_type": "packed"}, task="order", instance=task_id
            )
            env.stats.task_finished("order", task_id)
        env.stats.flow_finished("orders")

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(order(f"orders-{number}", number - 1))

    env.process(source())
    env.process(run_scenario(env, SCHEDULE, until=UNTIL))
