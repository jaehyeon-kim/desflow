"""GA-8: An arrival rate changes while the run continues, from a Kafka message.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event` and each reading with `env.stats`. The Kafka ingress writes
a registry value, which the arrival reads at each gap: `env.setup_ingress` takes it, and
this form leaves it out, as the other two forms have it taken away for the run.
"""

import itertools

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

UNTIL = 7200
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.015)},
    service={"pack": DistributionConfig(dist="normal", mean=240, std=60)},
    resources={"packers": CapacityConfig(current_cap=3)},
)
STATS = "minimal"


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    packers = DynamicResource(env, "Shop", "packers")
    gap = env.registry.get_config(
        "Shop.arrival.orders"
    )  # the registry value the message sets
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
