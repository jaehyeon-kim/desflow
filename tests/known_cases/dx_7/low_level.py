"""DX-7: Custom readings every 30 seconds.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event` and each reading with `publish_telemetry`.
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

UNTIL = 120
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.6)},
    service={"packing": DistributionConfig(dist="gamma", shape=4, scale=0.75)},
    resources={"packers": CapacityConfig(current_cap=2, max_cap=5)},
)


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    packers = DynamicResource(env, "Shop", "packers")
    gap = env.registry.get_config("Shop.arrival.orders")
    gap.stream = "arrival.orders"  # the random sequence the blueprint's draw uses
    service = env.registry.get_config("Shop.service.packing")
    service.stream = "service.packing"
    path_id = "Shop.service.packing"

    def pack_order(key, task_id):
        env.publish_event(
            key,
            {"path_id": path_id, "status": "queued"},
            task="pack_order",
            instance=task_id,
        )
        with packers.request() as request:
            yield request
            env.publish_event(
                key,
                {"path_id": path_id, "status": "started"},
                task="pack_order",
                instance=task_id,
            )
            yield env.timeout(sampler.sample(service))
            env.publish_event(
                key, {"event_type": "order_packed"}, task="pack_order", instance=task_id
            )

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(pack_order(f"orders-{number}", number - 1))

    def readings():
        while True:
            env.publish_telemetry(
                "Shop.waiting_per_packer", len(packers.queue) / max(packers.capacity, 1)
            )
            env.publish_telemetry(
                "Shop.arrival_rate", env.registry.get("Shop.arrival.orders.rate").value
            )
            yield env.timeout(30)

    env.process(source())
    env.process(readings())
