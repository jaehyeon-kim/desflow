"""DX-4: A payload that is constants and the instance number.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

import itertools

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

UNTIL = 40
PARAMS = SimParameter(
    sim_id="Shop", resources={"packers": CapacityConfig(current_cap=1, max_cap=3)}
)


def build(env: DynamicRealtimeEnvironment) -> None:
    packers = DynamicResource(env, "Shop", "packers")

    def pack(key, task_id):
        env.publish_event(key, {"status": "queued"}, task="pack", instance=task_id)
        with packers.request() as request:
            yield request
            env.publish_event(key, {"status": "started"}, task="pack", instance=task_id)
            yield env.timeout(2)
            env.publish_event(
                key,
                {
                    "order_id": task_id,
                    "amount": 19.99,
                    "event_type": "prediction",
                    "task_id": key,
                    "confidence": 0.9,
                },
                task="pack",
                instance=task_id,
            )

    def source():
        for number in itertools.count(1):
            yield env.timeout(5)
            env.process(pack(f"orders-{number}", number - 1))

    env.process(source())
