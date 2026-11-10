"""DX-3: A payload that draws the delivery.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The delivery is drawn with `Sampler`, from the stream the
blueprint's `weighted` uses.
"""

import itertools

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

UNTIL = 6000
PARAMS = SimParameter(
    sim_id="Shop", resources={"packers": CapacityConfig(current_cap=1, max_cap=3)}
)


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=1)
    delivery = sampler.generator("tasks.pack_order.payload.delivery")
    packers = DynamicResource(env, "Shop", "packers")

    def pack_order(key, task_id):
        env.publish_event(
            key, {"status": "queued"}, task="pack_order", instance=task_id
        )
        with packers.request() as request:
            yield request
            env.publish_event(
                key, {"status": "started"}, task="pack_order", instance=task_id
            )
            yield env.timeout(2)
            # a point in the weights 0.9 and 0.1: below 0.9 is standard
            point = delivery.random() * (0.9 + 0.1)
            env.publish_event(
                key,
                {
                    "status": "finished",
                    "order_id": task_id,
                    "delivery": "standard" if point < 0.9 else "express",
                },
                task="pack_order",
                instance=task_id,
            )

    def source():
        for number in itertools.count(1):
            yield env.timeout(5)
            env.process(pack_order(f"orders-{number}", number - 1))

    env.process(source())
