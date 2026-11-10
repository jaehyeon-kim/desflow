"""DX-18: A sink that keeps only the finished orders.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The sink is a setting of a connector, so this form has the
records only.
"""

import itertools
from datetime import datetime, timezone

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

UNTIL = 20
PARAMS = SimParameter(
    sim_id="Shop",
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)
ENVIRONMENT = {"logical_start_time": datetime(2026, 1, 1, tzinfo=timezone.utc)}


def build(env: DynamicRealtimeEnvironment) -> None:
    packers = DynamicResource(env, "Shop", "packers")
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
            yield env.timeout(1.5)
            env.publish_event(
                key,
                {"event_type": "order_packed"},
                task="pack_order",
                instance=task_id,
            )

    def source():
        for number in itertools.count(1):
            yield env.timeout(2)
            env.process(pack_order(f"orders-{number}", number - 1))

    env.process(source())
