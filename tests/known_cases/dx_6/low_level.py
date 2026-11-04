"""DX-6: A process that takes every packer off for a break.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The capacity is written to the registry, as a task's `on:`
does.
"""

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

UNTIL = 180
CAPACITY = "Shop.resources.packers.current_cap"
PARAMS = SimParameter(
    sim_id="Shop",
    resources={"packers": CapacityConfig(current_cap=3, max_cap=3)},
    variables={"normal": 3},
)


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    DynamicResource(env, "Shop", "packers")
    key = "packers-break"

    instances = {"down": 0, "up": 0}  # each task numbers its instances over all clocks

    def clock():
        while True:
            number = instances["down"]
            instances["down"] += 1
            env.publish_event(key, {"status": "queued"}, task="down", instance=number)
            yield env.timeout(50)
            env.publish_event(key, {"status": "started"}, task="down", instance=number)
            env.publish_event(key, {"status": "down"}, task="down", instance=number)
            env.registry.update(CAPACITY, 0)
            number = instances["up"]
            instances["up"] += 1
            env.publish_event(key, {"status": "queued"}, task="up", instance=number)
            yield env.timeout(5)
            env.publish_event(key, {"status": "started"}, task="up", instance=number)
            env.publish_event(key, {"status": "up"}, task="up", instance=number)
            env.registry.update(
                CAPACITY, env.registry.get("Shop.variables.normal").value
            )

    for _ in range(size):
        env.process(clock())
