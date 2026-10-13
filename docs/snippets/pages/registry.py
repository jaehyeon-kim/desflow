"""The registry calls of the Live Parameters page.

The file is a complete low-level run: tests/unit runs it unpaced, so every call shown
on the page is checked.
"""

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

# --8<-- [start:paths]
params = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=2, max_cap=5)},
    variables={"conveyor_speed": 1.5},
)
env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(params)
# Registered paths include:
#   Shop.arrival.orders.rate
#   Shop.service.packing.mean, Shop.service.packing.std
#   Shop.resources.packers.current_cap, Shop.resources.packers.max_cap
#   Shop.variables.conveyor_speed
print(env.registry.get("Shop.resources.packers.current_cap").value)  # 2
# --8<-- [end:paths]

packers = DynamicResource(env, "Shop", "packers")

# --8<-- [start:update]
env.registry.update("Shop.resources.packers.current_cap", 3)  # applied
env.registry.update("Shop.resources.packers.current_cap", 9)  # stored as 5, the max_cap
env.registry.update("Shop.service.packing.std", -1.0)  # refused, the old 0.5 stays
env.registry.update("Shop.resources.lath.current_cap", 3)  # no such path: ignored
# --8<-- [end:update]

# --8<-- [start:follow]
packing = env.registry.get_config("Shop.service.packing")
print(packing.mean)  # 3.0: the Sampler reads this object on its next draw
env.registry.update("Shop.service.packing.mean", 4.0)
print(packing.mean)  # 4.0: the same object, updated in place
# --8<-- [end:follow]


# --8<-- [start:wait]
def watch_speed(env):
    """Logs the conveyor speed every time it changes."""
    speed = env.registry.get("Shop.variables.conveyor_speed")
    while True:
        yield speed.wait_for_change()
        print(f"t={env.now}: conveyor speed is now {speed.value}")


env.process(watch_speed(env))
# --8<-- [end:wait]


def change_speed(env):
    yield env.timeout(5.0)
    env.registry.update("Shop.variables.conveyor_speed", 2.0)


env.process(change_speed(env))

if __name__ == "__main__":
    try:
        env.run(until=10.0)
    finally:
        env.teardown()
    assert env.registry.get("Shop.resources.packers.current_cap").value == 5
    assert env.registry.get("Shop.service.packing.std").value == 0.5
