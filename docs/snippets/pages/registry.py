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
    sim_id="Line_A",
    arrival={"standard": DistributionConfig(dist="exponential", rate=0.5)},
    service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"lathe": CapacityConfig(current_cap=2, max_cap=5)},
    variables={"conveyor_speed": 1.5},
)
env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(params)
# Registered paths include:
#   Line_A.arrival.standard.rate
#   Line_A.service.milling.mean, Line_A.service.milling.std
#   Line_A.resources.lathe.current_cap, Line_A.resources.lathe.max_cap
#   Line_A.variables.conveyor_speed
print(env.registry.get("Line_A.resources.lathe.current_cap").value)  # 2
# --8<-- [end:paths]

lathe = DynamicResource(env, "Line_A", "lathe")

# --8<-- [start:update]
env.registry.update("Line_A.resources.lathe.current_cap", 3)  # applied
env.registry.update("Line_A.resources.lathe.current_cap", 9)  # stored as 5, the max_cap
env.registry.update("Line_A.service.milling.std", -1.0)  # refused, the old 0.5 stays
env.registry.update("Line_A.resources.lath.current_cap", 3)  # no such path: ignored
# --8<-- [end:update]

# --8<-- [start:follow]
milling = env.registry.get_config("Line_A.service.milling")
print(milling.mean)  # 3.0: the Sampler reads this object on its next draw
env.registry.update("Line_A.service.milling.mean", 4.0)
print(milling.mean)  # 4.0: the same object, updated in place
# --8<-- [end:follow]


# --8<-- [start:wait]
def watch_speed(env):
    """Logs the conveyor speed every time it changes."""
    speed = env.registry.get("Line_A.variables.conveyor_speed")
    while True:
        yield speed.wait_for_change()
        print(f"t={env.now}: conveyor speed is now {speed.value}")


env.process(watch_speed(env))
# --8<-- [end:wait]


def change_speed(env):
    yield env.timeout(5.0)
    env.registry.update("Line_A.variables.conveyor_speed", 2.0)


env.process(change_speed(env))

if __name__ == "__main__":
    try:
        env.run(until=10.0)
    finally:
        env.teardown()
    assert env.registry.get("Line_A.resources.lathe.current_cap").value == 5
    assert env.registry.get("Line_A.service.milling.std").value == 0.5
