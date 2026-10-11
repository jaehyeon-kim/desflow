"""A complete low-level simulation, as the Low-level API page explains it step by step.

One lathe line with a coolant tank and a finished-goods store. tests/unit runs this
file unpaced, so every call on the page is checked.
"""

# --8<-- [start:imports]
import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    DynamicStore,
    Sampler,
    SimParameter,
    time_to_seconds,
)
# --8<-- [end:imports]

# --8<-- [start:environment]
env = DynamicRealtimeEnvironment(factor=0.0)  # unpaced; 1.0 follows the wall clock
# --8<-- [end:environment]

# --8<-- [start:parameters]
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Line_A",
        arrival={"parts": DistributionConfig(dist="exponential", rate=0.5)},
        service={"milling": DistributionConfig(dist="normal", mean=1.5, std=0.3)},
        resources={"lathe": CapacityConfig(current_cap=1, max_cap=3)},
        containers={"coolant": CapacityConfig(current_cap=100.0, max_cap=100.0)},
        stores={"finished": CapacityConfig(current_cap=50, max_cap=50)},
        variables={"coolant_per_part": 0.5},
    )
)
# --8<-- [end:parameters]

# --8<-- [start:connectors]
env.setup_egress([ConsoleEgress()])  # starts the egress thread
# env.setup_ingress([KafkaIngress(...)]) would start the ingress thread
# --8<-- [end:connectors]

# --8<-- [start:resources]
lathe = DynamicResource(env, "Line_A", "lathe")  # reads Line_A.resources.lathe.*
coolant = DynamicContainer(env, "Line_A", "coolant", init=100.0)
finished = DynamicStore(env, "Line_A", "finished")
sampler = Sampler(rng=np.random.default_rng(42))  # one seeded generator for every draw
# --8<-- [end:resources]


# --8<-- [start:part]
def part(env, part_id):
    """One part: queue for the lathe, use coolant, machine, then store the result."""
    key, path_id = f"task-{part_id}", "Line_A.service.milling"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})
    with lathe.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        litres = env.registry.get("Line_A.variables.coolant_per_part").value
        yield coolant.get(litres)  # waits if the tank is too low
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    yield finished.put({"part_id": part_id})
    env.publish_event(key, {"status": "finished", "part_id": part_id})


# --8<-- [end:part]


# --8<-- [start:arrivals]
def arrivals(env):
    gap = env.registry.get_config("Line_A.arrival.parts")  # the live config object
    part_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))  # each draw reads the current rate
        env.process(part(env, part_id))
        part_id += 1


# --8<-- [end:arrivals]


# --8<-- [start:telemetry]
def telemetry(env):
    while True:
        env.publish_telemetry("Line_A.lathe.in_use", lathe.in_use)
        env.publish_telemetry("Line_A.coolant.level", coolant.level)
        yield env.timeout(10.0)


# --8<-- [end:telemetry]


# --8<-- [start:control]
def refill_and_rush(env):
    """Refills coolant every minute, and doubles arrivals after five minutes."""
    while True:
        yield env.timeout(60.0)
        missing = coolant.capacity - coolant.level
        if missing > 0:  # SimPy refuses a put of 0
            yield coolant.put(missing)
        if env.now >= 300 and env.registry.get("Line_A.arrival.parts.rate").value < 1.0:
            env.registry.update("Line_A.arrival.parts.rate", 1.0)
            env.registry.update("Line_A.resources.lathe.current_cap", 2)


# --8<-- [end:control]


# --8<-- [start:start]
env.process(arrivals(env))
env.process(telemetry(env))
env.process(refill_and_rush(env))
# --8<-- [end:start]

if __name__ == "__main__":
    # --8<-- [start:run]
    try:
        env.run(until=time_to_seconds("10 min"))  # until is in simulation seconds
    finally:
        env.teardown()  # flushes the buffers and stops the threads
    # --8<-- [end:run]
    assert (
        lathe.capacity == 2
        and env.registry.get("Line_A.arrival.parts.rate").value == 1.0
    )
    assert len(finished.items) > 0
