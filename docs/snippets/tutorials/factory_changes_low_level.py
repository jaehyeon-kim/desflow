"""The First Simulation tutorial, step 2, on the low-level API."""

import logging

import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s")

# --8<-- [start:random]
PARAMS = SimParameter(
    sim_id="Factory_A",
    arrival={"parts": DistributionConfig(dist="exponential", rate=0.5)},
    # Milling takes 3 seconds on average, give or take 0.5
    service={"milling": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},  # up to 5 lathes
)
SEED = 42  # Sampler(rng=np.random.default_rng(SEED)) in run: same draws every run
# --8<-- [end:random]


def process_part(env, sampler, lathe, part_id):
    key, path_id = f"task-{part_id}", "Factory_A.service.milling"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})
    with lathe.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"part_id": part_id})


def parts_generator(env, sampler, lathe):
    arrival = env.registry.get_config("Factory_A.arrival.parts")
    part_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(process_part(env, sampler, lathe, part_id))
        part_id += 1


# --8<-- [start:changes]
def capacity_plan(env):
    """Three lathes from t=10, two from t=20, on the simulation clock."""
    path = "Factory_A.resources.lathe.current_cap"
    yield env.timeout(10.0)
    env.registry.update(path, 3)
    yield env.timeout(10.0)
    env.registry.update(path, 2)
# --8<-- [end:changes]


def run(until=30.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    lathe = DynamicResource(env, "Factory_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(SEED))
    env.process(parts_generator(env, sampler, lathe))
    env.process(capacity_plan(env))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return lathe


if __name__ == "__main__":
    run()
