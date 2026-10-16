"""The Queues and Capacity tutorial: the shop's busy hour, on the low-level API.

Orders arrive at 1.8 a second and each takes 1.5 seconds to pack. The script runs the
hour once for each number of packers, unpaced, and prints the mean wait of each run.
"""

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

UNTIL = 3600  # the busy hour, in seconds


def params(packers: int) -> SimParameter:
    """The shop in its busy hour, with `packers` packers."""
    return SimParameter(
        sim_id="Shop",
        # --8<-- [start:load]
        service={"packing": DistributionConfig(dist="constant", value=1.5)},
        # The busy hour: 1.8 orders a second, more than one packer can pack
        arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
        # --8<-- [end:load]
        # --8<-- [start:packers]
        resources={"packers": CapacityConfig(current_cap=packers, max_cap=10)},
        # --8<-- [end:packers]
    )


def pack_order(env, sampler, packers, task_id):
    """One order, which reports each step to the statistics."""
    path_id = "Shop.service.packing"
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    env.stats.task_finished("pack_order", task_id)


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, task_id))
        task_id += 1


def run(packers: int, until=UNTIL, factor=0.0):
    """Runs the busy hour with `packers` packers, unpaced, and returns the environment."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(params(packers))
    # --8<-- [start:stats]
    # The mean wait of pack_order, kept by the environment and read at the end
    env.setup_stats("Shop", "typical", interval=3600)
    # --8<-- [end:stats]
    packers_resource = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=42)
    env.process(order_generator(env, sampler, packers_resource))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


# --8<-- [start:table]
def mean_wait(packers: int) -> float:
    """Runs the busy hour with `packers` packers and returns the mean wait."""
    env = run(packers)
    return env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


if __name__ == "__main__":
    for packers in range(1, 5):
        print(f"packers {packers}: mean wait {mean_wait(packers):8.2f} s")
# --8<-- [end:table]
