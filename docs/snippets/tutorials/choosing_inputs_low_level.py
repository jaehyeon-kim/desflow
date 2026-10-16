"""The Choosing Inputs tutorial: the busy hour with four candidate packing times.

Each candidate has a mean of 1.5 seconds. The script runs the busy hour once per
candidate, unpaced, and prints the mean wait of each run.
"""

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

CANDIDATES = ["constant", "triangular", "empirical", "exponential"]
# --8<-- [start:triangular]
# An expert's estimate: at least 1 s, usually 1.25 s, at most 2.25 s
TRIANGULAR = DistributionConfig(dist="triangular", low=1, mode=1.25, high=2.25)
# --8<-- [end:triangular]
# --8<-- [start:empirical]
# 40 packing times measured at the bench, in seconds
MEASURED = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip
EMPIRICAL = DistributionConfig(dist="empirical", values=MEASURED)
# --8<-- [end:empirical]


def params() -> SimParameter:
    """The busy hour with 3 packers and the four candidates."""
    return SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
        # Four candidates for packing, each with a mean of 1.5 seconds
        service={
            "constant": DistributionConfig(dist="constant", value=1.5),
            "exponential": DistributionConfig(dist="exponential", rate=0.6667),
            "triangular": TRIANGULAR,
            "empirical": EMPIRICAL,
        },
        resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
    )


# --8<-- [start:compare]
def pack_order(env, sampler, packers, packing, task_id):
    """One order, packed with the candidate called `packing`."""
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        env.stats.task_started("pack_order", task_id)
        service = env.registry.get_config(f"Shop.service.{packing}")
        yield env.timeout(sampler.sample(service))
    env.stats.task_finished("pack_order", task_id)


# --8<-- [end:compare]


def order_generator(env, sampler, packers, packing):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, packing, task_id))
        task_id += 1


def mean_wait(packing: str) -> float:
    """Runs the busy hour with one candidate and returns the mean wait."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(params())
    env.setup_stats("Shop", "typical", interval=3600)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, Sampler(seed=42), packers, packing))
    try:
        env.run(until=3600)
    finally:
        env.teardown()
    return env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


if __name__ == "__main__":
    for packing in CANDIDATES:
        print(f"{packing:<12} mean wait {mean_wait(packing):5.2f} s")
