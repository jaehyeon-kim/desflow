"""Measuring a queue, for the Queues and Capacity page.

One lathe, parts arriving at `rate` per second, machining 1.5 s on average. The run is
unpaced. tests/unit runs it at utilisation 0.75 and checks the measured mean wait
against the queueing formula.
"""

import numpy as np

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)


# --8<-- [start:measure]
def mean_wait(rate, service, lathes=1, until=200_000, seed=1):
    """Mean time a part waits for a lathe, over `until` simulated seconds."""
    env = DynamicRealtimeEnvironment(factor=0.0)  # unpaced: as fast as possible
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="F",
            arrival={"parts": DistributionConfig(dist="exponential", rate=rate)},
            service={"machining": service},
            resources={"lathe": CapacityConfig(current_cap=lathes, max_cap=lathes)},
        )
    )
    lathe = DynamicResource(env, "F", "lathe")
    sampler = Sampler(rng=np.random.default_rng(seed))
    waits = []

    def part():
        queued = env.now
        with lathe.request() as request:
            yield request
            waits.append(env.now - queued)  # time spent in the queue
            yield env.timeout(sampler.sample(env.registry.get_config("F.service.machining")))

    def arrivals():
        gap = env.registry.get_config("F.arrival.parts")
        while True:
            yield env.timeout(sampler.sample(gap))
            env.process(part())

    env.process(arrivals())
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return float(np.mean(waits))
# --8<-- [end:measure]


if __name__ == "__main__":
    # --8<-- [start:check]
    utilisation = 0.75
    rate = utilisation / 1.5  # parts per second, for 1.5 s of work each
    exponential = DistributionConfig(dist="exponential", rate=1 / 1.5)
    measured = mean_wait(rate, exponential)
    expected = utilisation / (1 - utilisation) * 1.5  # 4.5 s, for one lathe
    print(f"measured {measured:.2f} s, expected {expected:.2f} s")
    # --8<-- [end:check]
    assert abs(measured - expected) / expected < 0.1
