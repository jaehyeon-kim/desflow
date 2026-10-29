"""The Vary Arrivals by Time guide, low-level API: what a scenario step does at its
boundary.

python time_varying_arrivals_low_level.py prints the arrival times around a step, and
the gap in force after a run whose until is the step's time.
"""

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
    SimParameter,
    run_scenario,
)


# --8<-- [start:boundary]
def arrivals(until: float) -> tuple:
    """Orders every 10 s, every 5 s from 30 s: the arrival times, and the gap in
    force at the end."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="constant", value=10)},
        )
    )
    sampler, times = Sampler(seed=1), []

    def orders():
        gap = env.registry.get_config("Shop.arrival.orders")
        while True:
            yield env.timeout(sampler.sample(gap))
            times.append(env.now)

    steps = [{"at": 30, "path": "Shop.arrival.orders.value", "value": 5}]
    env.process(run_scenario(env, steps, until=until))
    env.process(orders())
    env.run(until=until)
    env.teardown()
    return times, env.registry.get("Shop.arrival.orders.value").value


# --8<-- [end:boundary]

if __name__ == "__main__":
    print("until 50:", arrivals(50))
    print("until 30:", arrivals(30))
