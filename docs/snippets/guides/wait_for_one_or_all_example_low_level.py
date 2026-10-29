"""Wait for one or all, low-level API: a shop where orders wait for a restock or a
time limit.

The low-level version of wait_for_one_or_all_example.py. Each order waits for the
next restock or 30 seconds, whichever comes first, and is packed after a restock or
refunded at the limit. A restock waits for both its delivery and a stock count, then
fires the signal the orders wait for. The script runs 10 minutes at factor 0 and
prints how many orders were packed and refunded, and the times of the restocks:
python wait_for_one_or_all_example_low_level.py.
"""

import itertools

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
    SimParameter,
    Signal,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.1)},
)


def outcome() -> dict:
    """Runs 10 minutes and returns the orders packed and refunded, and the times of
    the restocks."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    restocked = Signal(env, "restocked")
    counts = {"pack": 0, "refund": 0}
    restocked_at = []  # the time of each restock

    # Each order waits for the next restock or 30 seconds, whichever comes first
    def order():
        stock = restocked.wait()
        done = yield env.any_of([stock, env.timeout(30)])
        counts["pack" if stock in done else "refund"] += 1

    def count_stock():
        yield env.timeout(20)

    # A restock waits for its delivery, 10 seconds, and a stock count, a child
    # process of 20 seconds; when both are done it fires restocked
    def restock():
        yield env.all_of([env.timeout(10), env.process(count_stock())])
        restocked_at.append(env.now)
        restocked.fire()

    def orders(sampler):
        gap = env.registry.get_config("Shop.arrival.orders")
        while True:
            yield env.timeout(sampler.sample(gap))
            env.process(order())

    def deliveries():
        for _ in itertools.count():
            yield env.timeout(60)
            env.process(restock())

    env.process(orders(Sampler(seed=42)))
    env.process(deliveries())
    env.run(until=600)
    env.teardown()
    return {**counts, "restocked_at": restocked_at}


if __name__ == "__main__":
    print(outcome())
