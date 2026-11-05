"""Stock queries read the latest level of each product.

The Low-level form of stock_queries_read_the_latest_level.yaml, with the same records,
built on `DynamicRealtimeEnvironment`. Each draw is made with `Sampler`, from the
random sequence the blueprint's generator at that place uses, so the draws are the
blueprint's. A product's new level is its last level plus the change. A query draws one
of the products, which is what `lookup` with `latest_by` does, and reads its last level.

Needs: nothing
Run it with: python examples/gallery/stock_queries_read_the_latest_level_low_level.py
"""

import itertools
import logging
from datetime import datetime

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

SEED = 42
UNTIL = 200


def build(env):
    """Starts the five product loops and the query source on `env`."""
    sampler = Sampler(seed=SEED)
    change = sampler.generator("tasks.update_stock.payload.level.args.1.args.1")
    pick = sampler.generator("tasks.read_stock.vars.row")
    gap = DistributionConfig(dist="exponential", rate=0.5)
    gap.stream = "arrival.queries"
    instances = itertools.count()
    levels = {}  # the last level of each product

    def product(number):
        sku = f"sku-{number}"
        for _ in itertools.count():
            level = max(0, levels.get(sku, 100) + int(change.integers(-12, 11)))
            levels[sku] = level
            env.publish_event(
                sku,
                {"sku": sku, "level": level},
                task="update_stock",
                instance=next(instances),
            )
            yield env.timeout(5)

    def queries():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            # The products first appear in the order 1 to 5, and each has a latest level
            sku = f"sku-{int(pick.integers(len(levels))) + 1}"
            env.publish_event(
                f"queries-{number}",
                {"sku": sku, "level": levels[sku]},
                task="read_stock",
                instance=number - 1,
            )

    for number in range(1, 6):
        env.process(product(number))
    env.process(queries())


def run():
    """Runs the model for 200 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
