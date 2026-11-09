"""The Call Python from YAML guide, low-level API.

A visit every 0.1 seconds, each shown the three products with the highest score from the
models in Valkey. The features come from `Sampler`, from the random sequence the
blueprint's generator at that place uses, so the draws are the blueprint's. Needs
call_python_ranking.py beside it. Run it with: python call_python_low_level.py
"""

import itertools
import logging

from call_python_ranking import top_products

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler


# --8<-- [start:shop]
def visits(env, sampler):
    """A visit every 0.1 seconds, shown the products the ranking function picks."""
    features = sampler.generator("tasks.recommend.vars.x.value")
    for number in itertools.count(1):
        yield env.timeout(0.1)
        # The visitor's three features, each between 0 and 1
        x = [round(float(features.uniform(0, 1)), 3) for _ in range(3)]
        # top_products(context, x) gives the three products
        record = {"features": x, "shown": top_products(env, x)}
        env.publish_event(f"visits-{number}", record, "recommend", number - 1)


def shop() -> DynamicRealtimeEnvironment:
    """The shop, with its visits started."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.setup_egress([ConsoleEgress(format="json")])
    env.process(visits(env, Sampler(seed=42)))
    return env


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    env = shop()
    try:
        env.run(until=1)
    finally:
        env.teardown()
# --8<-- [end:shop]
