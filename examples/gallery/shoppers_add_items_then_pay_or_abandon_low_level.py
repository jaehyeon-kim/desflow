"""A shopper keeps adding items to a cart, then pays or abandons it.

The Low-level form of shoppers_add_items_then_pay_or_abandon.yaml, with the same
records, built on `DynamicRealtimeEnvironment`. A shopper adds an item every 5 to 30
seconds, and after each item 60 percent add another, 25 percent pay and 15 percent
leave. Two tills serve the payments, each for about 25 seconds.

Needs: nothing
Run it with: python examples/gallery/shoppers_add_items_then_pay_or_abandon_low_level.py
"""

import itertools
import logging
from datetime import datetime

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Router,
    Sampler,
    SimParameter,
)

SEED = 42
UNTIL = 3600
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"shoppers": DistributionConfig(dist="exponential", rate=0.1)},
    service={"checkout": DistributionConfig(dist="normal", mean=25, std=5)},
    resources={"tills": CapacityConfig(current_cap=2, max_cap=2)},
)


def build(env):
    """Starts the shopper source on `env`."""
    env.registry.register_sim_parameter(PARAMS)
    sampler = Sampler(seed=SEED)
    browse = sampler.generator("tasks.add_item.service_time")
    router = Router(
        {"weights": {"add_item": 60, "pay": 25, "abandon": 15}},
        rng=sampler.generator("route.add_item"),
    )
    tills = DynamicResource(env, "Shop", "tills")
    path_id = "Shop.service.checkout"
    instances = {task: itertools.count() for task in ("add_item", "pay", "abandon")}

    def shopper(key):
        cart = {"items": 0}
        while True:
            instance = next(instances["add_item"])
            env.publish_event(
                key, {"status": "started"}, task="add_item", instance=instance
            )
            yield env.timeout(5 + (30 - 5) * browse.random())
            env.publish_event(
                key,
                {"event_type": "item_added", "items": cart["items"] + 1},
                task="add_item",
                instance=instance,
            )
            cart["items"] += 1  # the blueprint's `set`, applied after the record
            task = router.choose(None, env=env)
            if task != "add_item":
                break
        instance = next(instances[task])
        if task == "pay":
            env.publish_event(
                key,
                {
                    "path_id": path_id,
                    "sim_id": "Shop",
                    "service": "checkout",
                    "status": "queued",
                },
                task="pay",
                instance=instance,
            )
            with tills.request() as request:
                yield request
                env.publish_event(
                    key,
                    {
                        "path_id": path_id,
                        "sim_id": "Shop",
                        "service": "checkout",
                        "status": "started",
                    },
                    task="pay",
                    instance=instance,
                )
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                env.publish_event(
                    key,
                    {"event_type": "paid", "items": cart["items"]},
                    task="pay",
                    instance=instance,
                )
        else:
            env.publish_event(
                key,
                {"event_type": "abandoned", "items": cart["items"]},
                task="abandon",
                instance=instance,
            )

    def shoppers():
        gap = env.registry.get_config("Shop.arrival.shoppers")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(shopper(f"shoppers-{number}"))

    env.process(shoppers())


def run():
    """Runs the model for one simulation hour."""
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
