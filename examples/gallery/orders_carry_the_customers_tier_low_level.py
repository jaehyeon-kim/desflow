"""Each order carries the tier of the customer who placed it.

The Low-level form of orders_carry_the_customers_tier.yaml, with the same records, built
on `DynamicRealtimeEnvironment`. Customers sign up with a tier, and each order reads one
sign-up row, so its customer id and tier agree. An order placed before any customer has
signed up is dropped.

Needs: nothing
Run it with: python examples/gallery/orders_carry_the_customers_tier_low_level.py
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
UNTIL = 300
TIERS = {"basic": 70, "plus": 25, "pro": 5}


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def build(env):
    """Starts the sign-up and order sources on `env`."""
    sampler = Sampler(seed=SEED)
    tier = sampler.generator("tasks.sign_up.payload.tier")
    pick = sampler.generator("tasks.place_order.vars.customer")
    amount = sampler.generator("tasks.place_order.payload.amount")
    signup_gap = DistributionConfig(dist="exponential", rate=0.1)
    signup_gap.stream = "arrival.signups"
    order_gap = DistributionConfig(dist="exponential", rate=1)
    order_gap.stream = "arrival.orders"
    customers = []  # the sign-ups so far, oldest first
    order_ids = itertools.count()  # the `sequence` of the published orders

    def signups():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(signup_gap))
            customer = {"customer_id": number - 1, "tier": weighted(tier, TIERS)}
            customers.append(customer)
            env.publish_event(
                f"signups-{number}", customer, task="sign_up", instance=number - 1
            )

    def orders():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(order_gap))
            # The blueprint drops an order that arrives before the first sign-up, and
            # the dropped order keeps its number
            if not customers:
                continue
            customer = customers[int(pick.integers(len(customers)))]
            payload = {
                "order_id": next(order_ids),
                "customer_id": customer["customer_id"],
                "tier": customer["tier"],
                "amount": round(5 + (200 - 5) * amount.random(), 2),
            }
            env.publish_event(
                f"orders-{number}", payload, task="place_order", instance=number - 1
            )

    env.process(signups())
    env.process(orders())


def run():
    """Runs the model for 300 simulation seconds."""
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
