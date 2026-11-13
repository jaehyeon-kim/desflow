"""GA-4: Orders belong to existing customers.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. A customer is drawn with `Sampler`, from the stream the
blueprint's `lookup` uses.
"""

import itertools

from desflow import DistributionConfig, DynamicRealtimeEnvironment, Sampler

UNTIL = 1000


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    signup_gap = DistributionConfig(dist="exponential", rate=0.1)
    signup_gap.stream = "arrival.signups"
    order_gap = DistributionConfig(dist="exponential", rate=1)
    order_gap.stream = "arrival.orders"
    pick = sampler.generator("tasks.place_order.payload.customer_id")
    customers: list = []  # the customer ids of the sign-ups so far, oldest first

    def signup_source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(signup_gap))
            customer = len(customers)  # a sequence from 0
            customers.append(customer)
            env.publish_event(
                f"signups-{number}",
                {"customer_id": customer},
                task="sign_up",
                instance=number - 1,
            )

    def order_source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(order_gap))
            if not customers:  # no sign-up yet: the blueprint drops this start
                continue
            customer = customers[int(pick.integers(len(customers)))]
            env.publish_event(
                f"orders-{number}",
                {"customer_id": customer},
                task="place_order",
                instance=number - 1,
            )

    env.process(signup_source())
    env.process(order_source())
