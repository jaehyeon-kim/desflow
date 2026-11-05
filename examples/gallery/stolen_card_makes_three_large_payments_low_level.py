"""A stolen card makes three large payments seconds after a normal one.

The Low-level form of stolen_card_makes_three_large_payments.yaml, with the same
records, built on `DynamicRealtimeEnvironment`. Payments arrive from twenty cards, and 3
percent of them come from a card that has just been stolen. Two seconds later the same
card makes three large payments.

Needs: nothing
Run it with: python examples/gallery/stolen_card_makes_three_large_payments_low_level.py
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
UNTIL = 600


def build(env):
    """Starts the payment source on `env`."""
    sampler = Sampler(seed=SEED)
    gap = DistributionConfig(dist="exponential", rate=1)
    gap.stream = "arrival.payments"
    pick_card = sampler.generator("arrivals.payments.vars.card")
    amount = sampler.generator("tasks.payment.payload.amount")
    stolen = sampler.generator("tasks.payment.payload.stolen")
    large_amount = sampler.generator("tasks.large_payment.payload.amount")
    instances = {"payment": itertools.count(), "large_payment": itertools.count()}

    def stolen_card(card):
        """Three large payments on the card, two seconds after the normal one."""
        yield env.timeout(2)
        for _ in range(3):
            payload = {
                "card": card,
                "amount": round(500 + (2000 - 500) * large_amount.random(), 2),
                "fraud": True,
            }
            env.publish_event(
                card,
                payload,
                task="large_payment",
                instance=next(instances["large_payment"]),
            )

    def payments():
        for _ in itertools.count():
            yield env.timeout(sampler.sample(gap))
            card = f"card-{int(pick_card.integers(1, 21))}"
            payload = {
                "card": card,
                "amount": round(5 + (120 - 5) * amount.random(), 2),
                "stolen": bool(stolen.random() < 0.03),
            }
            env.publish_event(
                card, payload, task="payment", instance=next(instances["payment"])
            )
            if payload["stolen"]:
                env.process(stolen_card(card))

    env.process(payments())


def run():
    """Runs the model for 600 simulation seconds."""
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
