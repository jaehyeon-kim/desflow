"""Choices and numbers: weighted choices and drawn numbers, low-level API.

The low-level version of `choices_and_numbers.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's. The weighted choice is written out as
its generator makes it: a point drawn in the total weight of 100 is card when it is
below 60.

Needs: nothing.
Run it with: python docs/snippets/generate-data/choices_and_numbers_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 30
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}
CHANNELS = ["web", "app", "store"]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals on `env`."""
    sampler = Sampler(seed=42)
    method = sampler.generator("tasks.payment.payload.method")
    price = sampler.generator("tasks.purchase.payload.price")
    items = sampler.generator("tasks.purchase.payload.items")
    delivery = sampler.generator("tasks.purchase.payload.delivery_minutes")
    channel = sampler.generator("tasks.purchase.payload.channel")
    gift = sampler.generator("tasks.purchase.payload.gift")
    payments_gap = DistributionConfig(dist="constant", value=1)
    # The random sequence the blueprint's draw uses
    payments_gap.stream = "arrival.payments"
    purchases_gap = DistributionConfig(dist="exponential", rate=1)
    purchases_gap.stream = "arrival.purchases"
    sent = {"payment": 0, "purchase": 0}

    def publish(task, payload):
        """Publishes a finished payload."""
        env.publish_event(
            f"{task}s-{sent[task] + 1}", payload, task=task, instance=sent[task]
        )
        sent[task] += 1

    def payments_source():
        while True:
            yield env.timeout(sampler.sample(payments_gap))
            # Weights 60 and 40: card takes the draws below 60 of 100
            card = method.random() * (60 + 40) < 60
            publish("payment", {"method": "card" if card else "cash"})

    def purchases_source():
        while True:
            yield env.timeout(sampler.sample(purchases_gap))
            publish(
                "purchase",
                {
                    "price": round(float(price.uniform(1, 250)), 2),
                    "items": int(items.integers(1, 5 + 1)),
                    "delivery_minutes": round(float(delivery.normal(120, 15)), 1),
                    "channel": CHANNELS[int(channel.integers(len(CHANNELS)))],
                    "gift": bool(gift.random() < 0.1),
                },
            )

    env.process(payments_source())
    env.process(purchases_source())


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
