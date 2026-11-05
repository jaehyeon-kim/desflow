"""Build on the last value: prices that drift from the previous price, low-level API.

The low-level version of `build_on_the_last_value.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. A
low-level model keeps each product's last price and stock itself, and draws each step
from the random sequence the blueprint's generator uses, with `Sampler`.

Needs: nothing.
Run it with: python docs/snippets/generate-data/build_on_the_last_value_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 30
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the five products on `env`."""
    sampler = Sampler(seed=42)
    # The places of the two draws in the blueprint name their random sequences
    step = sampler.generator("tasks.price.payload.price.args.1")
    drift = sampler.generator("tasks.price.payload.stock.args.1.args.1.args.1")
    last: dict = {}  # product key -> its last price and stock, 20 and 250 at first
    counts = {"price": 0}

    def product(number):
        key = f"product-{number}"
        while True:
            yield env.timeout(1)
            price, stock = last.get(key, (20, 250))
            price = round(price + step.uniform(-1, 1), 2)
            stock = int(round(max(0, min(500, stock + float(drift.normal(0, 40))))))
            last[key] = (price, stock)
            env.publish_event(
                key,
                {"product": key, "price": price, "stock": stock},
                task="price",
                instance=counts["price"],
            )
            counts["price"] += 1

    for number in range(5):
        env.process(product(number))


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
