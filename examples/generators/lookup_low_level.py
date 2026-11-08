"""Lookup: a value from a finished record of another task, low-level API.

The low-level version of `lookup.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. The script keeps the `customer_id` of each
customer it has published, and an order takes one at an index drawn with
`integers(len(rows))`, as `lookup` does with `strategy: random`.

Needs: nothing.
Run it with: python examples/generators/lookup_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""
    sampler = Sampler(seed=42)
    seller = sampler.generator("tasks.order.payload.customer_id")
    customers: list = []  # the customer_id of each customer so far, oldest first

    def customer_arrivals():
        yield env.timeout(0)
        for number in range(1, 4):
            customers.append(100 + number - 1)
            env.publish_event(
                f"customers-{number}",
                {"customer_id": customers[-1]},
                task="customer",
                instance=number - 1,
            )

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            payload = {"customer_id": customers[int(seller.integers(len(customers)))]}
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=number - 1
            )

    env.process(customer_arrivals())
    env.process(orders())


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
