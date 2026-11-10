"""Skewed keys: 5 of 25 customers place 75 percent of the orders, low-level API.

The low-level version of `skewed_keys.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's. A draw that is true 75 percent of the time
picks the busy customers, and the customer is the record key.

Needs: nothing.
Run it with: python docs/snippets/generate-data/skewed_keys_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 1000
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrival on `env`."""
    # --8<-- [start:arrival]
    sampler = Sampler(seed=42)
    busy = sampler.generator("tasks.order.vars.busy")
    busy_customer = sampler.generator("tasks.order.payload.customer_id.then")
    quiet_customer = sampler.generator("tasks.order.payload.customer_id.else")
    orders_gap = DistributionConfig(dist="constant", value=1)
    # The random sequence the blueprint's draw uses
    orders_gap.stream = "arrival.orders"
    # --8<-- [end:arrival]

    # --8<-- [start:payload]
    def orders_source():
        number = 0
        while True:
            yield env.timeout(sampler.sample(orders_gap))
            number += 1
            if busy.random() < 0.75:
                customer_id = int(busy_customer.integers(1, 5 + 1))
            else:
                customer_id = int(quiet_customer.integers(6, 25 + 1))
            env.publish_event(
                str(customer_id),  # the record key, as the task's key_field gives it
                {"order_id": number, "customer_id": customer_id},
                task="order",
                instance=number - 1,
            )

    env.process(orders_source())
    # --8<-- [end:payload]


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
