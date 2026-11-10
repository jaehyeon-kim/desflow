"""Join across sinks: customers to one file, and the orders that name them to another.

The low-level version of `join_across_sinks.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's. The script keeps each customer row in a
list, so that an order's lookup is one draw of a row of that list, and each sink takes
the records of one task through a predicate.

Needs: the parquet extra, which holds the file writers. DEST_PATH sets the folder (data
by default).
Run it with: python docs/snippets/generate-data/join_across_sinks_low_level.py
"""

import itertools
import logging
import os
from datetime import datetime, timezone

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    JsonlStorageEgress,
    Sampler,
)

UNTIL = 60
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}
COUNTRIES = ["AU", "NZ", "SG"]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the customers and the orders on `env`."""
    # --8<-- [start:arrival]
    sampler = Sampler(seed=42)
    country = sampler.generator("tasks.customer.payload.country")
    pick = sampler.generator("tasks.order.payload.customer_id")
    total = sampler.generator("tasks.order.payload.total")
    customer_gap = DistributionConfig(dist="exponential", rate=0.1)
    # The random sequence the blueprint's draw uses
    customer_gap.stream = "arrival.customers"
    order_gap = DistributionConfig(dist="exponential", rate=1)
    order_gap.stream = "arrival.orders"
    customer_rows: list = []  # the customers written so far, for a lookup
    sent = {"customer": 0, "order": 0}
    # --8<-- [end:arrival]

    # --8<-- [start:payload]
    def write_customer(key):
        """Publishes the next customer and keeps its row."""
        row = {
            "customer_id": len(customer_rows) + 1,
            "country": COUNTRIES[int(country.integers(len(COUNTRIES)))],
        }
        customer_rows.append(row)
        env.publish_event(key, row, task="customer", instance=sent["customer"])
        sent["customer"] += 1

    def customers():
        for number in itertools.count(6):  # 1 to 5 exist at the start
            yield env.timeout(sampler.sample(customer_gap))
            write_customer(f"customers-{number}")

    def orders():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(order_gap))
            # The lookup: one row of the customers written so far, chosen at random
            customer = customer_rows[int(pick.integers(len(customer_rows)))]
            payload = {
                "order_id": number,
                "customer_id": customer["customer_id"],
                "total": round(float(total.uniform(5, 200)), 2),
            }
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=sent["order"]
            )
            sent["order"] += 1

    def first(number):
        yield env.timeout(0)
        write_customer(f"customers-{number}")

    env.process(customers())
    env.process(orders())
    for number in range(1, 6):
        env.process(first(number))
    # --8<-- [end:payload]


def run() -> None:
    """Runs the model unpaced, with a file for each kind of record."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    folder = os.environ.get("DEST_PATH", "data")
    # --8<-- [start:egress]
    env.setup_egress(
        [
            JsonlStorageEgress(default_path=f"{folder}/customers.jsonl"),
            JsonlStorageEgress(default_path=f"{folder}/orders.jsonl"),
        ],
        predicates=[
            lambda record: record.get("task") == "customer",
            lambda record: record.get("task") == "order",
        ],
    )
    # --8<-- [end:egress]
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
