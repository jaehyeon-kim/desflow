"""Look up other records: an order names a customer, a payment reads its order, and a
report summarises the recent orders, low-level API.

The low-level version of `look_up_other_records.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's. The script keeps the customers and the
orders in lists. The lookup is one draw of a row of the customers, the upstream is the
order the payment follows, and the aggregate is the mean and the largest total of the
last 10 orders.

Needs: nothing.
Run it with: python docs/snippets/generate-data/look_up_other_records_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 60
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}
COUNTRIES = ["AU", "NZ", "SG"]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the customers, the orders and the reports on `env`."""
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
    report_gap = DistributionConfig(dist="constant", value=10)
    report_gap.stream = "arrival.reports"
    customer_rows: list = []  # the customers written so far, for the lookup
    order_rows: list = []  # the orders written so far, for the aggregate
    sent = {"customer": 0, "report": 0}
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
            key, instance = f"orders-{number}", number - 1
            # The lookup: one row of the customers written so far, chosen at random
            customer = customer_rows[int(pick.integers(len(customer_rows)))]
            order = {
                "order_id": number,
                "customer_id": customer["customer_id"],
                "total": round(float(total.uniform(5, 200)), 2),
            }
            order_rows.append(order)
            env.publish_event(key, order, task="order", instance=instance)
            # The upstream: the payment reads the order it follows
            payment = {"order_id": order["order_id"], "amount": order["total"]}
            env.publish_event(key, payment, task="payment", instance=instance)

    def reports():
        while True:
            yield env.timeout(sampler.sample(report_gap))
            # The aggregate: the last 10 orders, newest first, as the blueprint does
            recent = [row["total"] for row in reversed(order_rows[-10:])]
            report = {
                "mean_total": sum(recent) / len(recent),
                "largest_total": max(recent),
            }
            env.publish_event(
                f"reports-{sent['report'] + 1}",
                report,
                task="report",
                instance=sent["report"],
            )
            sent["report"] += 1

    def first(number):
        yield env.timeout(0)
        write_customer(f"customers-{number}")

    env.process(customers())
    env.process(orders())
    env.process(reports())
    for number in range(1, 6):
        env.process(first(number))
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
