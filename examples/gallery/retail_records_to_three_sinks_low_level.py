"""Retail records go to PostgreSQL, Kafka and Parquet, low-level API.

The low-level version of `retail_records_to_three_sinks.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler.generator(<name>)`, from the random sequence the blueprint's
generator at that place uses, so the draws are the blueprint's. A name is drawn with a
Faker that is seeded from that sequence.

Needs: a broker and a database, with odctl up kafka-lite and odctl up postgres, and the
kafka, postgres, parquet and fake extras. KAFKA_BOOTSTRAP_SERVERS sets the broker
address, and DEST_PATH the folder of the Parquet file (data by default).
Run it with: python examples/gallery/retail_records_to_three_sinks_low_level.py
"""

import itertools
import logging
import os
from datetime import datetime, timezone

from faker import Faker

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    KafkaEgress,
    ParquetStorageEgress,
    PostgresEgress,
    Sampler,
)

UNTIL = 600
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"
COUNTRIES = {"AU": 5, "US": 3, "GB": 2}
TITLES = ["Desk lamp", "Notebook", "Water bottle", "Backpack", "Headphones", "Keyboard"]


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals on `env`."""
    sampler = Sampler(seed=42)
    faker = Faker("en_US")
    faker.seed_instance(
        int(sampler.generator("tasks.customer.payload.name#fake").integers(2**32))
    )
    country = sampler.generator("tasks.customer.payload.country")
    title = sampler.generator("tasks.product.payload.title")
    price = sampler.generator("tasks.product.payload.price")
    product_row = sampler.generator("tasks.order.vars.product")
    quantity = sampler.generator("tasks.order.vars.quantity")
    customer_row = sampler.generator("tasks.order.payload.customer_id")
    customers: list = []  # the ids of the customers so far, oldest first
    products: list = []  # the products so far, oldest first
    counts = {"customer": 0, "product": 0, "order": 0, "invoice": 0}

    def publish(key, task, payload):
        """Publishes a finished payload as the next instance of its task."""
        env.publish_event(key, payload, task=task, instance=counts[task])
        counts[task] += 1

    def customer():
        row = {
            "customer_id": len(customers) + 1,
            "name": faker.name(),
            "country": weighted(country, COUNTRIES),
        }
        customers.append(row["customer_id"])
        return row

    def product():
        row = {
            "product_id": len(products) + 1,
            "title": TITLES[int(title.integers(len(TITLES)))],
            "price": round(2 + (200 - 2) * price.random(), 2),
        }
        products.append(row)
        return row

    def order(number):
        """An order, and the invoice for it at once."""
        key = f"orders-{number}"
        chosen = products[int(product_row.integers(len(products)))]
        count = int(quantity.integers(1, 6))
        taken = {
            "order_id": number,
            "customer_id": customers[int(customer_row.integers(len(customers)))],
            "product_id": chosen["product_id"],
            "quantity": count,
            "total": round(chosen["price"] * count, 2),
        }
        publish(key, "order", taken)
        publish(key, "invoice", {"order_id": number, "amount": taken["total"]})

    gap = DistributionConfig(dist="exponential", rate=0.5)
    gap.stream = "arrival.orders"

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            order(number)

    def start(name, task, make, number):
        yield env.timeout(0)
        publish(f"{name}-{number}", task, make())

    env.process(source())
    for number in range(1, 21):
        env.process(start("customers", "customer", customer, number))
    for number in range(1, 11):
        env.process(start("products", "product", product, number))


def run() -> None:
    """Runs the model unpaced, with a sink for each kind of record."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    sinks = [
        (
            PostgresEgress(
                DSN,
                tables={
                    "customers": {
                        "columns": {
                            "customer_id": "INT",
                            "name": "TEXT",
                            "country": "TEXT",
                        },
                        "primary_key": "customer_id",
                    }
                },
            ),
            "customer",
        ),
        (
            PostgresEgress(
                DSN,
                tables={
                    "products": {
                        "columns": {
                            "product_id": "INT",
                            "title": "TEXT",
                            "price": "REAL",
                        },
                        "primary_key": "product_id",
                    }
                },
            ),
            "product",
        ),
        (
            KafkaEgress(bootstrap_servers=bootstrap, event_topic="retail-orders"),
            "order",
        ),
        (
            ParquetStorageEgress(
                default_path=f"{os.environ.get('DEST_PATH', 'data')}/invoices.parquet"
            ),
            "invoice",
        ),
    ]
    env.setup_egress(
        [sink for sink, _ in sinks],
        predicates=[
            lambda record, task=task: record.get("task") == task for _, task in sinks
        ],
    )
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
