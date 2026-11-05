"""Realistic fields: names, emails and places from Faker, and products from a CSV file.

The low-level version of `realistic_fields.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
field has a Faker of its own, seeded from the random sequence the blueprint's generator
at that place uses (its place with `#fake` added), and the product is one row of the
CSV file read with the `csv` module, chosen by `Sampler.generator(<place>)` with the
popularity column as the weights.

Needs: the fake extra.
Run it with: python docs/snippets/generate-data/realistic_fields_low_level.py
"""

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from faker import Faker

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

CSV = Path(__file__).with_name("realistic_fields_products.csv")
UNTIL = 20
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrival on `env`."""
    sampler = Sampler(seed=42)

    def faker(place, locale="en_US"):
        """A Faker seeded from the random sequence of the generator at `place`."""
        fake = Faker(locale)
        fake.seed_instance(int(sampler.generator(place + "#fake").integers(2**32)))
        return fake

    name = faker("tasks.signup.payload.name")
    email = faker("tasks.signup.payload.email")
    city = faker("tasks.signup.payload.city")
    german_city = faker("tasks.signup.payload.german_city", "de_DE")
    job = faker("tasks.signup.payload.job")
    # A cell of digits is a number, as the `row` generator reads it
    with CSV.open(encoding="utf-8") as file:
        products: list = [
            {
                "sku": row["sku"],
                "title": row["title"],
                "price": float(row["price"]),
                "popularity": int(row["popularity"]),
            }
            for row in csv.DictReader(file)
        ]
    total = sum(product["popularity"] for product in products)
    shares = [product["popularity"] / total for product in products]
    favourite = sampler.generator("tasks.signup.payload.favourite")
    signups_gap = DistributionConfig(dist="exponential", rate=0.5)
    # The random sequence the blueprint's draw uses
    signups_gap.stream = "arrival.signups"

    def signups_source():
        number = 0
        while True:
            yield env.timeout(sampler.sample(signups_gap))
            number += 1
            payload = {
                "customer_id": number,
                "name": name.name(),
                "email": email.email(),
                "city": city.city(),
                "german_city": german_city.city(),
                "job": job.job(),
                "favourite": dict(
                    products[int(favourite.choice(len(products), p=shares))]
                ),
            }
            env.publish_event(
                f"signups-{number}", payload, task="signup", instance=number - 1
            )

    env.process(signups_source())


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
