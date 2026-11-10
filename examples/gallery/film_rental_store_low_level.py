"""A film rental store: films, customers, rentals, payments and returns, low-level API.

The low-level version of `film_rental_store.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<name>)`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's. A name is drawn with a Faker that is
seeded from that sequence.

Needs: the fake extra.
Run it with: python examples/gallery/film_rental_store_low_level.py
"""

import itertools
import logging
from datetime import datetime, timedelta, timezone

from faker import Faker

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 7 * 86400
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
ADJECTIVES = ["Silent", "Broken", "Golden", "Hidden", "Last"]
NOUNS = ["River", "Garden", "Highway", "Letter", "Harbour"]
RATINGS = {"G": 2, "PG": 3, "PG-13": 3, "R": 3, "NC-17": 1}
RATES = [0.99, 2.99, 4.99]


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def one_of(rng, choices):
    """One of `choices`, each equally likely, as the `one_of` generator does."""
    return choices[int(rng.integers(len(choices)))]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals on `env`."""
    sampler = Sampler(seed=42)

    def faker_of(place):
        faker = Faker("en_US")
        faker.seed_instance(int(sampler.generator(f"{place}#fake").integers(2**32)))
        return faker

    first_names = faker_of("tasks.customer.payload.first_name")
    last_names = faker_of("tasks.customer.payload.last_name")
    adjective = sampler.generator("tasks.film.payload.title.names.adjective")
    noun = sampler.generator("tasks.film.payload.title.names.noun")
    rating = sampler.generator("tasks.film.payload.rating")
    rate = sampler.generator("tasks.film.payload.rental_rate")
    length = sampler.generator("tasks.film.payload.length")
    active = sampler.generator("tasks.customer.payload.active")
    film_row = sampler.generator("tasks.rental.vars.film")
    customer_row = sampler.generator("tasks.rental.payload.customer_id")
    stay = sampler.generator("tasks.return.wait.timeout")
    films: list = []  # the films so far, oldest first
    customers: list = []  # the ids of the customers so far, oldest first
    counts = {"film": 0, "customer": 0, "rental": 0, "payment": 0, "return": 0}

    def iso():
        """The logical time now, as a timestamp text."""
        moment = START + timedelta(seconds=env.now)
        return moment.isoformat(timespec="milliseconds")

    def publish(key, task, payload, instance=None):
        """Publishes a finished payload, as the next instance of its task unless the
        task has queued one."""
        if instance is None:
            instance = counts[task]
            counts[task] += 1
        env.publish_event(key, payload, task=task, instance=instance)

    def film():
        row = {
            "film_id": len(films) + 1,
            "title": f"{one_of(adjective, ADJECTIVES)} {one_of(noun, NOUNS)}",
            "rating": weighted(rating, RATINGS),
            "rental_rate": one_of(rate, RATES),
            "length": int(length.integers(60, 181)),
        }
        films.append(row)
        return row

    def customer():
        row = {
            "customer_id": len(customers) + 1,
            "first_name": first_names.first_name(),
            "last_name": last_names.last_name(),
            "active": bool(active.random() < 0.9),
        }
        customers.append(row["customer_id"])
        return row

    def rental(number):
        """A rental, its payment at once, and its return after the stay."""
        key = f"rentals-{number}"
        chosen = films[int(film_row.integers(len(films)))]
        rented = {
            "rental_id": number,
            "rental_date": iso(),
            "film_id": chosen["film_id"],
            "rental_rate": chosen["rental_rate"],
            "customer_id": customers[int(customer_row.integers(len(customers)))],
        }
        publish(key, "rental", rented)
        paid = {
            "rental_id": rented["rental_id"],
            "customer_id": rented["customer_id"],
            "amount": rented["rental_rate"],
            "payment_date": iso(),
        }
        publish(key, "payment", paid)
        instance = counts["return"]  # the return waits, so it is queued first
        counts["return"] += 1
        env.publish_event(key, {"status": "queued"}, task="return", instance=instance)
        yield env.timeout(86400 + (604800 - 86400) * stay.random())  # 1 to 7 days
        env.publish_event(key, {"status": "started"}, task="return", instance=instance)
        returned = {"rental_id": rented["rental_id"], "return_date": iso()}
        publish(key, "return", returned, instance)

    gap = DistributionConfig(dist="exponential", rate=0.002)
    gap.stream = "arrival.rentals"

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(rental(number))

    def start(name, task, make, number):
        yield env.timeout(0)
        publish(f"{name}-{number}", task, make())

    env.process(source())
    for number in range(1, 21):
        env.process(start("films", "film", film, number))
    for number in range(1, 16):
        env.process(start("customers", "customer", customer, number))


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
