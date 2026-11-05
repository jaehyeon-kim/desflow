"""Auction streams: people, auctions and bids, low-level API.

The low-level version of `auction_streams.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<name>)`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's. A name is drawn with a Faker that is
seeded from that sequence.

Needs: the fake extra.
Run it with: python examples/gallery/auction_streams_low_level.py
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

UNTIL = 600
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
CITIES = ["Phoenix", "Boise", "Cheyenne", "Portland", "Kent"]
STATES = {"AZ": 3, "ID": 2, "WY": 1, "OR": 3, "WA": 3}


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def integer(rng, low, high):
    """A whole number from `low` to `high`, both included, as `integer` draws it."""
    return int(rng.integers(low, high + 1))


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals on `env`."""
    sampler = Sampler(seed=42)
    faker = Faker("en_US")
    faker.seed_instance(
        int(sampler.generator("tasks.person.payload.name#fake").integers(2**32))
    )
    city = sampler.generator("tasks.person.payload.city")
    state = sampler.generator("tasks.person.payload.state")
    seller = sampler.generator("tasks.auction.payload.seller")
    category = sampler.generator("tasks.auction.payload.category")
    reserve = sampler.generator("tasks.auction.payload.reserve")
    auction = sampler.generator("tasks.bid.payload.auction")
    bidder = sampler.generator("tasks.bid.payload.bidder")
    price = sampler.generator("tasks.bid.payload.price")
    people: list = []  # the ids of the people so far, oldest first
    auctions: list = []  # the ids of the auctions so far, oldest first
    counts = {"person": 0, "auction": 0, "bid": 0}

    def iso(offset=0):
        """The logical time now plus `offset` seconds, as a timestamp text."""
        moment = START + timedelta(seconds=env.now + offset)
        return moment.isoformat(timespec="milliseconds")

    def person():
        counts["person"] += 1
        row = {
            "person_id": 999 + counts["person"],
            "name": faker.name(),
            "city": CITIES[int(city.integers(len(CITIES)))],
            "state": weighted(state, STATES),
        }
        people.append(row["person_id"])
        return row

    def new_auction():
        counts["auction"] += 1
        row = {
            "auction_id": counts["auction"],
            "seller": people[int(seller.integers(len(people)))],
            "category": integer(category, 1, 10),
            "reserve": integer(reserve, 100, 1000),
            "expires": iso(3600),
        }
        auctions.append(row["auction_id"])
        return row

    def bid():
        counts["bid"] += 1
        return {
            "auction": auctions[int(auction.integers(len(auctions)))],
            "bidder": people[int(bidder.integers(len(people)))],
            "price": integer(price, 100, 2000),
            "bid_time": iso(),
        }

    def arrival(name, task, make, rate, initial=0):
        """Starts `initial` entities at time 0, then one after each gap."""
        gap = DistributionConfig(dist="exponential", rate=rate)
        gap.stream = f"arrival.{name}"

        def start(number):
            key = f"{name}-{number}"
            payload = make()
            env.publish_event(key, payload, task=task, instance=counts[task] - 1)

        def source():
            for number in itertools.count(initial + 1):
                yield env.timeout(sampler.sample(gap))
                start(number)

        def first(number):
            yield env.timeout(0)
            start(number)

        env.process(source())
        for number in range(1, initial + 1):
            env.process(first(number))

    arrival("people", "person", person, 0.02, initial=10)
    arrival("auctions", "auction", new_auction, 0.06, initial=5)
    arrival("bids", "bid", bid, 0.92)


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
