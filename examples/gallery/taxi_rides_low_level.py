"""Taxi rides with a pickup, a drop-off, a distance and a fare, low-level API.

The low-level version of `taxi_rides.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<name>)`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's.

Needs: nothing
Run it with: python examples/gallery/taxi_rides_low_level.py
"""

import itertools
import logging
from datetime import datetime, timedelta, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 600
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
PASSENGERS = {1: 70, 2: 15, 3: 5, 4: 5, 5: 5}
PAYMENT = {"card": 65, "cash": 33, "dispute": 2}


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def uniform(rng, low, high, decimals):
    """A number in [low, high), as the `uniform` generator draws it."""
    return round(low + (high - low) * rng.random(), decimals)


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals on `env`."""
    sampler = Sampler(seed=42)
    place = "tasks.ride"
    passengers = sampler.generator(f"{place}.payload.passengers")
    latitude = sampler.generator(f"{place}.payload.pickup_latitude")
    longitude = sampler.generator(f"{place}.payload.pickup_longitude")
    payment = sampler.generator(f"{place}.payload.payment")
    distance = sampler.generator(f"{place}.vars.distance")
    minutes = sampler.generator(f"{place}.vars.minutes")
    gap = DistributionConfig(dist="exponential", rate=0.1)
    gap.stream = "arrival.rides"

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            km = uniform(distance, 0.5, 20, 2)
            length = uniform(minutes, 3, 40, 1)
            pickup = START + timedelta(seconds=env.now)
            dropoff = datetime.fromisoformat(
                pickup.isoformat(timespec="milliseconds")
            ) + timedelta(seconds=length * 60)
            ride = {
                "ride_id": number,
                "pickup_time": pickup.isoformat(timespec="milliseconds"),
                "dropoff_time": dropoff.isoformat(timespec="milliseconds"),
                "passengers": weighted(passengers, PASSENGERS),
                "pickup_latitude": uniform(latitude, 40.7, 40.8, 5),
                "pickup_longitude": uniform(longitude, -74.02, -73.93, 5),
                "distance_km": km,
                "fare": round(3 + 2.2 * km + 0.4 * length, 2),
                "payment": weighted(payment, PAYMENT),
            }
            env.publish_event(f"rides-{number}", ride, task="ride", instance=number - 1)

    env.process(source())


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
