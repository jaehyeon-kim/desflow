"""Object: a mapping that a modifier can apply to as a whole, low-level API.

The low-level version of `object.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. `null_rate` draws from the place with `#null`
added to its name: the value is null when `random()` is below the rate. The `fields` are
drawn from the places `fields.<name>` below the generator.

Needs: nothing.
Run it with: python examples/generators/object_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the order process on `env`."""
    sampler = Sampler(seed=1)
    missing = sampler.generator("tasks.order.payload.address#null")
    city = sampler.generator("tasks.order.payload.address.fields.city")
    postcode = sampler.generator("tasks.order.payload.address.fields.postcode")
    cities = ["Perth", "Hobart"]

    def make(number):
        """Draws the payload of order `number`."""
        if missing.random() < 0.5:
            return {"address": None}
        return {
            "address": {
                "city": cities[int(city.integers(len(cities)))],
                "postcode": int(postcode.integers(1000, 9999 + 1)),
            }
        }

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            env.publish_event(
                f"orders-{number}", make(number), task="order", instance=number - 1
            )

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
