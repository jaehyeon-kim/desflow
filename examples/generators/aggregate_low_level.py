"""Aggregate: a mean, sum, minimum, maximum or count over the finished records of another
task, low-level API.

The low-level version of `aggregate.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler.generator(<place>)`, the random sequence of the blueprint's
generator at that place, so the draws are the blueprint's. The script keeps the level of
each reading it has published, and an order takes the mean of the last 3, as `aggregate`
does with `function: mean`.

Needs: nothing.
Run it with: python examples/generators/aggregate_low_level.py
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
    level = sampler.generator("tasks.reading.payload.level")
    levels: list = []  # the level of each reading so far, oldest first

    def reading_arrivals():
        yield env.timeout(0)
        for number in range(1, 4):
            levels.append(int(level.integers(10, 20 + 1)))
            env.publish_event(
                f"readings-{number}",
                {"level": levels[-1]},
                task="reading",
                instance=number - 1,
            )

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            last = levels[-3:]
            env.publish_event(
                f"orders-{number}",
                {"average": sum(last) / len(last)},
                task="order",
                instance=number - 1,
            )

    env.process(reading_arrivals())
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
