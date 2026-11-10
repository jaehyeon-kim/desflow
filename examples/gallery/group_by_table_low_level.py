"""A table for group-by benchmarks, low-level API.

The low-level version of `group_by_table.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<name>)`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's.

Needs: nothing
Run it with: python examples/gallery/group_by_table_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

# The 1,000th row is at 1,000 s, and a run stops before the events due at its end
# time, so it ends 1 s later.
UNTIL = 1001
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def integer(rng, low, high):
    """A whole number from `low` to `high`, both included, as `integer` draws it."""
    return int(rng.integers(low, high + 1))


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals on `env`."""
    sampler = Sampler(seed=42)
    place = "tasks.row.payload"
    id1 = sampler.generator(f"{place}.id1.names.n")
    id2 = sampler.generator(f"{place}.id2.names.n")
    id3 = sampler.generator(f"{place}.id3.names.n")
    id4 = sampler.generator(f"{place}.id4")
    id5 = sampler.generator(f"{place}.id5")
    id6 = sampler.generator(f"{place}.id6")
    v1 = sampler.generator(f"{place}.v1")
    v2 = sampler.generator(f"{place}.v2")
    v3 = sampler.generator(f"{place}.v3")

    def source():
        for number in range(1, 1001):
            yield env.timeout(1)
            row = {
                "id1": f"id{integer(id1, 1, 100):03d}",
                "id2": f"id{integer(id2, 1, 100):03d}",
                "id3": f"id{integer(id3, 1, 100000):010d}",
                "id4": integer(id4, 1, 100),
                "id5": integer(id5, 1, 100),
                "id6": integer(id6, 1, 100000),
                "v1": integer(v1, 1, 5),
                "v2": integer(v2, 1, 15),
                "v3": round(100 * v3.random(), 6),
            }
            env.publish_event(f"rows-{number}", row, task="row", instance=number - 1)

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
