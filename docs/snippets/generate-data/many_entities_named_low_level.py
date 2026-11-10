"""Many entities, by name: one entity for each key in a list, low-level API.

The low-level version of `many_entities_named.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
warehouse is one process with its own name. Each draw is made with `Sampler`, from the
random sequence the blueprint's generator at that place uses, so the draws are the
blueprint's, and the three warehouses share that one sequence.

Needs: nothing.
Run it with: python docs/snippets/generate-data/many_entities_named_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 10
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the three warehouses on `env`."""
    # --8<-- [start:arrival]
    sampler = Sampler(seed=42)
    waiting = sampler.generator("tasks.report.payload.orders_waiting")
    warehouses_gap = DistributionConfig(dist="constant", value=2)
    # The random sequence the blueprint's draw uses
    warehouses_gap.stream = "arrival.warehouses"
    sent = {"report": 0}
    # --8<-- [end:arrival]

    # --8<-- [start:payload]
    def warehouse(key):
        while True:
            yield env.timeout(sampler.sample(warehouses_gap))
            payload = {
                "warehouse": key,
                "orders_waiting": int(waiting.integers(0, 40 + 1)),
            }
            env.publish_event(key, payload, task="report", instance=sent["report"])
            sent["report"] += 1

    for key in ["north", "south", "east"]:
        env.process(warehouse(key))
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
