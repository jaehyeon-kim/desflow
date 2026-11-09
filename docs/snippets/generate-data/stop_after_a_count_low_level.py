"""Stop after a count: an arrival that ends after exactly 12 records, low-level API.

The low-level version of `stop_after_a_count.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. The
arrival loop runs 12 times and then ends, so the run ends when the last record is
written.

Needs: nothing.
Run it with: python docs/snippets/generate-data/stop_after_a_count_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = None
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrival on `env`."""
    # --8<-- [start:arrival]
    sampler = Sampler(seed=42)
    orders_gap = DistributionConfig(dist="constant", value=2)
    # The random sequence the blueprint's draw uses
    orders_gap.stream = "arrival.orders"
    # --8<-- [end:arrival]

    # --8<-- [start:payload]
    def orders_source():
        for number in range(1, 13):
            yield env.timeout(sampler.sample(orders_gap))
            env.publish_event(
                f"orders-{number}",
                {"order_id": number},
                task="order",
                instance=number - 1,
            )

    env.process(orders_source())
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
