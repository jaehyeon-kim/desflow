"""Timestamps and gaps: generated times and random gaps between records, low-level API.

The low-level version of `timestamps_and_gaps.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. The gap
and the delivery date are drawn with `Sampler`, from the random sequence the blueprint's
generator at that place uses, so the draws are the blueprint's. The times come from the
environment: the start time plus the simulation time, written as the blueprint's `now`
and `format_time` write them.

Needs: nothing.
Run it with: python docs/snippets/generate-data/timestamps_and_gaps_low_level.py
"""

import itertools
import logging
from datetime import datetime, timedelta, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 10
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrival on `env`."""
    # --8<-- [start:arrival]
    sampler = Sampler(seed=42)
    gap = sampler.generator("arrivals.orders.gap")
    offset = sampler.generator("tasks.order.payload.deliver_by.offset")
    # --8<-- [end:arrival]

    # --8<-- [start:payload]
    def order_time(seconds=0.0):
        """The logical time now plus `seconds`, as the environment counts it."""
        return env.start_datetime + timedelta(seconds=env.now + seconds)

    def orders_source():
        for number in itertools.count(1):
            yield env.timeout(float(gap.normal(0.4, 0.03)))
            now = order_time()
            deliver_by = order_time(float(offset.uniform(0, 172800)))
            payload = {
                "placed_at": now.isoformat(timespec="milliseconds"),
                "deliver_by": deliver_by.isoformat(timespec="milliseconds"),
                "epoch_ms": int((now - EPOCH) // timedelta(milliseconds=1)),
            }
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=number - 1
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
