"""The "Write a File for Each Day" step of the Write to Cloud Storage page, low-level API.

The shop packs an order every hour for two days, and each order goes to the file of its
day. The path is built from the start time of the environment plus the simulation time,
as the blueprint's `format_time` builds it. Needs cloud_storage_stores.py beside it. Run
it with `python cloud_storage_daily_low_level.py`.
"""

# --8<-- [start:shop]
import itertools
import logging
from datetime import datetime, timedelta, timezone

from cloud_storage_stores import bucket, filesystem

from desflow import DynamicRealtimeEnvironment, JsonlStorageEgress

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def orders(env):
    for number in itertools.count():
        yield env.timeout(3600)
        # The file of the order: the day it was packed, in the simulated calendar
        day = env.start_datetime + timedelta(seconds=env.now)
        order = {
            "order_id": number,
            "amount": 19.99,
            "__path__": day.strftime(f"{bucket}/daily/%Y-%m-%d.jsonl"),
        }
        env.publish_event(f"orders-{number + 1}", order, "pack_order", number)


def shop() -> DynamicRealtimeEnvironment:
    """The shop, writing each order to the file of its day."""
    env = DynamicRealtimeEnvironment(
        factor=0.0, logical_start_time=START, timezone="UTC"
    )
    env.setup_egress(
        # The file of a record with no __path__
        [
            JsonlStorageEgress(
                default_path=f"{bucket}/daily/other.jsonl", filesystem=filesystem
            )
        ]
    )
    env.process(orders(env))
    return env


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    env = shop()
    try:
        env.run(until=2 * 24 * 3600)
    finally:
        env.teardown()
# --8<-- [end:shop]
