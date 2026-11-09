"""The Data at Scale guide, low-level API: a month of orders for 1,000 customers, written
as Parquet files, one for each day.

CUSTOMERS, DAYS, FIRST (the number of the first customer), SEED and OUT change the size,
the customers, the random seed and the folder. Run it with `python data_at_scale_low_level.py`.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    ParquetStorageEgress,
    Sampler,
)

CUSTOMERS = int(os.getenv("CUSTOMERS", "1000"))
DAYS = int(os.getenv("DAYS", "30"))
FIRST = int(os.getenv("FIRST", "1"))
OUT = os.getenv("OUT", "scale_output")


# --8<-- [start:span]
def new_env() -> DynamicRealtimeEnvironment:
    """An environment that runs without waiting and starts on 1 January 2026, UTC."""
    return DynamicRealtimeEnvironment(
        factor=0,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        timezone="UTC",
    )


# --8<-- [end:span]


# --8<-- [start:files]
def setup_files(env: DynamicRealtimeEnvironment) -> None:
    """A Parquet sink with a large batch. One file for each batch, so a large batch
    gives few, large files."""
    env.setup_egress(
        [ParquetStorageEgress(default_path=f"{OUT}/other.parquet")], batch_size=10000
    )


def order_row(env: DynamicRealtimeEnvironment, key: str, number: int, amount: float):
    """An order that names the file of its day."""
    # The file of the order: the day it was placed, in the simulated calendar
    day = env.start_datetime + timedelta(seconds=env.now)
    return {
        # Numbered within the run, so the first customer of the run is part of the id
        "order_id": f"{FIRST}-{number}",
        "customer": key,
        "amount": amount,
        "__path__": day.strftime(f"{OUT}/%Y-%m-%d.parquet"),
    }


# --8<-- [end:files]


# --8<-- [start:volume]
def add_customers(env: DynamicRealtimeEnvironment) -> None:
    """One process for each customer, ordering about once a day (12 to 36 hours apart)."""
    sampler = Sampler(seed=int(os.getenv("SEED", "42")))
    gap = DistributionConfig(dist="uniform", low=43200, high=129600)
    amounts = sampler.generator("tasks.place_order.payload.amount")
    sent = {"orders": 0}

    def customer(key):
        while True:
            yield env.timeout(sampler.sample(gap))
            amount = round(float(amounts.normal(40, 10)), 2)
            order = order_row(env, key, sent["orders"], amount)
            env.publish_event(key, order, task="place_order", instance=sent["orders"])
            sent["orders"] += 1

    for number in range(FIRST, FIRST + CUSTOMERS):
        env.process(customer(f"customer-{number}"))


# --8<-- [end:volume]


# --8<-- [start:run]
def shop() -> DynamicRealtimeEnvironment:
    env = new_env()
    setup_files(env)
    add_customers(env)
    return env


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    env = shop()
    try:
        env.run(until=DAYS * 24 * 3600)
    finally:
        env.teardown()
# --8<-- [end:run]
