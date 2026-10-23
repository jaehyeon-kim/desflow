"""The Filtered Inventory tutorial: each order waits for a shirt of its own size.

Shirts of three sizes arrive on one shelf, and orders of three sizes take them.
Every record is printed to the terminal. The script then prints, for each size, how
many orders were filled and how many were cancelled. To see only those lines, run:
python filtered_inventory_low_level.py 2>/dev/null
"""

import itertools
import logging
from collections import Counter

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicStore,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# A shirt every 10 seconds on average: 3 in 10 small, 4 medium, 3 large
SHIRTS = {"S": 0.03, "M": 0.04, "L": 0.03}
# An order every 10 seconds on average: a quarter small, half medium, a quarter large
ORDERS = {"S": 0.025, "M": 0.05, "L": 0.025}
PATIENCE = 600  # a customer cancels an order not filled within 10 minutes

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        **{
            f"shirts_{s.lower()}": DistributionConfig(dist="exponential", rate=r)
            for s, r in SHIRTS.items()
        },
        **{
            f"orders_{s.lower()}": DistributionConfig(dist="exponential", rate=r)
            for s, r in ORDERS.items()
        },
    },
    # --8<-- [start:shelf]
    # Shirts of every size on one shelf, each item a mapping such as {"size": "M"}
    stores={"shelf": CapacityConfig(current_cap=1000, max_cap=1000)},
    # --8<-- [end:shelf]
)


def restock(env, shelf, key, size, task_id):
    """The shirt goes on the shelf, as a mapping with its size."""
    yield shelf.put({"size": size})
    env.publish_event(key, {"size": size}, task="restock", instance=task_id)


# --8<-- [start:take]
def take_shirt(env, shelf, key, size, task_id):
    def publish(value):
        env.publish_event(key, value, task="take_shirt", instance=task_id)

    publish({"status": "queued"})
    # Takes the oldest shirt of the order's own size; waits while there is none
    get = shelf.get(filter=lambda shirt: shirt["size"] == size)
    # A customer cancels an order not filled within 10 minutes
    yield get | env.timeout(PATIENCE)
    if not get.triggered:
        get.cancel()  # out of the queue, so it never takes a shirt later
        publish({"status": "reneged"})
        return
    publish({"status": "started"})
    publish({"event_type": "order_filled", "shirt": get.value})


# --8<-- [end:take]


def generator(env, sampler, shelf, arrival, size, task, ids):
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(task(env, shelf, f"{arrival}-{number}", size, next(ids)))


class _Outcomes:
    """An egress that counts each order's outcome, by size, from its events."""

    def __init__(self):
        self.counts = Counter()

    async def run(self, egress_queue):
        import asyncio
        import queue

        try:
            while True:
                try:
                    for record in egress_queue.get_nowait():
                        count(self.counts, record)
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


def count(counts: Counter, record: dict) -> None:
    """Counts a filled or cancelled order, by the size in its key."""
    if record["stream_type"] != "event" or record["task"] != "take_shirt":
        return
    size = record["key"].split("-")[0][-1].upper()
    value = record["value"]
    if value.get("event_type") == "order_filled":
        counts[(size, "filled")] += 1
    elif value.get("status") == "reneged":
        counts[(size, "cancelled")] += 1


def run(until=8 * 3600, factor=0.0, sinks=()):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress(), *sinks])
    shelf = DynamicStore(env, "Shop", "shelf")
    sampler = Sampler(seed=42)
    restocks, takes = itertools.count(), itertools.count()
    for size in SHIRTS:
        arrival = f"shirts_{size.lower()}"
        env.process(generator(env, sampler, shelf, arrival, size, restock, restocks))
    for size in ORDERS:
        arrival = f"orders_{size.lower()}"
        env.process(generator(env, sampler, shelf, arrival, size, take_shirt, takes))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def outcomes() -> Counter:
    """Runs eight hours and counts filled and cancelled orders by size."""
    outcome = _Outcomes()
    run(sinks=[outcome])
    return outcome.counts


if __name__ == "__main__":
    counts = outcomes()
    for size in ORDERS:
        print(
            f"size {size}: {counts[(size, 'filled')]:4} filled, "
            f"{counts[(size, 'cancelled')]:4} cancelled"
        )
