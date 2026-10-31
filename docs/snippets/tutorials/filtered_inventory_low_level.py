"""The Filtered Inventory tutorial: each order waits for a shirt of its own size.

Shirts of three sizes arrive on one shelf, and orders of three sizes take them.
Every record is printed to the terminal. The script then prints, for each size, how
many orders started, were filled, were still waiting and were cancelled, and the shirts left
on the shelf. To see only those lines, run:
python filtered_inventory_low_level.py 2>/dev/null
"""

import itertools
import logging

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
    # --8<-- [start:arrivals]
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
    # --8<-- [end:arrivals]
    # --8<-- [start:shelf]
    # Shirts of every size on one shelf, each item a mapping such as {"size": "M"}
    stores={"shelf": CapacityConfig(current_cap=1000, max_cap=1000)},
    # --8<-- [end:shelf]
)


# --8<-- [start:restock]
def restock(env, shelf, key, size, task_id):
    """The shirt goes on the shelf, as a mapping with its size."""
    yield shelf.put({"size": size})
    env.publish_event(key, {"size": size}, task="restock", instance=task_id)


# --8<-- [end:restock]


# --8<-- [start:take]
def take_shirt(env, shelf, key, size, task_id):
    arrival = key.rsplit("-", 1)[0]

    def publish(value):
        env.publish_event(key, value, task="take_shirt", instance=task_id)

    publish({"status": "queued"})
    env.stats.flow_started(arrival)
    env.stats.task_queued("take_shirt", task_id)
    # Takes the oldest shirt of the order's own size; waits while there is none
    get = shelf.get(filter=lambda shirt: shirt["size"] == size)
    # A customer cancels an order not filled within 10 minutes
    yield get | env.timeout(PATIENCE)
    if not get.triggered:
        get.cancel()  # out of the queue, so it never takes a shirt later
        publish({"status": "reneged"})
        env.stats.task_reneged("take_shirt", task_id)
        env.stats.flow_ended(arrival)
        return
    publish({"status": "started"})
    env.stats.task_started("take_shirt", task_id)
    publish({"event_type": "order_filled", "shirt": get.value})
    env.stats.task_finished("take_shirt", task_id)
    env.stats.flow_finished(arrival)


# --8<-- [end:take]


def generator(env, sampler, shelf, arrival, size, task, ids):
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(task(env, shelf, f"{arrival}-{number}", size, next(ids)))


def run(until=8 * 3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # --8<-- [start:stats]
    # The counts of each arrival's orders, published once, at the end of the 8 hours
    env.setup_stats("Shop", "basic", interval=8 * 3600)
    # --8<-- [end:stats]
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


def outcomes() -> dict:
    """Runs eight hours; returns each size's order counts and the shirts left."""
    readings = run().stats.readings()
    result: dict = {}
    for size in ORDERS:
        flow = f"Shop.stats.flows.orders_{size.lower()}"
        started = readings[f"{flow}.started"]
        filled = readings[f"{flow}.finished"]
        waiting = readings[f"{flow}.in_system"]
        # A cancelled order leaves its flow without finishing it
        result[size] = (started, filled, waiting, started - filled - waiting)
    result["left"] = readings["Shop.stats.stores.shelf.level"]
    return result


if __name__ == "__main__":
    result = outcomes()
    for size in ORDERS:
        started, filled, waiting, cancelled = result[size]
        print(
            f"size {size}: {started:4} started, {filled:4} filled, "
            f"{waiting:2} waiting, {cancelled:3} cancelled"
        )
    print(f"shirts left on the shelf: {result['left']}")
