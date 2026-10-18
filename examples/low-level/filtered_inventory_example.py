"""Filtered inventory: couriers take only parcels for their own area, low-level API.

The low-level twin of `builder/filtered_inventory_example.py`. Packed orders go on
one shelf as parcels, each with the area it is going to. A courier for the north or
the south takes the oldest parcel for its own area, with a filter, and leaves
without one if none comes within 20 seconds.

Needs no containers. Runs unpaced and ends after 120 simulation seconds.
"""

import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    DynamicStore,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        "north_orders": DistributionConfig(dist="exponential", rate=0.3),
        "south_orders": DistributionConfig(dist="exponential", rate=0.1),
        "north_couriers": DistributionConfig(dist="constant", value=4),
        "south_couriers": DistributionConfig(dist="constant", value=6),
    },
    service={
        "packing": DistributionConfig(dist="triangular", low=1, mode=2, high=4),
    },
    resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
    # Parcels for every area on one shelf, each a mapping with its area
    stores={"parcels": CapacityConfig(current_cap=20, max_cap=20)},
)
AREAS = {
    "north_orders": "north",
    "south_orders": "south",
    "north_couriers": "north",
    "south_couriers": "south",
}


def pack(env, sampler, shop, key, area, task_id):
    """Packs one order and puts the parcel on the shelf with its area."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    with shop["packers"].request() as req:
        yield req
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack", task_id)
        payload = {"event_type": "order_packed", "area": area}
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        yield shop["parcels"].put(payload)
        publish(payload)
    env.stats.task_finished("pack", task_id)
    env.stats.flow_finished(key.rsplit("-", 1)[0])


# --8<-- [start:collect]
def collect(env, sampler, shop, key, area, task_id):
    """Takes the oldest parcel for its own area, or leaves after 20 seconds."""

    def publish(value):
        env.publish_event(key, value, task="collect", instance=task_id)

    publish({"status": "queued"})
    env.stats.task_queued("collect", task_id)
    get = shop["parcels"].get(filter=lambda parcel: parcel["area"] == area)
    yield get | env.timeout(20)
    if not get.triggered:
        get.cancel()  # out of the queue, so it takes no parcel later
        publish({"status": "reneged"})
        env.stats.task_reneged("collect", task_id)
        return
    publish({"status": "started"})
    env.stats.task_started("collect", task_id)
    publish({"event_type": "parcel_collected", "parcel": get.value})
    env.stats.task_finished("collect", task_id)
    env.stats.flow_finished(key.rsplit("-", 1)[0])


# --8<-- [end:collect]
def generator(env, sampler, shop, arrival, task, ids):
    """Spawns `task` for every arrival, keyed north_orders-1 and so on."""
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        key = f"{arrival}-{number}"
        env.process(task(env, sampler, shop, key, AREAS[arrival], next(ids)))


def build(env):
    """Builds the packers and the shelf, in the builder's order."""
    return {
        "packers": DynamicResource(env, "Shop", "packers"),
        "parcels": DynamicStore(env, "Shop", "parcels"),
    }


def processes(env, sampler, shop):
    """The four arrivals, in the builder's order."""
    packs, collects = itertools.count(), itertools.count()
    return [
        generator(env, sampler, shop, "north_orders", pack, packs),
        generator(env, sampler, shop, "south_orders", pack, packs),
        generator(env, sampler, shop, "north_couriers", collect, collects),
        generator(env, sampler, shop, "south_couriers", collect, collects),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Every 30 seconds, the shelf's readings and how many couriers left empty-handed
    env.setup_stats("Shop", "basic", interval=30.0)
    sampler = Sampler(seed=42)
    shop = build(env)
    for process in processes(env, sampler, shop):
        env.process(process)
    try:
        env.run(until=120)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
