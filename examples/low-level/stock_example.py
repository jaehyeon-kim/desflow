"""Stock, parcels and express orders, low-level API.

The low-level twin of `builder/stock_example.py`. Packing an order takes one box from
the `stock` container and puts the packed parcel in the `parcels` store, where a
courier collects it. The packers' queue order is `priority`, so express orders
(priority 1) are served before standard ones (priority 2). The stock runs out, so
packing waits until a restock at 40 seconds sets the level to 30.

Needs no containers. Runs unpaced and ends after 60 simulation seconds.
"""

import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
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
        "orders": DistributionConfig(dist="exponential", rate=0.5),
        "express": DistributionConfig(dist="exponential", rate=0.1),
        "couriers": DistributionConfig(dist="constant", value=2),
    },
    service={
        "packing": DistributionConfig(dist="triangular", low=1, mode=2, high=4),
        "collecting": DistributionConfig(dist="uniform", low=1, high=3),
    },
    resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
    stores={"parcels": CapacityConfig(current_cap=10, max_cap=10)},
    # current_cap is the most it holds; level is what it holds at the start
    containers={"stock": ContainerConfig(current_cap=50, max_cap=50, level=15)},
)
PRIORITIES = {"orders": 2, "express": 1}


# --8<-- [start:pack]
def pack(env, sampler, shop, key, priority, task_id):
    """Queued, a packer by priority, a box from stock, packing, then a parcel."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    with shop["packers"].request(priority=priority) as req:
        yield req
        # Waits while the stock is empty
        yield shop["stock"].get(1)
        publish({"path_id": path_id, "status": "started"})
        payload = {"event_type": "order_packed", "priority": priority}
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        # Waits while the store is full
        yield shop["parcels"].put(payload)
        publish(payload)


# --8<-- [end:pack]
def collect(env, sampler, shop, key, task_id):
    """A courier waits for a parcel, then takes 1 to 3 seconds to load it."""
    path_id = "Shop.service.collecting"

    def publish(value):
        env.publish_event(key, value, task="collect", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    yield shop["parcels"].get()
    publish({"path_id": path_id, "status": "started"})
    yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    publish({"event_type": "parcel_collected"})


def order_generator(env, sampler, shop, arrival, pack_ids):
    """Spawns a pack task for every arrival, keyed orders-1, express-1 and so on."""
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        key = f"{arrival}-{number}"
        env.process(pack(env, sampler, shop, key, PRIORITIES[arrival], next(pack_ids)))


def courier_generator(env, sampler, shop):
    """Spawns a collect task for every courier."""
    gap = env.registry.get_config("Shop.arrival.couriers")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(collect(env, sampler, shop, f"couriers-{number}", number - 1))


def restock(env):
    """A restock at 40 seconds: the level path sets what the container holds."""
    yield env.timeout(40)
    env.registry.update("Shop.containers.stock.level", 30)


def build(env):
    """Builds the packers, the store and the container, in the builder's order."""
    return {
        "packers": DynamicResource(env, "Shop", "packers", queue="priority"),
        "parcels": DynamicStore(env, "Shop", "parcels"),
        "stock": DynamicContainer(env, "Shop", "stock"),
    }


def processes(env, sampler, shop):
    """The arrivals and the restock, in the builder's order."""
    pack_ids = itertools.count()
    return [
        order_generator(env, sampler, shop, "orders", pack_ids),
        order_generator(env, sampler, shop, "express", pack_ids),
        courier_generator(env, sampler, shop),
        restock(env),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # The packers', parcels' and stock's readings every 10 seconds
    env.setup_stats("Shop", "minimal", interval=10.0)
    sampler = Sampler(seed=42)
    shop = build(env)
    for process in processes(env, sampler, shop):
        env.process(process)
    try:
        env.run(until=60)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
