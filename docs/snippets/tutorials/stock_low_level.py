"""The Stock tutorial: packing takes a box from stock, and deliveries restock it.

One packer, an order every 2 seconds on average, and a delivery of boxes every 100
seconds. Every record is printed to the terminal, and the script prints the mean wait
for 60-box and 40-box deliveries at the end: python stock_low_level.py 2>/dev/null
shows only those lines.
"""

import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        # One order every 2 seconds on average: 50 boxes every 100 seconds
        "orders": DistributionConfig(dist="exponential", rate=0.5),
        # A delivery every 100 seconds
        "deliveries": DistributionConfig(dist="constant", value=100),
    },
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
    # --8<-- [start:container]
    # Boxes on the shelf: room for 200, and 50 when the run starts
    containers={"stock": ContainerConfig(current_cap=200, max_cap=200, level=50)},
    # --8<-- [end:container]
)


def pack_order(env, sampler, shop, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)
    # --8<-- [start:take]
    with shop["packers"].request() as request:
        yield request
        # One box per order, taken once the order has a packer; waits while stock is 0
        yield shop["stock"].get(1)
        # --8<-- [end:take]
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
    env.stats.task_finished("pack_order", task_id)


def order_generator(env, sampler, shop):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, shop, f"orders-{task_id + 1}", task_id))
        task_id += 1


# --8<-- [start:restock]
def restock(env, shop, delivery, key, task_id):
    """Adds the delivery to the shelf; waits while there is no room for it all."""
    yield shop["stock"].put(delivery)
    value = {"event_type": "stock_delivered"}
    env.publish_event(key, value, task="restock", instance=task_id)


def delivery_generator(env, sampler, shop, delivery):
    arrival = env.registry.get_config("Shop.arrival.deliveries")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        key = f"deliveries-{task_id + 1}"
        env.process(restock(env, shop, delivery, key, task_id))
        task_id += 1


# --8<-- [end:restock]


def run(delivery=60, until=3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # --8<-- [start:watch]
    # The stock's level and waiting tasks every 10 seconds, and the mean wait
    env.setup_stats("Shop", "typical", interval=10)
    # --8<-- [end:watch]
    shop = {
        "packers": DynamicResource(env, "Shop", "packers"),
        # Starts at the level its ContainerConfig gives, 50
        "stock": DynamicContainer(env, "Shop", "stock"),
    }
    sampler = Sampler(seed=42)
    env.process(order_generator(env, sampler, shop))
    env.process(delivery_generator(env, sampler, shop, delivery))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def mean_wait(delivery: float) -> float:
    """Runs one hour and returns the mean wait for packing."""
    return run(delivery).stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


if __name__ == "__main__":
    for delivery in (60, 40):
        print(f"deliveries of {delivery}: mean wait {mean_wait(delivery):6.2f} s")
