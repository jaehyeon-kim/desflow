"""Order fulfilment: several distributions, a priority resource, a second resource
tasks compete for, a store of stock refilled from a container, patience and
statistics, low-level API.

The low-level twin of `builder/order_fulfilment_example.py`, a regression scenario
(see scripts/regression.py), not an example. Standard and express orders wait for a
picker (express first) and for a shelf item; one that runs out of patience leaves.
Restocking moves one item at a time from the warehouse to the shelf and uses the same
benches as packing.
"""

import itertools

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

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        "orders": DistributionConfig(dist="exponential", rate=0.18),
        "express": DistributionConfig(dist="weibull", shape=1.5, scale=30),
        "deliveries": DistributionConfig(dist="uniform", low=4, high=8),
    },
    service={
        "picking": DistributionConfig(dist="lognormal", mean=6, std=3),
        "packing": DistributionConfig(dist="gamma", shape=3, scale=1),
        "restocking": DistributionConfig(dist="triangular", low=1, mode=2, high=5),
    },
    resources={
        "pickers": CapacityConfig(current_cap=2, max_cap=4),
        "benches": CapacityConfig(current_cap=2, max_cap=3),
    },
    stores={"shelf": CapacityConfig(current_cap=30, max_cap=30)},
    containers={"warehouse": ContainerConfig(current_cap=300, max_cap=300, level=120)},
)
# Each arrival's attributes: the picker queue rank and the patience in seconds
ORDERS = {"orders": (2, 40), "express": (1, 25)}


class Shop:
    """The shop's parts, its sampler and the task counters its processes share."""

    def __init__(self, env, sampler):
        self.env, self.sampler = env, sampler
        self.pickers = DynamicResource(env, "Shop", "pickers", queue="priority")
        self.benches = DynamicResource(env, "Shop", "benches")
        self.shelf = DynamicStore(env, "Shop", "shelf")
        self.warehouse = DynamicContainer(env, "Shop", "warehouse")
        self.ids = {}

    def next_id(self, task):
        return next(self.ids.setdefault(task, itertools.count()))

    def step(self, key, task, service):
        """The publish and service-time helpers of one task instance."""
        task_id, path_id = self.next_id(task), f"Shop.service.{service}"

        def publish(value):
            self.env.publish_event(key, value, task=task, instance=task_id)

        def status(state):
            publish({"path_id": path_id, "status": state})

        def duration():
            return self.sampler.sample(self.env.registry.get_config(path_id))

        return task_id, publish, status, duration


def pick(shop, key, arrival):
    """Waits for a picker and a shelf item within its patience, then picks."""
    env, stats = shop.env, shop.env.stats
    rank, patience = ORDERS[arrival]
    task_id, publish, status, duration = shop.step(key, "pick", "picking")
    stats.flow_started(arrival)
    status("queued")
    stats.task_queued("pick", task_id)

    def reneged(*held):
        for request in held:
            request.cancel()
            shop.pickers.release(request)
        status("reneged")
        stats.task_reneged("pick", task_id)
        stats.flow_ended(arrival)

    left = float(patience)
    request = shop.pickers.request(priority=rank)
    since = env.now
    yield request | env.timeout(left)
    left -= env.now - since
    if not request.triggered:
        reneged(request)
        return
    take = shop.shelf.get()
    since = env.now
    yield take | env.timeout(left)
    left -= env.now - since
    if not take.triggered:
        take.cancel()
        reneged(request)
        return
    status("started")
    stats.task_started("pick", task_id)
    yield env.timeout(duration())
    publish({"event_type": "order_picked"})
    stats.task_finished("pick", task_id)
    env.notify_change()
    shop.pickers.release(request)
    env.process(pack(shop, key, arrival))


def pack(shop, key, arrival):
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "pack", "packing")
    status("queued")
    stats.task_queued("pack", task_id)
    request = shop.benches.request()
    yield request
    status("started")
    stats.task_started("pack", task_id)
    yield env.timeout(duration())
    publish({"event_type": "order_packed"})
    stats.task_finished("pack", task_id)
    env.notify_change()
    shop.benches.release(request)
    stats.flow_finished(arrival)


def restock(shop, key):
    """Takes one item from the warehouse and puts it on the shelf."""
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "restock", "restocking")
    stats.flow_started("deliveries")
    status("queued")
    stats.task_queued("restock", task_id)
    request = shop.benches.request()
    yield request
    yield shop.warehouse.get(1)
    status("started")
    stats.task_started("restock", task_id)
    yield env.timeout(duration())
    payload = {"event_type": "shelf_filled"}
    yield shop.shelf.put(payload)
    publish(payload)
    stats.task_finished("restock", task_id)
    env.notify_change()
    shop.benches.release(request)
    stats.flow_finished("deliveries")


def generator(shop, arrival, start):
    """Starts a task for every arrival, keyed orders-1, express-1 and so on."""
    env = shop.env
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(shop.sampler.sample(gap))
        env.process(start(shop, f"{arrival}-{number}", arrival))


def build(env, sampler):
    return Shop(env, sampler)


def processes(env, sampler, shop):
    """The three arrivals, in the builder's order."""
    deliver = lambda shop, key, arrival: restock(shop, key)  # noqa: E731
    return [
        generator(shop, "orders", pick),
        generator(shop, "express", pick),
        generator(shop, "deliveries", deliver),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    env.setup_stats("Shop", "basic", interval=50.0)
    sampler = Sampler(seed=42)
    shop = build(env, sampler)
    for process in processes(env, sampler, shop):
        env.process(process)
    try:
        env.run(until=600)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
