"""Competing resources: tasks that hold several resources, and a handoff, low-level API.

The low-level twin of `builder/competing_resources_example.py`. A picker collects an
order and carries it to a packing bench; the order keeps the picker until it is on
the bench, then gives it back. A gift needs the picker and the bench together for
its wrapping: nested requests, the picker first, the same order every process uses,
so no two orders can wait for each other.

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
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        "orders": DistributionConfig(dist="exponential", rate=0.25),
        "gifts": DistributionConfig(dist="exponential", rate=0.05),
    },
    service={
        "picking": DistributionConfig(dist="uniform", low=1, high=2),
        "packing": DistributionConfig(dist="triangular", low=1, mode=2, high=4),
        "wrapping": DistributionConfig(dist="constant", value=3),
    },
    resources={
        "picker": CapacityConfig(current_cap=1, max_cap=2),
        "bench": CapacityConfig(current_cap=1, max_cap=2),
    },
)


class Shop:
    """The picker, the bench and the counters the processes share."""

    def __init__(self, env, sampler):
        self.env, self.sampler = env, sampler
        self.picker = DynamicResource(env, "Shop", "picker")
        self.bench = DynamicResource(env, "Shop", "bench")
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


# --8<-- [start:tasks]
def pick(shop, key):
    """Picks an order and keeps the picker for pack, which gives it back."""
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "pick", "picking")
    status("queued")
    stats.task_queued("pick", task_id)
    picker = shop.picker.request()
    yield picker
    status("started")
    stats.task_started("pick", task_id)
    yield env.timeout(duration())
    publish({"event_type": "order_picked"})
    stats.task_finished("pick", task_id)
    env.process(pack(shop, key, picker))


def pack(shop, key, picker):
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "pack", "packing")
    status("queued")
    stats.task_queued("pack", task_id)
    with shop.bench.request() as bench:
        yield bench
        status("started")
        stats.task_started("pack", task_id)
        shop.picker.release(picker)  # the order is on the bench
        yield env.timeout(duration())
        publish({"event_type": "order_packed"})
        stats.task_finished("pack", task_id)
    stats.flow_finished("orders")


def wrap(shop, key):
    """Holds the picker, then the bench too: acquired in this order."""
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "wrap", "wrapping")
    status("queued")
    stats.task_queued("wrap", task_id)
    with shop.picker.request() as picker:
        yield picker
        with shop.bench.request() as bench:
            yield bench
            status("started")
            stats.task_started("wrap", task_id)
            yield env.timeout(duration())
            publish({"event_type": "gift_wrapped"})
            stats.task_finished("wrap", task_id)
    stats.flow_finished("gifts")


# --8<-- [end:tasks]
def generator(shop, arrival, task):
    """Starts `task` for every arrival, keyed orders-1, gifts-1 and so on."""
    env = shop.env
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(shop.sampler.sample(gap))
        env.process(task(shop, f"{arrival}-{number}"))


def build(env, sampler):
    return Shop(env, sampler)


def processes(env, sampler, shop):
    """The two arrivals, in the builder's order."""
    return [generator(shop, "orders", pick), generator(shop, "gifts", wrap)]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Every 30 seconds, each resource's readings and the tasks finished so far
    env.setup_stats("Shop", "basic", interval=30.0)
    sampler = Sampler(seed=42)
    shop = build(env, sampler)
    for process in processes(env, sampler, shop):
        env.process(process)
    try:
        env.run(until=120)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
