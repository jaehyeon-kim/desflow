"""Process interaction: a signal, a wait with a time limit, a child flow and a failure, low-level API.

The low-level twin of `builder/process_interaction_example.py`. A packed order is
inspected by a child process it waits for; a gift fails inspection, and is wrapped
instead, which ends the child. The order then waits for the courier, who leaves every
50 seconds and fires the signal courier_left, taking every order that waits; an
order that has waited 40 seconds goes by post instead.

Needs no containers. Runs unpaced and ends after 300 simulation seconds.
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
    Signal,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        "orders": DistributionConfig(dist="exponential", rate=0.12),
        "gifts": DistributionConfig(dist="exponential", rate=0.04),
        "couriers": DistributionConfig(dist="constant", value=50),
    },
    service={
        "packing": DistributionConfig(dist="triangular", low=1, mode=2, high=4),
        "inspecting": DistributionConfig(dist="constant", value=2),
        "wrapping": DistributionConfig(dist="constant", value=3),
    },
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
)
GIFT = {"orders": 0, "gifts": 1}


class Shop:
    """The shop's parts and counters, shared by its processes."""

    def __init__(self, env, sampler):
        self.env, self.sampler = env, sampler
        self.packers = DynamicResource(env, "Shop", "packers")
        self.courier_left = Signal(env, "courier_left")
        self.ids = {}

    def next_id(self, task):
        return next(self.ids.setdefault(task, itertools.count()))

    def publish(self, key, task, task_id, value):
        self.env.publish_event(key, value, task=task, instance=task_id)

    def service(self, name):
        path_id = f"Shop.service.{name}"
        return path_id, self.sampler.sample(self.env.registry.get_config(path_id))


def pack(shop, key, arrival):
    env, stats, task_id = shop.env, shop.env.stats, shop.next_id("pack")
    path_id = "Shop.service.packing"
    stats.flow_started(arrival)  # the order enters the shop
    shop.publish(key, "pack", task_id, {"path_id": path_id, "status": "queued"})
    stats.task_queued("pack", task_id)
    with shop.packers.request() as req:
        yield req
        shop.publish(key, "pack", task_id, {"path_id": path_id, "status": "started"})
        stats.task_started("pack", task_id)
        yield env.timeout(shop.service("packing")[1])
        shop.publish(key, "pack", task_id, {"event_type": "order_packed"})
        stats.task_finished("pack", task_id)
    env.process(check(shop, key, arrival))


# --8<-- [start:check]
def check(shop, key, arrival):
    """Starts the child process inspect and waits for it to end."""
    env, stats, task_id = shop.env, shop.env.stats, shop.next_id("check")
    shop.publish(key, "check", task_id, {"status": "queued"})
    stats.task_queued("check", task_id)
    yield env.any_of([env.process(inspect(shop, key, arrival))])
    shop.publish(key, "check", task_id, {"status": "started"})
    stats.task_started("check", task_id)
    shop.publish(key, "check", task_id, {"event_type": "order_checked"})
    stats.task_finished("check", task_id)
    env.process(dispatch(shop, key, arrival))


def inspect(shop, key, arrival):
    """A gift fails inspection and is wrapped instead; returns the last payload."""
    env, stats, task_id = shop.env, shop.env.stats, shop.next_id("inspect")
    path_id = "Shop.service.inspecting"
    shop.publish(key, "inspect", task_id, {"path_id": path_id, "status": "started"})
    stats.task_started("inspect", task_id)
    yield env.timeout(shop.service("inspecting")[1])
    if GIFT[arrival] == 1:
        failed = {
            "path_id": path_id,
            "status": "failed",
            "reason": "gift needs wrapping",
        }
        shop.publish(key, "inspect", task_id, failed)
        stats.task_failed("inspect", task_id)
        return (yield env.process(wrap(shop, key)))
    payload = {"event_type": "inspected"}
    shop.publish(key, "inspect", task_id, payload)
    stats.task_finished("inspect", task_id)
    return payload


def wrap(shop, key):
    env, stats, task_id = shop.env, shop.env.stats, shop.next_id("wrap")
    path_id = "Shop.service.wrapping"
    shop.publish(key, "wrap", task_id, {"path_id": path_id, "status": "started"})
    stats.task_started("wrap", task_id)
    yield env.timeout(shop.service("wrapping")[1])
    payload = {"event_type": "wrapped"}
    shop.publish(key, "wrap", task_id, payload)
    stats.task_finished("wrap", task_id)
    return payload


# --8<-- [end:check]
# --8<-- [start:dispatch]
def dispatch(shop, key, arrival):
    """The courier's departure, or 40 seconds, whichever comes first."""
    env, stats, task_id = shop.env, shop.env.stats, shop.next_id("dispatch")
    shop.publish(key, "dispatch", task_id, {"status": "queued"})
    stats.task_queued("dispatch", task_id)
    courier = shop.courier_left.wait()
    done = yield env.any_of([courier, env.timeout(40)])
    shop.publish(key, "dispatch", task_id, {"status": "started"})
    stats.task_started("dispatch", task_id)
    shop.publish(key, "dispatch", task_id, {"event_type": "dispatched"})
    stats.task_finished("dispatch", task_id)
    task = "by_courier" if courier in done else "by_post"
    env.process(send(shop, key, arrival, task))


# --8<-- [end:dispatch]
def send(shop, key, arrival, task):
    task_id = shop.next_id(task)
    event = "sent_by_courier" if task == "by_courier" else "sent_by_post"
    shop.publish(key, task, task_id, {"event_type": event})
    shop.env.stats.task_finished(task, task_id)
    shop.env.stats.flow_finished(arrival)
    yield shop.env.timeout(0)


# --8<-- [start:courier]
def courier(shop, key):
    """Fires courier_left; every order waiting for it resumes."""
    task_id = shop.next_id("courier")
    shop.env.stats.flow_started("couriers")
    payload = {"event_type": "courier_left"}
    shop.publish(key, "courier", task_id, payload)
    shop.env.stats.task_finished("courier", task_id)
    shop.courier_left.fire(payload)
    shop.env.stats.flow_finished("couriers")
    yield shop.env.timeout(0)


# --8<-- [end:courier]
def generator(shop, arrival):
    """Starts a pack, or a courier, for every arrival, keyed orders-1 and so on."""
    env = shop.env
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(shop.sampler.sample(gap))
        key = f"{arrival}-{number}"
        if arrival == "couriers":
            env.process(courier(shop, key))
        else:
            env.process(pack(shop, key, arrival))


def build(env, sampler):
    """Builds the packers and the signal."""
    return Shop(env, sampler)


def processes(env, sampler, shop):
    """The three arrivals, in the builder's order."""
    return [generator(shop, a) for a in ("orders", "gifts", "couriers")]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Every 60 seconds, finished and failed counts so far
    env.setup_stats("Shop", "basic", interval=60.0)
    sampler = Sampler(seed=42)
    shop = build(env, sampler)
    for process in processes(env, sampler, shop):
        env.process(process)
    try:
        env.run(until=300)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
