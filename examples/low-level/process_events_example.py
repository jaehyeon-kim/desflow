"""Process events: a one-shot signal, a spawn after a delay, a subscribe and an on: action, low-level API.

The low-level version of `builder/process_events_example.py`. A guard starts an alarm
round and is told when it ends; a packed order fires the `ready` signal from its own
payload, which a shipper waiting for it reads; and closing time starts a sweep one
second later, as a new entity. In the low-level API each of these is plain SimPy:
`Signal`, an interrupt handled by `env.any_of`, and `simpy.util.start_delayed`.

Needs no containers. Runs unpaced and ends after 12 simulation seconds.
"""

import itertools
import logging

import simpy.util

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
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
        "orders": DistributionConfig(dist="constant", value=6),
        "shippers": DistributionConfig(dist="constant", value=7),
        "closers": DistributionConfig(dist="constant", value=9),
        "guards": DistributionConfig(dist="constant", value=8),
    },
    service={
        "packing": DistributionConfig(dist="constant", value=2),
        "alarming": DistributionConfig(dist="constant", value=2),
        "sweeping": DistributionConfig(dist="constant", value=1),
        "guarding": DistributionConfig(dist="constant", value=100),
    },
)


class Shop:
    """The shop's signal and counters, shared by its processes."""

    def __init__(self, env, sampler):
        self.env, self.sampler = env, sampler
        self.ready = Signal(env, "ready")
        self.ids = {}

    def next_id(self, task):
        return next(self.ids.setdefault(task, itertools.count()))

    def publish(self, key, task, task_id, value):
        self.env.publish_event(key, value, task=task, instance=task_id)

    def service(self, name):
        return self.sampler.sample(self.env.registry.get_config(f"Shop.service.{name}"))


def pack(shop, key):
    """Packs an order, then fires ready with its finished payload."""
    env, task_id = shop.env, shop.next_id("pack")
    shop.publish(
        key, "pack", task_id, {"path_id": "Shop.service.packing", "status": "started"}
    )
    yield env.timeout(shop.service("packing"))
    payload = {"event_type": "order_packed"}
    shop.publish(key, "pack", task_id, payload)
    shop.ready.fire(payload)


def ship(shop, key):
    """Waits for the ready signal, then ships."""
    task_id = shop.next_id("ship")
    shop.publish(key, "ship", task_id, {"status": "queued"})
    yield shop.ready.wait()
    shop.publish(key, "ship", task_id, {"status": "started"})
    shop.publish(key, "ship", task_id, {"event_type": "order_shipped"})


def sweep(shop, key):
    """Sweeps the floor, a timed step started after the shop closes."""
    env, task_id = shop.env, shop.next_id("sweep")
    shop.publish(
        key, "sweep", task_id, {"path_id": "Shop.service.sweeping", "status": "started"}
    )
    yield env.timeout(shop.service("sweeping"))
    shop.publish(key, "sweep", task_id, {"event_type": "floor_swept"})


def close(shop, key):
    """Closes the shop and starts a sweep one second later, as a new entity."""
    env, task_id = shop.env, shop.next_id("close")
    shop.publish(key, "close", task_id, {"event_type": "shop_closed"})
    simpy.util.start_delayed(env, sweep(shop, "sweep-1"), 1)
    yield env.timeout(0)


def alarm(shop, key):
    """The alarm round: a timed step that ends with its payload."""
    env, task_id = shop.env, shop.next_id("alarm")
    shop.publish(
        key, "alarm", task_id, {"path_id": "Shop.service.alarming", "status": "started"}
    )
    yield env.timeout(shop.service("alarming"))
    payload = {"event_type": "alarm_rang"}
    shop.publish(key, "alarm", task_id, payload)
    return payload


def guard(shop, key):
    """Starts the alarm round and is interrupted when it ends, then ends the shift."""
    env, task_id = shop.env, shop.next_id("guard")
    shop.publish(
        key, "guard", task_id, {"path_id": "Shop.service.guarding", "status": "started"}
    )
    alarm_round = env.process(alarm(shop, key))
    done = yield env.any_of([alarm_round, env.timeout(100)])
    if alarm_round in done:
        cause = {"event": "alarm", "value": alarm_round.value}
        shop.publish(
            key,
            "guard",
            task_id,
            {
                "path_id": "Shop.service.guarding",
                "status": "interrupted",
                "cause": cause,
            },
        )


TASKS = {"orders": pack, "shippers": ship, "closers": close, "guards": guard}


def generator(shop, arrival):
    """Starts the arrival's task for every arrival, keyed orders-1 and so on."""
    env = shop.env
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    task = TASKS[arrival]
    for number in itertools.count(1):
        yield env.timeout(shop.sampler.sample(gap))
        env.process(task(shop, f"{arrival}-{number}"))


def build(env, sampler):
    """Builds the shop's signal and counters."""
    return Shop(env, sampler)


def processes(env, sampler, shop):
    """The four arrivals, in the builder's order."""
    return [generator(shop, a) for a in ("orders", "shippers", "closers", "guards")]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    sampler = Sampler(seed=42)
    shop = build(env, sampler)
    for process in processes(env, sampler, shop):
        env.process(process)
    try:
        env.run(until=12)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
