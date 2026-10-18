"""Interruptions: express orders preempt regular ones, and a fire drill interrupts packing, low-level API.

The low-level twin of `builder/interruptions_example.py`. One packer serves express
orders (priority 0) before regular ones (priority 1), and an express order takes the
packer from a regular order being packed: SimPy delivers a `simpy.Interrupt` whose
cause is `Preempted`. The regular order queues again and resumes with the packing
time it had left. At 60 seconds a fire drill interrupts the order being packed,
which resumes when it gets the packer back.

Needs no containers. Runs unpaced and ends after 120 simulation seconds.
"""

import itertools
import logging

import simpy
from simpy.resources.resource import Preempted

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
        "orders": DistributionConfig(dist="exponential", rate=0.3),
        "express": DistributionConfig(dist="exponential", rate=0.1),
    },
    service={
        "packing": DistributionConfig(dist="triangular", low=1, mode=2, high=4),
    },
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
)
PRIORITIES = {"orders": 1, "express": 0}


# --8<-- [start:pack]
def pack(env, sampler, shop, key, priority, task_id):
    """Packs one order; a preemption or an interrupt sends it back to the queue
    with the packing time it had left."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    remaining, started = None, False
    shop["packing"].append(env.active_process)
    while True:
        request = shop["packers"].request(priority=priority)
        since = None
        try:
            yield request
            if not started:
                started = True
                publish({"path_id": path_id, "status": "started"})
                env.stats.task_started("pack", task_id)
            else:
                publish({"path_id": path_id, "status": "resumed"})
                env.stats.task_resumed("pack", task_id)
            if remaining is None:
                remaining = sampler.sample(env.registry.get_config(path_id))
            since = env.now
            yield env.timeout(remaining)
            payload = {"event_type": "order_packed", "priority": priority}
            publish(payload)
            env.stats.task_finished("pack", task_id)
            shop["packers"].release(request)
            break
        except simpy.Interrupt as interrupt:
            request.cancel()
            shop["packers"].release(request)  # nothing to give back if preempted
            if since is not None:
                remaining -= env.now - since
            if isinstance(interrupt.cause, Preempted):
                value = {
                    "path_id": path_id,
                    "status": "preempted",
                    "resource": "packers",
                }
                publish(value)
                env.stats.task_interrupted("pack", task_id, preempted=True)
            else:
                value = {"path_id": path_id, "status": "interrupted"}
                publish({**value, "cause": interrupt.cause})
                env.stats.task_interrupted("pack", task_id)
    shop["packing"].remove(env.active_process)
    env.stats.flow_finished(key.rsplit("-", 1)[0])


# --8<-- [end:pack]
def order_generator(env, sampler, shop, arrival, pack_ids):
    """Spawns a pack task for every arrival, keyed orders-1, express-1 and so on."""
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        key = f"{arrival}-{number}"
        env.process(pack(env, sampler, shop, key, PRIORITIES[arrival], next(pack_ids)))


# --8<-- [start:drill]
def fire_drill(env, shop):
    """Interrupts every order being packed or waiting to be."""
    yield env.timeout(60)
    for process in list(shop["packing"]):
        process.interrupt("fire drill")


# --8<-- [end:drill]
def build(env):
    """Builds the preemptive packers, and a list of the orders being handled."""
    return {
        # A lower priority value takes the packer from a higher one
        "packers": DynamicResource(
            env, "Shop", "packers", queue="priority", preempt=True
        ),
        "packing": [],
    }


def processes(env, sampler, shop):
    """The arrivals and the drill, in the builder's order."""
    pack_ids = itertools.count()
    return [
        order_generator(env, sampler, shop, "orders", pack_ids),
        order_generator(env, sampler, shop, "express", pack_ids),
        fire_drill(env, shop),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Every 30 seconds, how many orders were preempted, interrupted and resumed
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
