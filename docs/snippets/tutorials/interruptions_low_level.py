"""The Interruptions tutorial: express orders take the packer from regular ones.

One packer and 1.5 seconds of packing. A regular order arrives every 2.22 seconds on
average (rate 0.45), and an express order every 6.67 seconds (rate 0.15). Every record
is printed to the terminal, and the script prints what preemption did to regular orders
under each policy at the end. To see only those lines, run:
python interruptions_low_level.py 2>/dev/null
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
        # An order every 1.67 s on average in all (0.6 a second), one in four express
        "orders": DistributionConfig(dist="exponential", rate=0.45),
        "express": DistributionConfig(dist="exponential", rate=0.15),
    },
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
)
TASKS = {"orders": ("pack_order", 1), "express": ("pack_express", 0)}


# --8<-- [start:policy]
def pack(env, sampler, shop, arrival, key, task_id, policy):
    """Packs one order; when it loses the packer, it follows `policy`."""
    task, priority = TASKS[arrival]
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task=task, instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued(task, task_id)
    shop["running"].setdefault(task, []).append(env.active_process)
    shop["keys"][env.active_process] = key
    remaining, started = None, False
    while True:
        request = shop["packers"].request(priority=priority)
        since = None
        try:
            yield request
            status = "resumed" if started else "started"
            publish({"path_id": path_id, "status": status})
            if started:
                env.stats.task_resumed(task, task_id)
            else:
                env.stats.task_started(task, task_id)
            started = True
            if remaining is None:
                remaining = sampler.sample(env.registry.get_config(path_id))
            since = env.now
            yield env.timeout(remaining)
            publish({"event_type": "order_packed" if priority else "express_packed"})
            env.stats.task_finished(task, task_id)
            shop["packers"].release(request)
            break
        except simpy.Interrupt as interrupt:
            request.cancel()
            shop["packers"].release(request)
            if since is not None:
                # resume serves what is left; restart draws a new packing time
                remaining = (
                    remaining - (env.now - since) if policy == "resume" else None
                )
            preempted = isinstance(interrupt.cause, Preempted)
            if preempted:
                value = {
                    "status": "preempted",
                    "resource": "packers",
                    "preempted_by": shop["keys"].get(interrupt.cause.by),
                    "usage_since": interrupt.cause.usage_since,
                }
            else:
                value = {"status": "interrupted", "cause": interrupt.cause}
            publish({"path_id": path_id, **value})
            env.stats.task_interrupted(task, task_id, preempted=preempted)
            if policy == "abandon":
                env.stats.task_dropped(task, task_id)
                shop["running"][task].remove(env.active_process)
                del shop["keys"][env.active_process]
                return
    shop["running"][task].remove(env.active_process)
    del shop["keys"][env.active_process]
    env.stats.flow_finished(arrival)


# --8<-- [end:policy]


def order_generator(env, sampler, shop, arrival, policy):
    config = env.registry.get_config(f"Shop.arrival.{arrival}")
    for task_id in itertools.count():
        yield env.timeout(sampler.sample(config))
        key = f"{arrival}-{task_id + 1}"
        env.process(pack(env, sampler, shop, arrival, key, task_id, policy))


# --8<-- [start:interrupt]
def called_away(env, shop):
    """The packer is called away at 30 minutes: every regular order in the shop,
    waiting or being packed, is interrupted."""
    yield env.timeout(1800)
    for process in list(shop["running"].get("pack_order", [])):
        process.interrupt("called away")


# --8<-- [end:interrupt]


def run(policy="resume", until=3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # --8<-- [start:watch]
    # Counts of preemptions and resumptions, and the mean wait and service, at the end
    env.setup_stats("Shop", "typical", interval=3600)
    # --8<-- [end:watch]
    shop = {
        # --8<-- [start:packers]
        # An express order takes the packer from a regular one being packed
        "packers": DynamicResource(
            env, "Shop", "packers", queue="priority", preempt=True
        ),
        # --8<-- [end:packers]
        "running": {},
        # The key of the order each process packs, for the `preempted_by` of a record
        "keys": {},
    }
    sampler = Sampler(seed=42)
    env.process(order_generator(env, sampler, shop, "orders", policy))
    env.process(order_generator(env, sampler, shop, "express", policy))
    env.process(called_away(env, shop))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def outcomes(policy: str) -> dict:
    """Runs one hour and returns what preemption did to regular orders."""
    readings = run(policy).stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "preempted": readings[name("preempted")],
        "interrupted": readings.get(name("interrupted"), 0),
        "finished": readings[name("finished")],
        "wait_mean": readings[name("wait_mean")],
        "service_mean": readings[name("service_mean")],
    }


if __name__ == "__main__":
    for policy in ("resume", "restart", "abandon"):
        r = outcomes(policy)
        print(
            f"{policy:<7}: {r['preempted']} preempted, {r['finished']} packed, "
            f"mean wait {r['wait_mean']:5.2f} s, mean packing {r['service_mean']:4.2f} s"
        )
