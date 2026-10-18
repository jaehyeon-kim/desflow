"""The Competing Resources tutorial: an order keeps the picker until it is on the bench.

One picker, one packing bench, and an order every 2.5 seconds on average. Every
record is printed to the terminal, and the script prints the waits and the picker's
busy share with and without the handoff at the end:
python competing_resources_low_level.py 2>/dev/null shows only those lines.
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
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={
        "picking": DistributionConfig(dist="constant", value=1),
        "packing": DistributionConfig(dist="exponential", rate=0.5),
    },
    resources={
        "picker": CapacityConfig(current_cap=1, max_cap=2),
        "bench": CapacityConfig(current_cap=1, max_cap=2),
    },
)


def step(env, sampler, key, task, task_id, service):
    """The publish and service-time helpers of one task instance."""
    path_id = f"Shop.service.{service}"

    def status(state):
        value = {"path_id": path_id, "status": state}
        env.publish_event(key, value, task=task, instance=task_id)

    def finished(value):
        env.publish_event(key, value, task=task, instance=task_id)

    return status, finished, lambda: sampler.sample(env.registry.get_config(path_id))


# --8<-- [start:handoff]
def pick_order(env, sampler, shop, key, task_id, keep):
    status, finished, duration = step(
        env, sampler, key, "pick_order", task_id, "picking"
    )
    status("queued")
    env.stats.task_queued("pick_order", task_id)
    picker = shop["picker"].request()
    yield picker
    status("started")
    env.stats.task_started("pick_order", task_id)
    yield env.timeout(duration())
    finished({"event_type": "order_picked"})
    env.stats.task_finished("pick_order", task_id)
    if not keep:
        shop["picker"].release(picker)
        picker = None
    # With keep, the picker carries the order and stays with it until it is on the bench
    env.process(pack_order(env, sampler, shop, key, task_id, picker))


def pack_order(env, sampler, shop, key, task_id, picker):
    status, finished, duration = step(
        env, sampler, key, "pack_order", task_id, "packing"
    )
    status("queued")
    env.stats.task_queued("pack_order", task_id)
    with shop["bench"].request() as bench:
        yield bench
        status("started")
        env.stats.task_started("pack_order", task_id)
        if picker is not None:
            shop["picker"].release(picker)  # the order is on the bench
        yield env.timeout(duration())
        finished({"event_type": "order_packed"})
        env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")


# --8<-- [end:handoff]


def order_generator(env, sampler, shop, keep):
    arrival = env.registry.get_config("Shop.arrival.orders")
    for task_id in itertools.count():
        yield env.timeout(sampler.sample(arrival))
        key = f"orders-{task_id + 1}"
        env.process(pick_order(env, sampler, shop, key, task_id, keep))


def run(keep=True, until=3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # --8<-- [start:watch]
    # The mean wait of each task and the picker's busy share, at the end
    env.setup_stats("Shop", "typical", interval=3600)
    # --8<-- [end:watch]
    shop = {
        "picker": DynamicResource(env, "Shop", "picker"),
        "bench": DynamicResource(env, "Shop", "bench"),
    }
    env.process(order_generator(env, Sampler(seed=42), shop, keep))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def outcomes(keep: bool) -> dict:
    """Runs one hour and returns each task's mean wait and the picker's busy share."""
    r = run(keep).stats.readings()
    return {
        "pick_wait": r["Shop.stats.tasks.pick_order.wait_mean"],
        "pack_wait": r["Shop.stats.tasks.pack_order.wait_mean"],
        "picker_busy": r["Shop.stats.resources.picker.utilization_avg"],
    }


if __name__ == "__main__":
    for keep in (False, True):
        r = outcomes(keep)
        label = "keeps the picker" if keep else "frees the picker"
        print(
            f"{label}: wait to pick {r['pick_wait']:6.2f} s, to pack "
            f"{r['pack_wait']:5.2f} s, picker busy {r['picker_busy']:4.1f}%"
        )
