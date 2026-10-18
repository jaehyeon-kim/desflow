"""End conditions: a drain after closing time, and a stop when the boxes run out, low-level API.

The low-level twin of `builder/end_conditions_example.py`. Orders arrive until the
shop closes at 120 seconds; then the source process is interrupted and the run goes
on until every order in the shop is packed, for at most 60 seconds more. Each order
takes a box, and the run stops at once if the boxes run out first.

Needs no containers. Runs unpaced and ends on its own.
"""

import itertools
import logging

import simpy

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
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.45)},
    service={
        "packing": DistributionConfig(dist="triangular", low=1, mode=2, high=4),
    },
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
    containers={"boxes": ContainerConfig(current_cap=100, max_cap=100, level=60)},
)


def pack(env, sampler, shop, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    env.stats.flow_started("orders")
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    with shop["packers"].request() as req:
        yield req
        yield shop["boxes"].get(1)
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack", task_id)
    env.stats.flow_finished("orders")


def order_generator(env, sampler, shop):
    gap = env.registry.get_config("Shop.arrival.orders")
    try:
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            process = env.process(
                pack(env, sampler, shop, f"orders-{number}", number - 1)
            )
            shop["orders"].append(process)
    except simpy.Interrupt:
        pass  # closing time: no new orders


def build(env):
    return {
        "packers": DynamicResource(env, "Shop", "packers"),
        "boxes": DynamicContainer(env, "Shop", "boxes"),
        "orders": [],
    }


def processes(env, sampler, shop):
    shop["source"] = order_generator(env, sampler, shop)
    return [shop["source"]]


# --8<-- [start:run]
def run_to_end(env, shop, source):
    """Runs to closing time, then drains for at most 60 s, or until no boxes are left."""
    out_of_boxes = env.event()

    def check():
        if shop["boxes"].level == 0 and not out_of_boxes.triggered:
            out_of_boxes.succeed()

    env.watch_changes(check)
    env.run(until=120)  # no new orders after closing time
    source.interrupt()
    packed = env.all_of([p for p in shop["orders"] if p.is_alive])
    env.run(until=packed | env.timeout(60) | out_of_boxes)


# --8<-- [end:run]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Every 30 seconds and at the end: flows.orders.started, finished and in_system
    env.setup_stats("Shop", "basic", interval=30.0)
    sampler = Sampler(seed=42)
    shop = build(env)
    source = env.process(processes(env, sampler, shop)[0])
    try:
        run_to_end(env, shop, source)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
