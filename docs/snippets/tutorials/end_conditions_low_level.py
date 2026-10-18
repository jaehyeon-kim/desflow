"""The End Conditions tutorial: a fixed horizon against a drain.

One packer, 0.7 orders a second and 1.5 seconds of packing, for an hour. Every
record is printed to the terminal, and the script prints the counts and the mean
wait with a fixed horizon and with a drain at the end:
python end_conditions_low_level.py 2>/dev/null shows only those lines.
"""

import itertools
import logging

import simpy

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
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.7)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
)


def pack_order(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    env.stats.flow_started("orders")  # the order enters the shop
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")


def order_generator(env, sampler, packers, orders):
    arrival = env.registry.get_config("Shop.arrival.orders")
    try:
        for task_id in itertools.count():
            yield env.timeout(sampler.sample(arrival))
            key = f"orders-{task_id + 1}"
            orders.append(env.process(pack_order(env, sampler, packers, key, task_id)))
    except simpy.Interrupt:
        pass  # no new orders after the hour


def run(drain=False, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # --8<-- [start:watch]
    # Orders started, finished and still in the shop, and the mean wait, at the end
    env.setup_stats("Shop", "typical", interval=7200)
    # --8<-- [end:watch]
    packers = DynamicResource(env, "Shop", "packers")
    orders = []
    source = env.process(order_generator(env, Sampler(seed=42), packers, orders))
    try:
        # --8<-- [start:run]
        env.run(until=3600)  # no new orders after one hour
        if drain:
            # Stop the source, then run until every admitted order has ended
            source.interrupt()
            env.run(until=env.all_of([o for o in orders if o.is_alive]))
        # --8<-- [end:run]
    finally:
        env.teardown()
    return env


def outcomes(drain: bool) -> dict:
    """Runs the hour, with or without a drain, and returns the counts and the wait."""
    env = run(drain)
    r = env.stats.readings()
    name = "Shop.stats.{}".format
    return {
        "started": r[name("flows.orders.started")],
        "finished": r[name("flows.orders.finished")],
        "in_system": r[name("flows.orders.in_system")],
        "wait_mean": r[name("tasks.pack_order.wait_mean")],
        "end": env.now,
    }


if __name__ == "__main__":
    for drain in (False, True):
        r = outcomes(drain)
        label = "drain  " if drain else "horizon"
        print(
            f"{label}: ends at {r['end']:7.1f} s, {r['started']} started, "
            f"{r['finished']} finished, {r['in_system']} left, "
            f"mean wait {r['wait_mean']:5.2f} s"
        )
