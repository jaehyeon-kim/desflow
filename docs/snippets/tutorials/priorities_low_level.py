"""The Priorities tutorial: express orders go before regular ones, low-level.

One packer and 1.5 seconds of packing. A regular order arrives every 2.22 seconds on
average (rate 0.45), and an express order every 6.67 seconds (rate 0.15). Every record
is printed to the terminal, and the script prints the mean wait of each kind of order
under each queue order at the end. To see only those lines, run:
python priorities_low_level.py 2>/dev/null
To use other rates, give the two as arguments: python priorities_low_level.py 0.375 0.125
"""

import copy
import logging
import sys

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
# --8<-- [start:priority]
# Express orders are served before any regular order that is waiting
TASKS = {"orders": ("pack_order", 1), "express": ("pack_express", 0)}
# --8<-- [end:priority]


def pack(env, sampler, packers, arrival, key, task_id):
    task, priority = TASKS[arrival]
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task=task, instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued(task, task_id)
    # --8<-- [start:request]
    # Under fifo and lifo the priority is ignored
    with packers.request(priority=priority) as request:
        # --8<-- [end:request]
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started(task, task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        event = "express_packed" if arrival == "express" else "order_packed"
        publish({"event_type": event})
    env.stats.task_finished(task, task_id)


def order_generator(env, sampler, packers, arrival):
    config = env.registry.get_config(f"Shop.arrival.{arrival}")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(config))
        key = f"{arrival}-{task_id + 1}"
        env.process(pack(env, sampler, packers, arrival, key, task_id))
        task_id += 1


def run(queue="priority", until=3600, factor=0.0, orders_rate=0.45, express_rate=0.15):
    params = copy.deepcopy(PARAMS)
    params.arrival["orders"] = DistributionConfig(dist="exponential", rate=orders_rate)
    params.arrival["express"] = DistributionConfig(
        dist="exponential", rate=express_rate
    )
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(params)
    env.setup_egress([ConsoleEgress()])
    env.setup_stats("Shop", "typical", interval=3600)  # the mean wait of each task
    # --8<-- [start:queue]
    # The packer serves the lowest priority first, the oldest first among equals
    packers = DynamicResource(env, "Shop", "packers", queue=queue)
    # --8<-- [end:queue]
    sampler = Sampler(seed=42)
    env.process(order_generator(env, sampler, packers, "orders"))
    env.process(order_generator(env, sampler, packers, "express"))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def mean_waits(
    queue: str, orders_rate: float = 0.45, express_rate: float = 0.15
) -> dict:
    """Runs one hour and returns the mean wait of each task."""
    readings = run(
        queue, orders_rate=orders_rate, express_rate=express_rate
    ).stats.readings()
    return {
        task: readings[f"Shop.stats.tasks.{task}.wait_mean"]
        for task in ("pack_express", "pack_order")
    }


if __name__ == "__main__":
    rates = [float(rate) for rate in sys.argv[1:3]]
    for queue in ("fifo", "priority"):
        for task, wait in mean_waits(queue, *rates).items():
            print(f"{queue:<8} {task:<12} mean wait {wait:5.2f} s")
