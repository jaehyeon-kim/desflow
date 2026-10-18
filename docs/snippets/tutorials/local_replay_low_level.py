"""The Local Replay tutorial: a recorded morning of orders, against a model of it.

One packer and 1.5 seconds of packing. The orders of 09:00 to 10:00 on 1 October
2026 arrive at their recorded times from local_replay_orders.csv; a second run
draws exponential gaps at the same mean rate, 0.545 orders a second. Every record
is printed to the terminal, and the script prints the waits of both runs:
python local_replay_low_level.py 2>/dev/null shows only those lines.
"""

import itertools
import logging
from datetime import datetime
from pathlib import Path

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    read_trace,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

TRACE = Path(__file__).resolve().parent / "local_replay_orders.csv"
START = datetime(2026, 10, 1, 9, 0)  # simulation time 0 is 09:00 on the recorded day
PARAMS = SimParameter(
    sim_id="Shop",
    # The same mean rate: 1,963 orders in the hour
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.545)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=2)},
)


def pack_order(env, sampler, packers, key, task_id, fields):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack_order", task_id)


# --8<-- [start:replay]
def replayed_orders(env, sampler, packers):
    """Starts one order at each row's time, carrying its order_id and items."""
    rows = read_trace(
        TRACE,
        time="ordered_at",
        unit="timestamp",
        fields=["order_id", "items"],
        # A row without a valid time is left out, with a warning naming it
        on_invalid="skip",
        start=START,
    )
    for task_id, row in enumerate(rows):
        # A row at or after the end of the run never arrives
        yield env.timeout(row.time - env.now)
        key = f"orders-{task_id + 1}"
        env.process(pack_order(env, sampler, packers, key, task_id, row.fields))


# --8<-- [end:replay]


def modelled_orders(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    for task_id in itertools.count():
        yield env.timeout(sampler.sample(arrival))
        key = f"orders-{task_id + 1}"
        env.process(pack_order(env, sampler, packers, key, task_id, {}))


def run(replay=True, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor, logical_start_time=START)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Orders packed, and the mean and longest wait, at the end
    env.setup_stats("Shop", "typical", interval=7200)
    packers = DynamicResource(env, "Shop", "packers")
    orders = replayed_orders if replay else modelled_orders
    env.process(orders(env, Sampler(seed=42), packers))
    try:
        env.run(until=3600)
    finally:
        env.teardown()
    return env


def outcomes(replay: bool) -> dict:
    """Runs the hour and returns the orders packed and their waits."""
    r = run(replay).stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "finished": r[name("finished")],
        "wait_mean": r[name("wait_mean")],
        "wait_max": r[name("wait_max")],
    }


if __name__ == "__main__":
    for replay in (True, False):
        r = outcomes(replay)
        label = "recorded" if replay else "modelled"
        print(
            f"{label}: {r['finished']} packed, "
            f"mean wait {r['wait_mean']:5.2f} s, longest {r['wait_max']:6.2f} s"
        )
