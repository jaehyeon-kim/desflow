"""Local replay: the orders of a recorded morning, at their recorded times, low-level API.

The low-level version of `builder/local_replay_example.py`. `read_trace` reads and
checks the rows of ../local_replay_orders.csv; a SimPy loop waits for each row's
time and starts its order, which carries the row's order_id and items. The last
row is at 73 seconds; after it the arrival stops, and the run ends at 120.

Needs no containers. Runs unpaced and ends after 120 simulation seconds.
"""

import logging
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

TRACE = Path(__file__).resolve().parent.parent / "local_replay_orders.csv"
PARAMS = SimParameter(
    sim_id="Shop",
    service={"packing": DistributionConfig(dist="triangular", low=1, mode=2, high=3)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=2)},
)


def pack(env, sampler, packers, key, task_id, fields):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed", **fields})


# --8<-- [start:replay]
def replay(env, sampler, packers):
    """Starts one order at each row's time, carrying the row's fields."""
    rows = read_trace(TRACE, time="time", unit="s", fields=["order_id", "items"])
    for number, row in enumerate(rows, start=1):
        yield env.timeout(row.time - env.now)
        key = f"orders-{number}"
        env.process(pack(env, sampler, packers, key, number - 1, row.fields))


# --8<-- [end:replay]


def build(env):
    return DynamicResource(env, "Shop", "packers")


def processes(env, sampler, packers):
    return [replay(env, sampler, packers)]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    sampler = Sampler(seed=42)
    packers = build(env)
    for process in processes(env, sampler, packers):
        env.process(process)
    try:
        env.run(until=120)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
