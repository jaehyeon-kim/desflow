"""Breakdowns: a packing station that fails and is repaired, low-level API.

The low-level version of `builder/breakdowns_example.py`. `run_breakdowns` is a SimPy
process that puts the station down about once a minute and repairs it after 5 to 15
seconds. A breakdown interrupts the orders being packed: each catches the
`simpy.Interrupt`, gives the station back, queues again, and packs what it had left.

Needs no containers. Runs unpaced and ends after 300 simulation seconds.
"""

import itertools
import logging

import simpy

from desflow import (
    BreakdownConfig,
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    run_breakdowns,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    # An order every 1.67 s on average (rate 0.6)
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.6)},
    service={
        "packing": DistributionConfig(dist="triangular", low=1, mode=2, high=4),
    },
    resources={"station": CapacityConfig(current_cap=2, max_cap=3)},
    # --8<-- [start:station]
    breakdowns={
        "station": BreakdownConfig(
            # Calendar time from a repair, or the start, to the next failure
            # A failure every 60 s on average (rate 0.0167)
            time_to_failure=DistributionConfig(dist="exponential", rate=0.0167),
            repair=DistributionConfig(dist="uniform", low=5, high=15),
            # The orders being packed are interrupted
            work="interrupt",
        )
    },
    # --8<-- [end:station]
)


def pack(env, sampler, station, key, task_id):
    """Packs one order; a breakdown sends it back to the queue with what it had left."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    env.stats.flow_started("orders")
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    remaining, started = None, False
    while True:
        request = station.request()
        since = None
        try:
            yield request
            if started:
                publish({"path_id": path_id, "status": "resumed"})
                env.stats.task_resumed("pack", task_id)
            else:
                publish({"path_id": path_id, "status": "started"})
                env.stats.task_started("pack", task_id)
                started = True
            if remaining is None:
                remaining = sampler.sample(env.registry.get_config(path_id))
            since = env.now
            yield env.timeout(remaining)
            publish({"event_type": "order_packed"})
            env.stats.task_finished("pack", task_id)
            station.release(request)
            break
        except simpy.Interrupt as interrupt:
            request.cancel()
            station.release(request)
            if since is not None:
                remaining -= env.now - since
            value = {"path_id": path_id, "status": "interrupted"}
            publish({**value, "cause": interrupt.cause})
            env.stats.task_interrupted("pack", task_id)
    env.stats.flow_finished("orders")


def order_generator(env, sampler, station):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack(env, sampler, station, f"orders-{number}", number - 1))


def build(env):
    return DynamicResource(env, "Shop", "station")


# --8<-- [start:processes]
def processes(env, sampler, station):
    """The breakdowns and the arrivals, in the builder's order."""
    return [
        # A SimPy process: down at each failure, repaired after each repair time
        run_breakdowns(env, station, sampler, work="interrupt"),
        order_generator(env, sampler, station),
    ]


# --8<-- [end:processes]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Every 60 seconds: the station's down state, downtime and failures
    env.setup_stats("Shop", "basic", interval=60.0)
    sampler = Sampler(seed=42)
    station = build(env)
    for process in processes(env, sampler, station):
        env.process(process)
    try:
        env.run(until=300)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
