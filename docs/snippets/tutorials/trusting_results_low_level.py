"""The Trusting Results tutorial: the busy hour, measured after a warm-up, low-level.

Three packers pack orders that arrive at 1.8 a second on average, with the packing
times measured at the bench. Every record is printed to the terminal. At the end the
script prints the mean wait without and with the warm-up, the mean wait of 5 runs
after each of three candidate warm-ups, then the mean and half-width of two outputs
over 5, 10, 20 and 40 replications. To see only those lines, run:
python trusting_results_low_level.py 2>/dev/null
With the argument start, it runs the first 15 minutes without a warm-up, to plot:
python trusting_results_low_level.py start 2>&1
"""

import logging
import sys

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    LowLevelModel,
    Sampler,
    SimParameter,
    replicate,
)
from desflow.analysis import interval

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# The 40 packing times measured at the bench, in seconds
MEASURED = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
    service={"packing": DistributionConfig(dist="empirical", values=MEASURED)},
    resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
)


def pack_order(env, sampler, packers, key, task_id):
    """One order, which reports each step to the statistics."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.flow_started("orders")  # the order enters the shop
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
    env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")  # the order leaves the shop


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


# --8<-- [start:warmup]
# Records and statistics start after the first 2 minutes; None keeps everything
WARMUP = 120.0  # seconds, passed to setup_egress in run below
# --8<-- [end:warmup]


def run(warmup=WARMUP, until=3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    # --8<-- [start:plot]
    # Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
    env.setup_stats("Shop", "typical", interval=10)
    # The warm-up is set with the sinks: no sink receives a record stamped before it
    # ends, and the statistics start again from zero when it ends
    env.setup_egress([ConsoleEgress()], warmup=warmup)
    # --8<-- [end:plot]
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, Sampler(seed=42), packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def mean_wait(warmup) -> float:
    """Runs the busy hour and returns the mean wait it measured."""
    return run(warmup).stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


# --8<-- [start:replicate]
MEASURES = ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"]


def model(env, sampler):
    """The busy hour in a given environment: replicate sets the statistics."""
    env.registry.register_sim_parameter(PARAMS)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, sampler, packers))


def replications(n=40):
    """Runs the busy hour n times, each after the warm-up, with seeds spawned from 42."""
    shop = LowLevelModel(model, sim_id="Shop")
    return replicate(shop, n, seed=42, until=3600, warmup=WARMUP, measures=MEASURES)


# --8<-- [end:replicate]
# --8<-- [start:warmups]
def warmups(n=5) -> dict:
    """The mean wait of n runs after each candidate warm-up, with seeds spawned from 42."""
    shop = LowLevelModel(model, sim_id="Shop")
    wait = ["tasks.pack_order.wait_mean"]
    return {
        warmup: replicate(shop, n, seed=42, until=3600, warmup=warmup, measures=wait)
        for warmup in (60, WARMUP, 240)
    }


# --8<-- [end:warmups]
def precision(result) -> dict:
    """Each output's mean and half-width over the first 5, 10, 20 and 40 replications."""
    return {
        (measure, n): interval(result.values[measure][:n])
        for measure in MEASURES
        for n in (5, 10, 20, 40)
    }


WAIT = "Shop.stats.tasks.pack_order.wait_mean"
PACKED = "Shop.stats.tasks.pack_order.finished"
QUEUE = "Shop.stats.resources.packers.queue_length_avg"


def report() -> None:
    """Prints the mean waits, the Little's law figures and the precision."""
    print(f"without a warm-up: mean wait {mean_wait(None):.3f} s")
    warmed = run(WARMUP).stats.readings()
    print(f"with a 2 min warm-up: mean wait {warmed[WAIT]:.3f} s")
    print(
        f"after the warm-up: {warmed[PACKED]} orders packed, "
        f"queue_length_avg {warmed[QUEUE]:.2f}"
    )
    for warmup, result in warmups().items():
        row = result.summary[0]
        print(
            f"warm-up {warmup:g} s: mean wait {row['mean']:.3f} ± {row['half_width']:.2f} s"
        )
    for (measure, n), result in precision(replications()).items():
        share = result.half_width / result.mean
        print(
            f"{measure}, {n} runs: {result.mean:.2f} ± {result.half_width:.2f} ({share:.1%})"
        )


if __name__ == "__main__":
    if sys.argv[1:] == ["start"]:
        # The statistics every 10 seconds of the first 15 minutes, without a warm-up
        run(None, until=900)
    else:
        report()
