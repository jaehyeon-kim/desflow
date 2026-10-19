"""An experiment, low-level API: does a third packer keep the mean wait under 2 seconds?

The low-level twin of `builder/experiment_example.py`. `model` builds the shop in an
environment and with a sampler it is given; `LowLevelModel` names it with its sim
id. The experiment runs one hour of it 20 times with two packers and 20 times with three,
with the same seeds, after a 3-minute warm-up. It compares the two designs seed by
seed, checks the three-packer design against a 2-second bound, and compares the
two-packer design with the mean wait observed in the real shop. It writes its
artifacts and report to results/packers beside this file, and prints the
comparison and the check.

Needs no containers.
"""

import logging
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicResource,
    LowLevelModel,
    SimParameter,
    run_experiment,
)

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)
HERE = Path(__file__).resolve().parent

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={"packing": DistributionConfig(dist="gamma", shape=4, scale=1)},
    resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
)


def pack(env, sampler, packers, key, task_id):
    """The task lifecycle, reported to the statistics at each step."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    env.stats.flow_started("orders")  # the order enters the shop
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    with packers.request() as req:
        yield req
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack", task_id)
    env.stats.flow_finished("orders")


def order_generator(env, sampler, packers):
    """Spawns a pack task for every order, keyed orders-1, orders-2 and so on."""
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def build(env):
    """Builds the packers."""
    return DynamicResource(env, "Shop", "packers")


def processes(env, sampler, packers):
    """The orders."""
    return [order_generator(env, sampler, packers)]


def model(env, sampler):
    """The whole shop in a given environment: the experiment sets up the statistics."""
    env.registry.register_sim_parameter(PARAMS)
    for process in processes(env, sampler, build(env)):
        env.process(process)


EXPERIMENT = {
    "experiment": "packers",
    "question": "Does a third packer keep the mean wait under 2 seconds?",
    "model": LowLevelModel(model, sim_id="Shop"),
    "assumptions": [
        "Orders arrive at random, at a steady rate, through the hour.",
        "Every packer works at the same speed.",
    ],
    "inputs": {"Shop.service.packing": "40 packing times measured at the bench"},
    "until": 3600,
    "warmup": 180,
    "replications": 20,
    "seed": 42,
    # --8<-- [start:compare]
    # Both designs run with the same 20 seeds, so they are compared seed by seed
    "pairing": "common",
    "measures": ["tasks.pack.wait_mean", "resources.packers.utilization_avg"],
    "variants": {"two": {}, "three": {"Shop.resources.packers.current_cap": 3}},
    "compare": [{"baseline": "two", "variant": "three"}],
    # The upper end of the interval must be under 2 seconds
    "checks": [{"measure": "tasks.pack.wait_mean", "variant": "three", "max": 2}],
    # The mean wait measured in the real shop, which has two packers
    "observed": {"file": str(HERE.parent / "observed_waits.csv"), "variant": "two"},
    # --8<-- [end:compare]
    "limits": ["One hour of steady demand; the rest of the day is not modelled."],
    "output": str(HERE / "results" / "packers"),
}


def run(output=None):
    """Runs the experiment and prints the comparison and the check."""
    result = run_experiment(EXPERIMENT, output=output)
    for row in result.comparisons:
        print(f"{row['measure']}: {row['difference']:.2f} ± {row['half_width']:.2f}")
    for row in result.checks:
        print(f"check {row['design']}: {row['value']:.3f} passed={row['passed']}")
    print(f"report: {result.output / 'report.md'}")
    return result


if __name__ == "__main__":
    run()
