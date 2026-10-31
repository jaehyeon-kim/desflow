"""The Checking a Model tutorial: checks of the busy hour, written as tests, low-level.

A run of the shop with fixed inputs is compared with a trace worked by hand. An
experiment then runs the busy hour of Trusting Results with one, three and ten packers
and with four packers at a higher rate, checks each against a bound, and places the
three-packer design beside the values observed in the real shop. Run the checks with:
pytest checking_a_model_low_level.py
"""

import logging
import statistics
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    LowLevelModel,
    Sampler,
    SimParameter,
    run_experiment,
)
from trusting_results_low_level import WARMUP, model, order_generator

# The imported model logs at INFO; the checks log only their warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

HERE = Path(__file__).resolve().parent

# --8<-- [start:fixed]
# Three packers; an order arrives every second and takes 4 seconds to pack
FIXED = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="constant", value=1)},
    service={"packing": DistributionConfig(dist="constant", value=4)},
    resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
)
# --8<-- [end:fixed]


def fixed_run() -> dict:
    """The statistics of the fixed shop after 10 seconds, without the Shop.stats. prefix."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(FIXED)
    env.setup_stats("Shop", "typical", interval=3600)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, Sampler(seed=42), packers))
    try:
        env.run(until=10)
    finally:
        env.teardown()
    readings = env.stats.readings()
    return {key.removeprefix("Shop.stats."): value for key, value in readings.items()}


EXPERIMENT = {
    "experiment": "checking-a-model",
    "question": "Is the busy hour model adequate for choosing the number of packers?",
    # --8<-- [start:model]
    # The function that builds the busy hour, and its 2-minute warm-up
    "model": LowLevelModel(model, sim_id="Shop"),
    "warmup": WARMUP,
    "until": 3600,
    "replications": 20,
    "seed": 42,
    "measures": [
        "tasks.pack_order.wait_mean",
        "resources.packers.utilization_avg",
        "resources.packers.queue_length_avg",
    ],
    # --8<-- [end:model]
    "variants": {
        "three": {},
        "one": {"Shop.resources.packers.current_cap": 1},
        "ten": {"Shop.resources.packers.current_cap": 10},
        "four-busier": {
            "Shop.resources.packers.current_cap": 4,
            "Shop.arrival.orders.rate": 1.9,
        },
    },
    "checks": [
        {
            "measure": "resources.packers.utilization_avg",
            "variant": "one",
            "min": 99.9,
        },
        {"measure": "tasks.pack_order.wait_mean", "variant": "ten", "max": 0.01},
        {
            "measure": "tasks.pack_order.wait_mean",
            "variant": "four-busier",
            "max": 0.5,
        },
    ],
    # Illustrative values for the real shop, which has three packers;
    # a real check reads the shop's own records
    "observed": {
        "file": str(HERE / "checking_a_model_observed.csv"),
        "variant": "three",
    },
    "output": "results/checking-a-model",
}


def experiment(output=None):
    """Runs the checks; raises CheckFailed when one fails, after writing every file."""
    return run_experiment(EXPERIMENT, output=output)


HORIZON = 3600 - WARMUP  # the measured part of the hour, in seconds


def littles_law(result, design):
    """The mean queue, and the arrival rate and mean wait whose product it should be."""
    means = {r["statistic"]: r["mean"] for r in result.summary if r["design"] == design}
    rate = means["flows.orders.started"] / HORIZON
    return (
        means["resources.packers.queue_length_avg"],
        rate,
        means["tasks.pack_order.wait_mean"],
    )


# --8<-- [start:agreement]
def agreement(result, design="three"):
    """For each observed value: the model's mean, the standard deviation of one run,
    the distance in standard deviations, and how many of the runs lie inside the
    interval for the mean."""
    rows = []
    for row in result.observed:
        runs = [
            r["value"]
            for r in result.results
            if r["design"] == design and r["statistic"] == row["measure"]
        ]
        mean, sd = statistics.mean(runs), statistics.stdev(runs)
        inside = sum(row["low"] <= value <= row["high"] for value in runs)
        rows.append(
            (row["measure"], mean, sd, abs(row["value"] - mean) / sd, inside, len(runs))
        )
    return rows


# --8<-- [end:agreement]


def test_fixed_inputs_match_the_hand_trace():
    readings = fixed_run()
    assert readings["tasks.pack_order.finished"] == 4
    assert readings["tasks.pack_order.in_progress"] == 5
    assert readings["tasks.pack_order.wait_mean"] == 5 / 7
    assert readings["tasks.pack_order.wait_max"] == 2


def test_extremes_sensitivity_and_littles_law(tmp_path):
    result = experiment(output=tmp_path)  # a failed check raises CheckFailed
    queue, rate, wait = littles_law(result, "three")
    assert abs(queue - rate * wait) < 0.05 * queue


if __name__ == "__main__":
    print(fixed_run())
    result = experiment()
    means = {(r["design"], r["statistic"]): r["mean"] for r in result.summary}
    for design in ("one", "ten"):
        print(
            f"{design}: {means[design, 'tasks.pack_order.finished']:.0f} finished, "
            f"utilisation {means[design, 'resources.packers.utilization_avg']:.2f}%, "
            f"mean wait {means[design, 'tasks.pack_order.wait_mean']:.2f} s"
        )
    for design in ("three", "one"):
        queue, rate, wait = littles_law(result, design)
        print(
            f"{design}: queue {queue:.3f}, {rate:.4f} x {wait:.3f} = {rate * wait:.3f}"
        )
    for row in result.observed:
        print(f"observed {row['measure']}: {row['value']} inside={row['inside']}")
    for measure, mean, sd, distance, inside, runs in agreement(result):
        print(
            f"{measure}: mean {mean:.2f}, sd {sd:.2f}, distance {distance:.2f} sd, "
            f"{inside} of {runs} runs inside"
        )
