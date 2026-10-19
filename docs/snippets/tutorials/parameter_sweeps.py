"""The Parameter Sweeps tutorial: the wait at four arrival rates, builder.

The busy hour of Trusting Results with three packers and with four, each at four
arrival rates, run 20 times with the same seeds. The experiment writes its artifacts and
report to results/demand, and the script prints each design's mean wait.
"""

import logging

from desflow import run_experiment
from trusting_results import shop

# The imported model logs at INFO; an experiment logs only its warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

EXPERIMENT = {
    "experiment": "demand",
    "question": "How sensitive is the busy hour's mean wait to the arrival rate?",
    "model": shop,
    "until": "1 h",
    "replications": 20,
    "seed": 42,
    "measures": ["tasks.pack_order.wait_mean"],
    # --8<-- [start:sweep]
    "variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
    # Each variant at each rate: 8 designs, all run with the same 20 seeds
    "sweep": {"Shop.arrival.orders.rate": [1.6, 1.7, 1.8, 1.9]},
    # --8<-- [end:sweep]
    "output": "results/demand",
}


def experiment(output=None):
    """Runs every design of the sweep."""
    return run_experiment(EXPERIMENT, output=output)


if __name__ == "__main__":
    for row in experiment().summary:
        if row["statistic"] == "tasks.pack_order.wait_mean":
            print(f"{row['design']}: {row['mean']:.2f} ± {row['half_width']:.2f} s")
