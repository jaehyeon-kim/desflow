"""The Parameter Sweeps tutorial: how much does the mean wait depend on demand? builder.

The busy hour of Trusting Results with three packers and with four, each at four
arrival rates, run 20 times with the same seeds. The study writes its artifacts and
report to results/demand, and the script prints each design's mean wait:
python parameter_sweeps.py 2>/dev/null shows only those lines.
"""

import logging

from desflow import run_study
from trusting_results import shop

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)

STUDY = {
    "study": "demand",
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


def study(output=None):
    """Runs every design of the sweep."""
    return run_study(STUDY, output=output)


if __name__ == "__main__":
    for row in study().summary:
        if row["statistic"] == "tasks.pack_order.wait_mean":
            print(f"{row['design']}: {row['mean']:.2f} ± {row['half_width']:.2f} s")
