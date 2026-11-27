"""Does a second packer cut the mean wait of the drafted shop, and by how much?

The Builder form of drafted_shop_experiment.yaml: it gives the same comparison. Keep it
beside drafted_shop_builder.py. Run it with: python drafted_shop_experiment_builder.py
"""

import logging
from pathlib import Path

from desflow import SimulationContext, load_context, run_experiment

# The model logs at INFO; an experiment logs only its warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

HERE = Path(__file__).resolve().parent


def shop() -> SimulationContext:
    """A new drafted shop for each run, built from the builder file."""
    return load_context(HERE, "drafted_shop_builder.py")


EXPERIMENT = {
    "experiment": "second-packer",
    "question": "Does a second packer cut the mean wait of an order, and by how much?",
    # --8<-- [start:study]
    "model": shop,
    "until": "1 h",
    "replications": 20,
    "seed": 42,
    # Every design runs with the same 20 seeds, so they are compared seed by seed
    "pairing": "common",
    "measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
    "variants": {
        "one": {},
        "two": {
            "Shop.resources.packers.max_cap": 2,
            "Shop.resources.packers.current_cap": 2,
        },
    },
    "compare": [{"baseline": "one", "variant": "two"}],
    # --8<-- [end:study]
    "assumptions": [
        "Orders arrive at random, at a steady one every 2 seconds.",
        "Every order takes 1.5 seconds to pack.",
    ],
    "limits": ["Each run starts with an empty shop and has no warm-up."],
    "output": "results/second-packer",
}

if __name__ == "__main__":
    result = run_experiment(EXPERIMENT)
    for row in result.comparisons:
        print(f"{row['measure']}: {row['difference']:.3f} ± {row['half_width']:.4f}")
