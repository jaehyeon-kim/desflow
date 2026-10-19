"""The Comparing Designs tutorial: a fourth packer, low-level.

The busy hour of Trusting Results, run 20 times with three packers and 20 times with
four, with the same seeds, and compared seed by seed. The experiment writes its artifacts
and report to results/four-packers, and the script prints the comparison and the
check.
"""

import logging

from desflow import LowLevelModel, run_experiment
from trusting_results_low_level import WARMUP, model

# The imported model logs at INFO; an experiment logs only its warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

EXPERIMENT = {
    "experiment": "four-packers",
    "question": "Does a fourth packer cut the mean wait of the busy hour, and by how much?",
    # --8<-- [start:model]
    # The function that builds the busy hour, and its 2-minute warm-up
    "model": LowLevelModel(model, sim_id="Shop"),
    "warmup": WARMUP,
    "until": 3600,
    "replications": 20,
    "seed": 42,
    # --8<-- [end:model]
    "pairing": "common",
    "measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
    "variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
    "compare": [{"baseline": "three", "variant": "four"}],
    "checks": [
        {"measure": "tasks.pack_order.wait_mean", "variant": "four", "max": 0.5}
    ],
    "assumptions": [
        "Orders arrive at random, at a steady 1.8 a second, through the busy hour.",
        "The 40 packing times measured at the bench stand for every packer.",
    ],
    "inputs": {
        "Shop.service.packing": "40 packing times measured at the bench, in seconds"
    },
    "limits": ["One busy hour only; the rest of the day is not modelled."],
    "output": "results/four-packers",
}


def experiment(pairing="common", output=None):
    """Runs the experiment; independent pairing gives each design seeds of its own."""
    settings = dict(EXPERIMENT, pairing=pairing)
    if pairing == "independent":
        settings["compare"] = []  # a comparison needs the same seeds
    return run_experiment(settings, output=output)


if __name__ == "__main__":
    result = experiment()
    for row in result.comparisons:
        print(f"{row['measure']}: {row['difference']:.2f} ± {row['half_width']:.2f}")
    for row in result.checks:
        print(f"check {row['design']}: {row['value']:.3f} passed={row['passed']}")
