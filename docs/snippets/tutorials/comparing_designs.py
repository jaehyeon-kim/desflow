"""The Comparing Designs tutorial: does a fourth packer cut the mean wait? builder.

The busy hour of Trusting Results, run 20 times with three packers and 20 times with
four, with the same seeds, and compared seed by seed. The study writes its artifacts
and report to results/four-packers, and the script prints the comparison and the
check: python comparing_designs.py 2>/dev/null shows only those lines.
"""

import logging

from desflow import run_study
from trusting_results import shop

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)

STUDY = {
    "study": "four-packers",
    "question": "Does a fourth packer cut the mean wait of the busy hour, and by how much?",
    # --8<-- [start:model]
    # A function returning a new busy hour, with its 2-minute warm-up
    "model": shop,
    "until": "1 h",
    "replications": 20,
    "seed": 42,
    # --8<-- [end:model]
    # --8<-- [start:compare]
    # Every design runs with the same 20 seeds, so they are compared seed by seed
    "pairing": "common",
    "measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
    "variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
    "compare": [{"baseline": "three", "variant": "four"}],
    # --8<-- [end:compare]
    # --8<-- [start:checks]
    # The upper end of the interval must be under half a second
    "checks": [
        {"measure": "tasks.pack_order.wait_mean", "variant": "four", "max": 0.5}
    ],
    # --8<-- [end:checks]
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


def study(pairing="common", output=None):
    """Runs the study; independent pairing gives each design seeds of its own."""
    settings = dict(STUDY, pairing=pairing)
    if pairing == "independent":
        settings["compare"] = []  # a comparison needs the same seeds
    return run_study(settings, output=output)


if __name__ == "__main__":
    result = study()
    for row in result.comparisons:
        print(f"{row['measure']}: {row['difference']:.2f} ± {row['half_width']:.2f}")
    for row in result.checks:
        print(f"check {row['design']}: {row['value']:.3f} passed={row['passed']}")
