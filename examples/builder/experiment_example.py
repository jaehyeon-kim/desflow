"""An experiment, builder API: does a third packer keep the mean wait under 2 seconds?

`shop` returns a new shop: two packers, orders every 2.5 seconds on average and a
packing time of 4 seconds on average. The experiment runs one hour of it 20 times with
two packers and 20 times with three, with the same seeds, after a 3-minute warm-up.
It compares the two designs seed by seed, checks the three-packer design against a
2-second bound, and compares the two-packer design with the mean wait observed in
the real shop. It writes its artifacts and report to results/packers beside this
file, and prints the comparison and the check.

Needs no containers.
"""

import logging
from pathlib import Path

from desflow import SimulationContext, run_experiment

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
HERE = Path(__file__).resolve().parent


def shop() -> SimulationContext:
    """A new shop: the experiment calls this once per run."""
    app = (
        # The experiment gives each run its own seed; 42 is for a run on its own
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=2, max_cap=4)
        # A mean of 4 seconds (shape times scale)
        .add_service("packing", dist="gamma", shape=4, scale=1)
        .add_arrival("orders", dist="exponential", rate=0.4)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id, context):
        """Packs one order."""
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_loop(context):
        """Spawns a pack task for every order."""
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack(task_id, context, entity=entity))
            task_id += 1

    return app


EXPERIMENT = {
    "experiment": "packers",
    "question": "Does a third packer keep the mean wait under 2 seconds?",
    "model": shop,
    "assumptions": [
        "Orders arrive at random, at a steady rate, through the hour.",
        "Every packer works at the same speed.",
    ],
    "inputs": {"Shop.service.packing": "40 packing times measured at the bench"},
    "until": "1 h",
    "warmup": "3 min",
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
