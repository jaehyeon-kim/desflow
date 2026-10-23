"""The Fit the Inputs tutorial: which distribution fits the 40 packing times? builder.

Fits the 40 packing times measured at the bench to every family, prints the table
of candidates, then runs the busy hour 40 times, each after a 2-minute warm-up, with
the best fit, with the gamma fit and with the 40 times themselves, and prints the
mean wait of each.
"""

import logging

from desflow import SimulationContext, fit_column, replicate
from measuring_a_run import MEASURED

# The imported model logs at INFO; replications log only their warnings
logging.getLogger("desflow").setLevel(logging.WARNING)


def fitted():
    """Every family fitted to the measured times."""
    # --8<-- [start:fit]
    # The 40 packing times, in seconds; none can be below 0
    result = fit_column(MEASURED, unit="s", lower=0)
    print(result.text())  # every candidate, its status and diagnostics
    # --8<-- [end:fit]
    return result


def shop(packing=None) -> SimulationContext:
    """The busy hour; `packing` is a fit's settings, or None for the 40 times."""
    if packing is None:
        packing = {"dist": "empirical", "values": MEASURED}
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup="2 min")
        .add_resource("packers", current_cap=3, max_cap=10)
        .add_arrival("orders", dist="exponential", rate=1.8)
        # --8<-- [start:service]
        # The best fit's settings: dist="triangular", low, mode and high
        .add_service("packing", **packing)
        # --8<-- [end:service]
        .with_stats("typical")
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def mean_waits(result) -> dict:
    """The mean wait over 40 replications, with each packing distribution."""
    choices = {
        "triangular": result.settings(),
        "gamma": result.settings("gamma"),
        "empirical": None,
    }
    return {
        name: replicate(
            lambda: shop(packing),
            40,
            seed=42,
            until="1 h",
            measures=["tasks.pack_order.wait_mean"],
        ).summary[0]
        for name, packing in choices.items()
    }


if __name__ == "__main__":
    for name, row in mean_waits(fitted()).items():
        print(f"{name}: mean wait {row['mean']:.2f} ± {row['half_width']:.2f} s")
