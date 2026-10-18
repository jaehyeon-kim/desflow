"""The Fit the Inputs tutorial: which distribution fits the 40 packing times? low-level.

Fits the 40 packing times measured at the bench to every family, prints the table
of candidates, then runs the busy hour of Trusting Results 40 times with the best
fit, with the gamma fit and with the 40 times themselves, and prints the mean wait
of each.
"""

import logging

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicResource,
    LowLevelModel,
    SimParameter,
    fit_column,
    replicate,
)
from trusting_results_low_level import MEASURED, WARMUP, order_generator

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


def busy_hour(packing):
    """The busy hour's model, with a packing distribution's settings."""
    # --8<-- [start:service]
    params = SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
        # The best fit's settings: dist="triangular", low, mode and high
        service={"packing": DistributionConfig(**packing)},
        resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
    )
    # --8<-- [end:service]

    def model(env, sampler):
        env.registry.register_sim_parameter(params)
        packers = DynamicResource(env, "Shop", "packers")
        env.process(order_generator(env, sampler, packers))

    return LowLevelModel(model, sim_id="Shop")


def mean_waits(result) -> dict:
    """The mean wait over 40 replications, with each packing distribution."""
    choices = {
        "triangular": result.settings(),
        "gamma": result.settings("gamma"),
        "empirical": {"dist": "empirical", "values": MEASURED},
    }
    return {
        name: replicate(
            busy_hour(packing),
            40,
            seed=42,
            until=3600,
            warmup=WARMUP,
            measures=["tasks.pack_order.wait_mean"],
        ).summary[0]
        for name, packing in choices.items()
    }


if __name__ == "__main__":
    for name, row in mean_waits(fitted()).items():
        print(f"{name}: mean wait {row['mean']:.2f} ± {row['half_width']:.2f} s")
