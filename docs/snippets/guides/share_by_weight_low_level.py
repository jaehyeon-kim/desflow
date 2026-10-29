"""Share Work by Weight, low-level API: one packer shared between two tiers of
orders, 3 to 1.

The low-level version of share_by_weight.py: nine orders at time 0, the first of
tier 2, then six of tier 1, then two more of tier 2. `low_level_form` returns the
tier of each order in the order the packer granted them.
"""

import logging
import sys
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    SimParameter,
)

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # share_by_weight_logic.py is beside the blueprint
from share_by_weight_logic import WeightedPackers  # noqa: E402

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)
# The tier of each order, in the order the orders arrive
TIERS = [2, 1, 1, 1, 1, 1, 1, 2, 2]


def low_level_form():
    """The same shop with the environment and the resource."""
    # --8<-- [start:low-level]
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            service={"packing": DistributionConfig(dist="constant", value=1.0)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
        )
    )
    packers = WeightedPackers(env, "Shop", "packers", queue="priority")

    def order(env, tier):
        # The tier is the priority of the request
        with packers.request(priority=tier) as request:
            yield request
            yield env.timeout(1.0)

    for tier in TIERS:
        env.process(order(env, tier))
    # --8<-- [end:low-level]
    env.run(until=20)
    return packers.granted


if __name__ == "__main__":
    print(f"{'low_level_form':15} {low_level_form()}")
