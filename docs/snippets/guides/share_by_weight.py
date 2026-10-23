"""Share by Weight: one packer shared between two tiers of orders, 3 to 1.

Nine orders arrive at time 0: the first is tier 2, then six of tier 1, then two more
of tier 2. The first takes the packer and the rest wait. The packer grants the
waiting orders by weight, three of tier 1 for each of tier 2, in YAML, in the
builder and in the low-level API. Each function returns the tier of each order in
the order the packer granted them.
"""

import logging
import sys
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    SimParameter,
    SimulationContext,
)

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # share_by_weight_logic.py is beside the blueprint
from share_by_weight_logic import WeightedPackers  # noqa: E402

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)
# The tier of each order, in the order the orders arrive
TIERS = [2, 1, 1, 1, 1, 1, 1, 2, 2]


def yaml_form():
    """The blueprint beside the class."""
    app = SimulationContext.from_yaml(HERE / "share_by_weight.yaml")
    app.run()
    return app.get_resource("packers").granted


def builder_form():
    """The same shop with the builder."""
    # --8<-- [start:builder]
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_service("packing", dist="constant", value=1.0)
        .add_arrival_replay(
            "orders", [{"time": 0, "tier": tier} for tier in TIERS], fields=["tier"]
        )
    )
    # queue="priority" lets a task set a priority; the class orders the line itself
    app.add_resource(
        "packers", current_cap=1, max_cap=4, queue="priority", type=WeightedPackers
    )

    # The tier of the order is the class of its request
    @app.task(service_id="packing", resource_id="packers", priority="attr.tier")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    # --8<-- [end:builder]
    app.run(until=20)
    return app.get_resource("packers").granted


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
    for form in (yaml_form, builder_form, low_level_form):
        print(f"{form.__name__:15} {form()}")
