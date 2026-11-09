"""The Test a Pipeline page, low-level form: the shop with a planted fault. Save it as
pipeline_shop.py for the tests."""

import itertools
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    JsonlStorageEgress,
    Sampler,
    SimParameter,
    run_scenario,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=3, max_cap=5)},
)

# The fault: the order rate triples at 300 seconds, and the label says so
STEPS = [
    {
        "at": 300,
        "path": "Shop.arrival.orders.rate",
        "value": 1.5,
        "record": {"fault": "order_surge"},
    },
    # The end of the fault, which needs no label
    {"at": 360, "path": "Shop.arrival.orders.rate", "value": 0.5},
]


def pack_order(env, packers, sampler, key, task_id):
    with packers.request() as unit:
        yield unit
        yield env.timeout(
            sampler.sample(env.registry.get_config("Shop.service.packing"))
        )
        env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


# --8<-- [start:shop]
def run_shop(folder: Path) -> None:
    """Runs the shop, writing its data and labels under `folder`."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(
        [
            # The data under test: events only, with no label
            JsonlStorageEgress(default_path=f"{folder}/data/events.jsonl"),
            # The labels only, for the scorer
            JsonlStorageEgress(default_path=f"{folder}/labels/labels.jsonl"),
        ],
        predicates=[lambda record: record["stream_type"] == "event", None],
        labels=[False, True],
    )
    packers = DynamicResource(env, "Shop", "packers")
    env.process(run_scenario(env, STEPS, until=600))
    env.process(orders(env, packers, Sampler(seed=42)))
    try:
        env.run(until=600)
    finally:
        env.teardown()


# --8<-- [end:shop]
