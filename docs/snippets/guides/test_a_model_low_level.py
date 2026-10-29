"""Test a Model, low-level API: the tests of test_a_model.py, run against the
shop written with the environment and the resource.

The tests and the saved results are those of test_a_model.py, so all three forms
give the same numbers. Run them with:
pytest test_a_model_low_level.py
"""

import itertools
import json
import logging
from pathlib import Path

import pytest

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.getLogger("desflow").setLevel(logging.WARNING)

SAVED = Path(__file__).resolve().parent / "test_a_model_saved.json"


# --8<-- [start:shop]
def pack_order(env, packers, task_id):
    key, stats = f"orders-{task_id + 1}", env.stats
    stats.task_queued("pack_order", task_id)
    with packers.request() as unit:
        yield unit
        stats.task_started("pack_order", task_id)
        yield env.timeout(1.5)
        env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)
        stats.task_finished("pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for task_id in itertools.count():
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, task_id))


def results(packers=2, rate=0.5, seed=42) -> dict:
    """Runs ten minutes of the shop and returns the orders packed and the mean wait."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=rate)},
            resources={"packers": CapacityConfig(current_cap=packers, max_cap=10)},
        )
    )
    env.setup_stats("Shop", "typical", interval=600)
    unit = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, unit, Sampler(seed=seed)))
    env.run(until=600)
    readings = env.stats.readings()
    env.teardown()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "finished": readings[name("finished")],
        "wait_mean": round(readings[name("wait_mean")], 6),
    }


# --8<-- [end:shop]


def test_more_packers_shorten_the_wait():
    """A functional test: change one input and check the output moves the right way."""
    waits = [results(packers=packers)["wait_mean"] for packers in (1, 2, 3)]
    assert waits[0] > waits[1] > waits[2]


@pytest.mark.parametrize(
    "settings",
    [
        {"dist": "exponential", "rate": 0},  # no orders ever
        {"dist": "constant", "value": -1},  # a negative time to pack
    ],
)
def test_a_distribution_that_cannot_happen_is_refused(settings):
    """A unit test: one component, alone, refuses an input that means nothing."""
    with pytest.raises(ValueError):
        DistributionConfig(**settings)


def test_results_match_the_saved_run():
    """A back test: the code reproduces what an earlier version of it produced."""
    assert results() == json.loads(SAVED.read_text())


def save_results():
    """Saves the results the back test compares with."""
    SAVED.write_text(json.dumps(results(), indent=2) + "\n")
