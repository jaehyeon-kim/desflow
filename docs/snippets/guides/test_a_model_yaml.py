"""Test a Model, YAML: the tests of test_a_model.py, run against the blueprint.

test_a_model.yaml reads its inputs from the variables PACKERS, RATE and SEED, so
`shop` sets them and loads the blueprint again. The tests and the saved results are
those of test_a_model.py, so all three forms give the same numbers. Run them with:
pytest test_a_model_yaml.py
"""

import json
import logging
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from desflow import DistributionConfig, SimulationContext

logging.getLogger("desflow").setLevel(logging.WARNING)

HERE = Path(__file__).resolve().parent
SAVED = HERE / "test_a_model_saved.json"
BLUEPRINT = HERE / "test_a_model.yaml"


# --8<-- [start:shop]
def shop(packers=2, rate=0.5, seed=42) -> SimulationContext:
    """The blueprint, with its inputs set through the variables it reads."""
    inputs = {"PACKERS": str(packers), "RATE": str(rate), "SEED": str(seed)}
    with patch.dict(os.environ, inputs):  # restored when the blueprint is loaded
        return SimulationContext.from_yaml(BLUEPRINT)


def results(**inputs) -> dict:
    """Runs ten minutes of the shop and returns the orders packed and the mean wait."""
    app = shop(**inputs)
    app.run()  # until 600, from the blueprint
    readings = app.env.stats.readings()
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
