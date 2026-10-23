"""Test a Model: functional, unit and back tests of the shop, with pytest.

The shop has packers, exponential orders at 0.5 a second and 1.5 seconds of packing.
Write the saved results once with `save_results()`, then run the tests with:
pytest test_a_model.py
`save_results()` writes test_a_model_saved.json, the results the back test compares
with. Run it again only when a change to the results is intended.
"""

import json
import logging
from pathlib import Path

import pytest

from desflow import DistributionConfig, SimulationContext

logging.getLogger("desflow").setLevel(logging.WARNING)

SAVED = Path(__file__).resolve().parent / "test_a_model_saved.json"


# --8<-- [start:shop]
def shop(packers=2, rate=0.5, seed=42) -> SimulationContext:
    """The shop with `packers` packers and orders at `rate` a second."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=seed)
        .add_resource("packers", current_cap=packers, max_cap=10)
        .add_arrival("orders", dist="exponential", rate=rate)
        .add_service("packing", dist="constant", value=1.5)
        .with_stats("typical", interval=600)
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


def results(**inputs) -> dict:
    """Runs ten minutes of the shop and returns the orders packed and the mean wait."""
    app = shop(**inputs)
    app.run(until=600)
    readings = app.env.stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "finished": readings[name("finished")],
        "wait_mean": round(readings[name("wait_mean")], 6),
    }


# --8<-- [end:shop]


# --8<-- [start:functional]
def test_more_packers_shorten_the_wait():
    """A functional test: change one input and check the output moves the right way."""
    waits = [results(packers=packers)["wait_mean"] for packers in (1, 2, 3)]
    assert waits[0] > waits[1] > waits[2]


# --8<-- [end:functional]


# --8<-- [start:unit]
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


# --8<-- [end:unit]


# --8<-- [start:back]
def test_results_match_the_saved_run():
    """A back test: the code reproduces what an earlier version of it produced."""
    assert results() == json.loads(SAVED.read_text())


# --8<-- [end:back]


# --8<-- [start:save]
def save_results():
    """Saves the results the back test compares with."""
    SAVED.write_text(json.dumps(results(), indent=2) + "\n")


# --8<-- [end:save]
