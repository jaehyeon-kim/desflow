# Test a Model

Write tests for a model that run with pytest and fail when a change breaks it: a functional test of what the model does when an input changes, a unit test of one component on its own, and a back test against results saved from an earlier run. It builds on [Checking a Model](../tutorials/checking-a-model.md), which checks the shop by hand, against observed values and against Little's law, and on [Checking and Validating](../des/checking-and-validating.md), which says what each check proves. The three kinds of test follow the [Tests chapter](https://des.hsma.co.uk/tests.html) of the HSMA book on discrete event simulation. You need pytest: `pip install pytest`.

---

## Build a Model to Test

A test changes one input and runs the model again, so write the model as a function of its inputs and return the numbers a test reads. This shop takes the number of packers, the order rate and the seed, and `results` runs ten minutes of it:

```python title="docs/snippets/guides/test_a_model.py:shop"
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
```

---

## Write a Functional Test

A functional test checks that the model behaves as expected when an input changes. Add packers and the mean wait must fall, so run one, two and three packers and compare the three waits:

```python title="docs/snippets/guides/test_a_model.py:functional"
def test_more_packers_shorten_the_wait():
    """A functional test: change one input and check the output moves the right way."""
    waits = [results(packers=packers)["wait_mean"] for packers in (1, 2, 3)]
    assert waits[0] > waits[1] > waits[2]
```

The test asserts the direction of the change, not the waits themselves. They are **2.055**, **0.111** and **0.016** seconds, and a test of the direction still passes when a change to the model moves them a little.

---

## Write a Unit Test

A unit test checks one component alone. A distribution refuses a rate of 0, which would mean no orders ever, and a negative time to pack, so `DistributionConfig` raises `ValueError` for each. `pytest.mark.parametrize` runs one test for each setting:

```python title="docs/snippets/guides/test_a_model.py:unit"
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
```

---

## Write a Back Test

A back test checks that the code gives the results an earlier version of it gave, with the same inputs and the same seed. Save the results once, and the test compares every later run with the file:

```python title="docs/snippets/guides/test_a_model.py:back"
def test_results_match_the_saved_run():
    """A back test: the code reproduces what an earlier version of it produced."""
    assert results() == json.loads(SAVED.read_text())
```

The saved file holds **319** orders packed and a mean wait of **0.110574** seconds for two packers, written by `save_results`:

```python title="docs/snippets/guides/test_a_model.py:save"
def save_results():
    """Saves the results the back test compares with."""
    SAVED.write_text(json.dumps(results(), indent=2) + "\n")
```

A back test fails on any change to the results, so a failure is either a fault or an intended change. Review the change. If it is intended, run `save_results` and commit the new file with the change that caused it.

---

## Run the Tests

The back test needs the saved results, so write them once from the folder of the file:

```bash
python -c "import test_a_model; test_a_model.save_results()"
```

pytest collects the `test_` functions of a file whose name starts with `test_`:

```bash
pytest test_a_model.py
```

---

## Example

The whole file.

```python title="docs/snippets/guides/test_a_model.py"
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


```
