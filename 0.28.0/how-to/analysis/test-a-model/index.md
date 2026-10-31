# Test a Model

Write tests for a model that run with pytest and fail when a change breaks it. This page writes three kinds: a functional test of what the model does when an input changes, a unit test of one component on its own, and a back test against results saved from an earlier run.

______________________________________________________________________

## Before You Start

- [Checking a Model](https://jaehyeon.me/desflow/latest/learn/studies/checking-a-model/index.md) checks the shop by hand, against observed values and against [Little's law](https://jaehyeon.me/desflow/latest/learn/concepts/queueing-basics/#littles-law).
- [Checking and Validating](https://jaehyeon.me/desflow/latest/learn/concepts/checking-and-validating/index.md) says what each check proves.
- The three kinds of test follow the [Tests chapter](https://des.hsma.co.uk/tests.html) of the HSMA book on discrete event simulation.
- You need pytest: `pip install pytest`.

______________________________________________________________________

## Steps

### Build a Model to Test

A test changes one input and runs the model again, so write the model as a function of its inputs and return the numbers a test reads. This shop takes the number of packers, the order rate and the seed. `results` runs ten minutes of it. A blueprint reads its inputs from variables, so the YAML form sets them before it loads the blueprint:

```yaml
simulation:
  sim_id: Shop
  factor: 0
  random_seed: ${SEED:-42}

resources:
  packers: {current_cap: "${PACKERS:-2}", max_cap: 10}

arrivals:
  orders: {dist: exponential, rate: "${RATE:-0.5}", spawn: pack_order}

services:
  packing: {dist: constant, value: 1.5}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

stats: {level: typical, interval: 600}

run:
  until: 600
```

`shop` sets the variables and loads the blueprint, and `results` runs it:

```python
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
```

```python
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

```python
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
```

### Write a Functional Test

The tests call only `results`, so they are the same in the file of every form. A functional test checks that the model behaves as expected when an input changes. Add packers and the mean wait must fall, so run one, two and three packers and compare the three waits:

```python
def test_more_packers_shorten_the_wait():
    """A functional test: change one input and check the output moves the right way."""
    waits = [results(packers=packers)["wait_mean"] for packers in (1, 2, 3)]
    assert waits[0] > waits[1] > waits[2]
```

The test asserts the direction of the change, not the waits themselves. The waits are **2.055**, **0.111** and **0.016** seconds, from one 10-minute run each of this shop, not the busy hour of the tutorials. A test of the direction still passes when a change to the model moves them a little.

### Write a Unit Test

A unit test checks one component alone. A distribution refuses a rate of 0, which would mean no orders ever. It also refuses a negative time to pack. `DistributionConfig` raises `ValueError` for each. `pytest.mark.parametrize` runs one test for each setting:

```python
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

### Write a Back Test

A back test checks that the code gives the results an earlier version of it gave, with the same inputs and the same seed. Save the results once, and the test compares every later run with the file:

```python
def test_results_match_the_saved_run():
    """A back test: the code reproduces what an earlier version of it produced."""
    assert results() == json.loads(SAVED.read_text())
```

The saved file holds **319** orders packed and a mean wait of **0.110574** seconds for two packers, written by `save_results`:

```python
def save_results():
    """Saves the results the back test compares with."""
    SAVED.write_text(json.dumps(results(), indent=2) + "\n")
```

A back test fails on any change to the results, so a failure is either a fault or an intended change. Review the change. If it is intended, run `save_results` and commit the new file with the change that caused it.

______________________________________________________________________

## Check the Result

Save the files of your form from [Full Files](#full-files) in a folder of their own, each under the last part of its title, such as `test_a_model.yaml`. The back test needs the saved results, so write them once from that folder. pytest then collects the `test_` functions of a file whose name starts with `test_`. The three forms give the same results, so each passes the same four tests:

The files are `test_a_model_yaml.py` and `test_a_model.yaml`:

```bash
python -c "import test_a_model_yaml; test_a_model_yaml.save_results()"
pytest test_a_model_yaml.py
```

```text
test_a_model_yaml.py ....                                                [100%]

============================== 4 passed in 0.32s ===============================
```

The file is `test_a_model.py`:

```bash
python -c "import test_a_model; test_a_model.save_results()"
pytest test_a_model.py
```

```text
test_a_model.py ....                                                     [100%]

============================== 4 passed in 0.14s ===============================
```

The file is `test_a_model_low_level.py`:

```bash
python -c "import test_a_model_low_level; test_a_model_low_level.save_results()"
pytest test_a_model_low_level.py
```

```text
test_a_model_low_level.py ....                                           [100%]

============================== 4 passed in 0.27s ===============================
```

The times differ from run to run.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Test a Model guide: the shop that test_a_model_yaml.py tests. Its inputs are
# variables, so a test sets PACKERS, RATE and SEED and loads it again; each has the
# value the tests use by default.
simulation:
  sim_id: Shop
  factor: 0
  random_seed: ${SEED:-42}

resources:
  packers: {current_cap: "${PACKERS:-2}", max_cap: 10}

arrivals:
  orders: {dist: exponential, rate: "${RATE:-0.5}", spawn: pack_order}

services:
  packing: {dist: constant, value: 1.5}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

stats: {level: typical, interval: 600}

run:
  until: 600
```

```python
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

```python
"""Test a Model: functional, unit and back tests of the shop, with pytest.

The shop has packers, an order every 2 seconds on average (exponential, rate 0.5)
and 1.5 seconds of packing.
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

```python
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
