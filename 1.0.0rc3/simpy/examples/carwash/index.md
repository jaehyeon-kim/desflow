# Carwash

Cars take a washing machine and keep it while the wash runs. This is SimPy's Carwash example, written in desflow.

______________________________________________________________________

## Before You Start

SimPy's carwash has two machines. Four cars are waiting when it opens, and then a car arrives every 5 to 9 minutes. A wash takes 5 minutes and removes 50 to 99 percent of the dirt. The run lasts 20 minutes. desflow reads each of SimPy's minutes as one second, so the run lasts 20 seconds.

The example needs only desflow, which each Quick Start in [Run It](#run-it) installs, and no other service. `desflow examples get simpy-carwash --api yaml` downloads it, as [Get an Example](https://jaehyeon.me/desflow/latest/cli/#get-an-example) shows. [SimPy Examples](https://jaehyeon.me/desflow/latest/simpy/examples/index.md) says how these pages read SimPy's times and numbers. SimPy's own page for the example is [on its documentation site](https://simpy.readthedocs.io/en/4.1.2/examples/carwash.html).

______________________________________________________________________

## Steps

### Machines Wash

SimPy's `Carwash` class holds the machines and has a `wash` method that takes the wash time and removes dirt.

```python
class Carwash:
    """A carwash has a limited number of machines (``NUM_MACHINES``) to
    clean cars in parallel.

    Cars have to request one of the machines. When they got one, they
    can start the washing processes and wait for it to finish (which
    takes ``washtime`` minutes).

    """

    def __init__(self, env, num_machines, washtime):
        self.env = env
        self.machine = simpy.Resource(env, num_machines)
        self.washtime = washtime

    def wash(self, car):
        """The washing processes. It takes a ``car`` processes and tries
        to clean it."""
        yield self.env.timeout(self.washtime)
        pct_dirt = random.randint(50, 99)
        print(f"Carwash removed {pct_dirt}% of {car}'s dirt.")
```

`machine` is a resource with two units, and `wash` is a service that takes 5 seconds. The `wash` task is a task of its own: it takes the service time and publishes the dirt removed, an integer from 50 to 99, in `dirt_removed_pct`.

```yaml
resources:
  machine: {current_cap: 2}

services:
  wash: {dist: constant, value: 5}
```

```yaml
wash:
  service: wash
  payload:
    event_type: washed
    dirt_removed_pct: {gen: integer, bounds: [50, 99]}
```

`add_resource` and `add_service` declare the machines and the wash time, and the `wash` task publishes `dirt_removed_pct` from a template.

```python
app.add_resource("machine", current_cap=2)
app.add_service("wash", dist="constant", value=5)
```

```python
wash_payload = Template(
    {
        "event_type": "washed",
        "dirt_removed_pct": {"gen": "integer", "bounds": [50, 99]},
    },
    # The name gives the random sequence the YAML version's payload draws from
    name="tasks.wash",
)

@app.task(name="wash", service_id="wash")
def wash(task_id):
    """Washes the car."""
    return wash_payload
```

The low-level version has SimPy's `simpy.Resource` for the machines and a `wash` function that draws the dirt removed from desflow's `Sampler`.

```python
sampler = Sampler(seed=42)
gaps = sampler.generator("arrivals.cars.gap")
dirt = sampler.generator("tasks.wash.payload.dirt_removed_pct")
machine = simpy.Resource(env, NUM_MACHINES)
numbers = {task: itertools.count() for task in ("car", "washing", "wash")}
```

```python
def wash(name):
    number = next(numbers["wash"])
    publish(
        name,
        "wash",
        number,
        {
            "path_id": WASH,
            "sim_id": "Carwash",
            "service": "wash",
            "status": "started",
        },
    )
    yield env.timeout(WASHTIME)
    removed = int(dirt.integers(50, 100))
    publish(
        name, "wash", number, {"event_type": "washed", "dirt_removed_pct": removed}
    )
```

### Car Is Washed

SimPy's `car` requests a machine, keeps it while the wash runs, and then leaves.

```python
def car(env, name, cw):
    """The car process (each car has a ``name``) arrives at the carwash
    (``cw``) and requests a cleaning machine.

    It then starts the washing process, waits for it to finish and
    leaves to never come back ...

    """
    print(f'{name} arrives at the carwash at {env.now:.2f}.')
    with cw.machine.request() as request:
        yield request

        print(f'{name} enters the carwash at {env.now:.2f}.')
        yield env.process(cw.wash(name))

        print(f'{name} leaves the carwash at {env.now:.2f}.')
```

`car` takes a machine with `seize: machine`, which keeps the unit after the task ends. `washing` waits for the `wash` task to finish with `wait: {task: wash}`, and then gives the machine back with `release: machine`.

```yaml
tasks:
  car:
    seize: machine
    payload: {event_type: enters}
    next: washing
  # The car keeps its machine until the wash, a task of its own, has finished
  washing:
    wait: {task: wash}
    payload: {event_type: leaves}
    release: machine
```

The `car` and `washing` tasks take `seize`, `wait` and `release` as the YAML does.

```python
@app.task(name="car", seize="machine", next="washing")
def car(task_id):
    """Takes a machine and keeps it."""
    return {"event_type": "enters"}

# The car keeps its machine until the wash, a task of its own, has finished
@app.task(name="washing", wait={"task": "wash"}, release="machine")
def washing(task_id):
    """Leaves once its wash has finished."""
    return {"event_type": "leaves"}
```

The `car` function is SimPy's `with machine.request()` block. It runs the wash as a process and publishes the records of the three tasks.

```python
def car(name):
    number = next(numbers["car"])
    with machine.request() as request:
        publish(name, "car", number, {"status": "queued"})
        yield request
        publish(name, "car", number, {"status": "started"})
        publish(name, "car", number, {"event_type": "enters"})
        # The car keeps its machine until the wash has finished
        waiting = next(numbers["washing"])
        publish(name, "washing", waiting, {"status": "queued"})
        yield env.process(wash(name))
        publish(name, "washing", waiting, {"status": "started"})
        publish(name, "washing", waiting, {"event_type": "leaves"})
```

### Cars Arrive

SimPy's `setup` starts four cars at time 0 and then a car after each gap of 5 to 9 minutes.

```python
def setup(env, num_machines, washtime, t_inter):
    """Create a carwash, a number of initial cars and keep creating cars
    approx. every ``t_inter`` minutes."""
    # Create the carwash
    carwash = Carwash(env, num_machines, washtime)

    car_count = itertools.count()

    # Create 4 initial cars
    for _ in range(4):
        env.process(car(env, f'Car {next(car_count)}', carwash))

    # Create more cars while the simulation is running
    while True:
        yield env.timeout(random.randint(t_inter - 2, t_inter + 2))
        env.process(car(env, f'Car {next(car_count)}', carwash))
```

`initial: 4` starts four cars at time 0. After that, a car arrives after `gap`, an integer from 5 to 9 seconds.

```yaml
arrivals:
  cars:
    initial: 4
    gap: {gen: integer, bounds: [5, 9]}
    spawn: car
    key: {gen: sequence, format: "Car {}"}
```

`add_arrival` takes `initial` and `gap` as the YAML does. The `size` argument of `build` sets the number of cars at the start.

```python
app.add_arrival(
    "cars",
    gap=generator({"gen": "integer", "bounds": [5, 9]}),
    key=lambda n: "Car {}".format(n - 1),
    initial=size,
    spawn="car",
)
```

The `setup` function is SimPy's. It draws each gap from desflow's `Sampler`.

```python
def setup():
    for i in range(size):
        env.process(car(f"Car {i}"))
    i = size
    while True:
        yield env.timeout(int(gaps.integers(5, 10)))
        env.process(car(f"Car {i}"))
        i += 1

env.process(setup())
```

### Run It

`desflow run` loads the blueprint and runs it unpaced, because it sets `factor: 0`. Each record is printed to the terminal.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get simpy-carwash --api yaml
cd simpy-carwash/yaml

# 3. Run it
desflow run carwash.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get simpy-carwash --api yaml
cd simpy-carwash/yaml

# 3. Run it
desflow run carwash.yaml
```

The script builds the model with the builder API, runs it unpaced, and prints each record to the terminal.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get simpy-carwash --api builder
cd simpy-carwash/builder

# 3. Run it
uv run --no-project --with desflow carwash_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get simpy-carwash --api builder
cd simpy-carwash/builder

# 3. Run it
python carwash_example.py
```

The script wires the model by hand on `DynamicRealtimeEnvironment`, runs it unpaced, and prints each record to the terminal.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get simpy-carwash --api low-level
cd simpy-carwash/low-level

# 3. Run it
uv run --no-project --with desflow carwash_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get simpy-carwash --api low-level
cd simpy-carwash/low-level

# 3. Run it
python carwash_example.py
```

______________________________________________________________________

## Check the Result

Six cars enter the carwash in the 20 seconds. Cars 0 to 3 are waiting at time 0, car 4 arrives at 5 seconds and car 5 at 14 seconds. Cars 0 and 1 finish washing at 5 seconds, cars 2 and 3 at 10 seconds, car 4 at 15 seconds and car 5 at 19 seconds.

SimPy's example prints the percentage of dirt removed in a line of text. Here it is the `dirt_removed_pct` field of each `washed` record. desflow draws each random value from its own stream, named by the place where the value is used, such as `Carwash.service.patience`. A seed of 42 therefore gives different numbers from SimPy's `random.seed(42)`. The system is the same. The figures are not.

```text
Carwash
Check out http://youtu.be/fXXmeP9TvBg while simulating ... ;-)
Car 0 arrives at the carwash at 0.00.
Car 1 arrives at the carwash at 0.00.
Car 2 arrives at the carwash at 0.00.
Car 3 arrives at the carwash at 0.00.
Car 0 enters the carwash at 0.00.
Car 1 enters the carwash at 0.00.
Car 4 arrives at the carwash at 5.00.
Carwash removed 97% of Car 0's dirt.
Carwash removed 67% of Car 1's dirt.
Car 0 leaves the carwash at 5.00.
Car 1 leaves the carwash at 5.00.
Car 2 enters the carwash at 5.00.
Car 3 enters the carwash at 5.00.
Car 5 arrives at the carwash at 10.00.
Carwash removed 64% of Car 2's dirt.
Carwash removed 58% of Car 3's dirt.
Car 2 leaves the carwash at 10.00.
Car 3 leaves the carwash at 10.00.
Car 4 enters the carwash at 10.00.
Car 5 enters the carwash at 10.00.
Carwash removed 97% of Car 4's dirt.
Carwash removed 56% of Car 5's dirt.
Car 4 leaves the carwash at 15.00.
Car 5 leaves the carwash at 15.00.
Car 6 arrives at the carwash at 16.00.
Car 6 enters the carwash at 16.00.
```

The three versions in desflow print the same records, which [a test](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc3/tests/docs/test_example_twins.py) checks by running each version unpaced. Each record has `sim_ts`, the simulation time, and a key for the entity it belongs to. The statistics and the parameters that a live run can change are in the same stream, which SimPy's file does not have.

______________________________________________________________________

## Full Files

Full files

The blueprint is `carwash.yaml`.

```yaml
# Carwash: SimPy's carwash.py example, in YAML.
#
# The YAML version of carwash_example.py, which --api builder downloads. A carwash has
# two washing machines. Four cars wait when it opens. A car takes a machine, keeps it
# while the wash runs (a task of its own that removes 50 to 99 percent of the dirt),
# and then leaves.
#
# SimPy's example counts minutes, and here each of its minutes is one second. So a car
# arrives every 5 to 9 seconds, a wash takes 5 seconds, and the run ends after 20
# seconds, SimPy's 20 minutes. Each value is drawn from desflow's random sequence for
# its place in the file, so the numbers differ from those SimPy prints.
#
# Needs no containers. Runs unpaced and ends after 20 simulation seconds.
# Run it from its folder with: desflow run carwash.yaml

simulation: {sim_id: Carwash, factor: 0, random_seed: 42}

egress:
  - {type: Console}

resources:
  machine: {current_cap: 2}

services:
  wash: {dist: constant, value: 5}

arrivals:
  cars:
    initial: 4
    gap: {gen: integer, bounds: [5, 9]}
    spawn: car
    key: {gen: sequence, format: "Car {}"}

tasks:
  car:
    seize: machine
    payload: {event_type: enters}
    next: washing
  # The car keeps its machine until the wash, a task of its own, has finished
  washing:
    wait: {task: wash}
    payload: {event_type: leaves}
    release: machine
  wash:
    service: wash
    payload:
      event_type: washed
      dirt_removed_pct: {gen: integer, bounds: [50, 99]}

run: {until: 20}
```

The script builds the model with the builder API.

```python
"""Carwash: SimPy's carwash.py example, builder API.

A carwash has two washing machines. Four cars wait when it opens. A car takes a
machine, keeps it while the wash runs (a task of its own that removes 50 to 99 percent
of the dirt), and then leaves.

SimPy's example counts minutes, and here each of its minutes is one second. So a car
arrives every 5 to 9 seconds, a wash takes 5 seconds, and the run ends after 20
seconds, SimPy's 20 minutes. Each value is drawn from desflow's random sequence for its
place in the model, so the numbers differ from those SimPy prints.

Needs no containers. Runs unpaced and ends after 20 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator


def build(size: int = 4) -> SimulationContext:
    """Builds the carwash, with `size` cars waiting when it opens."""
    app = SimulationContext(sim_id="Carwash", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    app.add_resource("machine", current_cap=2)
    app.add_service("wash", dist="constant", value=5)
    app.add_arrival(
        "cars",
        gap=generator({"gen": "integer", "bounds": [5, 9]}),
        key=lambda n: "Car {}".format(n - 1),
        initial=size,
        spawn="car",
    )

    @app.task(name="car", seize="machine", next="washing")
    def car(task_id):
        """Takes a machine and keeps it."""
        return {"event_type": "enters"}

    # The car keeps its machine until the wash, a task of its own, has finished
    @app.task(name="washing", wait={"task": "wash"}, release="machine")
    def washing(task_id):
        """Leaves once its wash has finished."""
        return {"event_type": "leaves"}


    wash_payload = Template(
        {
            "event_type": "washed",
            "dirt_removed_pct": {"gen": "integer", "bounds": [50, 99]},
        },
        # The name gives the random sequence the YAML version's payload draws from
        name="tasks.wash",
    )

    @app.task(name="wash", service_id="wash")
    def wash(task_id):
        """Washes the car."""
        return wash_payload


    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs 20 simulation seconds."""
    model.run(until=20)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```

The script wires the model by hand on `DynamicRealtimeEnvironment`, with SimPy's own processes.

```python
"""Carwash: SimPy's carwash.py example, low-level API.

The low-level version of `builder/simpy/carwash_example.py`: SimPy's own processes on
a `DynamicRealtimeEnvironment`. They publish with `publish_event` the records that the
YAML and builder versions publish: each task's `queued` and `started`, its own
events, and the number of the task's instance. A carwash has two washing machines.
Four cars wait when it opens. A car takes a machine, keeps it while the wash runs
(removing 50 to 99 percent of the dirt), and then leaves.

SimPy's example counts minutes, and here each of its minutes is one second. So a car
arrives every 5 to 9 seconds, a wash takes 5 seconds, and the run ends after 20
seconds, SimPy's 20 minutes. Each value is drawn from desflow's `Sampler`, from the
random sequence the YAML and builder versions use for the same place, so the three
versions publish the same records.

Needs no containers. Runs unpaced and ends after 20 simulation seconds.
"""

import itertools
import logging

import simpy

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

NUM_MACHINES = 2
WASHTIME = 5
UNTIL = 20
WASH = "Carwash.service.wash"  # the service that the wash's started record names


def build(env: DynamicRealtimeEnvironment, size: int = 4) -> None:
    """Starts the carwash on `env`, with `size` cars waiting when it opens."""
    sampler = Sampler(seed=42)
    gaps = sampler.generator("arrivals.cars.gap")
    dirt = sampler.generator("tasks.wash.payload.dirt_removed_pct")
    machine = simpy.Resource(env, NUM_MACHINES)
    numbers = {task: itertools.count() for task in ("car", "washing", "wash")}

    def publish(name, task, number, value):
        env.publish_event(name, value, task=task, instance=number)

    def wash(name):
        number = next(numbers["wash"])
        publish(
            name,
            "wash",
            number,
            {
                "path_id": WASH,
                "sim_id": "Carwash",
                "service": "wash",
                "status": "started",
            },
        )
        yield env.timeout(WASHTIME)
        removed = int(dirt.integers(50, 100))
        publish(
            name, "wash", number, {"event_type": "washed", "dirt_removed_pct": removed}
        )


    def car(name):
        number = next(numbers["car"])
        with machine.request() as request:
            publish(name, "car", number, {"status": "queued"})
            yield request
            publish(name, "car", number, {"status": "started"})
            publish(name, "car", number, {"event_type": "enters"})
            # The car keeps its machine until the wash has finished
            waiting = next(numbers["washing"])
            publish(name, "washing", waiting, {"status": "queued"})
            yield env.process(wash(name))
            publish(name, "washing", waiting, {"status": "started"})
            publish(name, "washing", waiting, {"event_type": "leaves"})


    def setup():
        for i in range(size):
            env.process(car(f"Car {i}"))
        i = size
        while True:
            yield env.timeout(int(gaps.integers(5, 10)))
            env.process(car(f"Car {i}"))
            i += 1

    env.process(setup())


def run() -> None:
    """Runs 20 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```

This is SimPy 4.1.2's `carwash.py`, with SimPy's licence notice added at the top and comment lines that mark the pieces shown above. Run with Python, it prints the output shown under Check the Result.

```python
# SimPy 4.1.2, docs/examples/code/carwash.py, under SimPy's licence (LICENSE.rst):
#
# The MIT License (MIT)
#
# Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke (also see AUTHORS.txt)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
Carwash example.

Covers:

- Waiting for other processes
- Resources: Resource

Scenario:
  A carwash has a limited number of washing machines and defines
  a washing processes that takes some (random) time.

  Car processes arrive at the carwash at a random time. If one washing
  machine is available, they start the washing process and wait for it
  to finish. If not, they wait until they can use one.

"""

import itertools
import random

import simpy

# fmt: off
RANDOM_SEED = 42
NUM_MACHINES = 2  # Number of machines in the carwash
WASHTIME = 5      # Minutes it takes to clean a car
T_INTER = 7       # Create a car every ~7 minutes
SIM_TIME = 20     # Simulation time in minutes
# fmt: on


class Carwash:
    """A carwash has a limited number of machines (``NUM_MACHINES``) to
    clean cars in parallel.

    Cars have to request one of the machines. When they got one, they
    can start the washing processes and wait for it to finish (which
    takes ``washtime`` minutes).

    """

    def __init__(self, env, num_machines, washtime):
        self.env = env
        self.machine = simpy.Resource(env, num_machines)
        self.washtime = washtime

    def wash(self, car):
        """The washing processes. It takes a ``car`` processes and tries
        to clean it."""
        yield self.env.timeout(self.washtime)
        pct_dirt = random.randint(50, 99)
        print(f"Carwash removed {pct_dirt}% of {car}'s dirt.")


def car(env, name, cw):
    """The car process (each car has a ``name``) arrives at the carwash
    (``cw``) and requests a cleaning machine.

    It then starts the washing process, waits for it to finish and
    leaves to never come back ...

    """
    print(f'{name} arrives at the carwash at {env.now:.2f}.')
    with cw.machine.request() as request:
        yield request

        print(f'{name} enters the carwash at {env.now:.2f}.')
        yield env.process(cw.wash(name))

        print(f'{name} leaves the carwash at {env.now:.2f}.')


def setup(env, num_machines, washtime, t_inter):
    """Create a carwash, a number of initial cars and keep creating cars
    approx. every ``t_inter`` minutes."""
    # Create the carwash
    carwash = Carwash(env, num_machines, washtime)

    car_count = itertools.count()

    # Create 4 initial cars
    for _ in range(4):
        env.process(car(env, f'Car {next(car_count)}', carwash))

    # Create more cars while the simulation is running
    while True:
        yield env.timeout(random.randint(t_inter - 2, t_inter + 2))
        env.process(car(env, f'Car {next(car_count)}', carwash))


# Setup and start the simulation
print('Carwash')
print('Check out http://youtu.be/fXXmeP9TvBg while simulating ... ;-)')
random.seed(RANDOM_SEED)  # This helps to reproduce the results

# Create an environment and start the setup process
env = simpy.Environment()
env.process(setup(env, NUM_MACHINES, WASHTIME, T_INTER))

# Execute!
env.run(until=SIM_TIME)
```

______________________________________________________________________

## Sources

- [SimPy 4.1.2, Carwash](https://simpy.readthedocs.io/en/4.1.2/examples/carwash.html): the original code and its output.
- [SimPy's licence](https://gitlab.com/team-simpy/simpy/-/blob/master/LICENSE.rst): MIT, Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke. The SimPy file above carries this notice.
- [`examples/` in the desflow repository](https://github.com/jaehyeon-kim/desflow/tree/v1.0.0rc3/examples): the three versions in desflow, under `simpy/` in each folder.
