# Bank Renege

One counter serves five customers, and a customer who waits longer than their patience goes away. This is SimPy's Bank Renege example, written in desflow.

______________________________________________________________________

## Before You Start

SimPy's example has one counter and 5 customers. The first customer arrives at time 0 and the others at exponential gaps with a mean of 10. Each customer has a patience drawn between 1 and 3, and goes away (reneges) if the counter is not free by then. A customer who reaches the counter is served for an exponential time with a mean of 12. SimPy counts no unit, and desflow reads each unit as one second.

The example needs only desflow, which each Quick Start in [Run It](#run-it) installs, and no other service. `desflow examples get simpy-bank-renege --api yaml` downloads it, as [Get an Example](https://jaehyeon.me/desflow/latest/cli/#get-an-example) shows. [SimPy Examples](https://jaehyeon.me/desflow/latest/simpy/examples/index.md) says how these pages read SimPy's times and numbers. SimPy's own page for the example is [on its documentation site](https://simpy.readthedocs.io/en/4.1.2/examples/bank_renege.html).

______________________________________________________________________

## Steps

### Customers Arrive

SimPy's `source` starts a customer at time 0 and then one after each gap.

```python
def source(env, number, interval, counter):
    """Source generates customers randomly"""
    for i in range(number):
        c = customer(env, f'Customer{i:02d}', counter, time_in_bank=12.0)
        env.process(c)
        t = random.expovariate(1.0 / interval)
        yield env.timeout(t)
```

The counter is a resource with one unit. The arrival `customers` has `first_at: 0`, so the first customer comes at time 0, and `max_events: 5`, so only five come. Its `key` names them `Customer00` to `Customer04`.

```yaml
resources:
  counter: {current_cap: 1}

arrivals:
  customers:
    dist: exponential
    rate: 0.1
    first_at: 0
    max_events: 5
    key: {gen: sequence, format: "Customer{:02d}"}
    spawn: customer
```

`add_resource` declares the counter, and `add_arrival` takes the same `first_at`, `max_events` and `key` as the YAML.

```python
app.add_resource("counter", current_cap=1)
app.add_arrival(
    "customers",
    "exponential",
    rate=0.1,
    first_at=0,
    max_events=5,
    key=lambda n: "Customer{:02d}".format(n - 1),
    spawn="customer",
)
```

The low-level version keeps SimPy's `source` and draws each gap from desflow's `Sampler`, from the stream the arrival uses in the other two versions.

```python
sampler = Sampler(seed=42)
gap = DistributionConfig(dist="exponential", rate=0.1)
gap.stream = "arrival.customers"  # the sequence the blueprint's arrival draws from
patiences = sampler.generator("tasks.customer.patience")
services = sampler.generator("tasks.customer.service_time")
counter = simpy.Resource(env, capacity=1)
```

```python
def source():
    for i in range(NEW_CUSTOMERS):
        env.process(customer(f"Customer{i:02d}", i))
        yield env.timeout(sampler.sample(gap))

env.process(source())
```

### Customer Waits or Reneges

SimPy's `customer` requests the counter and waits for either the counter or its patience, whichever comes first.

```python
def customer(env, name, counter, time_in_bank):
    """Customer arrives, is served and leaves."""
    arrive = env.now
    print(f'{arrive:7.4f} {name}: Here I am')

    with counter.request() as req:
        patience = random.uniform(MIN_PATIENCE, MAX_PATIENCE)
        # Wait for the counter or abort at the end of our tether
        results = yield req | env.timeout(patience)

        wait = env.now - arrive

        if req in results:
            # We got to the counter
            print(f'{env.now:7.4f} {name}: Waited {wait:6.3f}')

            tib = random.expovariate(1.0 / time_in_bank)
            yield env.timeout(tib)
            print(f'{env.now:7.4f} {name}: Finished')

        else:
            # We reneged
            print(f'{env.now:7.4f} {name}: RENEGED after {wait:6.3f}')
```

`patience` is the longest wait for the counter, drawn uniform from 1 to 3 seconds for each customer. A customer whose patience ends first publishes `reneged` and leaves. The service time is exponential with a mean of 12 seconds, written as the rate 1/12.

```yaml
tasks:
  customer:
    resource: counter
    patience: {gen: uniform, low: 1, high: 3}
    service_time: {gen: sample, dist: exponential, rate: 0.08333333333333333}
    payload: {event_type: finished}
```

The `customer` task takes `patience` and `service_time` as generators.

```python
@app.task(
    name="customer",
    resource_id="counter",
    # Waits for the counter at most this long, then goes away
    patience=generator({"gen": "uniform", "low": 1, "high": 3}),
    service_time=generator(
        {"gen": "sample", "dist": "exponential", "rate": 0.08333333333333333}
    ),
)
def customer(task_id):
    """Is served at the counter."""
    return {"event_type": "finished"}
```

The `customer` function is SimPy's, with `req | env.timeout(patience)`. It publishes `reneged` when `req` is not in the result.

```python
def customer(name, number):
    def publish(value):
        env.publish_event(name, value, task="customer", instance=number)

    with counter.request() as req:
        publish({"status": "queued"})
        patience = float(patiences.uniform(1, 3))
        # Waits for the counter at most `patience`, then goes away
        results = yield req | env.timeout(patience)
        if req in results:
            publish({"status": "started"})
            yield env.timeout(float(services.exponential(12.0)))
            publish({"event_type": "finished"})
        else:
            publish({"status": "reneged"})
```

### Run It

`desflow run` loads the blueprint and runs it unpaced, because it sets `factor: 0`. Each record is printed to the terminal.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get simpy-bank-renege --api yaml
cd simpy-bank-renege/yaml

# 3. Run it
desflow run bank_renege.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get simpy-bank-renege --api yaml
cd simpy-bank-renege/yaml

# 3. Run it
desflow run bank_renege.yaml
```

The script builds the model with the builder API, runs it unpaced, and prints each record to the terminal.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get simpy-bank-renege --api builder
cd simpy-bank-renege/builder

# 3. Run it
uv run --no-project --with desflow bank_renege_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get simpy-bank-renege --api builder
cd simpy-bank-renege/builder

# 3. Run it
python bank_renege_example.py
```

The script wires the model by hand on `DynamicRealtimeEnvironment`, runs it unpaced, and prints each record to the terminal.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get simpy-bank-renege --api low-level
cd simpy-bank-renege/low-level

# 3. Run it
uv run --no-project --with desflow bank_renege_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get simpy-bank-renege --api low-level
cd simpy-bank-renege/low-level

# 3. Run it
python bank_renege_example.py
```

______________________________________________________________________

## Check the Result

Two of the five customers renege: `Customer03` and `Customer04`. `Customer00` is served from time 0 and finishes at 3.536 seconds. `Customer01` reaches the counter at 5.384 seconds and finishes at 10.53 seconds. `Customer02` reaches the counter at 10.53 seconds and finishes at 18.266 seconds. The run has no end time, so it stops when the last customer leaves.

SimPy's own output, below, has customers who are served and customers who renege too. desflow draws each random value from its own stream, named by the place where the value is used, such as `tasks.customer.patience`. A seed of 42 therefore gives different numbers from SimPy's `random.seed(42)`. The system is the same. The figures are not.

```text
Bank renege
 0.0000 Customer00: Here I am
 0.0000 Customer00: Waited  0.000
 3.8595 Customer00: Finished
10.2006 Customer01: Here I am
10.2006 Customer01: Waited  0.000
12.7265 Customer02: Here I am
13.9003 Customer02: RENEGED after  1.174
23.7507 Customer01: Finished
34.9993 Customer03: Here I am
34.9993 Customer03: Waited  0.000
37.9599 Customer03: Finished
40.4798 Customer04: Here I am
40.4798 Customer04: Waited  0.000
43.1401 Customer04: Finished
```

The three versions in desflow print the same records, which [a test](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0/tests/docs/test_example_twins.py) checks by running each version unpaced. Each record has `sim_ts`, the simulation time, and a key for the entity it belongs to. The statistics and the parameters that a live run can change are in the same stream, which SimPy's file does not have.

______________________________________________________________________

## Full Files

Full files

The blueprint is `bank_renege.yaml`.

```yaml
# Bank Renege: SimPy's bank_renege.py example, in YAML.
#
# The YAML version of bank_renege_example.py, which --api builder downloads. One
# counter serves five customers. The first arrives at 0 and the others about 10
# seconds apart (exponential gaps). Each customer waits for the counter for a patience
# drawn between 1 and 3 seconds, and goes away (reneged) if the counter is not free by
# then. A customer at the counter is served for about 12 seconds (exponential).
#
# SimPy's example counts seconds, as desflow does. Each value is drawn from desflow's
# random sequence for its place in the file, so the numbers differ from those SimPy
# prints.
#
# Needs no containers. Runs unpaced and ends when the last customer leaves.
# Run it from its folder with: desflow run bank_renege.yaml

simulation: {sim_id: Bank, factor: 0, random_seed: 42}

egress:
  - {type: Console}

resources:
  counter: {current_cap: 1}

arrivals:
  customers:
    dist: exponential
    rate: 0.1
    first_at: 0
    max_events: 5
    key: {gen: sequence, format: "Customer{:02d}"}
    spawn: customer

tasks:
  customer:
    resource: counter
    patience: {gen: uniform, low: 1, high: 3}
    service_time: {gen: sample, dist: exponential, rate: 0.08333333333333333}
    payload: {event_type: finished}
```

The script builds the model with the builder API.

```python
"""Bank Renege: SimPy's bank_renege.py example, builder API.

One counter serves five customers. The first arrives at 0 and the others about 10
seconds apart (exponential gaps). Each customer waits for the counter for a patience
drawn between 1 and 3 seconds, and goes away (reneged) if the counter is not free by
then. A customer at the counter is served for about 12 seconds (exponential).

SimPy's example counts seconds, as desflow does. Each value is drawn from desflow's
random sequence for its place in the model, so the numbers differ from those SimPy
prints.

Needs no containers. Runs unpaced and ends when the last customer leaves.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    """Builds the bank."""
    app = SimulationContext(sim_id="Bank", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    app.add_resource("counter", current_cap=1)
    app.add_arrival(
        "customers",
        "exponential",
        rate=0.1,
        first_at=0,
        max_events=5,
        key=lambda n: "Customer{:02d}".format(n - 1),
        spawn="customer",
    )

    @app.task(
        name="customer",
        resource_id="counter",
        # Waits for the counter at most this long, then goes away
        patience=generator({"gen": "uniform", "low": 1, "high": 3}),
        service_time=generator(
            {"gen": "sample", "dist": "exponential", "rate": 0.08333333333333333}
        ),
    )
    def customer(task_id):
        """Is served at the counter."""
        return {"event_type": "finished"}


    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs until the last customer leaves."""
    model.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```

The script wires the model by hand on `DynamicRealtimeEnvironment`, with SimPy's own processes.

```python
"""Bank Renege: SimPy's bank_renege.py example, low-level API.

The low-level version of `builder/simpy/bank_renege_example.py`: SimPy's own processes
on a `DynamicRealtimeEnvironment`. They publish with `publish_event` the records that
the YAML and builder versions publish: each customer's `queued` and `started`, its
`reneged` or `finished`, and the number of the task's instance. One counter serves
five customers. The first arrives at 0 and the others about 10 seconds apart. Each
customer waits for the counter for a patience between 1 and 3 seconds, and goes away
(reneged) if the counter is not free by then. A customer at the counter is served for
about 12 seconds.

SimPy's example counts seconds, as desflow does. Each value is drawn from desflow's
`Sampler`, from the random sequence the YAML and builder versions use for the same
place, so the three versions publish the same records.

Needs no containers. Runs unpaced and ends when the last customer leaves.
"""

import logging

import simpy

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

NEW_CUSTOMERS = 5
UNTIL = None  # the run ends when no event is left


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the customers' source on `env`."""
    sampler = Sampler(seed=42)
    gap = DistributionConfig(dist="exponential", rate=0.1)
    gap.stream = "arrival.customers"  # the sequence the blueprint's arrival draws from
    patiences = sampler.generator("tasks.customer.patience")
    services = sampler.generator("tasks.customer.service_time")
    counter = simpy.Resource(env, capacity=1)

    def customer(name, number):
        def publish(value):
            env.publish_event(name, value, task="customer", instance=number)

        with counter.request() as req:
            publish({"status": "queued"})
            patience = float(patiences.uniform(1, 3))
            # Waits for the counter at most `patience`, then goes away
            results = yield req | env.timeout(patience)
            if req in results:
                publish({"status": "started"})
                yield env.timeout(float(services.exponential(12.0)))
                publish({"event_type": "finished"})
            else:
                publish({"status": "reneged"})


    def source():
        for i in range(NEW_CUSTOMERS):
            env.process(customer(f"Customer{i:02d}", i))
            yield env.timeout(sampler.sample(gap))

    env.process(source())


def run() -> None:
    """Runs until the last customer leaves."""
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

This is SimPy 4.1.2's `bank_renege.py`, with SimPy's licence notice added at the top and comment lines that mark the pieces shown above. Run with Python, it prints the output shown under Check the Result.

```python
# SimPy 4.1.2, docs/examples/code/bank_renege.py, under SimPy's licence (LICENSE.rst):
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
Bank renege example

Covers:

- Resources: Resource
- Condition events

Scenario:
  A counter with a random service time and customers who renege. Based on the
  program bank08.py from TheBank tutorial of SimPy 2. (KGM)

"""

import random

import simpy

RANDOM_SEED = 42
NEW_CUSTOMERS = 5  # Total number of customers
INTERVAL_CUSTOMERS = 10.0  # Generate new customers roughly every x seconds
MIN_PATIENCE = 1  # Min. customer patience
MAX_PATIENCE = 3  # Max. customer patience


def source(env, number, interval, counter):
    """Source generates customers randomly"""
    for i in range(number):
        c = customer(env, f'Customer{i:02d}', counter, time_in_bank=12.0)
        env.process(c)
        t = random.expovariate(1.0 / interval)
        yield env.timeout(t)


def customer(env, name, counter, time_in_bank):
    """Customer arrives, is served and leaves."""
    arrive = env.now
    print(f'{arrive:7.4f} {name}: Here I am')

    with counter.request() as req:
        patience = random.uniform(MIN_PATIENCE, MAX_PATIENCE)
        # Wait for the counter or abort at the end of our tether
        results = yield req | env.timeout(patience)

        wait = env.now - arrive

        if req in results:
            # We got to the counter
            print(f'{env.now:7.4f} {name}: Waited {wait:6.3f}')

            tib = random.expovariate(1.0 / time_in_bank)
            yield env.timeout(tib)
            print(f'{env.now:7.4f} {name}: Finished')

        else:
            # We reneged
            print(f'{env.now:7.4f} {name}: RENEGED after {wait:6.3f}')


# Setup and start the simulation
print('Bank renege')
random.seed(RANDOM_SEED)
env = simpy.Environment()

# Start processes and run
counter = simpy.Resource(env, capacity=1)
env.process(source(env, NEW_CUSTOMERS, INTERVAL_CUSTOMERS, counter))
env.run()
```

______________________________________________________________________

## Sources

- [SimPy 4.1.2, Bank Renege](https://simpy.readthedocs.io/en/4.1.2/examples/bank_renege.html): the original code and its output.
- [SimPy's licence](https://gitlab.com/team-simpy/simpy/-/blob/master/LICENSE.rst): MIT, Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke. The SimPy file above carries this notice.
- [`examples/` in the desflow repository](https://github.com/jaehyeon-kim/desflow/tree/v1.0.0/examples): the three versions in desflow, under `simpy/` in each folder.
