# Randomness and Seeds

Run the busy shop twice, and the orders arrive at different times and take different times to pack. A stochastic model, one that draws random times, behaves this way, but its results are then hard to check or to compare. This page asks: **why do runs differ, and how can you reproduce one?**

- **Decision:** how to set up runs so that a result can be checked and two versions of the model compared.
- **Measures:** the times of one order's events, on repeated runs and after a second arrival is added.
- **Runs:** runs of one minute: two with no seed, then repeated runs with the seed 42, with and without a second arrival.
- **Assumptions:** orders arrive at random at 1.8 a second, and each packing time is drawn on its own, whatever the others were.

## What You Will Learn

This page shows how to:

- see why two runs of a stochastic model differ;
- repeat a run exactly with a seed;
- add a part to a model without changing the random times of the others.

## What You Add

This page adds a seed, then a second arrival: returned parcels. The model simulates the busy hour from [Queues and Capacity](https://jaehyeon.me/desflow/latest/learn/first-model/queues-and-capacity/index.md), using its answer of 3 packers. Packing times now vary: 1.5 seconds on average with a standard deviation of 0.5 seconds, drawn from a `normal` distribution.

This page uses the normal only to get times that vary. [Choosing Inputs](https://jaehyeon.me/desflow/latest/learn/inputs/choosing-inputs/index.md) chooses packing times from data. [Common Mistakes](https://jaehyeon.me/desflow/latest/learn/concepts/common-mistakes/#choosing-a-normal-or-uniform-by-habit) explains why a normal is a poor model of a time. This one draws a value below 0 about once in 740 draws, and desflow raises such a draw to its minimum delay, as [Distributions](https://jaehyeon.me/desflow/latest/reference/distributions/index.md) says.

______________________________________________________________________

## Run Twice

A simulation draws its random times from a **random number stream**: a long sequence of numbers that looks random but is computed from a starting value, the **seed**. The same seed gives the same sequence, and so the same run. A simulation given no seed starts from a new seed each time, so two runs give two different sets of records.

Save the files from [Full Files](#full-files) in one folder and run the commands there. desflow never runs without a seed. When you give none, it draws one, uses it, and logs it, so any run can be repeated afterwards:

Leave `random_seed` out of the `simulation` section and run the blueprint twice. Each run logs its seed with the command that repeats it. Each run draws its own seed, so your number differs from this one:

```text
No random_seed was given, so this run uses 3166812249. Repeat it with: desflow run randomness_and_seeds.yaml --seed 3166812249
```

Leave `random_seed` out of `SimulationContext` and run the script twice. Each run logs its seed with the argument that repeats it. Each run draws its own seed, so your number differs from this one:

```text
No random_seed was given, so this run uses 3166812249. Pass random_seed=3166812249 to repeat it.
```

The low-level API never draws a seed for you: you pass one to `Sampler`. A `Sampler()` with no seed draws nothing at random, as [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/#how-it-works) explains. To start from a new seed each run, draw one yourself, for example with `secrets.randbelow(2**32)`, and print it.

______________________________________________________________________

## Fix the Seed

Set the seed once, and every run draws the same numbers:

```yaml
# The same random numbers on every run; leave it out to draw a new seed
random_seed: 42
```

`desflow run randomness_and_seeds.yaml --seed 7` runs with another seed without editing the file; `--seed` overrides `random_seed`.

```python
# The same random numbers on every run; leave it out to draw a new seed
SimulationContext(sim_id="Shop", factor=0, random_seed=42)
```

```python
# The same random numbers on every run, passed to Sampler(seed=SEED) in run below
SEED = 42
```

Repeating a run exactly is called **reproducibility**. It is what lets you check a result, show it to someone else, and see what a change to the model does on its own. Pick one order and follow it: the twelfth order, `orders-12`, has these events on every run with the seed 42:

```text
[EVT] {'sim_ts': 5.993, 'timestamp': '...', 'key': 'orders-12', 'task': 'pack_order', 'instance': 11, 'value': {'path_id': 'Shop.service.packing', 'status': 'queued'}}
[EVT] {'sim_ts': 6.695, 'timestamp': '...', 'key': 'orders-12', 'task': 'pack_order', 'instance': 11, 'value': {'path_id': 'Shop.service.packing', 'status': 'started'}}
[EVT] {'sim_ts': 8.443, 'timestamp': '...', 'key': 'orders-12', 'task': 'pack_order', 'instance': 11, 'value': {'event_type': 'order_packed'}}
```

It is `queued` at 5.993, gets a packer at 6.695 and is packed at 8.443: a wait of 0.702 seconds.

______________________________________________________________________

## Add Returns

Now add returned parcels: one every 5 seconds on average (12 a minute), each recorded as it arrives. They do not use the packers.

```yaml
arrivals:
  orders: {dist: exponential, rate: 1.8, spawn: pack_order}
  # Returned parcels: one every 5 seconds on average
  returns: {dist: exponential, rate: 0.2, spawn: receive_return}

tasks:
  # A return is recorded as it arrives; it does not use a packer
  receive_return:
    payload: {event_type: return_received}
```

```python
# Returned parcels: one every 5 seconds on average
app.add_arrival("returns", dist="exponential", rate=0.2)


@app.task()
def receive_return(task_id: int):
    # A return is recorded as it arrives; it does not use a packer
    return {"event_type": "return_received"}


@app.arrival_loop("returns")
def return_generator(context: SimulationContext):
    task_id = 0
    while True:
        parcel = yield context.wait_for_arrival("returns")
        context.spawn(receive_return(task_id, entity=parcel))
        task_id += 1
```

```python
# Returned parcels: one every 5 seconds on average
PARAMS.arrival["returns"] = DistributionConfig(dist="exponential", rate=0.2)


def return_generator(env, sampler):
    """Records each return as it arrives; it does not use a packer."""
    # Once registered, the config draws from sampler.generator("arrival.returns")
    arrival = env.registry.get_config("Shop.arrival.returns")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        key = f"returns-{task_id + 1}"
        value = {"event_type": "return_received"}
        env.publish_event(key, value, task="receive_return", instance=task_id)
        task_id += 1
```

`Sampler(seed=SEED)` makes one generator per name, `sampler.generator("arrival.returns")`, from the seed and the name. The registry names each distribution it registers by its path without the sim id, such as `arrival.returns`. `sampler.sample(config)` draws from the generator of that name. Your own code can take a sequence of its own the same way.

A task with no service and no resource, such as `receive_return`, takes no time: it publishes its payload as soon as the arrival starts it.

Run it with the seed 42. `orders-12` has the same three events as before, at the same times. Adding an arrival changed nothing for the orders, because each distribution draws from a random number stream of its own, named by its registry path without the sim id. The orders draw from `arrival.orders`, packing from `service.packing`, and the new returns from `arrival.returns`. Look at the two runs side by side:

With one shared stream, the returns would take some of the numbers the orders and packing used to get, and every later time would shift. The name is part of the stream. Renaming a service, an arrival or a task that routes by weights therefore gives it a new stream, and a run with the same seed then gives other results. [Busy Hour Figures](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/#busy-hour-figures) lists two such runs: the same measured packing times give 1.35 or 1.38 seconds of mean wait, depending on the service's name.

______________________________________________________________________

## What You Get and Why

Why do runs differ, and how can you reproduce one? Runs differ because they start their random number streams from different seeds. Repeat a run by supplying the same seed: `random_seed: 42` in the blueprint, `random_seed=42` on `SimulationContext`, `Sampler(seed=42)` in the low-level API, or `desflow run --seed 42`. With it, **the twelfth order waits 0.702 seconds** on every run, and still 0.702 seconds after the returns are added.

One stream per input means that two versions of a model run with the same seed see the same arrivals and the same service times wherever they share them. A difference between their results then comes from the change, not from different random numbers. This is called **common random numbers** ([Output Analysis](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#comparing-designs) explains it), and it is how two designs are compared fairly. [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/index.md) describes how each stream is made, and [Distributions](https://jaehyeon.me/desflow/latest/reference/distributions/index.md) lists every family with its parameters.

## Key Takeaways

- A seed fixes the random numbers, so the same seed gives the same run. A YAML or builder run that was given no seed draws one and logs it, so it can be repeated.
- Each distribution draws from its own random number stream, named after its registry path, so adding an arrival leaves the other draws as they were.
- Renaming a service, an arrival or a weighted route gives it a new stream, and the results change with the same seed.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Randomness and Seeds tutorial: the busy shop, seeded, with returns.
# Run it with: desflow run randomness_and_seeds.yaml
simulation:
  sim_id: Shop
  factor: 0
  # The same random numbers on every run; leave it out to draw a new seed
  random_seed: 42

resources:
  packers: {current_cap: 3, max_cap: 10}

services:
  # Packing varies: 1.5 seconds on average, with a standard deviation of 0.5
  packing: {dist: normal, mean: 1.5 s, std: 0.5 s}

arrivals:
  orders: {dist: exponential, rate: 1.8, spawn: pack_order}
  # Returned parcels: one every 5 seconds on average
  returns: {dist: exponential, rate: 0.2, spawn: receive_return}

tasks:
  # A return is recorded as it arrives; it does not use a packer
  receive_return:
    payload: {event_type: return_received}
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

egress:
  - type: Console

run:
  until: 1 min
```

```python
"""The Randomness and Seeds tutorial: the busy shop, seeded, with returns, builder.

Orders arrive at 1.8 a second, and three packers pack each in 1.5 seconds on average,
with a standard deviation of 0.5 seconds. Returned parcels arrive every 5 seconds on
average and are recorded as they arrive. Every record is printed to the terminal.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

app = (
    # The same random numbers on every run; leave it out to draw a new seed
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_resource("packers", current_cap=3, max_cap=10)
    # Packing varies: 1.5 seconds on average, with a standard deviation of 0.5
    .add_service("packing", dist="normal", mean=1.5, std=0.5)
    .add_arrival("orders", dist="exponential", rate=1.8)
    .add_egress(ConsoleEgress())
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


# Returned parcels: one every 5 seconds on average
app.add_arrival("returns", dist="exponential", rate=0.2)


@app.task()
def receive_return(task_id: int):
    # A return is recorded as it arrives; it does not use a packer
    return {"event_type": "return_received"}


@app.arrival_loop("returns")
def return_generator(context: SimulationContext):
    task_id = 0
    while True:
        parcel = yield context.wait_for_arrival("returns")
        context.spawn(receive_return(task_id, entity=parcel))
        task_id += 1


if __name__ == "__main__":
    app.run(until="1 min")
```

```python
"""The Randomness and Seeds tutorial: the busy shop, seeded, with returns, low-level.

Orders arrive at 1.8 a second, and three packers pack each in 1.5 seconds on average,
with a standard deviation of 0.5 seconds. Returned parcels arrive every 5 seconds on
average and are recorded as they arrive. Every record is printed to the terminal.
"""

import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
    # Packing varies: 1.5 seconds on average, with a standard deviation of 0.5
    service={"packing": DistributionConfig(dist="normal", mean=1.5, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
)
# The same random numbers on every run, passed to Sampler(seed=SEED) in run below
SEED = 42


def pack_order(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


# Returned parcels: one every 5 seconds on average
PARAMS.arrival["returns"] = DistributionConfig(dist="exponential", rate=0.2)


def return_generator(env, sampler):
    """Records each return as it arrives; it does not use a packer."""
    # Once registered, the config draws from sampler.generator("arrival.returns")
    arrival = env.registry.get_config("Shop.arrival.returns")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        key = f"returns-{task_id + 1}"
        value = {"event_type": "return_received"}
        env.publish_event(key, value, task="receive_return", instance=task_id)
        task_id += 1


def run(until=60, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=SEED)
    env.process(order_generator(env, sampler, packers))
    env.process(return_generator(env, sampler))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


if __name__ == "__main__":
    run()
```
