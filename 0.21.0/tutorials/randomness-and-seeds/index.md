# Randomness and Seeds

Run the busy shop twice: orders arrive at different times and packing durations vary. That is what a stochastic model does, but it makes a result hard to check or to compare. This page asks: **why do runs differ, and how can you reproduce one?**

## What You Add

A seed, then a second arrival. The model simulates the busy hour from [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md), using its answer of 3 packers. Packing times now vary: 1.5 seconds on average, give or take 0.5, drawn from a `normal` distribution. The run is one minute long.

______________________________________________________________________

## Run Twice

A simulation draws its random times from a **random number stream**: a long sequence of numbers that looks random but is computed from a starting value, the **seed**. The same seed gives the same sequence, and so the same run. A run without a seed takes a new seed each time, so two runs give two different sets of records.

desflow never runs without a seed. When you give none, it draws one, uses it, and logs it, so any run can be repeated afterwards:

Leave `random_seed` out of the `simulation` section and run the blueprint twice. Each run logs its seed with the command that repeats it:

```text
No random_seed was given, so this run uses 3166812249. Repeat it with: desflow run randomness_and_seeds.yaml --seed 3166812249
```

Leave `random_seed` out of `SimulationContext` and run the script twice. Each run logs its seed with the argument that repeats it:

```text
No random_seed was given, so this run uses 3166812249. Pass random_seed=3166812249 to repeat it.
```

The low-level API never draws a seed for you: you pass one to `Sampler`. A `Sampler()` with no seed draws nothing at random, and every sample is the distribution's mean (for a `truncated_normal`, its `mean` or its `low`, whichever is higher), which suits a test that needs fixed times. To start from a new seed each run, draw one yourself, for example with `secrets.randbelow(2**32)`, and print it.

______________________________________________________________________

## Fix the Seed

Set the seed once, and every run draws the same numbers:

docs/snippets/yaml/randomness_and_seeds.yaml:seed

```yaml
# The same random numbers on every run; leave it out to draw a new seed
random_seed: 42
```

`desflow run randomness_and_seeds.yaml --seed 7` runs with another seed without editing the file; `--seed` overrides `random_seed`.

docs/snippets/tutorials/randomness_and_seeds.py:seed

```python
# The same random numbers on every run; leave it out to draw a new seed
SimulationContext(sim_id="Shop", factor=0, random_seed=42)
```

docs/snippets/tutorials/randomness_and_seeds_low_level.py:seed

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

Now add returned parcels: about one every 5 seconds, recorded as they arrive. They do not use the packers.

docs/snippets/yaml/randomness_and_seeds.yaml:returns

```yaml
arrivals:
  orders: {dist: exponential, rate: 1.8, spawn: pack_order}
  # Returned parcels: about one every 5 seconds
  returns: {dist: exponential, rate: 0.2, spawn: receive_return}

tasks:
  # A return is recorded as it arrives; it does not use a packer
  receive_return:
    payload: {event_type: return_received}
```

docs/snippets/tutorials/randomness_and_seeds.py:returns

```python
# Returned parcels: about one every 5 seconds
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

docs/snippets/tutorials/randomness_and_seeds_low_level.py:returns

```python
# Returned parcels: about one every 5 seconds
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

`Sampler(seed=SEED)` makes one generator per name, `sampler.generator("arrival.returns")`, from the seed and the name. When the registry registers a distribution it names it by its path without the sim id, and `sampler.sample(config)` draws from the generator of that name. Your own code can take a sequence of its own the same way.

A task with no service and no resource, such as `receive_return`, takes no time: it publishes its payload as soon as the arrival starts it.

Run it with the seed 42. `orders-12` has the same three events as before, at the same times. Adding an arrival changed nothing for the orders, because of how desflow hands out random numbers: each distribution draws from a stream of its own, named by its registry path without the sim id. The orders draw from `arrival.orders`, packing from `service.packing`, and the new returns from `arrival.returns`. Look at the two runs side by side:

With one shared stream, the returns would take some of the numbers the orders and packing used to get, and every later time would shift.

______________________________________________________________________

## Result

Why do runs differ, and how can you reproduce one? Runs differ because they start their random number streams from different seeds. Repeat a run by supplying the same seed: `random_seed: 42` in the blueprint, `random_seed=42` on `SimulationContext`, `Sampler(seed=42)` in the low-level API, or `desflow run --seed 42`. With it, **the twelfth order waits 0.702 seconds** on every run, and still 0.702 seconds after the returns are added.

## Why It Works

One stream per input means that two versions of a model run with the same seed see the same arrivals and the same service times wherever they share them. A difference between their results then comes from the change, not from different random numbers. This is called **common random numbers**, and it is how two designs are compared fairly. [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/arrivals-and-services/index.md) describes how each stream is made, and [Distributions](https://jaehyeon.me/desflow/latest/reference/distributions/index.md) lists every family with its parameters.

______________________________________________________________________

## Full Files

Full files

docs/snippets/yaml/randomness_and_seeds.yaml

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
  # Packing varies: 1.5 seconds on average, give or take 0.5
  packing: {dist: normal, mean: 1.5 s, std: 0.5 s}

arrivals:
  orders: {dist: exponential, rate: 1.8, spawn: pack_order}
  # Returned parcels: about one every 5 seconds
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

docs/snippets/tutorials/randomness_and_seeds.py

```python
"""The Randomness and Seeds tutorial: the busy shop, seeded, with returns, builder.

Orders arrive at 1.8 a second and three packers pack each in 1.5 seconds on average,
give or take 0.5. Returned parcels arrive about every 5 seconds and are recorded as
they arrive. Every record is printed to the terminal.
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
    # Packing varies: 1.5 seconds on average, give or take 0.5
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


# Returned parcels: about one every 5 seconds
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

docs/snippets/tutorials/randomness_and_seeds_low_level.py

```python
"""The Randomness and Seeds tutorial: the busy shop, seeded, with returns, low-level.

Orders arrive at 1.8 a second and three packers pack each in 1.5 seconds on average,
give or take 0.5. Returned parcels arrive about every 5 seconds and are recorded as
they arrive. Every record is printed to the terminal.
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
    # Packing varies: 1.5 seconds on average, give or take 0.5
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


# Returned parcels: about one every 5 seconds
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

Next, [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/index.md) asks which distribution fits packing times.
