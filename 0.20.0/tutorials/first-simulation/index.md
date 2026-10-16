# First Simulation

This tutorial builds an online shop that packs orders: one packer, orders arriving about every 2 seconds, and 1.5 seconds of packing per order. It then adds random packing times and capacity changes, and connects the shop to Kafka.

Each step shows the code three ways, in tabs: a **YAML** blueprint, the **Builder** (`SimulationContext`) and the **Low-level** API (`DynamicRealtimeEnvironment` and SimPy processes). Pick the tab of the way you write; every tab on the page switches with it. The three build the same simulation, so the records are the same. [Ways to Write a Simulation](https://jaehyeon.me/desflow/latest/architecture/overview/index.md) explains how they relate.

______________________________________________________________________

## Install

The core package includes the `desflow` command:

```bash
pip install desflow
```

______________________________________________________________________

## Describe the Shop

A simulation has a `sim_id`, here `Shop`, and its parameters: a resource called `packers` with one unit, a service called `packing` that sets how long an order takes, and an arrival stream called `orders` that sets how often an order arrives.

docs/snippets/yaml/first_shop.yaml:shop

```yaml
simulation:
  sim_id: Shop
  factor: 1.0  # 1 simulated second takes 1 real second

resources:
  packers: {current_cap: 1, max_cap: 1}

services:
  # With no std, a normal distribution returns its mean every time
  packing: {dist: normal, mean: 1.5}

arrivals:
  # One order every 2 seconds on average; each arrival spawns pack_order
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}
```

docs/snippets/tutorials/first_shop.py:shop

```python
app = (
    # 1 simulated second = 1 real second
    SimulationContext(sim_id="Shop", factor=1.0)
    .add_resource("packers", current_cap=1, max_cap=1)
    # With no std, a normal distribution returns its mean every time
    .add_service("packing", dist="normal", mean=1.5)
    # One order every 2 seconds on average
    .add_arrival("orders", dist="exponential", rate=0.5)
)
```

docs/snippets/tutorials/first_shop_low_level.py:shop

```python
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="normal", mean=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)
```

Each value gets a path in the registry, such as `Shop.arrival.orders.rate` or `Shop.resources.packers.current_cap`. A connector or a scenario changes a value through its path while the run continues; [Live Parameters](https://jaehyeon.me/desflow/latest/architecture/registry/index.md) explains how.

______________________________________________________________________

## Say What an Order Does

An order asks for a packer, waits until it gets one, is packed for a time drawn from `packing`, and releases the packer. It publishes a `queued` event when it asks, `started` when it gets the packer, and a finished event at the end.

docs/snippets/yaml/first_shop.yaml:order

```yaml
tasks:
  pack_order:
    service: packing
    resource: packers
    # The finished event's value; id_field adds the task id as order_id
    payload: {}
    id_field: order_id
```

docs/snippets/tutorials/first_shop.py:order

```python
@app.task(service_id="packing", resource_id="packers")
def pack_order(order_id: int):
    # Requests a packer, waits the packing time, releases the packer.
    # The returned dictionary is the finished event's value.
    return {"order_id": order_id}


@app.arrival_loop("orders")
def order_generator(context: SimulationContext):
    order_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(pack_order(order_id))
        order_id += 1
```

docs/snippets/tutorials/first_shop_low_level.py:order

```python
def pack_order(env, sampler, packers, order_id):
    """One order: wait for a packer, pack it, release the packer."""
    key = f"task-{order_id}"
    path_id = "Shop.service.packing"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})

    with packers.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        service = env.registry.get_config(path_id)
        yield env.timeout(sampler.sample(service))
        env.publish_event(key, {"order_id": order_id})


def order_generator(env, sampler, packers):
    """Starts one pack_order for every arrival."""
    arrival = env.registry.get_config("Shop.arrival.orders")
    order_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, order_id))
        order_id += 1
```

The YAML task and the builder's `@app.task` publish the three events and handle the packer for you; the low-level process does each step itself. The arrival loop starts one order for every arrival: `spawn` in YAML, `@app.arrival_loop` in the builder, and `order_generator` in the low-level API.

______________________________________________________________________

## Print the Records

A sink, called an egress, receives every record the simulation publishes. `ConsoleEgress` prints them; [Connectors](https://jaehyeon.me/desflow/latest/architecture/connectors/index.md) lists the others.

docs/snippets/yaml/first_shop.yaml:records

```yaml
egress:
  - type: Console
```

docs/snippets/tutorials/first_shop.py:records

```python
app.add_egress(ConsoleEgress())
```

docs/snippets/tutorials/first_shop_low_level.py:records

```python
SINKS = [ConsoleEgress()]  # passed to env.setup_egress in run below
```

______________________________________________________________________

## Run It

docs/snippets/yaml/first_shop.yaml:run

```yaml
run:
  until: 10
```

docs/snippets/tutorials/first_shop.py:run

```python
if __name__ == "__main__":
    app.run(until=10.0)
```

docs/snippets/tutorials/first_shop_low_level.py:run

```python
def run(until=10.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(SINKS)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng())

    env.process(order_generator(env, sampler, packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    print("Starting simulation...")
    run()
```

Start it from a terminal:

```bash
desflow run first_shop.yaml
```

```bash
python first_shop.py
```

```bash
python first_shop_low_level.py
```

At `factor=1.0` one simulated second takes one real second, so the run takes 10 seconds. `[EVT]` lines are events and `[TEL]` lines are telemetry:

```text
[TEL] {'sim_ts': 0.0, 'timestamp': '...', 'path_id': 'system.simulation.lag_seconds', 'value': 0.0}
[EVT] {'sim_ts': 0.171, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Shop.service.packing', 'status': 'queued'}}
[EVT] {'sim_ts': 0.171, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Shop.service.packing', 'status': 'started'}}
[EVT] {'sim_ts': 1.671, 'timestamp': '...', 'key': 'task-0', 'value': {'order_id': 0}}
```

The only telemetry is `system.simulation.lag_seconds`, which every run with an egress publishes once per simulation second: how far the simulation is behind the wall clock. The arrival times are random, so each run shows different values.

______________________________________________________________________

## Add Randomness and Capacity Changes

Real packing times vary. `packing` now takes 3 seconds on average, give or take 0.5, and a seed makes the draws the same on every run. The packers get room for up to 5 units:

docs/snippets/yaml/shop_changes.yaml:random

```yaml
simulation:
  sim_id: Shop
  factor: 1.0
  random_seed: 42  # the same draws on every run

resources:
  packers: {current_cap: 1, max_cap: 5}  # room for up to 5 packers

services:
  # Packing takes 3 seconds on average, give or take 0.5
  packing: {dist: normal, mean: 3.0, std: 0.5}
```

docs/snippets/tutorials/shop_changes.py:random

```python
app = (
    # Same draws every run
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=5)  # room for up to 5 packers
    # Packing takes 3 seconds on average, give or take 0.5
    .add_service("packing", dist="normal", mean=3.0, std=0.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(ConsoleEgress())
)
```

docs/snippets/tutorials/shop_changes_low_level.py:random

```python
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    # Packing takes 3 seconds on average, give or take 0.5
    service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=5)},  # up to 5 packers
)
SEED = 42  # Sampler(rng=np.random.default_rng(SEED)) in run: same draws every run
```

Now change the capacity while the shop runs: three packers from 10 seconds, two from 20. The YAML `scenario` and the process in the other two tabs both act on the simulation clock, so the changes land at exactly those times on every run, at any `factor`:

docs/snippets/yaml/shop_changes.yaml:changes

```yaml
scenario:
  - {at: 10, path: Shop.resources.packers.current_cap, value: 3}
  - {at: 20, path: Shop.resources.packers.current_cap, value: 2}
```

docs/snippets/tutorials/shop_changes.py:changes

```python
def capacity_plan(context: SimulationContext):
    """Three packers from t=10, two from t=20, on the simulation clock."""
    path = "Shop.resources.packers.current_cap"
    yield context.env.timeout(10.0)
    context.env.registry.update(path, 3)
    yield context.env.timeout(10.0)
    context.env.registry.update(path, 2)


app.add_process(capacity_plan)
```

docs/snippets/tutorials/shop_changes_low_level.py:changes

```python
def capacity_plan(env):
    """Three packers from t=10, two from t=20, on the simulation clock."""
    path = "Shop.resources.packers.current_cap"
    yield env.timeout(10.0)
    env.registry.update(path, 3)
    yield env.timeout(10.0)
    env.registry.update(path, 2)
```

In the first 10 seconds orders arrive faster than one packer finishes them, so the queue grows. At 10 seconds three packers drain it. At 20 seconds the capacity drops to two; an order already with a third packer finishes, and no new order starts until two or fewer packers are busy, as [Resources](https://jaehyeon.me/desflow/latest/building-blocks/resources/index.md) shows.

______________________________________________________________________

## Connect to Kafka

Install the Kafka extra and start a broker with [odctl](https://github.com/jaehyeon-kim/odctl), a separate command-line tool installed once with `pip install "odctl>=1.0,<2"`:

```bash
pip install "desflow[kafka]"
odctl up kafka-lite
```

Replace the console sink with Kafka, and add a Kafka ingress so the run takes parameter changes from the `sim-config` topic. The rest of the step 2 shop stays as it is:

docs/snippets/yaml_build/shop_kafka.yaml:kafka

```yaml
ingress:
  # Reads {"path_id": ..., "value": ...} messages
  - type: Kafka
    config: {topic: sim-config, bootstrap_servers: localhost:9092}

egress:
  - type: Kafka
    config:
      bootstrap_servers: localhost:9092
      event_topic: sim-events
      telemetry_topic: sim-telemetry
```

docs/snippets/tutorials/shop_kafka.py:kafka

```python
app.add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092"))
app.add_egress(
    KafkaEgress(
        bootstrap_servers="localhost:9092",
        event_topic="sim-events",
        telemetry_topic="sim-telemetry",
    )
)
```

docs/snippets/tutorials/shop_kafka_low_level.py:kafka

```python
env.setup_ingress(
    [KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092")]
)
env.setup_egress(
    [
        KafkaEgress(
            bootstrap_servers="localhost:9092",
            event_topic="sim-events",
            telemetry_topic="sim-telemetry",
        )
    ]
)
```

Run it, and while it runs, send a capacity change from another terminal. The broker's own tools are inside its container:

```bash
echo '{"path_id": "Shop.resources.packers.current_cap", "value": 3}' | docker exec -i kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:19092 --topic sim-config
```

The ingress applies the change to the registry, and the packers grow to three units without stopping the run. Stop the run with Ctrl+C, and the broker with `odctl down kafka-lite --volumes`.

______________________________________________________________________

## Full Files

The complete first shop, which the documentation tests run:

docs/snippets/yaml/first_shop.yaml

```yaml
# The First Simulation tutorial: the shop as a blueprint.
# Run it with: desflow run first_shop.yaml
simulation:
  sim_id: Shop
  factor: 1.0  # 1 simulated second takes 1 real second

resources:
  packers: {current_cap: 1, max_cap: 1}

services:
  # With no std, a normal distribution returns its mean every time
  packing: {dist: normal, mean: 1.5}

arrivals:
  # One order every 2 seconds on average; each arrival spawns pack_order
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    # The finished event's value; id_field adds the task id as order_id
    payload: {}
    id_field: order_id

egress:
  - type: Console

run:
  until: 10
```

docs/snippets/tutorials/first_shop.py

```python
"""The First Simulation tutorial: the shop, with the SimulationContext builder.

One packer, orders arriving about every 2 seconds, and 1.5 seconds of packing per
order. Every record is printed to the terminal.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

app = (
    # 1 simulated second = 1 real second
    SimulationContext(sim_id="Shop", factor=1.0)
    .add_resource("packers", current_cap=1, max_cap=1)
    # With no std, a normal distribution returns its mean every time
    .add_service("packing", dist="normal", mean=1.5)
    # One order every 2 seconds on average
    .add_arrival("orders", dist="exponential", rate=0.5)
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(order_id: int):
    # Requests a packer, waits the packing time, releases the packer.
    # The returned dictionary is the finished event's value.
    return {"order_id": order_id}


@app.arrival_loop("orders")
def order_generator(context: SimulationContext):
    order_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(pack_order(order_id))
        order_id += 1


app.add_egress(ConsoleEgress())

if __name__ == "__main__":
    app.run(until=10.0)
```

docs/snippets/tutorials/first_shop_low_level.py

```python
"""The First Simulation tutorial: the shop, on the low-level API.

One packer, orders arriving about every 2 seconds, and 1.5 seconds of packing per
order. Every record is printed to the terminal.
"""

import logging

import numpy as np

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
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="normal", mean=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


def pack_order(env, sampler, packers, order_id):
    """One order: wait for a packer, pack it, release the packer."""
    key = f"task-{order_id}"
    path_id = "Shop.service.packing"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})

    with packers.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        service = env.registry.get_config(path_id)
        yield env.timeout(sampler.sample(service))
        env.publish_event(key, {"order_id": order_id})


def order_generator(env, sampler, packers):
    """Starts one pack_order for every arrival."""
    arrival = env.registry.get_config("Shop.arrival.orders")
    order_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, order_id))
        order_id += 1


SINKS = [ConsoleEgress()]  # passed to env.setup_egress in run below


def run(until=10.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(SINKS)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng())

    env.process(order_generator(env, sampler, packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    print("Starting simulation...")
    run()
```

Next, [Ways to Write a Simulation](https://jaehyeon.me/desflow/latest/architecture/overview/index.md) compares the three APIs, and the [examples](https://jaehyeon.me/desflow/latest/examples/local/index.md) show each connector.
