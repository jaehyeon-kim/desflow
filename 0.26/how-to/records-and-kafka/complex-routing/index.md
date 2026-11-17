# Route Records to Topics

Records often belong in different Kafka topics: task events in one, telemetry in another, and alarms in a topic that someone watches. `KafkaEgress` takes a `topic_router` function that picks the topic for each record, instead of one event topic and one telemetry topic.

______________________________________________________________________

## Before You Start

- [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#kafka) explains `KafkaEgress` and its two topics.
- The scripts publish to Kafka, so start a broker with `odctl up kafka-lite`. [Installation](https://jaehyeon.me/desflow/latest/installation/index.md) shows the one-time odctl install. Tear it down afterwards with `odctl down kafka-lite --volumes`.

______________________________________________________________________

## Steps

### Write a Router

A topic router is a Python function that receives one record, as a dictionary with its `stream_type`, `key` or `path_id`, `task`, `instance` and `value`, and returns the topic name as a string. `KafkaEgress` sends every record it receives, so the router must always return a topic. To drop records, give the provider a `when` predicate on `add_egress`, which filters them before they reach the router.

This router sends telemetry, alarms and other events to three topics. Every form uses it unchanged, from `complex_routing_logic.py`:

```python
def custom_kafka_router(data: Dict[str, Any]) -> str:
    """Picks a topic for each record by its kind and its value."""
    if data.get("stream_type") == "telemetry":
        return "shop-telemetry"

    value = data.get("value")
    if isinstance(value, dict) and (
        value.get("status") == "packing_failed" or value.get("level") == "ERROR"
    ):
        return "system-alarms"

    # Every other event, including a payload that is not a dict.
    return "shop-events"
```

### Attach the Router

Pass the router as `topic_router` to `KafkaEgress`:

```yaml
egress:
  - type: Kafka
    config:
      bootstrap_servers: 127.0.0.1:9092
      topic_router: !python complex_routing_logic.custom_kafka_router
```

```python
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_egress(
        KafkaEgress(
            bootstrap_servers="127.0.0.1:9092",
            topic_router=custom_kafka_router,
        )
    )
)
```

```python
def kafka_egress():
    """Sends each record to the topic the router picks."""
    return KafkaEgress(
        bootstrap_servers="127.0.0.1:9092",
        topic_router=custom_kafka_router,
    )
```

`run` passes it to `env.setup_egress`, as the full file shows.

### Run the Shop

The shop packs an order about every 2 seconds, and every tenth order fails, so all three topics receive records. The minimal statistics give `shop-telemetry` a reading every 10 seconds. The payload comes from `packed` in `complex_routing_logic.py`, which returns `{"status": "packing_failed"}` for every tenth order and `{"status": "packed"}` for the others:

```yaml
resources:
  packers: {current_cap: 1, max_cap: 1}

services:
  packing: {dist: constant, value: 1.5 s}

arrivals:
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    # Called as packed(task_id, context): every tenth order fails
    payload: !python complex_routing_logic.packed

stats: {level: minimal, interval: 10}  # readings for shop-telemetry
```

```python
app.add_arrival("orders", dist="exponential", rate=0.5)
app.add_service("packing", dist="constant", value=1.5)
app.with_stats("minimal", interval=10)  # readings for shop-telemetry


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return packed(task_id)  # every tenth order fails


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))
```

```python
def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    stats = env.stats
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    stats.task_queued("pack_order", task_id)
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, packed(task_id), "pack_order", task_id)
        stats.task_finished("pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))
```

### Create the Topics

With a router, `KafkaEgress` does not create topics at start, because only the router knows which topics it uses. This function in `complex_routing_logic.py` creates them:

```python
def create_topics() -> None:
    """With a router, KafkaEgress creates no topics, so create them first."""
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    admin.create_topics([{"name": topic} for topic in TOPICS])
```

Each form calls it before the run:

```yaml
run:
  until: 1 min
  # With a router, KafkaEgress creates no topics, so create them first
  before:
    - !python complex_routing_logic.create_topics
```

```python
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    create_topics()  # with a router, KafkaEgress creates no topics
    app.run(until="1 min")
```

```python
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    create_topics()  # with a router, KafkaEgress creates no topics
    run()
```

______________________________________________________________________

## Check the Result

Run `desflow run complex_routing.yaml`, `python complex_routing.py` or `python complex_routing_low_level.py` while `kafka-lite` is up, beside `complex_routing_logic.py`. Each runs for one minute. The Kafka UI of `kafka-lite`, at http://127.0.0.1:8086, shows the records arriving in `shop-events`, `shop-telemetry` and `system-alarms`. Only the orders whose instance ends in 9 reach `system-alarms`. The three forms use seed 42, so they send the same events.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Route Records to Topics guide: a shop whose records go to three Kafka topics.
# Needs a broker on 127.0.0.1:9092 (odctl up kafka-lite). Run it with
# desflow run complex_routing.yaml, beside complex_routing_logic.py
simulation:
  sim_id: Shop
  factor: 1
  random_seed: 42

egress:
  - type: Kafka
    config:
      bootstrap_servers: 127.0.0.1:9092
      topic_router: !python complex_routing_logic.custom_kafka_router

resources:
  packers: {current_cap: 1, max_cap: 1}

services:
  packing: {dist: constant, value: 1.5 s}

arrivals:
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    # Called as packed(task_id, context): every tenth order fails
    payload: !python complex_routing_logic.packed

stats: {level: minimal, interval: 10}  # readings for shop-telemetry

run:
  until: 1 min
  # With a router, KafkaEgress creates no topics, so create them first
  before:
    - !python complex_routing_logic.create_topics
```

```python
"""The Route Records to Topics guide: the router, the topics and the payload that the
blueprint, the builder and the low-level script share.
"""

from typing import Any, Dict

from desflow import KafkaAdminConnector

TOPICS = ["shop-events", "shop-telemetry", "system-alarms"]


def custom_kafka_router(data: Dict[str, Any]) -> str:
    """Picks a topic for each record by its kind and its value."""
    if data.get("stream_type") == "telemetry":
        return "shop-telemetry"

    value = data.get("value")
    if isinstance(value, dict) and (
        value.get("status") == "packing_failed" or value.get("level") == "ERROR"
    ):
        return "system-alarms"

    # Every other event, including a payload that is not a dict.
    return "shop-events"


def create_topics() -> None:
    """With a router, KafkaEgress creates no topics, so create them first."""
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    admin.create_topics([{"name": topic} for topic in TOPICS])


def packed(task_id: int, context=None) -> Dict[str, str]:
    """Every tenth order fails, for system-alarms."""
    if task_id % 10 == 9:
        return {"status": "packing_failed"}
    return {"status": "packed"}
```

```python
"""The Route Records to Topics guide: a shop whose records go to three Kafka topics.

It needs a Kafka broker on 127.0.0.1:9092: start one with `odctl up kafka-lite`. Run it
with `python complex_routing.py`, beside complex_routing_logic.py; it runs for one
minute.
"""

import logging

from complex_routing_logic import create_topics, custom_kafka_router, packed
from desflow import KafkaEgress, SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_egress(
        KafkaEgress(
            bootstrap_servers="127.0.0.1:9092",
            topic_router=custom_kafka_router,
        )
    )
)

app.add_arrival("orders", dist="exponential", rate=0.5)
app.add_service("packing", dist="constant", value=1.5)
app.with_stats("minimal", interval=10)  # readings for shop-telemetry


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return packed(task_id)  # every tenth order fails


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    create_topics()  # with a router, KafkaEgress creates no topics
    app.run(until="1 min")
```

```python
"""The Route Records to Topics guide, low-level API: a shop whose records go to three
Kafka topics.

It needs a Kafka broker on 127.0.0.1:9092: start one with `odctl up kafka-lite`. Run it
with `python complex_routing_low_level.py`, beside complex_routing_logic.py; it runs
for one minute.
"""

import itertools
import logging

from complex_routing_logic import create_topics, custom_kafka_router, packed
from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaEgress,
    Sampler,
    SimParameter,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


def kafka_egress():
    """Sends each record to the topic the router picks."""
    return KafkaEgress(
        bootstrap_servers="127.0.0.1:9092",
        topic_router=custom_kafka_router,
    )


def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    stats = env.stats
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    stats.task_queued("pack_order", task_id)
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, packed(task_id), "pack_order", task_id)
        stats.task_finished("pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def run(until=60.0, factor=1.0, egress=None):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(egress or [kafka_egress()])
    env.setup_stats("Shop", "minimal", interval=10)  # readings for shop-telemetry
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    create_topics()  # with a router, KafkaEgress creates no topics
    run()
```
