# Kafka Digital Twin

An online shop that packs orders, takes parameter updates from a Kafka topic and publishes its events and telemetry to Kafka topics. Each tab shows the example written one way: with the builder API, with the low-level API, or as a YAML blueprint. [Ways to write a simulation](https://jaehyeon.me/dynamic-des/latest/architecture/overview/index.md) compares the three.

With `KafkaIngress` and `KafkaEgress` in place of the local connectors, the simulation runs as a separate service. It reads JSON parameter updates from a topic, and publishes telemetry and typed Pydantic events to other topics.

This example builds the simulation of the builder example in the Builder tab from a YAML blueprint, with no Python. The connectors, the parameters, the task and the telemetry are all declared in the file.

**Quick Start**

`ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "dynamic-des[kafka]" --with-executables-from "odctl>=1.0,<2"

# 2. Download the example
ddes examples get kafka --api yaml
cd kafka/yaml

# 3. Start its service
odctl up kafka-lite

# 4. Run it (Ctrl + C to stop)
ddes run kafka.yaml

# 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
uv run --no-project --with "dynamic-des[kafka]" --with nicegui ../kafka_dashboard.py

# 6. Stop its service when finished
odctl down kafka-lite --volumes
```

```bash
# 1. Install
pip install "dynamic-des[kafka]" "odctl>=1.0,<2" nicegui

# 2. Download the example
ddes examples get kafka --api yaml
cd kafka/yaml

# 3. Start its service
odctl up kafka-lite

# 4. Run it (Ctrl + C to stop)
ddes run kafka.yaml

# 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
python ../kafka_dashboard.py

# 6. Stop its service when finished
odctl down kafka-lite --volumes
```

**What It Does**

The run keeps going until you stop it. When it starts, `KafkaEgress` creates `sim-events` and `sim-telemetry` if they do not exist. The run then publishes task lifecycle events to `sim-events` and packer metrics to `sim-telemetry`, and applies parameter updates sent to `sim-config`.

The dashboard in the Quick Start works with this run unchanged, because it reads the same topics and the same four packer metrics.

`KAFKA_BOOTSTRAP_SERVERS` overrides the broker address, because the blueprint reads it with `${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}`.

**Full Source Code**

The blueprint wires both Kafka connectors and declares the packers, the packing service, the order arrival and the task. The finished event of each task is the fixed `payload`, and the `telemetry` entry publishes four built-in statistics of the packers every 2 simulation seconds, the same four the builder and low-level versions publish.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

examples/yaml/kafka.yaml

```yaml
# Kafka digital twin in YAML.
#
# The YAML version of examples/builder/kafka_example.py, an online shop packing
# orders. KafkaIngress reads parameter updates from sim-config, and KafkaEgress
# publishes lifecycle events to sim-events and packer metrics to sim-telemetry.
# KafkaEgress creates its two topics when the run starts.
#
# Needs a broker: odctl up kafka-lite. Runs until interrupted with Ctrl + C.
# KAFKA_BOOTSTRAP_SERVERS overrides the broker address.
# Run it with: ddes run examples/yaml/kafka.yaml

simulation:
  sim_id: Shop
  factor: 1.0
  random_seed: 42

ingress:
  - type: Kafka
    config:
      topic: sim-config
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

egress:
  - type: Kafka
    config:
      event_topic: sim-events
      telemetry_topic: sim-telemetry
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

resources:
  packers: {current_cap: 1, max_cap: 10}

services:
  packing: {dist: normal, mean: 3.0, std: 0.5}

arrivals:
  orders: {dist: exponential, rate: 1.0, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {path_id: Shop.service.packing, status: finished}

telemetry:
  # Samples the packers every 2 simulation seconds.
  - interval: 2.0
    publish:
      packers.capacity: packers.capacity
      packers.in_use: packers.in_use
      packers.queue_length: packers.queue_length
      packers.utilization: packers.utilization
```

This example demonstrates how to integrate `dynamic-des` into a full event-driven architecture using the **builder API (`SimulationContext`)**.

**Quick Start**

`ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "dynamic-des[kafka]" --with-executables-from "odctl>=1.0,<2"

# 2. Download the example
ddes examples get kafka --api builder
cd kafka/builder

# 3. Start its service
odctl up kafka-lite

# 4. Run it (Ctrl + C to stop)
uv run --no-project --with "dynamic-des[kafka]" kafka_example.py

# 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
uv run --no-project --with "dynamic-des[kafka]" --with nicegui ../kafka_dashboard.py

# 6. Stop its service when finished
odctl down kafka-lite --volumes
```

```bash
# 1. Install
pip install "dynamic-des[kafka]" "odctl>=1.0,<2" nicegui

# 2. Download the example
ddes examples get kafka --api builder
cd kafka/builder

# 3. Start its service
odctl up kafka-lite

# 4. Run it (Ctrl + C to stop)
python kafka_example.py

# 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
python ../kafka_dashboard.py

# 6. Stop its service when finished
odctl down kafka-lite --volumes
```

The run keeps going until you stop it. It logs one line per order as a packer starts on it, publishes task lifecycle events to `sim-events` and resource metrics to `sim-telemetry`.

**Full Source Code**

This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

examples/builder/kafka_example.py

```python
"""Kafka Digital Twin, builder API.

Builds `Shop`, an online shop packing orders, with `SimulationContext` and connects it
to Kafka in both directions. `KafkaIngress` reads parameter updates from `sim-config`,
so the running simulation can be steered without restarting it. `KafkaEgress`
publishes lifecycle events to `sim-events` and metrics to `sim-telemetry`, with
Pydantic models giving the events a declared shape.

Needs a broker: `odctl up kafka-lite`. Runs until interrupted with Ctrl + C.
"""

import logging
import os
import time

from pydantic import BaseModel

from dynamic_des import (
    KafkaAdminConnector,
    KafkaEgress,
    KafkaIngress,
    SimulationContext,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
)
logger = logging.getLogger("kafka_example")


# ==========================================
# 1. Define Strongly-Typed Event Payloads
# ==========================================
class TaskEvent(BaseModel):
    """
    Strongly typed event payload to guarantee schema consistency
    when shipping data over the wire to Kafka.
    """

    path_id: str
    status: str


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ==========================================
# 2. Infrastructure Builder
# ==========================================
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS))
    .add_egress(
        KafkaEgress(
            event_topic="sim-events",
            telemetry_topic="sim-telemetry",
            bootstrap_servers=BOOTSTRAP_SERVERS,
        )
    )
    .add_resource("packers", current_cap=1, max_cap=10)
    .add_service("packing", dist="normal", mean=3.0, std=0.5)
    .add_arrival("orders", dist="exponential", rate=1.0)
)


# ==========================================
# 3. Simulation Logic
# ==========================================
@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, context):
    """
    The @task decorator automatically locks the resource and emits the
    'queued' and 'started' events. We just execute our custom logic
    and return the final payload.
    """
    logger.info(f"Task {task_id} started at sim time: {context._env.now:.2f}s")

    # Return the strongly-typed Pydantic model for the 'finished' state
    return TaskEvent(path_id="Shop.service.packing", status="finished").model_dump(
        mode="json"
    )


@app.arrival_loop("orders")
def arrival_generator(context):
    task_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, context))
        task_id += 1


@app.telemetry_loop(interval=2.0)
def telemetry_monitor(context):
    """Low-volume system health stream."""
    res = context.get_resource("packers")

    # The same metrics as the low-level and YAML versions
    context.publish("packers.capacity", res.capacity)
    context.publish("packers.in_use", res.in_use)
    context.publish("packers.queue_length", len(res.queue.items))

    util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0
    context.publish("packers.utilization", util)


# ==========================================
# 4. Execution
# ==========================================
def run():
    TOPICS_CONFIG = [
        {"name": "sim-config", "partitions": 1},
        {"name": "sim-telemetry", "partitions": 1},
        {"name": "sim-events", "partitions": 1},
    ]

    logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    try:
        admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
        admin.create_topics(topics_config=TOPICS_CONFIG)
        time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not explicitly create topics: {e}")

    print("Simulation started.")
    print("  - Listen to 'sim-telemetry' for system vitals.")
    print("  - Listen to 'sim-events' for task lifecycles.")
    print("  - Send to 'sim-config' to update parameters.")

    app.run()  # Starts the clock and orchestrates all connectors


if __name__ == "__main__":
    run()
```

This example demonstrates how to integrate `dynamic-des` into a full event-driven architecture using the low-level **Low-level API**.

**Quick Start**

`ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "dynamic-des[kafka]" --with-executables-from "odctl>=1.0,<2"

# 2. Download the example
ddes examples get kafka --api low-level
cd kafka/low-level

# 3. Start its service
odctl up kafka-lite

# 4. Run it (Ctrl + C to stop)
uv run --no-project --with "dynamic-des[kafka]" kafka_example.py

# 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
uv run --no-project --with "dynamic-des[kafka]" --with nicegui ../kafka_dashboard.py

# 6. Stop its service when finished
odctl down kafka-lite --volumes
```

```bash
# 1. Install
pip install "dynamic-des[kafka]" "odctl>=1.0,<2" nicegui

# 2. Download the example
ddes examples get kafka --api low-level
cd kafka/low-level

# 3. Start its service
odctl up kafka-lite

# 4. Run it (Ctrl + C to stop)
python kafka_example.py

# 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
python ../kafka_dashboard.py

# 6. Stop its service when finished
odctl down kafka-lite --volumes
```

The run keeps going until you stop it. It logs one line per order as a packer starts on it, publishes task lifecycle events to `sim-events` and resource metrics to `sim-telemetry`.

**Full Source Code**

This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

examples/low-level/kafka_example.py

```python
"""Kafka Digital Twin, low-level API.

The low-level twin of `builder/kafka_example.py`, an online shop packing orders, with
the environment, registry and connectors wired by hand. Like the builder version, it
creates its topics first with `KafkaAdminConnector`.

Needs a broker: `odctl up kafka-lite`. Runs until interrupted with Ctrl + C.
"""

import logging
import os
import time

import numpy as np
from pydantic import BaseModel

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaAdminConnector,
    KafkaEgress,
    KafkaIngress,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
)
logger = logging.getLogger("kafka_example")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# ==========================================
# 1. Define Strongly-Typed Event Payloads
# ==========================================
class TaskEvent(BaseModel):
    """
    Thanks to dynamic-des's duck-typing, we can pass this Pydantic model
    directly into env.publish_event(). KafkaEgress converts it to a dict and
    serializes it, to JSON or to Avro.
    """

    path_id: str
    status: str


# ==========================================
# 2. Initial System State
# ==========================================
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},  # 1 a second
    service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=10)},
)


# ==========================================
# 3. Simulation Logic
# ==========================================
def pack_order(env, sampler, packers, task_id):
    """Publishes a Pydantic model instead of a dict for each lifecycle event."""
    key, path_id = f"task-{task_id}", "Shop.service.packing"
    env.publish_event(key, TaskEvent(path_id=path_id, status="queued"))

    with packers.request() as req:
        yield req

        logger.info(f"Task {task_id} started at sim time: {env.now:.2f}s")
        env.publish_event(key, TaskEvent(path_id=path_id, status="started"))

        # Use the latest service config from the registry
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))

        env.publish_event(key, TaskEvent(path_id=path_id, status="finished"))


def arrival_generator(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, sampler, packers, task_id))
        task_id += 1


def telemetry_generator(env, packers):
    """Low-volume system health stream, pushed to the 'sim-telemetry' topic."""
    while True:
        env.publish_telemetry("Shop.packers.capacity", packers.capacity)
        env.publish_telemetry("Shop.packers.in_use", packers.in_use)
        env.publish_telemetry("Shop.packers.queue_length", len(packers.queue.items))

        util = (packers.in_use / packers.capacity) * 100 if packers.capacity > 0 else 0
        env.publish_telemetry("Shop.packers.utilization", util)

        yield env.timeout(2.0)


# ==========================================
# 4. Execution
# ==========================================
def run():
    TOPICS_CONFIG = [
        {"name": "sim-config", "partitions": 1},
        {"name": "sim-telemetry", "partitions": 1},
        {"name": "sim-events", "partitions": 1},
    ]

    logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    admin_connector = KafkaAdminConnector(
        bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100
    )
    admin_connector.create_topics(topics_config=TOPICS_CONFIG)
    time.sleep(2)

    # (Optional: Pass **kwargs like `security_protocol="SASL_SSL"` for enterprise clusters)
    ingress = KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS)

    # By default, this uses JsonSerializer. To use Avro for enterprise environments:
    # from dynamic_des.connectors.egress.kafka import ConfluentAvroSerializer
    # avro_serializer = ConfluentAvroSerializer(
    #     registry_url="http://127.0.0.1:8081", schema_str=AVRO_SCHEMA
    # )
    # Then pass: topic_serializers={"sim-events": avro_serializer}
    # Generate AVRO_SCHEMA from AvroBaseModel rather than a plain Pydantic model;
    # see the Avro and Pydantic guide. TaskEvent above has no .avro_schema().
    egress = KafkaEgress(
        telemetry_topic="sim-telemetry",
        event_topic="sim-events",
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_ingress([ingress])
    env.setup_egress([egress])

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(42))

    env.process(arrival_generator(env, sampler, packers))
    env.process(telemetry_generator(env, packers))

    print("Simulation started.")
    print("  - Listen to 'sim-telemetry' for system vitals.")
    print("  - Listen to 'sim-events' for task lifecycles.")
    print("  - Send to 'sim-config' to update parameters.")

    try:
        env.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
```
