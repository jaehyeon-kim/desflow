# Local Simulation

This example models a shop packing orders. The simulation prints events and telemetry to the terminal. It needs no container, so it is the place to start. Each tab shows the example written one way: as a YAML blueprint, with the builder API, or with the low-level API. [Ways to write a simulation](https://jaehyeon.me/desflow/latest/user-guide/ways-to-write/index.md) compares the three.

Local connectors need no Docker, Kafka or other external store, so this example is useful for testing and benchmarking.

This example builds the same simulation as the builder example in the Builder tab, from a YAML blueprint and nothing else. No Python is written: the arrival loop, the task and the statistics are all declared in the file.

It writes to `ConsoleEgress` and needs no container, so it is the place to start.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local --api yaml
cd local/yaml

# 3. Run it
desflow run local.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local --api yaml
cd local/yaml

# 3. Run it
desflow run local.yaml
```

**What It Does**

The run prints every record to the terminal and stops after 30 simulation seconds, which at `factor: 1.0` is 30 real seconds. Telemetry lines carry `[TEL]` and events carry `[EVT]`. These are the events of the first order, and the packers' capacity reading before and after the first change:

```text
[EVT] {'sim_ts': 1.276, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'status': 'queued'}}
[EVT] {'sim_ts': 1.276, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'status': 'started'}}
[TEL] {'sim_ts': 2.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.capacity', 'value': 1}
[EVT] {'sim_ts': 4.651, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'event_type': 'order_packed', 'shipping': 'standard', 'order_id': 0}}
[TEL] {'sim_ts': 12.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.capacity', 'value': 3}
```

Each order produces a `queued`, a `started` and a finished event. Every event carries the order's key, such as `orders-1`, with the task's name and instance. The finished event is the task's `payload`, with the task id added as `order_id` because of `id_field`. The capacity reading shows the scenario: one packer, three from 10 seconds, then two from 20 seconds. Add `--until 10` to stop after 10 simulation seconds instead.

**Full Source Code**

The blueprint declares one resource, one service and one arrival. `spawn` names the task each arrival starts, `stats: minimal` publishes the packers' four readings and the task's `in_progress` every 2 simulation seconds, and the `scenario` changes the number of packers at 10 and 20 seconds.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml
# Local simulation in YAML, with no Python and no containers.
#
# The twin of examples/builder/local_example.py. This simulation models a shop
# packing orders. ConsoleEgress prints events and telemetry to the terminal.
# A scenario adds packers at 10 seconds and removes one at 20 seconds.
# The run ends on its own after 30 simulation seconds.
#
# Run it from its folder with: desflow run local.yaml

simulation:
  sim_id: Shop
  factor: 1.0
  random_seed: 42

egress:
  - type: Console

resources:
  packers: {current_cap: 1, max_cap: 5}

services:
  packing: {dist: normal, mean: 3.0, std: 0.5}

arrivals:
  # Each order spawns one pack_order task.
  orders: {dist: exponential, rate: 1.0, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    # The value of the task's finished event. id_field adds the task id as order_id.
    payload: {event_type: order_packed, shipping: standard}
    id_field: order_id

# Publishes the packers' capacity, in_use, queue_length and utilization every 2
# simulation seconds, as Shop.stats.resources.packers.<reading>.
stats: {level: minimal, interval: 2}

scenario:
  # Three packers from 10 seconds, then two from 20 seconds.
  - {at: 10, path: Shop.resources.packers.current_cap, value: 3}
  - {at: 20, path: Shop.resources.packers.current_cap, value: 2}

run:
  until: 30
```

This example demonstrates how to build a dynamic simulation using the **builder API (`SimulationContext`)** and **Local Connectors**.

This example adds `ConsoleEgress` and no ingress. `add_process` starts `capacity_changes`, which waits on the simulation clock and changes the number of packers at 10 and 20 seconds, as the YAML `scenario` does.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local --api builder
cd local/builder

# 3. Run it
uv run --no-project --with desflow local_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local --api builder
cd local/builder

# 3. Run it
python local_example.py
```

**Full Source Code**

This script builds the model, runs it for 30 simulation seconds, and streams events and telemetry directly to your terminal.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

```python
"""Local simulation, builder API, no containers.

This example models a shop packing orders with `SimulationContext`.
`ConsoleEgress` prints events and telemetry to the terminal, so nothing external is
involved. A scenario adds packers at 10 seconds and removes one at 20 seconds.
The built-in statistics show the effect.

Start here. It needs no broker, no database and no object store, and it ends on its own
after 30 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

# Logging is configured here rather than in a wrapper, because this script is run
# directly. Without it the run produces no output at all.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

# ==========================================
# 1. Infrastructure Builder
# ==========================================
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=1, max_cap=5)
    .add_service("packing", dist="normal", mean=3.0, std=0.5)
    .add_arrival("orders", dist="exponential", rate=1.0)
    # The packers' readings every 2 simulation seconds
    .with_stats("minimal", interval=2.0)
)


# ==========================================
# 2. Simulation Logic (Decorators)
# ==========================================
@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, context):
    """Packs one order and returns the payload of its finished event."""
    return {"event_type": "order_packed", "shipping": "standard", "order_id": task_id}


@app.arrival_loop("orders")
def arrival_generator(context):
    """Spawns a pack_order task for every order the 'orders' distribution draws."""
    task_id = 0
    while True:
        # Each arrival is a new order, whose key (orders-1, orders-2) is on its events
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, context, entity=entity))
        task_id += 1


def capacity_changes(context):
    """Three packers from 10 seconds, then two from 20 seconds, on simulation time."""
    path = "Shop.resources.packers.current_cap"
    yield context.env.timeout(10)
    context.env.registry.update(path, 3)
    yield context.env.timeout(10)
    context.env.registry.update(path, 2)


app.add_process(capacity_changes)


# ==========================================
# 3. Execution
# ==========================================
def run():
    """Starts the local simulation for a fixed duration."""
    logger.info("Starting Builder Local Example. Running for 30 seconds...")
    app.run(until=30)


if __name__ == "__main__":
    run()
```

This example demonstrates how to build a dynamic simulation using the low-level **Low-level API** and **Local Connectors**.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local --api low-level
cd local/low-level

# 3. Run it
uv run --no-project --with desflow local_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local --api low-level
cd local/low-level

# 3. Run it
python local_example.py
```

**Full Source Code**

This script builds the model and runs it for 30 simulation seconds. The `capacity_changes` process changes the number of packers from 1 to 3 at 10 seconds, then down to 2 at 20 seconds. Events and telemetry stream directly to your terminal, driven by plain SimPy generators.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

```python
"""Local simulation, low-level API, no containers.

The low-level twin of `builder/local_example.py`, built on `DynamicRealtimeEnvironment`
directly. It models a shop packing orders and uses `ConsoleEgress` to print events
and telemetry. A scenario process adds packers at 10 seconds and removes one at
20 seconds. The built-in statistics show the effect.

Needs no containers. Ends on its own after 30 simulation seconds.
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
logger = logging.getLogger("local_example")

# The shop starts with 1 packer and has room for 5.
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
    service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=5)},
)


def pack_order(env, sampler, packers, key, task_id):
    """Models the task lifecycle: queued, started, then finished with its payload.

    Every event carries the order's key, and the task's name and instance id.
    """
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)  # counted as in progress
    with packers.request() as req:
        yield req
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        # Late binding: the service time is read when the work starts
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish(
            {"event_type": "order_packed", "shipping": "standard", "order_id": task_id}
        )
        env.stats.task_finished("pack_order", task_id)


def arrival_generator(env, sampler, packers):
    """Spawns a pack_order task for every order the arrival distribution draws."""
    # A reference: the config follows the registry when the rate changes
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        # Each arrival is a new order, keyed orders-1, orders-2 and so on
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def capacity_changes(env):
    """Three packers from 10 seconds, then two from 20 seconds, on simulation time."""
    path = "Shop.resources.packers.current_cap"
    yield env.timeout(10)
    env.registry.update(path, 3)
    yield env.timeout(10)
    env.registry.update(path, 2)


def run():
    env = DynamicRealtimeEnvironment(factor=1.0, strict=False)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # The packers' readings every 2 simulation seconds
    env.setup_stats("Shop", "minimal", interval=2.0)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(42))

    env.process(arrival_generator(env, sampler, packers))
    env.process(capacity_changes(env))

    logger.info("Simulation started. Watch the capacity change at 10 s and 20 s...")
    try:
        env.run(until=30)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
```
