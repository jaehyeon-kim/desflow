# Fast-Forward to Parquet

A week of an online shop's orders, generated at `factor=0.0` and written to Parquet files on the local disk or in S3-compatible storage. Each tab shows the example written one way: as a YAML blueprint, with the builder API, or with the low-level API. [Ways to write a simulation](https://jaehyeon.me/desflow/latest/user-guide/ways-to-write/index.md) compares the three.

desflow is built for real-time digital twins, but the same model can also run faster than real time. With the time factor at 0 and a chosen start time, a run generates a large amount of historical data in seconds, or predicts future states.

This example builds the simulation of the builder example in the Builder tab from a YAML blueprint, with no Python. With `factor: 0.0` the clock is detached from the wall clock, so a week of shop orders is written to Parquet as fast as the machine allows.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[parquet]"

# 2. Download the example
desflow examples get parquet --api yaml
cd parquet/yaml

# 3. Run it
desflow run parquet.yaml
```

```bash
# 1. Install
pip install "desflow[parquet]"

# 2. Download the example
desflow examples get parquet --api yaml
cd parquet/yaml

# 3. Run it
desflow run parquet.yaml
```

**What It Does**

The run writes Parquet chunks to a local `data/` folder by default and ends on its own. The folder is created on the first write. On a laptop a week of simulated time takes about 20 seconds and writes about 850 files with 3.6 million rows, one row per lifecycle event. Without a router, the egress drops telemetry and writes each event as one flat row, so the columns are `stream_type`, `sim_ts`, `timestamp`, `key`, `task`, `instance`, `path_id` and `status`. `logical_start_time: -7d` stamps the week as history that ends now.

To write to S3 instead, install `odctl` (`uv tool install "odctl>=1.0,<2"` or `pip install "odctl>=1.0,<2"`), start the object store, and set these variables before the run command. All three versions read the same ones. The chunks land under the `odctl-dev/history/` prefix, browsable at <http://localhost:8889>.

```bash
odctl up storage
export PARQUET_FILESYSTEM=s3 DEST_PATH=odctl-dev/history S3_ENDPOINT=http://localhost:8333 \
  S3_ACCESS_KEY=user S3_SECRET_KEY=password
```

Stop it with `odctl down storage --volumes` when you are finished.

**Full Source Code**

`filesystem` is a mapping. `type` picks the local disk or S3, and the other keys are passed to PyArrow's `S3FileSystem`. A key whose value is empty is left out, so with no variables set the mapping is the local disk.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

examples/yaml/parquet.yaml

```yaml
# Historical data generation in YAML.
#
# The YAML version of examples/builder/parquet_example.py. With factor 0 the
# clock is detached from the wall clock, so a week of shop orders is written to
# Parquet as fast as the machine allows. Without a router, each event is written as
# one flat row and telemetry is left out. The folder is created on the first write.
#
# Writes to ./data by default. To write to the odctl storage profile instead (odctl
# up storage), set PARQUET_FILESYSTEM=s3, DEST_PATH=odctl-dev/history,
# S3_ENDPOINT=http://localhost:8333, S3_ACCESS_KEY=user and S3_SECRET_KEY=password.
# Run it with: desflow run examples/yaml/parquet.yaml

simulation:
  sim_id: Shop
  factor: 0.0
  random_seed: 42
  # A week before now, so the records are stamped as history.
  logical_start_time: -7d

egress:
  - type: Parquet
    config:
      default_path: ${DEST_PATH:-data}/events.parquet
      # Empty values are left out, so with no variables set this is the local disk.
      filesystem:
        type: ${PARQUET_FILESYSTEM:-local}
        endpoint_override: ${S3_ENDPOINT:-}
        access_key: ${S3_ACCESS_KEY:-}
        secret_key: ${S3_SECRET_KEY:-}

batching:
  batch_size: 5000
  flush_interval: 86400

resources:
  packers: {current_cap: 4, max_cap: 10}

services:
  packing: {dist: normal, mean: 2.0, std: 0.2}

arrivals:
  orders: {dist: exponential, rate: 2.0, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {path_id: Shop.service.packing, status: finished}

run:
  until: 1 week
```

This example demonstrates how to run a simulation in **fast-forward mode** using the **builder API (`SimulationContext`)** and write compressed columnar data (Parquet) directly to local storage or an AWS S3 data lake using the `ParquetStorageEgress` connector.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[parquet]"

# 2. Download the example
desflow examples get parquet --api builder
cd parquet/builder

# 3. Run it
uv run --no-project --with "desflow[parquet]" parquet_example.py
```

```bash
# 1. Install
pip install "desflow[parquet]"

# 2. Download the example
desflow examples get parquet --api builder
cd parquet/builder

# 3. Run it
python parquet_example.py
```

To write to S3 instead, install `odctl` (`uv tool install "odctl>=1.0,<2"` or `pip install "odctl>=1.0,<2"`), start the object store, and set these variables before the run command. All three versions read the same ones. The chunks land under the `odctl-dev/history/` prefix, browsable at <http://localhost:8889>.

```bash
odctl up storage
export PARQUET_FILESYSTEM=s3 DEST_PATH=odctl-dev/history S3_ENDPOINT=http://localhost:8333 \
  S3_ACCESS_KEY=user S3_SECRET_KEY=password
```

Stop it with `odctl down storage --volumes` when you are finished.

**Full Source Code**

This script simulates the shop over a 7-day period. It demonstrates how to route lifecycle events to one Parquet dataset, flattened into rows, and write them out instantly.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

examples/builder/parquet_example.py

```python
"""
Historical Data Generation Example.

Demonstrates using SimulationContext as a fast-forward data engine.
By setting `factor=0.0`, the SimPy clock detaches from wall-clock time,
executing the same shop logic instantly to generate a week of
historical orders for machine learning models, written to Parquet on disk or S3.
"""

import logging
import os
from datetime import datetime, timedelta

from desflow import ParquetStorageEgress, SimulationContext

# Logging is configured here rather than in a wrapper, because this script is run
# directly. Without it the run produces no output at all.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


def create_history_router(base_path: str):
    """
    Router Factory: Generates a router function injected with the correct
    base path, and flattens nested event payloads for Parquet.
    """

    def history_router(data: dict) -> str | None:
        if data.get("path_id") == "system.simulation.lag_seconds":
            return None

        stream_type = data.get("stream_type")

        if stream_type == "telemetry":
            return None

        # FLATTEN EVENT FOR PARQUET
        if (
            stream_type == "event"
            and "value" in data
            and isinstance(data["value"], dict)
        ):
            nested_value = data.pop("value")
            data.update(nested_value)

        return f"{base_path}/events.parquet"

    return history_router


# ==========================================
# 1. DUAL-MODE STORAGE CONFIGURATION
# ==========================================
# The same variables as yaml/parquet.yaml. Unset, the run writes to data/ on the local
# disk. PARQUET_FILESYSTEM=s3 with the S3_* variables writes to S3-compatible storage,
# such as SeaweedFS from `odctl up storage`. The egress builds the filesystem from this
# mapping and creates the destination folder on its first write; an empty value is
# left out, so the PyArrow default applies.
base_path = os.getenv("DEST_PATH", "data")
filesystem = {
    "type": os.getenv("PARQUET_FILESYSTEM", "local"),
    "endpoint_override": os.getenv("S3_ENDPOINT", ""),
    "access_key": os.getenv("S3_ACCESS_KEY", ""),
    "secret_key": os.getenv("S3_SECRET_KEY", ""),
}


router = create_history_router(base_path)

# The week of backdating this example exists to demonstrate. It sits at module scope
# because the builder below needs it, and the builder has to stay at module scope for
# the decorators further down to attach to it. The low-level twin computes the same
# value inside run(), where it has no such constraint.
LOGICAL_START_TIME = datetime.now() - timedelta(days=7)

# ==========================================
# 2. Infrastructure Builder
# ==========================================
app = (
    SimulationContext(
        sim_id="Shop",
        factor=0.0,
        random_seed=42,
        # Without this the example logs that it is backdating a week and then timestamps
        # every record from the current clock, which is the opposite of what it claims.
        logical_start_time=LOGICAL_START_TIME,
    )
    .add_egress(ParquetStorageEgress(path_router=router, filesystem=filesystem))
    .with_batching(batch_size=5000, flush_interval=86400)
    .add_resource("packers", current_cap=4, max_cap=10)
    .add_service("packing", dist="normal", mean=2.0, std=0.2)
    .add_arrival("orders", dist="exponential", rate=2.0)
)


# ==========================================
# 3. Simulation Logic
# ==========================================
@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, context):
    """
    Returns the exact flat dictionary expected by the parquet router
    to represent the 'finished' state of the lifecycle.
    """
    return {"path_id": "Shop.service.packing", "status": "finished"}


@app.arrival_loop("orders")
def arrival_generator(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, context, entity=entity))
        task_id += 1


# ==========================================
# 4. Execution
# ==========================================
def run():
    """Generates 1 week of shop orders instantly."""

    logger.info(
        "Generating historical data mimicking start from "
        f"{LOGICAL_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("Fast-forwarding (factor=0.0)...")

    # The clock detaches and executes 1 week of operations instantaneously
    app.run(until="1 week")

    logger.info(f"Data generation complete. Check '{base_path}/' for chunks.")


if __name__ == "__main__":
    run()
```

This example demonstrates how to run a simulation in **fast-forward mode** using the low-level **Low-level API** and write compressed columnar data (Parquet) directly to local storage or an AWS S3 data lake using the `ParquetStorageEgress` connector.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[parquet]"

# 2. Download the example
desflow examples get parquet --api low-level
cd parquet/low-level

# 3. Run it
uv run --no-project --with "desflow[parquet]" parquet_example.py
```

```bash
# 1. Install
pip install "desflow[parquet]"

# 2. Download the example
desflow examples get parquet --api low-level
cd parquet/low-level

# 3. Run it
python parquet_example.py
```

To write to S3 instead, install `odctl` (`uv tool install "odctl>=1.0,<2"` or `pip install "odctl>=1.0,<2"`), start the object store, and set these variables before the run command. All three versions read the same ones. The chunks land under the `odctl-dev/history/` prefix, browsable at <http://localhost:8889>.

```bash
odctl up storage
export PARQUET_FILESYSTEM=s3 DEST_PATH=odctl-dev/history S3_ENDPOINT=http://localhost:8333 \
  S3_ACCESS_KEY=user S3_SECRET_KEY=password
```

Stop it with `odctl down storage --volumes` when you are finished.

**Full Source Code**

This script simulates the shop over a 7-day period. It demonstrates how to route lifecycle events to one Parquet dataset, flattened into rows, and write them out.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

examples/low-level/parquet_example.py

```python
"""Historical data generation, low-level API.

The low-level twin of `builder/parquet_example.py`. It wires
`DynamicRealtimeEnvironment`, the registry and the connectors by hand rather than
through the builder, which shows what `SimulationContext` does for you.

`factor=0.0` detaches the clock from real time, so seven days of shop orders are
generated as fast as the machine allows and written to Parquet. A router keeps the
lifecycle events and flattens them into rows. Writes to `data/`, or to S3 with the
variables `yaml/parquet.yaml` reads: `PARQUET_FILESYSTEM=s3` and `S3_ENDPOINT`,
`S3_ACCESS_KEY`, `S3_SECRET_KEY`.
"""

import logging
import os
from datetime import datetime, timedelta

import numpy as np

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    ParquetStorageEgress,
    Sampler,
    SimParameter,
)
from desflow.utils import time_to_seconds

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
logger = logging.getLogger("parquet_example")


def create_history_router(base_path: str):
    """
    Router Factory: Generates a router function injected with the correct
    base path, and flattens nested event payloads for Parquet.
    """

    def history_router(data: dict) -> str | None:
        if data.get("path_id") == "system.simulation.lag_seconds":
            return None

        stream_type = data.get("stream_type")

        if stream_type == "telemetry":
            return None

        # FLATTEN EVENT FOR PARQUET
        # If it's an event and has a nested 'value' dictionary, flatten it
        if (
            stream_type == "event"
            and "value" in data
            and isinstance(data["value"], dict)
        ):
            # Extract and remove the nested 'value' object
            nested_value = data.pop("value")
            # Merge the nested keys (path_id, status) directly into the root dict
            data.update(nested_value)

        return f"{base_path}/events.parquet"

    return history_router


# The shop starts with 4 packers and has room for 10.
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=2.0)},
    service={"packing": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
    resources={"packers": CapacityConfig(current_cap=4, max_cap=10)},
)


def pack_order(env, sampler, packers, key, task_id):
    """What @app.task does: queued, started, the service time, then finished."""
    path_id = "Shop.service.packing"
    env.publish_event(
        key,
        {"path_id": path_id, "status": "queued"},
        task="pack_order",
        instance=task_id,
    )
    with packers.request() as req:
        yield req
        env.publish_event(
            key,
            {"path_id": path_id, "status": "started"},
            task="pack_order",
            instance=task_id,
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(
            key,
            {"path_id": path_id, "status": "finished"},
            task="pack_order",
            instance=task_id,
        )


def arrival_generator(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def run():
    # ---------------------------------------------------------
    # 1. DUAL-MODE STORAGE CONFIGURATION
    # ---------------------------------------------------------
    # The same variables as yaml/parquet.yaml. Unset, the run writes to data/ on the local
    # disk. PARQUET_FILESYSTEM=s3 with the S3_* variables writes to S3-compatible storage,
    # such as SeaweedFS from `odctl up storage`. The egress builds the filesystem from this
    # mapping and creates the destination folder on its first write; an empty value is
    # left out, so the PyArrow default applies.
    base_path = os.getenv("DEST_PATH", "data")
    filesystem = {
        "type": os.getenv("PARQUET_FILESYSTEM", "local"),
        "endpoint_override": os.getenv("S3_ENDPOINT", ""),
        "access_key": os.getenv("S3_ACCESS_KEY", ""),
        "secret_key": os.getenv("S3_SECRET_KEY", ""),
    }

    # ---------------------------------------------------------
    # 2. SIMULATION SETUP
    # ---------------------------------------------------------
    start_time = datetime.now() - timedelta(days=7)
    env = DynamicRealtimeEnvironment(factor=0.0, logical_start_time=start_time)
    env.registry.register_sim_parameter(PARAMS)

    # Initialize Egress with the dynamic router and conditional filesystem
    router = create_history_router(base_path)
    egress = ParquetStorageEgress(path_router=router, filesystem=filesystem)

    # batch_size=5000 for compression, flush_interval=86400 (1 day in sim time)
    env.setup_egress([egress], batch_size=5000, flush_interval=86400)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(42))

    env.process(arrival_generator(env, sampler, packers))

    run_duration_str = "1 week"
    run_duration_sec = time_to_seconds(run_duration_str)

    logger.info(
        f"Generating historical data from {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("Fast-forwarding (factor=0.0)...")

    try:
        env.run(until=run_duration_sec)
    finally:
        env.teardown()
        logger.info(f"Data generation complete. Check '{base_path}/' for chunks.")


if __name__ == "__main__":
    run()
```
