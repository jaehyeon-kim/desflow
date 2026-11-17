# Write to Cloud Storage

`ParquetStorageEgress` and `JsonlStorageEgress` write each batch of records as a file. The same two sinks write to the local disk, Amazon S3, Google Cloud Storage or Azure Blob Storage: only their `filesystem` mapping changes. This page writes an hour of a shop's orders to a store you pick, then reads the files back as one table.

______________________________________________________________________

## Before You Start

- [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#parquet-and-json-lines) explains the two sinks, their file names and their columns.
- Install the extra: `pip install "desflow[parquet]"`.
- To write to a cloud, set up its credentials first, as [Choose the Store](#choose-the-store) describes, and create the bucket or container the files go to.

______________________________________________________________________

## Steps

### Choose the Store

A `filesystem` mapping names the store with `type`, and its other keys are the arguments of the PyArrow filesystem class of that store. Without credentials in the mapping, each cloud uses its standard settings:

- `s3`: the AWS settings, from environment variables such as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, the files in `~/.aws`, or the role the machine runs as.
- `gcs`: Google's default credentials, such as those `gcloud auth application-default login` saves.
- `azure`: Azure's default credentials for the storage account that `account_name` names.

The script keeps one mapping for each store. `STORAGE` picks one, and `STORAGE_BUCKET` names the bucket, the container or, for `local`, the folder:

```python
import os

# One filesystem mapping for each store; desflow builds it into a PyArrow filesystem
STORES: dict[str, dict] = {
    # The local disk, for a first run
    "local": {"type": "local"},
    # Amazon S3, with the standard AWS settings: environment variables, ~/.aws or a role
    "s3": {"type": "s3", "region": os.getenv("AWS_REGION", "us-east-1")},
    # Google Cloud Storage, with Google's default credentials
    "gcs": {"type": "gcs"},
    # Azure Blob Storage, with Azure's default credentials for the storage account
    "azure": {"type": "azure", "account_name": os.getenv("AZURE_STORAGE_ACCOUNT")},
}

filesystem = STORES[os.getenv("STORAGE", "local")]
bucket = os.getenv("STORAGE_BUCKET", "output")
```

The [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#parquet-and-json-lines) table lists the keys each store takes, such as `access_key` and `secret_key` for S3, `access_token` for GCS and `account_key` or `sas_token` for Azure. `endpoint_override` points `s3` or `gcs` at another service, such as SeaweedFS or MinIO.

### Write the Files

Both sinks take the same mapping. Each writes into its own folder of the bucket, so a reader of one folder reads one format:

```python
from desflow import JsonlStorageEgress, ParquetStorageEgress


def storage_sinks(filesystem: dict, bucket: str) -> list:
    """Every event as Parquet and as JSON Lines, in two folders of the bucket."""
    return [
        ParquetStorageEgress(
            default_path=f"{bucket}/parquet/events.parquet", filesystem=filesystem
        ),
        JsonlStorageEgress(
            default_path=f"{bucket}/jsonl/events.jsonl", filesystem=filesystem
        ),
    ]


sinks = storage_sinks(filesystem, bucket)
```

Each form runs the shop for an hour of simulated time at `factor=0.0`, so the run takes a few seconds:

```yaml
egress:
  # Every event as Parquet and as JSON Lines, in two folders of the bucket
  - type: Parquet
    config:
      default_path: ${STORAGE_BUCKET:-output}/parquet/events.parquet
      filesystem: !python cloud_storage.filesystem
  - type: Jsonl
    config:
      default_path: ${STORAGE_BUCKET:-output}/jsonl/events.jsonl
      filesystem: !python cloud_storage.filesystem
```

The blueprint loads the mapping of `cloud_storage.py` with `!python`, so `STORAGE` picks the store in every form. `${STORAGE_BUCKET:-output}` reads the bucket from the same variable.

```python
import logging

from desflow import SimulationContext


def build_shop(sinks: list) -> SimulationContext:
    """The shop, writing to the sinks given, as fast as the machine allows."""
    app = (
        SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
    )
    for sink in sinks:
        app.add_egress(sink)

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, entity):
        # The finished event is the order record
        return {"order_id": task_id, "amount": 19.99}

    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

    return app


app = build_shop(sinks)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 hour")
```

`shop` takes the sinks as an argument, and the script passes it `sinks` from the Builder file:

```python
def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        # The finished event is the order record
        order = {"order_id": task_id, "amount": 19.99}
        env.publish_event(key, order, "pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def shop(sinks: list, factor: float = 0.0) -> DynamicRealtimeEnvironment:
    """The shop, writing to the sinks given, as fast as the machine allows."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(sinks)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env
```

### Read the Files Back

`read_events` reads every file in a folder as one table, through a PyArrow filesystem of the same store:

```python
def read_events(store, folder: str, format: str = "parquet"):
    """Every file a sink wrote under `folder`, as one table.

    `store` is a PyArrow filesystem, such as pyarrow.fs.S3FileSystem(region="us-east-1").
    """
    import pyarrow.dataset as ds

    # format="json" reads the files of the JSON Lines sink
    return ds.dataset(folder, format=format, filesystem=store).to_table()
```

Give each run a folder of its own, or clear the folder before a run, because a second run adds its files beside the first run's.

______________________________________________________________________

## Check the Result

Run `python cloud_storage.py`, `python cloud_storage_low_level.py` or `desflow run cloud_storage.yaml`. With no variables set, the files go to the folder `output` on the local disk:

```bash
STORAGE=s3 STORAGE_BUCKET=my-bucket python cloud_storage.py
```

writes them to the bucket `my-bucket` on S3 instead. In every form, `output/parquet` and `output/jsonl` hold one file for each batch, with the same events: the `queued` and `started` events of each order, with `path_id` and `status`, and its finished event, with `order_id` and `amount`. A seeded run writes the same events in every form. Only `timestamp` differs, because it is the clock time at the start of the run plus the simulation time.

______________________________________________________________________

## Full Files

Every form uses the store and the sinks of `cloud_storage.py`, so keep the files in one folder.

Full files

```yaml
# The Write to Cloud Storage guide: the shop writes an hour of orders as Parquet and JSON
# Lines files, as fast as the machine allows. STORAGE picks the store (local, the default,
# s3, gcs or azure) and STORAGE_BUCKET the bucket, container or local folder. Needs
# cloud_storage.py and cloud_storage_logic.py beside it. Run it with:
# desflow run cloud_storage.yaml
simulation:
  sim_id: Shop
  factor: 0.0
  random_seed: 42

resources:
  packers: {current_cap: 1, max_cap: 1}

services:
  packing: {dist: constant, value: 1.5}

arrivals:
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    # The finished event is the order record
    payload: !python cloud_storage_logic.order_record

egress:
  # Every event as Parquet and as JSON Lines, in two folders of the bucket
  - type: Parquet
    config:
      default_path: ${STORAGE_BUCKET:-output}/parquet/events.parquet
      filesystem: !python cloud_storage.filesystem
  - type: Jsonl
    config:
      default_path: ${STORAGE_BUCKET:-output}/jsonl/events.jsonl
      filesystem: !python cloud_storage.filesystem

run:
  until: 1 hour
```

```python
"""Python for cloud_storage.yaml: the order record each packed order publishes. The store
is that of cloud_storage.py, which must be beside the blueprint."""


def order_record(task_id, context, entity):
    """The finished event of a packed order: the order record."""
    return {"order_id": task_id, "amount": 19.99}
```

```python
"""The stores, sinks and shop of the Write to Cloud Storage page.

The shop writes an hour of orders as Parquet and JSON Lines files, as fast as the machine
allows. STORAGE picks the store: local (the default), s3, gcs or azure. STORAGE_BUCKET
names the bucket or container, or the folder on the local disk. Run it with
`STORAGE=s3 STORAGE_BUCKET=my-bucket python cloud_storage.py`, with the credentials of
that cloud set up as the page describes.
cloud_storage.yaml and cloud_storage_low_level.py are the same shop in YAML and the
low-level API, and use the store and the sinks of this file.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

import os

# One filesystem mapping for each store; desflow builds it into a PyArrow filesystem
STORES: dict[str, dict] = {
    # The local disk, for a first run
    "local": {"type": "local"},
    # Amazon S3, with the standard AWS settings: environment variables, ~/.aws or a role
    "s3": {"type": "s3", "region": os.getenv("AWS_REGION", "us-east-1")},
    # Google Cloud Storage, with Google's default credentials
    "gcs": {"type": "gcs"},
    # Azure Blob Storage, with Azure's default credentials for the storage account
    "azure": {"type": "azure", "account_name": os.getenv("AZURE_STORAGE_ACCOUNT")},
}

filesystem = STORES[os.getenv("STORAGE", "local")]
bucket = os.getenv("STORAGE_BUCKET", "output")


from desflow import JsonlStorageEgress, ParquetStorageEgress


def storage_sinks(filesystem: dict, bucket: str) -> list:
    """Every event as Parquet and as JSON Lines, in two folders of the bucket."""
    return [
        ParquetStorageEgress(
            default_path=f"{bucket}/parquet/events.parquet", filesystem=filesystem
        ),
        JsonlStorageEgress(
            default_path=f"{bucket}/jsonl/events.jsonl", filesystem=filesystem
        ),
    ]


sinks = storage_sinks(filesystem, bucket)


import logging

from desflow import SimulationContext


def build_shop(sinks: list) -> SimulationContext:
    """The shop, writing to the sinks given, as fast as the machine allows."""
    app = (
        SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
    )
    for sink in sinks:
        app.add_egress(sink)

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, entity):
        # The finished event is the order record
        return {"order_id": task_id, "amount": 19.99}

    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

    return app


app = build_shop(sinks)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 hour")


def read_events(store, folder: str, format: str = "parquet"):
    """Every file a sink wrote under `folder`, as one table.

    `store` is a PyArrow filesystem, such as pyarrow.fs.S3FileSystem(region="us-east-1").
    """
    import pyarrow.dataset as ds

    # format="json" reads the files of the JSON Lines sink
    return ds.dataset(folder, format=format, filesystem=store).to_table()
```

```python
"""The Write to Cloud Storage guide, low-level API: the shop writes an hour of orders as
Parquet and JSON Lines files, as fast as the machine allows.

The store and the sinks are those of cloud_storage.py, which must be beside this script.
STORAGE picks the store (local, the default, s3, gcs or azure) and STORAGE_BUCKET the
bucket, container or local folder. Run it with `python cloud_storage_low_level.py`.
"""

import itertools
import logging

from cloud_storage import sinks

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        # The finished event is the order record
        order = {"order_id": task_id, "amount": 19.99}
        env.publish_event(key, order, "pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def shop(sinks: list, factor: float = 0.0) -> DynamicRealtimeEnvironment:
    """The shop, writing to the sinks given, as fast as the machine allows."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(sinks)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    env = shop(sinks)  # Parquet and JSON Lines files in the store STORAGE names
    try:
        env.run(until=3600)
    finally:
        env.teardown()
```
