# Write to Iceberg Tables

`IcebergStorageEgress` appends records to an Apache Iceberg table through a catalog, so any engine that reads Iceberg, such as Spark, Trino or DuckDB, can query them. This page appends an hour of a shop's orders to the table `shop.events` through a catalog you pick, then reads the table back.

______________________________________________________________________

## Before You Start

- [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#iceberg) explains the sink, the columns of a table it creates, and upserts.
- Install the extra: `pip install "desflow[iceberg]"`. It covers every catalog on this page.
- The SQLite catalog needs no service. The REST catalog needs `odctl up catalog`, and the PostgreSQL catalog `odctl up postgres`. See [Installation](https://jaehyeon.me/desflow/latest/installation/index.md) for the one-time odctl install. Stop a profile with `odctl down <profile> --volumes`.
- The Glue catalog needs AWS credentials and an S3 bucket for the table files.

______________________________________________________________________

## Steps

### Choose the Catalog

`catalog` is a mapping of PyIceberg catalog properties, which desflow passes to PyIceberg's `load_catalog` on the first write. `type` picks the kind of catalog, and the optional `name` key names it. The script keeps one mapping for each catalog, and `ICEBERG_CATALOG` picks one:

```python
import os
from pathlib import Path

# The SQLite database and the local table files go in the folder iceberg/
FOLDER = Path("iceberg").resolve()
FOLDER.mkdir(exist_ok=True)

# One mapping of PyIceberg catalog properties for each catalog
CATALOGS = {
    # A SQL catalog on SQLite, with no service, for a first run
    "sqlite": {
        "type": "sql",
        "uri": f"sqlite:///{FOLDER}/catalog.db",
        "warehouse": f"file://{FOLDER}/warehouse",
    },
    # The REST catalog of `odctl up catalog`, with the table files on its SeaweedFS
    "rest": {
        "type": "rest",
        "uri": "http://127.0.0.1:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://127.0.0.1:8333",
        "s3.access-key-id": "user",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    },
    # A SQL catalog on the PostgreSQL of `odctl up postgres`, with local table files
    "postgres": {
        "type": "sql",
        "uri": "postgresql+psycopg2://user:password@127.0.0.1:5432/odctl",
        "warehouse": f"file://{FOLDER}/warehouse",
    },
    # The AWS Glue Data Catalog, with the table files in S3 and the standard AWS settings
    "glue": {
        "type": "glue",
        "glue.region": "us-east-1",
        "s3.region": "us-east-1",
        "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://my-bucket/warehouse"),
    },
}

name = os.getenv("ICEBERG_CATALOG", "sqlite")
catalog = {"name": name, **CATALOGS[name]}
```

The Glue mapping gives no credentials, so PyIceberg uses the standard AWS settings: environment variables such as `AWS_ACCESS_KEY_ID`, the files in `~/.aws`, or the role the machine runs as. `ICEBERG_WAREHOUSE` names the S3 folder of the table files. [PyIceberg's configuration](https://py.iceberg.apache.org/configuration/) lists the properties of each catalog.

### Write the Table

The sink creates the table from the first batch, with a column for each key of the records. Each flush is one commit, and every commit writes new metadata, so the sink gets a large batch of its own:

```python
from desflow import IcebergStorageEgress


def iceberg_sink(catalog: dict, table: str = "shop.events") -> IcebergStorageEgress:
    """Every event as a row of `table`, which the sink creates from the first batch."""
    return IcebergStorageEgress(catalog=catalog, default_table=table)


sink = iceberg_sink(catalog)
# One commit per flush, so the sink gets a large batch of its own
BATCH_SIZE = 200_000
```

Each form runs the shop for an hour of simulated time at `factor=0.0`. The run flushes once, at its end, so the hour of orders is one commit:

```yaml
egress:
  # Every event as a row of shop.events, which the sink creates from the first batch
  - type: Iceberg
    config:
      default_table: shop.events
      catalog: !python iceberg_tables_catalog.catalog
    # One commit per flush, so the sink gets a large batch of its own
    batch_size: 200000
```

The blueprint loads the mapping of `iceberg_tables_catalog.py` with `!python`, so `ICEBERG_CATALOG` picks the catalog in every form.

```python
import logging

from iceberg_tables_catalog import BATCH_SIZE, sink

from desflow import IcebergStorageEgress, SimulationContext


def build_shop(sink: IcebergStorageEgress) -> SimulationContext:
    """The shop, appending to the table of the sink given, as fast as it can."""
    app = (
        SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        .add_egress(sink, batch_size=BATCH_SIZE)
    )

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


app = build_shop(sink)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 hour")
```

`shop` takes the sink as an argument, and the script passes it `sink` from `iceberg_tables_catalog.py`. `shop` sets the batch size of the sink, `BATCH_SIZE` from the same file, in `batch_sizes`:

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


def shop(sink, factor: float = 0.0) -> DynamicRealtimeEnvironment:
    """The shop, appending to the table of the sink given, as fast as it can."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    # One commit per flush, so the sink gets a large batch of its own
    env.setup_egress([sink], batch_sizes=[BATCH_SIZE])
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env
```

### Read the Table Back

`read_events` loads the table through the same catalog and reads every row:

```python
def read_events(catalog: dict, table: str = "shop.events"):
    """Every row of `table`, as one PyArrow table."""
    from pyiceberg.catalog import load_catalog

    properties = {key: value for key, value in catalog.items() if key != "name"}
    return (
        load_catalog(catalog["name"], **properties).load_table(table).scan().to_arrow()
    )
```

A second run appends its rows to the same table. To replace the rows of an earlier run instead, give the sink `upsert_keys`, as [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#iceberg) shows.

______________________________________________________________________

## Check the Result

Run `python iceberg_tables.py`, `python iceberg_tables_low_level.py` or `desflow run iceberg_tables.yaml`. With no variables set, the catalog is SQLite, and its database and table files go in the folder `iceberg`. With the catalog profile up,

```bash
ICEBERG_CATALOG=rest python iceberg_tables.py
```

writes through the REST catalog instead. In every form, `shop.events` gains one snapshot, with a row for each event: the `queued` and `started` events of each order, with `path_id` and `status`, and its finished event, with `order_id` and `amount`. A seeded run writes the same events in every form. Only `timestamp` differs, because it is the clock time at the start of the run plus the simulation time.

To count the rows, run the catalog file with the same variables. Its last lines call `read_events` and print the count:

```python
if __name__ == "__main__":
    events = read_events(catalog)
    print(f"{events.num_rows} rows in shop.events")
```

```bash
python iceberg_tables_catalog.py
```

```text
5291 rows in shop.events
```

The count of one run is 5,291 rows. A second run appends its rows to the table, so the count is then the sum of both runs.

______________________________________________________________________

## Full Files

Every form uses the catalog and the sink of `iceberg_tables_catalog.py`, which builds no shop, so keep the files in one folder.

Full files

```python
"""The catalogs, sink and reader of the Write Iceberg Tables page.

Every form of the shop imports the catalog and the sink from this file: iceberg_tables.py
(builder), iceberg_tables.yaml and iceberg_tables_low_level.py. It builds no simulation.
ICEBERG_CATALOG picks the catalog: sqlite (the default), rest, postgres or glue.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

import os
from pathlib import Path

# The SQLite database and the local table files go in the folder iceberg/
FOLDER = Path("iceberg").resolve()
FOLDER.mkdir(exist_ok=True)

# One mapping of PyIceberg catalog properties for each catalog
CATALOGS = {
    # A SQL catalog on SQLite, with no service, for a first run
    "sqlite": {
        "type": "sql",
        "uri": f"sqlite:///{FOLDER}/catalog.db",
        "warehouse": f"file://{FOLDER}/warehouse",
    },
    # The REST catalog of `odctl up catalog`, with the table files on its SeaweedFS
    "rest": {
        "type": "rest",
        "uri": "http://127.0.0.1:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://127.0.0.1:8333",
        "s3.access-key-id": "user",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    },
    # A SQL catalog on the PostgreSQL of `odctl up postgres`, with local table files
    "postgres": {
        "type": "sql",
        "uri": "postgresql+psycopg2://user:password@127.0.0.1:5432/odctl",
        "warehouse": f"file://{FOLDER}/warehouse",
    },
    # The AWS Glue Data Catalog, with the table files in S3 and the standard AWS settings
    "glue": {
        "type": "glue",
        "glue.region": "us-east-1",
        "s3.region": "us-east-1",
        "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://my-bucket/warehouse"),
    },
}

name = os.getenv("ICEBERG_CATALOG", "sqlite")
catalog = {"name": name, **CATALOGS[name]}


from desflow import IcebergStorageEgress


def iceberg_sink(catalog: dict, table: str = "shop.events") -> IcebergStorageEgress:
    """Every event as a row of `table`, which the sink creates from the first batch."""
    return IcebergStorageEgress(catalog=catalog, default_table=table)


sink = iceberg_sink(catalog)
# One commit per flush, so the sink gets a large batch of its own
BATCH_SIZE = 200_000


def read_events(catalog: dict, table: str = "shop.events"):
    """Every row of `table`, as one PyArrow table."""
    from pyiceberg.catalog import load_catalog

    properties = {key: value for key, value in catalog.items() if key != "name"}
    return (
        load_catalog(catalog["name"], **properties).load_table(table).scan().to_arrow()
    )


if __name__ == "__main__":
    events = read_events(catalog)
    print(f"{events.num_rows} rows in shop.events")
```

```yaml
# The Write to Iceberg Tables guide: the shop appends an hour of orders to the Iceberg
# table shop.events, as fast as the machine allows. ICEBERG_CATALOG picks the catalog
# (sqlite, the default, rest, postgres or glue). Needs iceberg_tables_catalog.py and
# iceberg_tables_logic.py beside it. Run it with: desflow run iceberg_tables.yaml
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
    payload: !python iceberg_tables_logic.order_record

egress:
  # Every event as a row of shop.events, which the sink creates from the first batch
  - type: Iceberg
    config:
      default_table: shop.events
      catalog: !python iceberg_tables_catalog.catalog
    # One commit per flush, so the sink gets a large batch of its own
    batch_size: 200000

run:
  until: 1 hour
```

```python
"""Python for iceberg_tables.yaml: the order record each packed order publishes. The catalog
is that of iceberg_tables_catalog.py, which must be beside the blueprint."""


def order_record(task_id, context, entity):
    """The finished event of a packed order: the order record."""
    return {"order_id": task_id, "amount": 19.99}
```

```python
"""The shop of the Write Iceberg Tables page, built with the builder API.

The shop appends an hour of orders to the Iceberg table shop.events, as fast as the
machine allows. Run it with `python iceberg_tables.py`; rest needs `odctl up catalog`,
postgres needs `odctl up postgres`, and glue needs AWS credentials and ICEBERG_WAREHOUSE.
iceberg_tables.yaml and iceberg_tables_low_level.py are the same shop in YAML and the
low-level API. All three import the catalog and the sink of iceberg_tables_catalog.py, which
must be beside them.
"""

# ruff: noqa: E402

import logging

from iceberg_tables_catalog import BATCH_SIZE, sink

from desflow import IcebergStorageEgress, SimulationContext


def build_shop(sink: IcebergStorageEgress) -> SimulationContext:
    """The shop, appending to the table of the sink given, as fast as it can."""
    app = (
        SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        .add_egress(sink, batch_size=BATCH_SIZE)
    )

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


app = build_shop(sink)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 hour")
```

```python
"""The Write to Iceberg Tables guide, low-level API: the shop appends an hour of orders
to the Iceberg table shop.events, as fast as the machine allows.

The catalog and the sink are those of iceberg_tables_catalog.py, which must be beside this
script. ICEBERG_CATALOG picks the catalog: sqlite (the default), rest, postgres or glue.
Run it with `python iceberg_tables_low_level.py`.
"""

import itertools
import logging

from iceberg_tables_catalog import BATCH_SIZE, sink

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


def shop(sink, factor: float = 0.0) -> DynamicRealtimeEnvironment:
    """The shop, appending to the table of the sink given, as fast as it can."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    # One commit per flush, so the sink gets a large batch of its own
    env.setup_egress([sink], batch_sizes=[BATCH_SIZE])
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    env = shop(sink)  # rows of shop.events in the catalog ICEBERG_CATALOG names
    try:
        env.run(until=3600)
    finally:
        env.teardown()
```
