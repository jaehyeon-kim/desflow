# PostgreSQL

This example models a shop receiving orders. The simulation writes order records to PostgreSQL and reads parameter updates from a PostgreSQL table. Each tab shows the example written one way: as a YAML blueprint, with the builder API, or with the low-level API. [Ways to write a simulation](https://jaehyeon.me/desflow/latest/user-guide/ways-to-write/index.md) compares the three.

With `PostgresEgress` in place of the console egress, the simulation runs as a separate data generator. Look for one row in `orders` for each order, and for orders arriving faster after you insert a new rate into `simulation_params`. The egress writes rows into PostgreSQL with asynchronous bulk inserts through `asyncpg`. The connection string names `odctl`, the database that the odctl `postgres` profile creates, with the user `user` and the password `password`.

This example builds the simulation of the builder example in the Builder tab from a YAML blueprint, with no Python. Each order is written to `orders`, and `PostgresIngress` polls `simulation_params` for updates. The [Related Records](https://jaehyeon.me/desflow/latest/examples/advanced-postgres-orders/index.md) example adds random line items in a second table, and keeps its order generator in Python.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[postgres]" --with-executables-from "odctl>=1.0,<2"

# 2. Download the example
desflow examples get postgres --api yaml
cd postgres/yaml

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
desflow run postgres.yaml

# 5. Stop its service when finished
odctl down postgres --volumes
```

```bash
# 1. Install
pip install "desflow[postgres]" "odctl>=1.0,<2"

# 2. Download the example
desflow examples get postgres --api yaml
cd postgres/yaml

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
desflow run postgres.yaml

# 5. Stop its service when finished
odctl down postgres --volumes
```

**What It Does**

When the run starts, `PostgresEgress` creates the `orders` table and `PostgresIngress` creates `simulation_params`, if they do not exist. The run then writes one order per arrival until you stop it. The order id is the task id, and the primary key of `orders`. A second run numbers its orders from 0 again. `PostgresEgress` skips each order whose `order_id` is already in the table, so drop the table to write those orders again.

In a second terminal, raise the arrival rate while it runs:

```bash
docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (path, value) VALUES ('Shop.arrival.orders.rate', '5.0');"
```

**Full Source Code**

`place_order` has no service or resource, so it publishes its fixed `payload` as soon as an arrival spawns it, with the task id added as `order_id`. `tables` gives the columns and the primary key of each table to create. The `timestamp` column is filled from the time the record carries.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/desflow/tree/v0.25.0/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml
# Relational output in YAML.
#
# The YAML version of postgres_example.py, which --api builder downloads: a simulation that
# writes order records to one table. Each order spawns place_order, a task with no
# service or resource, which publishes its payload at once. PostgresEgress creates
# the orders table at start and writes one row per order. PostgresIngress creates
# simulation_params and polls it for parameter updates.
#
# Needs a database: odctl up postgres. Runs until interrupted with Ctrl + C.
# Run it from its folder with: desflow run postgres.yaml

simulation:
  sim_id: Shop
  factor: 1.0
  random_seed: 42

ingress:
  - type: Postgres
    config:
      connection_dsn: postgresql://user:password@127.0.0.1:5432/odctl
      table_name: simulation_params

egress:
  - type: Postgres
    config:
      connection_dsn: postgresql://user:password@127.0.0.1:5432/odctl
      tables:
        orders:
          columns:
            order_id: INT
            customer_id: INT
            total_amount: REAL
            status: TEXT
            timestamp: TIMESTAMP
          primary_key: order_id

arrivals:
  orders: {dist: exponential, rate: 1.0, spawn: place_order}

tasks:
  place_order:
    # id_field adds the task id as order_id. timestamp comes from the record.
    payload: {customer_id: 42, total_amount: 59.97, status: pending}
    id_field: order_id
```

This version builds the simulation with the builder API (`SimulationContext`).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[postgres]" --with-executables-from "odctl>=1.0,<2"

# 2. Download the example
desflow examples get postgres --api builder
cd postgres/builder

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
uv run --no-project --with "desflow[postgres]" postgres_example.py

# 5. Stop its service when finished
odctl down postgres --volumes
```

```bash
# 1. Install
pip install "desflow[postgres]" "odctl>=1.0,<2"

# 2. Download the example
desflow examples get postgres --api builder
cd postgres/builder

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
python postgres_example.py

# 5. Stop its service when finished
odctl down postgres --volumes
```

**Writing Rows**

`PostgresEgress` creates each table in `tables` when it does not exist, and writes each record as one row. Record fields the table has no column for, such as `sim_ts`, are left out.

```python
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_ingress(PostgresIngress(DSN, table_name="simulation_params"))
    .add_egress(PostgresEgress(DSN, tables={"orders": ORDERS}))
    .add_arrival("orders", dist="exponential", rate=1.0)
)
```

**Generating Orders**

`place_order` has no service or resource, so it publishes its payload as soon as the arrival loop spawns it. The task id is the order id:

```python
@app.task()
def place_order(task_id: int, context):
    """A task with no service or resource: its payload is published at once."""
    return {
        "order_id": task_id,
        "customer_id": 42,
        "total_amount": 59.97,
        "status": "pending",
    }


@app.arrival_loop("orders")
def order_generator(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(place_order(task_id, context, entity=entity))
        task_id += 1
```

**Dynamic Parameter Updates (Ingress)**

This example also attaches a `PostgresIngress` listening to a `simulation_params` table. While the simulation runs, inserting a row with `path` and `value` into that table changes the parameter, such as the order arrival rate. The simulation reads the new value within its poll interval, which is 2 seconds by default.

**In a second terminal, change the parameter:**

```bash
# Insert the new arrival rate into simulation_params
docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (path, value) VALUES ('Shop.arrival.orders.rate', '5.0');"
```

Within the poll interval, the run logs `Ingested parameter update: Shop.arrival.orders.rate = 5.0`, and orders then arrive five times faster.

**Full Source Code**

This script connects the simulation to PostgreSQL, creates the `orders` table and writes a continuous stream of orders.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/v0.25.0/examples) of the repository, and the label on the block below is this one's path there.

```python
"""Relational output, builder API.

The simulation writes order records to the PostgreSQL table `orders`.
`PostgresEgress` creates the table at start and writes one row per order, and
`PostgresIngress` creates `simulation_params` and polls it for parameter updates, so
inserting a row there changes the arrival rate of a running simulation.

Needs a database: `odctl up postgres`. Runs until interrupted with Ctrl + C.
"""

import logging

from desflow import PostgresEgress, PostgresIngress, SimulationContext

logger = logging.getLogger(__name__)

# Connection string matching the odctl `postgres` profile
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"

# The table PostgresEgress creates when it does not exist. timestamp comes from the
# record, and the egress converts its ISO string for the TIMESTAMP column.
ORDERS = {
    "columns": {
        "order_id": "INT",
        "customer_id": "INT",
        "total_amount": "REAL",
        "status": "TEXT",
        "timestamp": "TIMESTAMP",
    },
    "primary_key": "order_id",
}

app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_ingress(PostgresIngress(DSN, table_name="simulation_params"))
    .add_egress(PostgresEgress(DSN, tables={"orders": ORDERS}))
    .add_arrival("orders", dist="exponential", rate=1.0)
)


@app.task()
def place_order(task_id: int, context):
    """A task with no service or resource: its payload is published at once."""
    return {
        "order_id": task_id,
        "customer_id": 42,
        "total_amount": 59.97,
        "status": "pending",
    }


@app.arrival_loop("orders")
def order_generator(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(place_order(task_id, context, entity=entity))
        task_id += 1


def run():
    logger.info("Starting Builder Postgres Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running: INSERT INTO simulation_params (path, "
        "value) VALUES ('Shop.arrival.orders.rate', '5.0');"
    )
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
```

This version wires the simulation by hand on `DynamicRealtimeEnvironment`, the low-level API.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[postgres]" --with-executables-from "odctl>=1.0,<2"

# 2. Download the example
desflow examples get postgres --api low-level
cd postgres/low-level

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
uv run --no-project --with "desflow[postgres]" postgres_example.py

# 5. Stop its service when finished
odctl down postgres --volumes
```

```bash
# 1. Install
pip install "desflow[postgres]" "odctl>=1.0,<2"

# 2. Download the example
desflow examples get postgres --api low-level
cd postgres/low-level

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
python postgres_example.py

# 5. Stop its service when finished
odctl down postgres --volumes
```

**Writing Rows**

`PostgresEgress` creates each table in `tables` when it does not exist, and writes each record as one row. `PostgresIngress` creates the `simulation_params` table it polls:

```python
# PostgresEgress creates the orders table, and PostgresIngress creates the
# simulation_params table it polls for parameter updates.
env.setup_egress([PostgresEgress(DSN, tables={"orders": ORDERS})])
env.setup_ingress([PostgresIngress(DSN, table_name="simulation_params")])
```

**Generating Orders**

For each arriving order, the SimPy process publishes one record under the key the builder's task would use. The task id is the order id:

```python
order = {
    "order_id": task_id,
    "customer_id": 42,
    "total_amount": 59.97,
    "status": "pending",
}
# The order key, and the task name and instance id the builder adds
env.publish_event(
    f"orders-{task_id + 1}", order, task="place_order", instance=task_id
)
```

**Dynamic Parameter Updates (Ingress)**

This example also attaches a `PostgresIngress` listening to a `simulation_params` table. While the simulation runs, inserting a row with `path` and `value` into that table changes the parameter, such as the order arrival rate. The simulation reads the new value within its poll interval, which is 2 seconds by default.

**In a second terminal, change the parameter:**

```bash
# Insert the new arrival rate into simulation_params
docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (path, value) VALUES ('Shop.arrival.orders.rate', '5.0');"
```

Within the poll interval, the run logs `Ingested parameter update: Shop.arrival.orders.rate = 5.0`, and orders then arrive five times faster.

**Full Source Code**

This script connects the simulation to PostgreSQL, creates the `orders` table and writes a continuous stream of orders.

Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/v0.25.0/examples) of the repository, and the label on the block below is this one's path there.

```python
"""Relational output, low-level API.

The low-level version of `builder/postgres_example.py`. The simulation writes order
records to the PostgreSQL table `orders`. `PostgresEgress` creates the table at start,
and `PostgresIngress` creates `simulation_params` and polls it for parameter updates.

Needs a database: `odctl up postgres`. Runs until interrupted with Ctrl + C.
"""

import logging

import numpy as np

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    PostgresEgress,
    PostgresIngress,
    Sampler,
    SimParameter,
)

logger = logging.getLogger("postgres_example")

# Connection string matching the odctl `postgres` profile
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"

# The table PostgresEgress creates when it does not exist. timestamp comes from the
# record, and the egress converts its ISO string for the TIMESTAMP column.
ORDERS = {
    "columns": {
        "order_id": "INT",
        "customer_id": "INT",
        "total_amount": "REAL",
        "status": "TEXT",
        "timestamp": "TIMESTAMP",
    },
    "primary_key": "order_id",
}

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
)


def order_generator(env, sampler):
    """Publishes one order per arrival, as the builder's task with no service does."""
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))

        order = {
            "order_id": task_id,
            "customer_id": 42,
            "total_amount": 59.97,
            "status": "pending",
        }
        # The order key, and the task name and instance id the builder adds
        env.publish_event(
            f"orders-{task_id + 1}", order, task="place_order", instance=task_id
        )
        task_id += 1


def run():
    env = DynamicRealtimeEnvironment(factor=1.0, strict=False)
    env.registry.register_sim_parameter(PARAMS)

    # PostgresEgress creates the orders table, and PostgresIngress creates the
    # simulation_params table it polls for parameter updates.
    env.setup_egress([PostgresEgress(DSN, tables={"orders": ORDERS})])
    env.setup_ingress([PostgresIngress(DSN, table_name="simulation_params")])

    sampler = Sampler(rng=np.random.default_rng(42))
    env.process(order_generator(env, sampler))

    logger.info("Starting Low-level Postgres Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running: INSERT INTO simulation_params (path, "
        "value) VALUES ('Shop.arrival.orders.rate', '5.0');"
    )

    try:
        env.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
```

## Records

The three tabs write the same records, which [a test](https://github.com/jaehyeon-kim/desflow/blob/v0.25.0/tests/docs/test_example_twins.py) checks by running each version unpaced. A real run writes each order as one row of the `orders` table. `desflow run postgres.yaml --sample 3 --stdout` prints the first three as JSON lines instead, with no database:

```text
{"stream_type":"event","sim_ts":1.276,"timestamp":"...","key":"orders-1","task":"place_order","instance":0,"value":{"customer_id":42,"total_amount":59.97,"status":"pending","order_id":0}}
{"stream_type":"event","sim_ts":1.348,"timestamp":"...","key":"orders-2","task":"place_order","instance":1,"value":{"customer_id":42,"total_amount":59.97,"status":"pending","order_id":1}}
{"stream_type":"event","sim_ts":1.869,"timestamp":"...","key":"orders-3","task":"place_order","instance":2,"value":{"customer_id":42,"total_amount":59.97,"status":"pending","order_id":2}}
```

The fields of `value` and the `timestamp` fill the columns of `orders`. The task id is `order_id`, the table's primary key.
