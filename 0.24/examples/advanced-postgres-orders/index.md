# Generate Related Records

Related records are rows that belong together across tables. Here every customer order has 1 to 5 line items with random products, prices and quantities, and a total computed from them, written to two PostgreSQL tables, `orders` and `order_items`. Each tab shows the example written one way, and the three publish the same records. In YAML a mapping payload is a constant, so the order generator is a process in `postgres_orders_logic.py`, referenced with `!python`; [Python in a Blueprint](https://jaehyeon.me/desflow/latest/guides/yaml-advanced/index.md) explains how references work.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[postgres]" --with-executables-from "odctl>=1.0,<2"

# 2. Download the example
desflow examples get related-records --api yaml
cd related-records/yaml

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
desflow run postgres_orders.yaml

# 5. Stop its service when finished
odctl down postgres --volumes
```

```bash
# 1. Install
pip install "desflow[postgres]" "odctl>=1.0,<2"

# 2. Download the example
desflow examples get related-records --api yaml
cd related-records/yaml

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
desflow run postgres_orders.yaml

# 5. Stop its service when finished
odctl down postgres --volumes
```

**Full Source Code**

The generator is listed under `processes` with `kwargs`, so it is called as `order_generator(context, arrival="orders", max_items=5)`. It draws from `context.sampler.rng`, the generator seeded by `random_seed`, so a seeded run repeats the same orders.

Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

```yaml
# Orders with random line items, in YAML with one Python function.
#
# Each order arrival builds an order with 1 to 5 items, random prices and
# quantities, and a total computed from them. A mapping payload is a constant, so
# the generator is Python in postgres_orders_logic.py beside this file, referenced
# with !python. Everything else is plain YAML.
#
# Two PostgresEgress instances are attached, one per table, and each keeps only the
# records whose __table__ key names its table. Each creates its table at start.
#
# Needs a database: odctl up postgres. Runs until interrupted with Ctrl + C.
# Run it from its folder with: desflow run postgres_orders.yaml

simulation:
  sim_id: Shop
  factor: 1.0
  random_seed: 42

ingress:
  - type: Postgres
    config:
      connection_dsn: postgresql://user:password@localhost:5432/odctl
      table_name: simulation_params

egress:
  - type: Postgres
    config:
      connection_dsn: postgresql://user:password@localhost:5432/odctl
      tables:
        orders:
          columns:
            order_id: INT
            customer_id: INT
            total_amount: REAL
            status: TEXT
            timestamp: TIMESTAMP
          primary_key: order_id
  - type: Postgres
    config:
      connection_dsn: postgresql://user:password@localhost:5432/odctl
      tables:
        order_items:
          columns:
            order_item_id: INT
            order_id: INT
            product_id: INT
            quantity: INT
            unit_price: REAL
          primary_key: order_item_id

arrivals:
  # No spawn: the process below waits on this arrival itself.
  orders: {dist: exponential, rate: 1.0}

processes:
  # Called as order_generator(context, arrival="orders", max_items=5).
  - function: !python postgres_orders_logic.order_generator
    kwargs: {arrival: orders, max_items: 5}
```

```python
"""Python for postgres_orders.yaml: an order generator with random line items."""


def order_generator(context, arrival: str, max_items: int):
    """Publishes an order and its items on every arrival.

    Draws from `context.sampler.rng`, the generator seeded by `random_seed`, so a
    seeded run repeats the same orders. The `__table__` key names the table each
    record is written to.
    """
    rng = context.sampler.rng
    order_id = 1
    item_id = 1
    while True:
        # The order's key, such as orders-1, is the record key of all its records
        entity = yield context.wait_for_arrival(arrival)

        total = 0.0
        for _ in range(int(rng.integers(1, max_items + 1))):
            price = round(float(rng.uniform(10.0, 50.0)), 2)
            quantity = int(rng.integers(1, 4))
            total += price * quantity
            context.env.publish_event(
                entity.key,
                {
                    "__table__": "order_items",
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": int(rng.integers(1, 51)),
                    "quantity": quantity,
                    "unit_price": price,
                },
            )
            item_id += 1

        context.env.publish_event(
            entity.key,
            {
                "__table__": "orders",
                "order_id": order_id,
                "customer_id": int(rng.integers(1, 101)),
                "total_amount": round(total, 2),
                "status": "pending",
            },
        )
        order_id += 1
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[postgres]" --with-executables-from "odctl>=1.0,<2"

# 2. Download the example
desflow examples get related-records --api builder
cd related-records/builder

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
uv run --no-project --with "desflow[postgres]" postgres_orders_example.py

# 5. Stop its service when finished
odctl down postgres --volumes
```

```bash
# 1. Install
pip install "desflow[postgres]" "odctl>=1.0,<2"

# 2. Download the example
desflow examples get related-records --api builder
cd related-records/builder

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
python postgres_orders_example.py

# 5. Stop its service when finished
odctl down postgres --volumes
```

**Full Source Code**

The generator is added with `add_process`, so it is called as `order_generator(context, arrival="orders", max_items=5)`. It draws from `context.sampler.rng`, the generator seeded by `random_seed`, so a seeded run repeats the same orders.

```python
"""
Orders with line items, builder API.

The builder twin of `yaml/advanced/postgres_orders.yaml`. Each order arrival
builds an order with 1 to 5 items, random prices and quantities, and a total computed
from them. Two PostgresEgress instances are attached, one per table, and each keeps
only the records whose `__table__` key names its table. Each creates its table at
start.

Needs a database: odctl up postgres. Runs until interrupted with Ctrl + C.
"""

import logging

from desflow import PostgresEgress, PostgresIngress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

DSN = "postgresql://user:password@localhost:5432/odctl"

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
ORDER_ITEMS = {
    "columns": {
        "order_item_id": "INT",
        "order_id": "INT",
        "product_id": "INT",
        "quantity": "INT",
        "unit_price": "REAL",
    },
    "primary_key": "order_item_id",
}


def order_generator(context: SimulationContext, arrival: str, max_items: int):
    """Publishes an order and its items on every arrival.

    Draws from `context.sampler.rng`, the generator seeded by `random_seed`, so a
    seeded run repeats the same orders. The `__table__` key names the table each
    record is written to.
    """
    rng = context.sampler.rng
    order_id = 1
    item_id = 1
    while True:
        # The order's key, such as orders-1, is the record key of all its records
        entity = yield context.wait_for_arrival(arrival)

        total = 0.0
        for _ in range(int(rng.integers(1, max_items + 1))):
            price = round(float(rng.uniform(10.0, 50.0)), 2)
            quantity = int(rng.integers(1, 4))
            total += price * quantity
            context.env.publish_event(
                entity.key,
                {
                    "__table__": "order_items",
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": int(rng.integers(1, 51)),
                    "quantity": quantity,
                    "unit_price": price,
                },
            )
            item_id += 1

        context.env.publish_event(
            entity.key,
            {
                "__table__": "orders",
                "order_id": order_id,
                "customer_id": int(rng.integers(1, 101)),
                "total_amount": round(total, 2),
                "status": "pending",
            },
        )
        order_id += 1


app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_ingress(PostgresIngress(DSN, table_name="simulation_params"))
    .add_egress(PostgresEgress(DSN, tables={"orders": ORDERS}))
    .add_egress(PostgresEgress(DSN, tables={"order_items": ORDER_ITEMS}))
    .add_arrival("orders", dist="exponential", rate=1.0)
    .add_process(order_generator, arrival="orders", max_items=5)
)

if __name__ == "__main__":
    app.run()
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[postgres]" --with-executables-from "odctl>=1.0,<2"

# 2. Download the example
desflow examples get related-records --api low-level
cd related-records/low-level

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
uv run --no-project --with "desflow[postgres]" postgres_orders_example.py

# 5. Stop its service when finished
odctl down postgres --volumes
```

```bash
# 1. Install
pip install "desflow[postgres]" "odctl>=1.0,<2"

# 2. Download the example
desflow examples get related-records --api low-level
cd related-records/low-level

# 3. Start its service
odctl up postgres

# 4. Run it (Ctrl + C to stop)
python postgres_orders_example.py

# 5. Stop its service when finished
odctl down postgres --volumes
```

**Full Source Code**

The generator is a SimPy process started with `env.process`. It draws the gaps and the order contents from one `Sampler` seeded with 42, in the same order as the other two, so it publishes the same orders.

```python
"""
Orders with line items, low-level API.

The low-level twin of `builder/postgres_orders_example.py` and
`yaml/advanced/postgres_orders.yaml`. It wires `DynamicRealtimeEnvironment`, the
registry and the connectors by hand. With the same seed it publishes the same orders
as the other two.

Needs a database: odctl up postgres. Runs until interrupted with Ctrl + C.
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

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

DSN = "postgresql://user:password@localhost:5432/odctl"

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
)

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
ORDER_ITEMS = {
    "columns": {
        "order_item_id": "INT",
        "order_id": "INT",
        "product_id": "INT",
        "quantity": "INT",
        "unit_price": "REAL",
    },
    "primary_key": "order_item_id",
}


def order_generator(env, sampler: Sampler, max_items: int):
    """Publishes an order and its items on every arrival.

    The gaps come from the arrival's own random sequence and the order contents from
    `sampler.rng`, as in the builder version, so the two publish the same records.
    """
    rng = sampler.rng
    arrival = env.registry.get_config("Shop.arrival.orders")
    order_id = 1
    item_id = 1
    while True:
        yield env.timeout(sampler.sample(arrival))

        total = 0.0
        for _ in range(int(rng.integers(1, max_items + 1))):
            price = round(float(rng.uniform(10.0, 50.0)), 2)
            quantity = int(rng.integers(1, 4))
            total += price * quantity
            env.publish_event(
                f"orders-{order_id}",
                {
                    "__table__": "order_items",
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": int(rng.integers(1, 51)),
                    "quantity": quantity,
                    "unit_price": price,
                },
            )
            item_id += 1

        env.publish_event(
            f"orders-{order_id}",
            {
                "__table__": "orders",
                "order_id": order_id,
                "customer_id": int(rng.integers(1, 101)),
                "total_amount": round(total, 2),
                "status": "pending",
            },
        )
        order_id += 1


def run(until=None, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor, strict=False)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_ingress([PostgresIngress(DSN, table_name="simulation_params")])
    env.setup_egress(
        [
            PostgresEgress(DSN, tables={"orders": ORDERS}),
            PostgresEgress(DSN, tables={"order_items": ORDER_ITEMS}),
        ]
    )
    sampler = Sampler(rng=np.random.default_rng(42))
    env.process(order_generator(env, sampler, max_items=5))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
```

______________________________________________________________________

## What It Does

When the run starts, each `PostgresEgress` creates its table, `orders` or `order_items`, and `PostgresIngress` creates `simulation_params`, if they do not exist. The run then writes orders and their items until you stop it. Both egresses receive every record and each keeps the records whose `__table__` key names its table.

In a second terminal, raise the arrival rate while it runs:

```bash
docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (path, value) VALUES ('Shop.arrival.orders.rate', '5.0');"
```
