# Join Across Sinks

Write the ids of one kind of record to one sink and use them in another: customers go to one JSON Lines file, and the orders that name those customers go to a second file, so the two files join on `customer_id`.

______________________________________________________________________

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

  ```bash
  git clone https://github.com/jaehyeon-kim/desflow.git
  cd desflow
  ```

- [First Simulation](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md) explains arrivals, tasks and the records a run writes.

- Install the file writers: `pip install "desflow[parquet]"`.

- [Send Data to a Sink](https://jaehyeon.me/desflow/latest/how-to/generate-data/send-data-to-a-sink/index.md) shows the sinks, and [Look Up Other Records](https://jaehyeon.me/desflow/latest/how-to/generate-data/look-up-other-records/index.md) shows `lookup`.

______________________________________________________________________

## Steps

### Write Customers and Orders

Five customers are written at the start and another about every 10 seconds. An order arrives about once a second, and [`lookup`](https://jaehyeon.me/desflow/latest/generators/related-records/lookup/index.md) gives it the `customer_id` of a customer already written.

```yaml
arrivals:
  customers: {initial: 5, dist: exponential, rate: 0.1, spawn: customer}
  orders: {dist: exponential, rate: 1, spawn: order}
```

```yaml
tasks:
  customer:
    payload:
      customer_id: {gen: sequence, start: 1}
      country: {gen: one_of, choices: [AU, NZ, SG]}
  order:
    payload:
      order_id: {gen: sequence, start: 1}
      customer_id: {gen: lookup, task: customer, path: customer_id}
      total: {gen: uniform, low: 5, high: 200, decimals: 2}
```

```python
app.add_arrival("customers", "exponential", rate=0.1, spawn="customer", initial=5)
app.add_arrival("orders", "exponential", rate=1, spawn="order")
```

```python
customer_payload = Template(
    {
        "customer_id": {"gen": "sequence", "start": 1},
        "country": {"gen": "one_of", "choices": ["AU", "NZ", "SG"]},
    },
    name="tasks.customer",
)
order_payload = Template(
    {
        "order_id": {"gen": "sequence", "start": 1},
        "customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"},
        "total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
    },
    name="tasks.order",
)

# A task that something looks up keeps its rows. `history` is the blueprint's
# default, and a builder task needs it because a `Template` payload is not searched
# for lookups.
@app.task(name="customer", history=1_000_000)
def customer(task_id):
    """Gives one customer record."""
    return customer_payload

@app.task(name="order")
def order(task_id):
    """Gives one order record."""
    return order_payload
```

A task that something looks up keeps its rows, and `history=` says how many. A Builder task needs it, because a `Template` payload is not searched for lookups. A blueprint keeps 1,000,000 rows by default.

```python
sampler = Sampler(seed=42)
country = sampler.generator("tasks.customer.payload.country")
pick = sampler.generator("tasks.order.payload.customer_id")
total = sampler.generator("tasks.order.payload.total")
customer_gap = DistributionConfig(dist="exponential", rate=0.1)
# The random sequence the blueprint's draw uses
customer_gap.stream = "arrival.customers"
order_gap = DistributionConfig(dist="exponential", rate=1)
order_gap.stream = "arrival.orders"
customer_rows: list = []  # the customers written so far, for a lookup
sent = {"customer": 0, "order": 0}
```

`Sampler(seed=42)` supplies every draw: a random generator for each payload field and for the gap. The `stream` line names the random sequence the blueprint uses for this arrival, so the three forms draw the same gaps.

```python
def write_customer(key):
    """Publishes the next customer and keeps its row."""
    row = {
        "customer_id": len(customer_rows) + 1,
        "country": COUNTRIES[int(country.integers(len(COUNTRIES)))],
    }
    customer_rows.append(row)
    env.publish_event(key, row, task="customer", instance=sent["customer"])
    sent["customer"] += 1

def customers():
    for number in itertools.count(6):  # 1 to 5 exist at the start
        yield env.timeout(sampler.sample(customer_gap))
        write_customer(f"customers-{number}")

def orders():
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(order_gap))
        # The lookup: one row of the customers written so far, chosen at random
        customer = customer_rows[int(pick.integers(len(customer_rows)))]
        payload = {
            "order_id": number,
            "customer_id": customer["customer_id"],
            "total": round(float(total.uniform(5, 200)), 2),
        }
        env.publish_event(
            f"orders-{number}", payload, task="order", instance=sent["order"]
        )
        sent["order"] += 1

def first(number):
    yield env.timeout(0)
    write_customer(f"customers-{number}")

env.process(customers())
env.process(orders())
for number in range(1, 6):
    env.process(first(number))
```

A Low-level script keeps the customers in a list and picks one at random for each order.

### Send Each Kind to Its File

A `when` on each sink lets through the records of one task. The first file takes the records of the task `customer` and the second the records of the task `order`.

```yaml
egress:
  - type: Jsonl
    when: {expr: "record.task == 'customer'"}
    config:
      default_path: ${DEST_PATH:-data}/customers.jsonl
  - type: Jsonl
    when: {expr: "record.task == 'order'"}
    config:
      default_path: ${DEST_PATH:-data}/orders.jsonl
```

```python
app.add_egress(
    JsonlStorageEgress(default_path=f"{folder}/customers.jsonl"),
    when=lambda record: record.get("task") == "customer",
)
app.add_egress(
    JsonlStorageEgress(default_path=f"{folder}/orders.jsonl"),
    when=lambda record: record.get("task") == "order",
)
```

```python
env.setup_egress(
    [
        JsonlStorageEgress(default_path=f"{folder}/customers.jsonl"),
        JsonlStorageEgress(default_path=f"{folder}/orders.jsonl"),
    ],
    predicates=[
        lambda record: record.get("task") == "customer",
        lambda record: record.get("task") == "order",
    ],
)
```

`env.setup_egress` takes one predicate for each sink.

______________________________________________________________________

## Check the Result

Run the model for 60 simulated seconds into a folder of your own, and list the files:

```bash
DEST_PATH=out desflow run docs/snippets/generate-data/join_across_sinks.yaml
ls out
```

```bash
DEST_PATH=out python docs/snippets/generate-data/join_across_sinks_builder.py
ls out
```

```bash
DEST_PATH=out python docs/snippets/generate-data/join_across_sinks_low_level.py
ls out
```

`out` holds files named `customers_<8 hex digits>.jsonl` and `orders_<8 hex digits>.jsonl`, one flat row for each record. To see the records without writing files, print the first 8 of them:

```bash
desflow run docs/snippets/generate-data/join_across_sinks.yaml --sample 8
```

The block keeps the first customer and the first two orders. Each order names a customer written by then.

```text
{"stream_type":"event","sim_ts":0.0,"timestamp":"...","key":"customers-1","task":"customer","instance":0,"value":{"customer_id":1,"country":"NZ"}}
{"stream_type":"event","sim_ts":0.3141556656469718,"timestamp":"...","key":"orders-1","task":"order","instance":0,"value":{"order_id":1,"customer_id":5,"total":161.55}}
{"stream_type":"event","sim_ts":2.4671694506095694,"timestamp":"...","key":"orders-2","task":"order","instance":1,"value":{"order_id":2,"customer_id":4,"total":177.63}}
```

The docs test runs all three forms into real files in a temporary folder. Each file holds the records of one task, every order in the orders file names a customer in the customers file, and the three forms write the same files.

______________________________________________________________________

## Full Files

Full files

```yaml
# Join across sinks: customers go to one file, and the orders that name them to another.
#
# Customers are written to customers.jsonl. Each order takes its customer id from a
# customer already written, with `lookup`, and goes to orders.jsonl. A `when` on each
# sink sends it the records of one task, so the two files join on customer_id.
#
# Needs: the parquet extra, which holds the file writers. DEST_PATH sets the folder
# (data by default).
# Run it with: desflow run docs/snippets/generate-data/join_across_sinks.yaml

simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - type: Jsonl
    when: {expr: "record.task == 'customer'"}
    config:
      default_path: ${DEST_PATH:-data}/customers.jsonl
  - type: Jsonl
    when: {expr: "record.task == 'order'"}
    config:
      default_path: ${DEST_PATH:-data}/orders.jsonl

arrivals:
  customers: {initial: 5, dist: exponential, rate: 0.1, spawn: customer}
  orders: {dist: exponential, rate: 1, spawn: order}

tasks:
  customer:
    payload:
      customer_id: {gen: sequence, start: 1}
      country: {gen: one_of, choices: [AU, NZ, SG]}
  order:
    payload:
      order_id: {gen: sequence, start: 1}
      customer_id: {gen: lookup, task: customer, path: customer_id}
      total: {gen: uniform, low: 5, high: 200, decimals: 2}

run: {until: 60}
```

```python
"""Join across sinks: customers to one file, and the orders that name them to another.

The builder version of `join_across_sinks.yaml`: 5 customers at the start and one more
about every 10 seconds, and an order about every second whose `lookup` takes the
customer id from a customer already written. Each sink takes the records of one task,
with `when`.

Needs: the parquet extra, which holds the file writers. DEST_PATH sets the folder (data
by default).
Run it with: python docs/snippets/generate-data/join_across_sinks_builder.py
"""

import logging
import os
from datetime import datetime, timezone

from desflow import JsonlStorageEgress, SimulationContext
from desflow.values import Template

UNTIL = 60


def build() -> SimulationContext:
    """Builds the shop, with a file for each kind of record."""
    folder = os.environ.get("DEST_PATH", "data")
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(
        JsonlStorageEgress(default_path=f"{folder}/customers.jsonl"),
        when=lambda record: record.get("task") == "customer",
    )
    app.add_egress(
        JsonlStorageEgress(default_path=f"{folder}/orders.jsonl"),
        when=lambda record: record.get("task") == "order",
    )
    app.add_arrival("customers", "exponential", rate=0.1, spawn="customer", initial=5)
    app.add_arrival("orders", "exponential", rate=1, spawn="order")
    customer_payload = Template(
        {
            "customer_id": {"gen": "sequence", "start": 1},
            "country": {"gen": "one_of", "choices": ["AU", "NZ", "SG"]},
        },
        name="tasks.customer",
    )
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"},
            "total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
        },
        name="tasks.order",
    )

    # A task that something looks up keeps its rows. `history` is the blueprint's
    # default, and a builder task needs it because a `Template` payload is not searched
    # for lookups.
    @app.task(name="customer", history=1_000_000)
    def customer(task_id):
        """Gives one customer record."""
        return customer_payload

    @app.task(name="order")
    def order(task_id):
        """Gives one order record."""
        return order_payload


    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Join across sinks: customers to one file, and the orders that name them to another.

The low-level version of `join_across_sinks.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's. The script keeps each customer row in a
list, so that an order's lookup is one draw of a row of that list, and each sink takes
the records of one task through a predicate.

Needs: the parquet extra, which holds the file writers. DEST_PATH sets the folder (data
by default).
Run it with: python docs/snippets/generate-data/join_across_sinks_low_level.py
"""

import itertools
import logging
import os
from datetime import datetime, timezone

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    JsonlStorageEgress,
    Sampler,
)

UNTIL = 60
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}
COUNTRIES = ["AU", "NZ", "SG"]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the customers and the orders on `env`."""
    sampler = Sampler(seed=42)
    country = sampler.generator("tasks.customer.payload.country")
    pick = sampler.generator("tasks.order.payload.customer_id")
    total = sampler.generator("tasks.order.payload.total")
    customer_gap = DistributionConfig(dist="exponential", rate=0.1)
    # The random sequence the blueprint's draw uses
    customer_gap.stream = "arrival.customers"
    order_gap = DistributionConfig(dist="exponential", rate=1)
    order_gap.stream = "arrival.orders"
    customer_rows: list = []  # the customers written so far, for a lookup
    sent = {"customer": 0, "order": 0}

    def write_customer(key):
        """Publishes the next customer and keeps its row."""
        row = {
            "customer_id": len(customer_rows) + 1,
            "country": COUNTRIES[int(country.integers(len(COUNTRIES)))],
        }
        customer_rows.append(row)
        env.publish_event(key, row, task="customer", instance=sent["customer"])
        sent["customer"] += 1

    def customers():
        for number in itertools.count(6):  # 1 to 5 exist at the start
            yield env.timeout(sampler.sample(customer_gap))
            write_customer(f"customers-{number}")

    def orders():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(order_gap))
            # The lookup: one row of the customers written so far, chosen at random
            customer = customer_rows[int(pick.integers(len(customer_rows)))]
            payload = {
                "order_id": number,
                "customer_id": customer["customer_id"],
                "total": round(float(total.uniform(5, 200)), 2),
            }
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=sent["order"]
            )
            sent["order"] += 1

    def first(number):
        yield env.timeout(0)
        write_customer(f"customers-{number}")

    env.process(customers())
    env.process(orders())
    for number in range(1, 6):
        env.process(first(number))


def run() -> None:
    """Runs the model unpaced, with a file for each kind of record."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    folder = os.environ.get("DEST_PATH", "data")
    env.setup_egress(
        [
            JsonlStorageEgress(default_path=f"{folder}/customers.jsonl"),
            JsonlStorageEgress(default_path=f"{folder}/orders.jsonl"),
        ],
        predicates=[
            lambda record: record.get("task") == "customer",
            lambda record: record.get("task") == "order",
        ],
    )
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```
