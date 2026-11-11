# Skewed Keys

Make a few keys carry most of the records: 5 of 25 customers place 75 orders in 100, and the customer is the record key, so a Kafka topic puts the busy customers' records in a few partitions.

______________________________________________________________________

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

  ```bash
  git clone https://github.com/jaehyeon-kim/desflow.git
  cd desflow
  ```

- [First Simulation](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md) explains arrivals, tasks and the records a run writes.

- [`when`](https://jaehyeon.me/desflow/latest/generators/conditions/when/index.md) and [`boolean`](https://jaehyeon.me/desflow/latest/generators/random-numbers/boolean/index.md) list the keys of the two generators that make the skew.

______________________________________________________________________

## Steps

### Describe the Arrival

An order arrives every second.

```yaml
arrivals:
  orders: {dist: constant, value: 1, spawn: order}
```

```python
app.add_arrival("orders", "constant", value=1, spawn="order")
```

```python
sampler = Sampler(seed=42)
busy = sampler.generator("tasks.order.vars.busy")
busy_customer = sampler.generator("tasks.order.payload.customer_id.then")
quiet_customer = sampler.generator("tasks.order.payload.customer_id.else")
orders_gap = DistributionConfig(dist="constant", value=1)
# The random sequence the blueprint's draw uses
orders_gap.stream = "arrival.orders"
```

`Sampler(seed=42)` supplies every draw: a random generator for each payload field and for the gap. The `stream` line names the random sequence the blueprint uses for this arrival, so the three forms draw the same gaps.

### Skew the Customers

A `vars` entry is drawn once for each order before its payload, and the payload reads it by name. `busy` is a [`boolean`](https://jaehyeon.me/desflow/latest/generators/random-numbers/boolean/index.md) that is true 75 times in 100. [`when`](https://jaehyeon.me/desflow/latest/generators/conditions/when/index.md) then draws the customer from the 5 busy customers (1 to 5) if `busy` is true, and from the other 20 (6 to 25) if it is not. `key_field: customer_id` makes that field the record key.

```yaml
tasks:
  order:
    vars:
      busy: {gen: boolean, p: 0.75}
    key_field: customer_id
    payload:
      order_id: {gen: sequence, start: 1}
      customer_id:
        gen: when
        condition: busy
        then: {gen: integer, bounds: [1, 5]}
        else: {gen: integer, bounds: [6, 25]}
```

```python
order_payload = Template(
    {
        "order_id": {"gen": "sequence", "start": 1},
        "customer_id": {
            "gen": "when",
            "condition": "busy",
            "then": {"gen": "integer", "bounds": [1, 5]},
            "else": {"gen": "integer", "bounds": [6, 25]},
        },
    },
    name="tasks.order",
    vars={"busy": {"gen": "boolean", "p": 0.75}},
)

@app.task(name="order", key_field="customer_id")
def order(task_id):
    """Gives one order record."""
    return order_payload
```

```python
def orders_source():
    number = 0
    while True:
        yield env.timeout(sampler.sample(orders_gap))
        number += 1
        if busy.random() < 0.75:
            customer_id = int(busy_customer.integers(1, 5 + 1))
        else:
            customer_id = int(quiet_customer.integers(6, 25 + 1))
        env.publish_event(
            str(customer_id),  # the record key, as the task's key_field gives it
            {"order_id": number, "customer_id": customer_id},
            task="order",
            instance=number - 1,
        )

env.process(orders_source())
```

______________________________________________________________________

## Check the Result

Run the model for 1,000 simulated seconds, print its first 6 records and read the key of each. The `key` of a record is its `customer_id` as text.

```bash
desflow run docs/snippets/generate-data/skewed_keys.yaml --sample 6
```

```bash
python docs/snippets/generate-data/skewed_keys_builder.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```bash
python docs/snippets/generate-data/skewed_keys_low_level.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```text
{"stream_type":"event","sim_ts":1.0,"timestamp":"...","key":"12","task":"order","instance":0,"value":{"order_id":1,"customer_id":12}}
{"stream_type":"event","sim_ts":2.0,"timestamp":"...","key":"11","task":"order","instance":1,"value":{"order_id":2,"customer_id":11}}
{"stream_type":"event","sim_ts":3.0,"timestamp":"...","key":"12","task":"order","instance":2,"value":{"order_id":3,"customer_id":12}}
{"stream_type":"event","sim_ts":4.0,"timestamp":"...","key":"2","task":"order","instance":3,"value":{"order_id":4,"customer_id":2}}
{"stream_type":"event","sim_ts":5.0,"timestamp":"...","key":"3","task":"order","instance":4,"value":{"order_id":5,"customer_id":3}}
{"stream_type":"event","sim_ts":6.0,"timestamp":"...","key":"4","task":"order","instance":5,"value":{"order_id":6,"customer_id":4}}
```

The docs test runs the model for 10,000 simulated seconds and counts the orders: customers 1 to 5 place about 75 percent of them, each about 15 percent, and customers 6 to 25 place the rest.

______________________________________________________________________

## Full Files

Full files

```yaml
# Skewed keys: 5 of 25 customers place 75 percent of the orders.
#
# An order arrives every second. A draw that is true 75 percent of the time makes the
# order come from one of 5 busy customers out of 25, and the rest come from the other
# 20. `key_field` makes the customer the record key, so a Kafka topic puts the busy
# customers' records in a few partitions.
#
# Needs: nothing.
# Run it with: desflow run docs/snippets/generate-data/skewed_keys.yaml

simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  orders: {dist: constant, value: 1, spawn: order}

tasks:
  order:
    vars:
      busy: {gen: boolean, p: 0.75}
    key_field: customer_id
    payload:
      order_id: {gen: sequence, start: 1}
      customer_id:
        gen: when
        condition: busy
        then: {gen: integer, bounds: [1, 5]}
        else: {gen: integer, bounds: [6, 25]}

run: {until: 1000}
```

```python
"""Skewed keys: 5 of 25 customers place 75 percent of the orders, builder API.

The builder version of `skewed_keys.yaml`: an order every second, 75 percent from the
5 busy customers of 25, with the customer as the key.

Needs: nothing.
Run it with: python docs/snippets/generate-data/skewed_keys_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 1000


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("orders", "constant", value=1, spawn="order")
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "customer_id": {
                "gen": "when",
                "condition": "busy",
                "then": {"gen": "integer", "bounds": [1, 5]},
                "else": {"gen": "integer", "bounds": [6, 25]},
            },
        },
        name="tasks.order",
        vars={"busy": {"gen": "boolean", "p": 0.75}},
    )

    @app.task(name="order", key_field="customer_id")
    def order(task_id):
        """Gives one order record."""
        return order_payload


    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Skewed keys: 5 of 25 customers place 75 percent of the orders, low-level API.

The low-level version of `skewed_keys.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's. A draw that is true 75 percent of the time
picks the busy customers, and the customer is the record key.

Needs: nothing.
Run it with: python docs/snippets/generate-data/skewed_keys_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 1000
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrival on `env`."""
    sampler = Sampler(seed=42)
    busy = sampler.generator("tasks.order.vars.busy")
    busy_customer = sampler.generator("tasks.order.payload.customer_id.then")
    quiet_customer = sampler.generator("tasks.order.payload.customer_id.else")
    orders_gap = DistributionConfig(dist="constant", value=1)
    # The random sequence the blueprint's draw uses
    orders_gap.stream = "arrival.orders"

    def orders_source():
        number = 0
        while True:
            yield env.timeout(sampler.sample(orders_gap))
            number += 1
            if busy.random() < 0.75:
                customer_id = int(busy_customer.integers(1, 5 + 1))
            else:
                customer_id = int(quiet_customer.integers(6, 25 + 1))
            env.publish_event(
                str(customer_id),  # the record key, as the task's key_field gives it
                {"order_id": number, "customer_id": customer_id},
                task="order",
                instance=number - 1,
            )

    env.process(orders_source())


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```
