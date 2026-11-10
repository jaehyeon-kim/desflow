# lookup

`lookup`: a row of the finished payloads of the task `task`, or the part of it at `path` (dotted; an integer part indexes a list).

A row is readable from the instant the task's finished record is published. The task keeps its last `history` rows (default 1,000,000).

`strategy` is `random` (the default), `first` (the oldest row kept) or `last` (the newest). `histogram` splits the rows kept, in the order they were written, into bins of the stated shares; a draw picks a bin by its frequency and then a row of the bin at random, so it is for `random` only. A bin that holds no row yet is left out of the draw, and while no bin with a frequency above 0 holds a row (a few rows and a last bin of frequency 0), the draw is over all the rows. `latest_by` keeps only the newest row for each value of that field of the row, so a lookup sees a customer's latest address; `strategy` and `histogram` then choose among those rows, oldest first by the time each was written. `distinct` draws without replacement within one `repeat`, so the items of one order differ.

Use it for a value from a record that another task already published, such as an existing customer.

## Keys

| Key         | Type                                  | Default    |
| ----------- | ------------------------------------- | ---------- |
| `gen`       | `lookup`                              | required   |
| `task`      | text                                  | required   |
| `path`      | text                                  | none       |
| `strategy`  | `random` or `first` or `last`         | `"random"` |
| `histogram` | list of [HistogramBin](#histogrambin) | none       |
| `latest_by` | text                                  | none       |
| `distinct`  | boolean                               | `false`    |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

### HistogramBin

One bin of a `lookup` histogram: `bin` is the share of the rows it covers, in the order they were written, and `frequency` is the chance that a draw falls in it. Both are scaled to add up to 1, so `{bin: 2, frequency: 8}, {bin: 8, frequency: 2}` means the same as 0.2 with 0.8, and 0.8 with 0.2.

| Key         | Type   | Default  |
| ----------- | ------ | -------- |
| `bin`       | number | required |
| `frequency` | number | required |

## Example

Three customers start at time 0. Each order picks one of them at random and publishes its `customer_id`. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Lookup: a value from a finished record of another task.
# Three customers start at time 0, and an order starts every 60 seconds. The run lasts
# 200 seconds, so three orders start.
#
# Run it with: desflow run examples/generators/lookup.yaml

simulation:
  sim_id: Gen
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  customers: {initial: 3, dist: constant, value: 3600, spawn: customer}
  orders: {dist: constant, value: 60, spawn: order}

tasks:
  customer:
    payload:
      customer_id: {gen: sequence, start: 100}
  order:
    payload:
      customer_id: {gen: lookup, task: customer, path: customer_id}

run: {until: 200}
```

```python
"""Lookup: a value from a finished record of another task, builder API.

The builder version of `lookup.yaml`. A task that something looks up keeps its finished
rows, so the customer task sets `history`.

Run it with: python examples/generators/lookup_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 200


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Gen",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("customers", "constant", value=3600, spawn="customer", initial=3)
    app.add_arrival("orders", "constant", value=60, spawn="order")
    customer_payload = Template(
        {"customer_id": {"gen": "sequence", "start": 100}}, name="tasks.customer"
    )
    order_payload = Template(
        {"customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"}},
        name="tasks.order",
    )

    @app.task(name="customer", history=1_000_000)
    def customer(task_id):
        """Gives one customer."""
        return customer_payload

    @app.task(name="order")
    def order(task_id):
        """Gives one order."""
        return order_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Lookup: a value from a finished record of another task, low-level API.

The low-level version of `lookup.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. The script keeps the `customer_id` of each
customer it has published, and an order takes one at an index drawn with
`integers(len(rows))`, as `lookup` does with `strategy: random`.

Needs: nothing.
Run it with: python examples/generators/lookup_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""
    sampler = Sampler(seed=42)
    seller = sampler.generator("tasks.order.payload.customer_id")
    customers: list = []  # the customer_id of each customer so far, oldest first

    def customer_arrivals():
        yield env.timeout(0)
        for number in range(1, 4):
            customers.append(100 + number - 1)
            env.publish_event(
                f"customers-{number}",
                {"customer_id": customers[-1]},
                task="customer",
                instance=number - 1,
            )

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            payload = {"customer_id": customers[int(seller.integers(len(customers)))]}
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=number - 1
            )

    env.process(customer_arrivals())
    env.process(orders())


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

All three print the same records:

```text
[EVT] {'sim_ts': 0.0, 'timestamp': '...', 'key': 'customers-1', 'task': 'customer', 'instance': 0, 'value': {'customer_id': 100}}
[EVT] {'sim_ts': 0.0, 'timestamp': '...', 'key': 'customers-2', 'task': 'customer', 'instance': 1, 'value': {'customer_id': 101}}
[EVT] {'sim_ts': 0.0, 'timestamp': '...', 'key': 'customers-3', 'task': 'customer', 'instance': 2, 'value': {'customer_id': 102}}
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'customer_id': 102}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'customer_id': 101}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'customer_id': 101}}
```

## JSON Schema

The schema of `Lookup`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`lookup`: a row of the finished payloads of the task `task`, or the part of it\nat `path` (dotted; an integer part indexes a list).\n\nA row is readable from the instant the task's finished record is published. The\ntask keeps its last `history` rows (default 1,000,000).\n\n`strategy` is `random` (the default), `first` (the oldest row kept) or `last` (the\nnewest). `histogram` splits the rows kept, in the order they were written, into\nbins of the stated shares; a draw picks a bin by its frequency and then a row of the\nbin at random, so it is for `random` only. A bin that holds no row yet is left out\nof the draw, and while no bin with a frequency above 0 holds a row (a few rows and\na last bin of frequency 0), the draw is over all the rows. `latest_by` keeps only\nthe newest row for each value of that field of the row, so a lookup sees a\ncustomer's latest address; `strategy` and `histogram` then choose among those rows,\noldest first by the time each was written. `distinct` draws without replacement\nwithin one `repeat`, so the items of one order differ.",
  "properties": {
    "null_rate": {
      "anyOf": [
        {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        {
          "type": "null"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ],
      "default": null
    },
    "unique": {
      "default": false,
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "omit_rate": {
      "anyOf": [
        {
          "maximum": 1,
          "minimum": 0,
          "type": "number"
        },
        {
          "type": "null"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ],
      "default": null
    },
    "omit_when": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "gen": {
      "anyOf": [
        {
          "const": "lookup",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "task": {
      "type": "string"
    },
    "path": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "strategy": {
      "default": "random",
      "anyOf": [
        {
          "enum": [
            "random",
            "first",
            "last"
          ],
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "histogram": {
      "anyOf": [
        {
          "items": {
            "$ref": "#/$defs/HistogramBin"
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "latest_by": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "distinct": {
      "default": false,
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    }
  },
  "required": [
    "gen",
    "task"
  ],
  "title": "Lookup",
  "type": "object"
}
```
