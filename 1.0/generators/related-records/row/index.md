# row

`row`: one row of a CSV or Parquet file as a mapping, read once at load. A cell of digits is read as a number, so a postal code with leading zeros loses them; write them with `{gen: format, template: "{n:05d}"}`. `where` is an expression over the row's columns that keeps some rows; `weight` names a column of weights; `order` is `random` (the default), `sequential`, `shuffle` (every row once, in a random order) or `circular` (in file order, again from the first); `on_end` says what a `sequential` or `shuffle` order does when the rows run out: `error` (the default), `loop` or `stop`, which ends the run.

Use it for a whole row of a CSV or Parquet file, such as a product from a price list.

## Keys

| Key      | Type                                                | Default    |
| -------- | --------------------------------------------------- | ---------- |
| `gen`    | `row`                                               | required   |
| `file`   | text                                                | required   |
| `where`  | text                                                | none       |
| `weight` | text                                                | none       |
| `order`  | `random` or `sequential` or `shuffle` or `circular` | `"random"` |
| `on_end` | `stop` or `loop` or `error`                         | `"error"`  |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`product` is one row of `products.csv`, and `shuffle` gives every row once, in a random order. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Row: one row of a CSV file.
# An order starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start. The file is `products.csv` in this folder.
#
# Run it with: desflow run examples/generators/row.yaml

simulation:
  sim_id: Gen
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  orders: {dist: constant, value: 60, spawn: order}

tasks:
  order:
    payload:
      product: {gen: row, file: products.csv, order: shuffle}

run: {until: 200}
```

```python
"""Row: one row of a CSV file, builder API.

The builder version of `row.yaml`. A builder script reads the file from the working
directory, so run it from the repository root.

Run it with: python examples/generators/row_builder.py
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
    app.add_arrival("orders", "constant", value=60, spawn="order")
    payload = Template(
        {
            "product": {
                "gen": "row",
                "file": "examples/generators/products.csv",
                "order": "shuffle",
            }
        },
        name="tasks.order",
    )

    @app.task(name="order")
    def order(task_id):
        """Gives one order."""
        return payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Row: one row of a CSV file, low-level API.

The low-level version of `row.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. The script reads the file from the working
directory, so run it from the repository root. A cell of digits is a number. `order:
shuffle` draws one permutation of the rows with `permutation(len(rows))` and gives the
rows in that order.

Needs: nothing.
Run it with: python examples/generators/row_low_level.py
"""

import csv
import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def _cell(text):
    """A cell of digits is a whole number, one with a point is a decimal number."""
    for kind in (int, float):
        try:
            return kind(text)
        except ValueError:
            pass
    return text


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""
    sampler = Sampler(seed=42)
    shuffle = sampler.generator("tasks.order.payload.product")
    with open("examples/generators/products.csv", newline="") as file:
        rows = [
            {name: _cell(text) for name, text in row.items()}
            for row in csv.DictReader(file)
        ]
    order = [int(i) for i in shuffle.permutation(len(rows))]

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            payload = {"product": rows[order[number - 1]]}
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=number - 1
            )

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'product': {'sku': 'B-2', 'price': 4, 'weight': 3}}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'product': {'sku': 'A-1', 'price': 2.5, 'weight': 1}}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'product': {'sku': 'C-3', 'price': 6, 'weight': 6}}}
```

## JSON Schema

The schema of `Row`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`row`: one row of a CSV or Parquet file as a mapping, read once at load. A cell\nof digits is read as a number, so a postal code with leading zeros loses them;\nwrite them with `{gen: format, template: \"{n:05d}\"}`. `where` is an expression over\nthe row's columns that keeps some rows; `weight` names a column of weights; `order`\nis `random` (the default), `sequential`, `shuffle` (every row once, in a random\norder) or `circular` (in file order, again from the first); `on_end` says what a\n`sequential` or `shuffle` order does when the rows run out: `error` (the default),\n`loop` or `stop`, which ends the run.",
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
          "$ref": "#/$defs/NonNegativeNumberText"
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
          "$ref": "#/$defs/NonNegativeNumberText"
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
          "const": "row",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "file": {
      "type": "string"
    },
    "where": {
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
    "weight": {
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
    "order": {
      "default": "random",
      "anyOf": [
        {
          "enum": [
            "random",
            "sequential",
            "shuffle",
            "circular"
          ],
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "on_end": {
      "default": "error",
      "anyOf": [
        {
          "enum": [
            "stop",
            "loop",
            "error"
          ],
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    }
  },
  "required": [
    "gen",
    "file"
  ],
  "title": "Row",
  "type": "object"
}
```
