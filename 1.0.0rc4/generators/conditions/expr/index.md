# expr

`expr`: the value of an expression, with the names of the language, the vars of the task and the entries of `names` (evaluated first) as bare names, and `t` for the simulation time. It can give any type; a number is rounded to `decimals`.

Use it for a value computed from vars and numbers by an expression.

## Keys

| Key        | Type                      | Default  |
| ---------- | ------------------------- | -------- |
| `gen`      | `expr`                    | required |
| `expr`     | text                      | required |
| `names`    | mapping of names to Value |          |
| `decimals` | integer                   | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`total` is the var `quantity` times a `price` of 2.5, rounded to 2 decimals. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Expr: the value of an expression over the task's vars.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/expr.yaml

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
    vars:
      quantity: {gen: integer, bounds: [1, 6]}
    payload:
      quantity: {gen: var, name: quantity}
      total:
        gen: expr
        expr: "quantity * price"
        names: {price: 2.5}
        decimals: 2

run: {until: 200}
```

```python
"""Expr: the value of an expression over the task's vars, builder API.

The builder version of `expr.yaml`: the same task, written with `Template`, which gives
the payload's generators their place and so their random streams.

Run it with: python examples/generators/expr_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 200


def build() -> SimulationContext:
    """Builds the model: an order every 60 seconds."""
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
            "quantity": {"gen": "var", "name": "quantity"},
            "total": {
                "gen": "expr",
                "expr": "quantity * price",
                "names": {"price": 2.5},
                "decimals": 2,
            },
        },
        vars={"quantity": {"gen": "integer", "bounds": [1, 6]}},
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
"""Expr: the value of an expression over the task's vars, low-level API.

The low-level version of `expr.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. A var is drawn once for each order, from the
place `vars.quantity` of the task, and `expr` computes its expression from it.

Needs: nothing.
Run it with: python examples/generators/expr_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the order process on `env`."""
    sampler = Sampler(seed=42)
    quantity = sampler.generator("tasks.order.vars.quantity")

    def make(number):
        """Draws the payload of order `number`."""
        count = int(quantity.integers(1, 6 + 1))
        return {"quantity": count, "total": round(count * 2.5, 2)}

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            env.publish_event(
                f"orders-{number}", make(number), task="order", instance=number - 1
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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'quantity': 1, 'total': 2.5}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'quantity': 3, 'total': 7.5}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'quantity': 4, 'total': 10.0}}
```

## JSON Schema

The schema of `Expr`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`expr`: the value of an expression, with the names of the language, the vars of\nthe task and the entries of `names` (evaluated first) as bare names, and `t` for\nthe simulation time. It can give any type; a number is rounded to `decimals`.",
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
          "const": "expr",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "expr": {
      "type": "string"
    },
    "names": {
      "additionalProperties": {
        "$ref": "#/$defs/Value"
      },
      "type": "object"
    },
    "decimals": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        },
        {
          "$ref": "#/$defs/NonNegativeIntegerText"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ],
      "default": null
    }
  },
  "required": [
    "gen",
    "expr"
  ],
  "title": "Expr",
  "type": "object"
}
```
