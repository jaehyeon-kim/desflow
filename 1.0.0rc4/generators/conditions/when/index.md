# when

`when`: `then` when the `condition` (an expression) holds, else `else`. Only the branch taken is evaluated.

Use it for a value that changes with one condition, such as a discount for a large order.

## Keys

| Key         | Type   | Default  |
| ----------- | ------ | -------- |
| `gen`       | `when` | required |
| `condition` | text   | required |
| `then`      | Value  | required |
| `else`      | Value  | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`quantity` is a var, a whole number from 1 to 6. `size` is large when `quantity` is above 3, and small otherwise. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# When: one of two values, by a condition.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/when.yaml

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
      size: {gen: when, condition: "quantity > 3", then: large, else: small}

run: {until: 200}
```

```python
"""When: one of two values, by a condition, builder API.

The builder version of `when.yaml`: the same task, written with `Template`, which gives
the payload's generators their place and so their random streams.

Run it with: python examples/generators/when_builder.py
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
            "size": {
                "gen": "when",
                "condition": "quantity > 3",
                "then": "large",
                "else": "small",
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
"""When: one of two values, by a condition, low-level API.

The low-level version of `when.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. A var is drawn once for each order, from the
place `vars.quantity` of the task, and `when` takes its `then` or its `else` by the
condition.

Needs: nothing.
Run it with: python examples/generators/when_low_level.py
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
        return {"quantity": count, "size": "large" if count > 3 else "small"}

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'quantity': 1, 'size': 'small'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'quantity': 3, 'size': 'small'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'quantity': 4, 'size': 'large'}}
```

## JSON Schema

The schema of `When`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`when`: `then` when the `condition` (an expression) holds, else `else`.\nOnly the branch taken is evaluated.",
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
          "const": "when",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "condition": {
      "type": "string"
    },
    "then": {
      "$ref": "#/$defs/Value"
    },
    "else": {
      "$ref": "#/$defs/Value",
      "default": null
    }
  },
  "required": [
    "gen",
    "condition",
    "then"
  ],
  "title": "When",
  "type": "object"
}
```
