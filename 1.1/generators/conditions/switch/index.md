# switch

`switch`: the value of the first case that matches `on`, evaluated only when picked. `cases` is a mapping from a value to its result, or a list of ranges; `default` is used when none matches (null when it is left out). In a blueprint, a key written `yes`, `no`, `on` or `off` is that text, not a boolean.

Use it for one value out of several, chosen by an exact match or by a range.

## Keys

| Key       | Type                                                             | Default  |
| --------- | ---------------------------------------------------------------- | -------- |
| `gen`     | `switch`                                                         | required |
| `on`      | Value                                                            | required |
| `cases`   | mapping of names to Value or list of [SwitchRange](#switchrange) | required |
| `default` | Value                                                            | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

### SwitchRange

One range case of `switch`: the first case whose `min` is at or below `on` and whose `max`, when given, is above it.

| Key     | Type              | Default  |
| ------- | ----------------- | -------- |
| `min`   | integer or number | none     |
| `max`   | integer or number | none     |
| `value` | Value             | required |

## Example

`band` is small below 3, medium from 3 up to, but not including, 5, and large otherwise. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Switch: the value of the first case that matches.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/switch.yaml

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
      band:
        gen: switch
        on: {gen: var, name: quantity}
        cases:
          - {max: 3, value: small}
          - {min: 3, max: 5, value: medium}
        default: large

run: {until: 200}
```

```python
"""Switch: the value of the first case that matches, builder API.

The builder version of `switch.yaml`: the same task, written with `Template`, which
gives the payload's generators their place and so their random streams.

Run it with: python examples/generators/switch_builder.py
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
            "band": {
                "gen": "switch",
                "on": {"gen": "var", "name": "quantity"},
                "cases": [
                    {"max": 3, "value": "small"},
                    {"min": 3, "max": 5, "value": "medium"},
                ],
                "default": "large",
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
"""Switch: the value of the first case that matches, low-level API.

The low-level version of `switch.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. A var is drawn once for each order, from the
place `vars.quantity` of the task, and `switch` takes the first range that holds it:
from the lowest, `max` is not included.

Needs: nothing.
Run it with: python examples/generators/switch_low_level.py
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
        if count < 3:
            band = "small"
        elif count < 5:
            band = "medium"
        else:
            band = "large"
        return {"quantity": count, "band": band}

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'quantity': 4, 'band': 'medium'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'quantity': 4, 'band': 'medium'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'quantity': 4, 'band': 'medium'}}
```

## JSON Schema

The schema of `Switch`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`switch`: the value of the first case that matches `on`, evaluated only when\npicked. `cases` is a mapping from a value to its result, or a list of ranges;\n`default` is used when none matches (null when it is left out). In a blueprint,\na key written `yes`, `no`, `on` or `off` is that text, not a boolean.",
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
          "const": "switch",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "on": {
      "$ref": "#/$defs/Value"
    },
    "cases": {
      "anyOf": [
        {
          "additionalProperties": {
            "$ref": "#/$defs/Value"
          },
          "type": "object"
        },
        {
          "items": {
            "$ref": "#/$defs/SwitchRange"
          },
          "type": "array"
        }
      ]
    },
    "default": {
      "$ref": "#/$defs/Value",
      "default": null
    }
  },
  "required": [
    "gen",
    "on",
    "cases"
  ],
  "title": "Switch",
  "type": "object"
}
```
