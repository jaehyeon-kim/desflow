# weighted

`weighted`: one choice, by weight. `options` maps each value to its weight, or `choices` lists `{value, weight}` for values that are mappings or generators. Each weight is 0 or more and at least one is above 0; weights are divided by their sum. Only the value picked is evaluated. In a blueprint, an option key written `yes`, `no`, `on` or `off` is that text, not a boolean.

Use it for one value out of a list in which some values are more likely, such as a region.

## Keys

| Key       | Type                                      | Default  |
| --------- | ----------------------------------------- | -------- |
| `gen`     | `weighted`                                | required |
| `options` | mapping of names to Value                 | none     |
| `choices` | list of [WeightedChoice](#weightedchoice) | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

### WeightedChoice

One entry of `weighted` `choices`: a value and its weight.

| Key      | Type  | Default  |
| -------- | ----- | -------- |
| `value`  | Value | required |
| `weight` | Value | required |

## Example

`region` is north with weight 3, south with weight 1 or east with weight 2, so north has a chance of one in two. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Weighted: one choice by weight.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/weighted.yaml

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
      region:
        gen: weighted
        options: {north: 3, south: 1, east: 2}

run: {until: 200}
```

```python
"""Weighted: one choice by weight, builder API.

The builder version of `weighted.yaml`: the same task, written with `Template`, which
gives the payload's generators their place and so their random streams.

Run it with: python examples/generators/weighted_builder.py
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
        {"region": {"gen": "weighted", "options": {"north": 3, "south": 1, "east": 2}}},
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
"""Weighted: one choice by weight, low-level API.

The low-level version of `weighted.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. `weighted` draws a point in the total weight,
and takes the first option whose running total passes it.

Needs: nothing.
Run it with: python examples/generators/weighted_low_level.py
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
    region = sampler.generator("tasks.order.payload.region")
    options = {"north": 3, "south": 1, "east": 2}

    def make(number):
        """Draws the payload of order `number`."""
        point = region.random() * sum(options.values())
        running = 0.0
        for name, weight in options.items():
            running += weight
            if point < running:
                break
        return {"region": name}

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'region': 'north'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'region': 'north'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'region': 'south'}}
```

## JSON Schema

The schema of `Weighted`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`weighted`: one choice, by weight. `options` maps each value to its weight, or\n`choices` lists `{value, weight}` for values that are mappings or generators. Each\nweight is 0 or more and at least one is above 0; weights are divided by their\nsum. Only the value picked is evaluated. In a blueprint, an option key written\n`yes`, `no`, `on` or `off` is that text, not a boolean.",
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
          "const": "weighted",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "options": {
      "anyOf": [
        {
          "additionalProperties": {
            "$ref": "#/$defs/Value"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "choices": {
      "anyOf": [
        {
          "items": {
            "$ref": "#/$defs/WeightedChoice"
          },
          "minItems": 1,
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    }
  },
  "required": [
    "gen"
  ],
  "title": "Weighted",
  "type": "object"
}
```
