# attr

`attr`: the entity's attribute `name`, or the part of it at `path`.

Use it for a value the entity has carried since it started, such as a size the arrival gave it.

## Keys

| Key    | Type   | Default  |
| ------ | ------ | -------- |
| `gen`  | `attr` | required |
| `name` | text   | required |
| `path` | text   | none     |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

Each order starts with a `size` attribute from its arrival, and the payload publishes it. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Attr: an attribute of the entity.
# An entity `orders-N` starts every 60 seconds with a `size`, and the run lasts 200
# seconds, so three orders start.
#
# Run it with: desflow run examples/generators/attr.yaml

simulation:
  sim_id: Gen
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  orders:
    dist: constant
    value: 60
    spawn: order
    attributes:
      size: {gen: integer, bounds: [1, 50]}

tasks:
  order:
    payload:
      size: {gen: attr, name: size}

run: {until: 200}
```

```python
"""Attr: an attribute of the entity, builder API.

The builder version of `attr.yaml`. The arrival's `attributes` take a typed generator
made with `generator`, which gives it the place `arrivals.orders.attributes.size`.

Run it with: python examples/generators/attr_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 200


def build() -> SimulationContext:
    """Builds the model: an order every 60 seconds, each with a size."""
    app = SimulationContext(
        sim_id="Gen",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival(
        "orders",
        "constant",
        value=60,
        spawn="order",
        attributes={"size": generator({"gen": "integer", "bounds": [1, 50]})},
    )
    payload = Template({"size": {"gen": "attr", "name": "size"}}, name="tasks.order")

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
"""Attr: an attribute of the entity, low-level API.

The low-level version of `attr.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. An entity is anything that holds attributes:
the script draws the `size` of each order when the order starts, from the arrival's
place, and the payload reads it back, as `attr` does.

Needs: nothing.
Run it with: python examples/generators/attr_low_level.py
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
    size = sampler.generator("arrivals.orders.attributes.size")

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            attributes = {"size": int(size.integers(1, 50 + 1))}
            env.publish_event(
                f"orders-{number}",
                {"size": attributes["size"]},
                task="order",
                instance=number - 1,
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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'size': 30}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'size': 25}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'size': 29}}
```

## JSON Schema

The schema of `Attr`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`attr`: the entity's attribute `name`, or the part of it at `path`.",
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
          "const": "attr",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "name": {
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
    }
  },
  "required": [
    "gen",
    "name"
  ],
  "title": "Attr",
  "type": "object"
}
```
