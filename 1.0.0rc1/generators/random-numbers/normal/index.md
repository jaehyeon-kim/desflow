# normal

`normal`: a float from a normal distribution with `mean` and `std`.

Use it for a measurement that clusters around a mean, such as a weight.

## Keys

| Key        | Type     | Default  |
| ---------- | -------- | -------- |
| `gen`      | `normal` | required |
| `mean`     | Value    | required |
| `std`      | Value    | required |
| `decimals` | integer  | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`weight_g` has a mean of 500 and a standard deviation of 20, rounded to 1 decimal. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Normal: a number from a bell curve.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/normal.yaml

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
      weight_g: {gen: normal, mean: 500, std: 20, decimals: 1}

run: {until: 200}
```

```python
"""Normal: a number from a bell curve, builder API.

The builder version of `normal.yaml`: the same task, written with `Template`, which
gives the payload's generators their place and so their random streams.

Run it with: python examples/generators/normal_builder.py
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
        {"weight_g": {"gen": "normal", "mean": 500, "std": 20, "decimals": 1}},
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
"""Normal: a number from a bell curve, low-level API.

The low-level version of `normal.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. `normal` draws with `normal(mean, std)`,
rounded.

Needs: nothing.
Run it with: python examples/generators/normal_low_level.py
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
    weight = sampler.generator("tasks.order.payload.weight_g")

    def make(number):
        """Draws the payload of order `number`."""
        return {"weight_g": round(float(weight.normal(500, 20)), 1)}

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'weight_g': 492.6}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'weight_g': 498.4}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'weight_g': 502.8}}
```

## JSON Schema

The schema of `Normal`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`normal`: a float from a normal distribution with `mean` and `std`.",
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
          "const": "normal",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "mean": {
      "$ref": "#/$defs/Value"
    },
    "std": {
      "$ref": "#/$defs/Value"
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
          "$ref": "#/$defs/Placeholder"
        }
      ],
      "default": null
    }
  },
  "required": [
    "gen",
    "mean",
    "std"
  ],
  "title": "Normal",
  "type": "object"
}
```
