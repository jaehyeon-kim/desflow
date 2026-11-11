# format

`format`: `template.format_map(...)` over `names`, then the vars of the task. A name in the template is a var, or an entry of `names`, which wins.

Use it for text built from parts, such as a label made of a region and a number.

## Keys

| Key        | Type                      | Default  |
| ---------- | ------------------------- | -------- |
| `gen`      | `format`                  | required |
| `template` | text                      | required |
| `names`    | mapping of names to Value |          |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`label` joins a region and a number, each from its own generator, with a dash. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Format: text built from a template and named parts.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/format.yaml

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
      label:
        gen: format
        template: "{region}-{number}"
        names:
          region: {gen: one_of, choices: [north, south]}
          number: {gen: integer, bounds: [10, 99]}

run: {until: 200}
```

```python
"""Format: text built from a template and named parts, builder API.

The builder version of `format.yaml`: the same task, written with `Template`, which
gives the payload's generators their place and so their random streams.

Run it with: python examples/generators/format_builder.py
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
            "label": {
                "gen": "format",
                "template": "{region}-{number}",
                "names": {
                    "region": {"gen": "one_of", "choices": ["north", "south"]},
                    "number": {"gen": "integer", "bounds": [10, 99]},
                },
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
"""Format: text built from a template and named parts, low-level API.

The low-level version of `format.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. `format` fills the template with the values of
its `names`, each drawn from the place `names.<name>` below the generator.

Needs: nothing.
Run it with: python examples/generators/format_low_level.py
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
    region = sampler.generator("tasks.order.payload.label.names.region")
    digits = sampler.generator("tasks.order.payload.label.names.number")
    regions = ["north", "south"]

    def make(number):
        """Draws the payload of order `number`."""
        name = regions[int(region.integers(len(regions)))]
        return {"label": f"{name}-{int(digits.integers(10, 99 + 1))}"}

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'label': 'south-89'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'label': 'north-59'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'label': 'south-36'}}
```

## JSON Schema

The schema of `Format`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`format`: `template.format_map(...)` over `names`, then the vars of the task.\nA name in the template is a var, or an entry of `names`, which wins.",
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
          "const": "format",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "template": {
      "type": "string"
    },
    "names": {
      "additionalProperties": {
        "$ref": "#/$defs/Value"
      },
      "type": "object"
    }
  },
  "required": [
    "gen",
    "template"
  ],
  "title": "Format",
  "type": "object"
}
```
