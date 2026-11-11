# sequence

`sequence`: the next integer of a counter, `start` plus `step` times the count of calls so far, restarting after `loop_after` calls. With `format`, the text `format.format(n)`, such as `"sensor-{}"`. There is one counter for the run, shared by every entity, so ids stay unique.

Use it for an order number or a label that counts up.

## Keys

| Key          | Type       | Default  |
| ------------ | ---------- | -------- |
| `gen`        | `sequence` | required |
| `start`      | integer    | `0`      |
| `step`       | integer    | `1`      |
| `loop_after` | integer    | none     |
| `format`     | text       | none     |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`order_id` counts up from 1000 and is written as `ord-1000`, `ord-1001` and so on. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Sequence: the next number of a counter, as text.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/sequence.yaml

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
      order_id: {gen: sequence, start: 1000, format: "ord-{}"}

run: {until: 200}
```

```python
"""Sequence: the next number of a counter, as text, builder API.

The builder version of `sequence.yaml`: the same task, written with `Template`, which
gives the payload's generators their place and so their random streams.

Run it with: python examples/generators/sequence_builder.py
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
        {"order_id": {"gen": "sequence", "start": 1000, "format": "ord-{}"}},
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
"""Sequence: the next number of a counter, as text, low-level API.

The low-level version of `sequence.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. `sequence` counts calls: `start + step
* count`, written with the format.

Needs: nothing.
Run it with: python examples/generators/sequence_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the order process on `env`."""

    def make(number):
        """Draws the payload of order `number`."""
        return {"order_id": f"ord-{1000 + (number - 1)}"}

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'order_id': 'ord-1000'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'order_id': 'ord-1001'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'order_id': 'ord-1002'}}
```

## JSON Schema

The schema of `Sequence`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`sequence`: the next integer of a counter, `start` plus `step` times the\ncount of calls so far, restarting after `loop_after` calls. With `format`, the\ntext `format.format(n)`, such as `\"sensor-{}\"`. There is one counter for the run,\nshared by every entity, so ids stay unique.",
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
          "const": "sequence",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "start": {
      "default": 0,
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "step": {
      "default": 1,
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "loop_after": {
      "anyOf": [
        {
          "minimum": 1,
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
    },
    "format": {
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
    "gen"
  ],
  "title": "Sequence",
  "type": "object"
}
```
