# entity

`entity`: the key of the entity the task runs for, such as `sensor-1`. It is given only inside a task, so an arrival's `attributes`, `vars` and `key` refuse it.

Use it for a payload field that names the entity the record belongs to.

## Keys

| Key   | Type     | Default  |
| ----- | -------- | -------- |
| `gen` | `entity` | required |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`order_key` is the key of the order, such as `orders-1`. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Entity: the key of the entity.
# An entity `orders-N` starts every 60 seconds, and the run lasts 200 seconds, so three
# orders start.
#
# Run it with: desflow run examples/generators/entity.yaml

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
      order_key: {gen: entity}

run: {until: 200}
```

```python
"""Entity: the key of the entity, builder API.

The builder version of `entity.yaml`.

Run it with: python examples/generators/entity_builder.py
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
    payload = Template({"order_key": {"gen": "entity"}}, name="tasks.order")

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
"""Entity: the key of the entity, low-level API.

The low-level version of `entity.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. The key of an order is
`orders-<number>`, and the payload holds it.

Needs: nothing.
Run it with: python examples/generators/entity_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            key = f"orders-{number}"
            env.publish_event(
                key, {"order_key": key}, task="order", instance=number - 1
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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'order_key': 'orders-1'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'order_key': 'orders-2'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'order_key': 'orders-3'}}
```

## JSON Schema

The schema of `Entity`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`entity`: the key of the entity the task runs for, such as `sensor-1`. It is\ngiven only inside a task, so an arrival's `attributes`, `vars` and `key` refuse it.",
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
          "const": "entity",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    }
  },
  "required": [
    "gen"
  ],
  "title": "Entity",
  "type": "object"
}
```
