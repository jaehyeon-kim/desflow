# uuid

`uuid`: a version 4 UUID from the generator's random sequence, so it repeats with the seed, or a version 7 UUID whose 48-bit millisecond field comes from the logical time.

Use it for a unique identifier, such as an order id.

## Keys

| Key       | Type       | Default  |
| --------- | ---------- | -------- |
| `gen`     | `uuid`     | required |
| `version` | `4` or `7` | `4`      |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`order_id` is a version 4 UUID, and the same seed gives the same UUIDs. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# UUID: a version 4 UUID that repeats with the seed.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/uuid.yaml

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
      order_id: {gen: uuid}

run: {until: 200}
```

```python
"""UUID: a version 4 UUID that repeats with the seed, builder API.

The builder version of `uuid.yaml`: the same task, written with `Template`, which gives
the payload's generators their place and so their random streams.

Run it with: python examples/generators/uuid_builder.py
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
        {"order_id": {"gen": "uuid"}},
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
"""UUID: a version 4 UUID that repeats with the seed, low-level API.

The low-level version of `uuid.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. `uuid` makes a version 4 UUID from 16 bytes of
the random sequence.

Needs: nothing.
Run it with: python examples/generators/uuid_low_level.py
"""

import itertools
import logging
import uuid
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the order process on `env`."""
    sampler = Sampler(seed=42)
    order_id = sampler.generator("tasks.order.payload.order_id")

    def make(number):
        """Draws the payload of order `number`."""
        return {"order_id": str(uuid.UUID(bytes=order_id.bytes(16), version=4))}

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'order_id': 'e770f19e-f419-4749-9cf9-9551739f102d'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'order_id': 'a3b69289-469e-4be2-b8ec-41e32b1d0fed'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'order_id': '97215c05-1e3e-4ff7-b54a-b0380a902655'}}
```

## JSON Schema

The schema of `Uuid`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`uuid`: a version 4 UUID from the generator's random sequence, so it repeats\nwith the seed, or a version 7 UUID whose 48-bit millisecond field comes from the\nlogical time.",
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
          "const": "uuid",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "version": {
      "default": 4,
      "anyOf": [
        {
          "enum": [
            4,
            7
          ],
          "type": "integer"
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
  "title": "Uuid",
  "type": "object"
}
```
