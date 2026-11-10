# previous

`previous`: the last finished payload of the same task for the same stream, or the part of it at `path`. The stream is the arrival, so all the entities it starts share one, except for an arrival with `count` or `keys`, where each entity has its own. `default` is evaluated only when the stream has no finished payload yet, and without one the first reading is null.

Use it for a value from the last finished record of the same task, such as the previous reading.

## Keys

| Key       | Type       | Default  |
| --------- | ---------- | -------- |
| `gen`     | `previous` | required |
| `path`    | text       | none     |
| `default` | Value      | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`before` is the `reading` of the order before, and 0 for the first order. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Previous: a value from the last finished record of the same task.
# An order starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/previous.yaml

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
      reading: {gen: integer, bounds: [10, 20]}
      before: {gen: previous, path: reading, default: 0}

run: {until: 200}
```

```python
"""Previous: a value from the last finished record of the same task, builder API.

The builder version of `previous.yaml`. The task keeps its last payload because its
template holds a `previous`.

Run it with: python examples/generators/previous_builder.py
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
    payload = Template(
        {
            "reading": {"gen": "integer", "bounds": [10, 20]},
            "before": {"gen": "previous", "path": "reading", "default": 0},
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
"""Previous: a value from the last finished record of the same task, low-level API.

The low-level version of `previous.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. The script keeps the last reading in a
variable, which is what `previous` reads, and uses the default 0 until there is one.

Needs: nothing.
Run it with: python examples/generators/previous_low_level.py
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
    reading = sampler.generator("tasks.order.payload.reading")

    def orders():
        before = 0  # the default, until there is a finished record
        for number in itertools.count(1):
            yield env.timeout(60)
            payload = {"reading": int(reading.integers(10, 20 + 1)), "before": before}
            before = payload["reading"]
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=number - 1
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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'reading': 17, 'before': 0}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'reading': 16, 'before': 17}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'reading': 18, 'before': 16}}
```

## JSON Schema

The schema of `Previous`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`previous`: the last finished payload of the same task for the same stream, or\nthe part of it at `path`. The stream is the arrival, so all the entities it starts\nshare one, except for an arrival with `count` or `keys`, where each entity has its\nown. `default` is evaluated only when the stream has no finished payload yet, and\nwithout one the first reading is null.",
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
          "const": "previous",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
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
    },
    "default": {
      "$ref": "#/$defs/Value",
      "default": null
    }
  },
  "required": [
    "gen"
  ],
  "title": "Previous",
  "type": "object"
}
```
