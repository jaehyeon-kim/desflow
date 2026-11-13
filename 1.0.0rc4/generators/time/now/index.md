# now

`now`: the logical time plus `offset` (seconds or a duration, signed), as the ISO text a record's `timestamp` uses. It reads the simulation clock, never the wall clock.

Use it for a time field of a record, with an offset when it is dated before or after the clock.

## Keys

| Key      | Type  | Default  |
| -------- | ----- | -------- |
| `gen`    | `now` | required |
| `offset` | Value | `0`      |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`placed_at` is the time of the order, and `due_at` is one hour later. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Now: the simulation time, with an offset.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/now.yaml

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
      placed_at: {gen: now}
      due_at: {gen: now, offset: 1 hour}

run: {until: 200}
```

```python
"""Now: the simulation time, with an offset, builder API.

The builder version of `now.yaml`: the same task, written with `Template`, which gives
the payload's generators their place and so their random streams.

Run it with: python examples/generators/now_builder.py
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
        {"placed_at": {"gen": "now"}, "due_at": {"gen": "now", "offset": "1 hour"}},
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
"""Now: the simulation time, with an offset, low-level API.

The low-level version of `now.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. `now` is the simulation time plus the
offset, as ISO text. The script reads `env.now`, never the wall clock.

Needs: nothing.
Run it with: python examples/generators/now_low_level.py
"""

import itertools
import logging
from datetime import datetime, timedelta, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the order process on `env`."""

    def make(number):
        """Draws the payload of order `number`."""

        def iso(offset=0):
            """The simulation time plus `offset` seconds, as ISO text."""
            moment = START + timedelta(seconds=env.now + offset)
            return moment.isoformat(timespec="milliseconds")

        return {"placed_at": iso(), "due_at": iso(3600)}

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'placed_at': '2026-01-01T00:01:00.000+00:00', 'due_at': '2026-01-01T01:01:00.000+00:00'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'placed_at': '2026-01-01T00:02:00.000+00:00', 'due_at': '2026-01-01T01:02:00.000+00:00'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'placed_at': '2026-01-01T00:03:00.000+00:00', 'due_at': '2026-01-01T01:03:00.000+00:00'}}
```

## JSON Schema

The schema of `Now`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`now`: the logical time plus `offset` (seconds or a duration, signed), as the\nISO text a record's `timestamp` uses. It reads the simulation clock, never the\nwall clock.",
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
          "const": "now",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "offset": {
      "$ref": "#/$defs/Value",
      "default": 0
    }
  },
  "required": [
    "gen"
  ],
  "title": "Now",
  "type": "object"
}
```
