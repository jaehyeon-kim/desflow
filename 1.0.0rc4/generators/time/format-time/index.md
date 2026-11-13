# format_time

`format_time`: a time as text or a number. `time` is the text `{gen: now}` gives, or seconds of simulation time (default: now). `format` is a `strftime` pattern, `iso`, `iso_utc` (milliseconds and a `Z`), `epoch_s` or `epoch_ms` (integers). `tz` is an IANA name and defaults to the run's time zone.

Use it for a time in the form another system expects, such as epoch milliseconds or a local time zone.

## Keys

| Key      | Type          | Default  |
| -------- | ------------- | -------- |
| `gen`    | `format_time` | required |
| `time`   | Value         | none     |
| `format` | text          | `"iso"`  |
| `tz`     | text          | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`day` is the time of the order as year, month, day, hour and minute. `epoch_s` is the same time as seconds since 1 January 1970. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Format Time: the simulation time in a chosen format.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/format_time.yaml

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
      day: {gen: format_time, format: "%Y-%m-%d %H:%M"}
      epoch_s: {gen: format_time, format: epoch_s}

run: {until: 200}
```

```python
"""Format Time: the simulation time in a chosen format, builder API.

The builder version of `format_time.yaml`: the same task, written with `Template`, which
gives the payload's generators their place and so their random streams.

Run it with: python examples/generators/format_time_builder.py
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
            "day": {"gen": "format_time", "format": "%Y-%m-%d %H:%M"},
            "epoch_s": {"gen": "format_time", "format": "epoch_s"},
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
"""Format Time: the simulation time in a chosen format, low-level API.

The low-level version of `format_time.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`.
`format_time` writes the simulation time with a `strftime` pattern, or as whole seconds
since 1 January 1970.

Needs: nothing.
Run it with: python examples/generators/format_time_low_level.py
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
        moment = START + timedelta(seconds=env.now)
        return {
            "day": moment.strftime("%Y-%m-%d %H:%M"),
            "epoch_s": int(moment.timestamp()),
        }

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'day': '2026-01-01 00:01', 'epoch_s': 1767225660}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'day': '2026-01-01 00:02', 'epoch_s': 1767225720}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'day': '2026-01-01 00:03', 'epoch_s': 1767225780}}
```

## JSON Schema

The schema of `FormatTime`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`format_time`: a time as text or a number. `time` is the text `{gen: now}`\ngives, or seconds of simulation time (default: now). `format` is a `strftime`\npattern, `iso`, `iso_utc` (milliseconds and a `Z`), `epoch_s` or `epoch_ms`\n(integers). `tz` is an IANA name and defaults to the run's time zone.",
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
          "const": "format_time",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "time": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "format": {
      "default": "iso",
      "type": "string"
    },
    "tz": {
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
  "title": "FormatTime",
  "type": "object"
}
```
