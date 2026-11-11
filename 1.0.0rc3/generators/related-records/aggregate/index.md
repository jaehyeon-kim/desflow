# aggregate

`aggregate`: `mean`, `sum`, `min`, `max` or `count` over the last `last` rows of the task `task` that `where` keeps. `where` is an expression over the row's own fields, such as `lead_days == 3 and precipitation > 0`; without it every row counts. `count` counts rows and takes no `path`; the others read the number at `path`. A reading looks back through the rows the task keeps (its `history`) until `last` rows are kept, and takes the rows there are when `where` keeps fewer.

Use it for a mean, sum, minimum, maximum or count over earlier records of another task.

## Keys

| Key        | Type                                         | Default  |
| ---------- | -------------------------------------------- | -------- |
| `gen`      | `aggregate`                                  | required |
| `task`     | text                                         | required |
| `path`     | text                                         | none     |
| `last`     | integer                                      | required |
| `function` | `mean` or `sum` or `min` or `max` or `count` | required |
| `where`    | text                                         | none     |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

Three readings start at time 0. Each order publishes the mean of the last 3 levels. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Aggregate: a mean, sum, minimum, maximum or count over the finished records of another
# task. Three readings start at time 0, and an order starts every 60 seconds. The run
# lasts 200 seconds, so three orders start.
#
# Run it with: desflow run examples/generators/aggregate.yaml

simulation:
  sim_id: Gen
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  readings: {initial: 3, dist: constant, value: 3600, spawn: reading}
  orders: {dist: constant, value: 60, spawn: order}

tasks:
  reading:
    payload:
      level: {gen: integer, bounds: [10, 20]}
  order:
    payload:
      average: {gen: aggregate, task: reading, path: level, last: 3, function: mean}

run: {until: 200}
```

```python
"""Aggregate: a mean, sum, minimum, maximum or count over the finished records of another
task, builder API.

The builder version of `aggregate.yaml`. A task that something aggregates keeps its
finished rows, so the reading task sets `history`.

Run it with: python examples/generators/aggregate_builder.py
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
    app.add_arrival("readings", "constant", value=3600, spawn="reading", initial=3)
    app.add_arrival("orders", "constant", value=60, spawn="order")
    reading_payload = Template(
        {"level": {"gen": "integer", "bounds": [10, 20]}}, name="tasks.reading"
    )
    order_payload = Template(
        {
            "average": {
                "gen": "aggregate",
                "task": "reading",
                "path": "level",
                "last": 3,
                "function": "mean",
            }
        },
        name="tasks.order",
    )

    @app.task(name="reading", history=1_000_000)
    def reading(task_id):
        """Gives one reading."""
        return reading_payload

    @app.task(name="order")
    def order(task_id):
        """Gives one order."""
        return order_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Aggregate: a mean, sum, minimum, maximum or count over the finished records of another
task, low-level API.

The low-level version of `aggregate.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler.generator(<place>)`, the random sequence of the blueprint's
generator at that place, so the draws are the blueprint's. The script keeps the level of
each reading it has published, and an order takes the mean of the last 3, as `aggregate`
does with `function: mean`.

Needs: nothing.
Run it with: python examples/generators/aggregate_low_level.py
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
    level = sampler.generator("tasks.reading.payload.level")
    levels: list = []  # the level of each reading so far, oldest first

    def reading_arrivals():
        yield env.timeout(0)
        for number in range(1, 4):
            levels.append(int(level.integers(10, 20 + 1)))
            env.publish_event(
                f"readings-{number}",
                {"level": levels[-1]},
                task="reading",
                instance=number - 1,
            )

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            last = levels[-3:]
            env.publish_event(
                f"orders-{number}",
                {"average": sum(last) / len(last)},
                task="order",
                instance=number - 1,
            )

    env.process(reading_arrivals())
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
[EVT] {'sim_ts': 0.0, 'timestamp': '...', 'key': 'readings-1', 'task': 'reading', 'instance': 0, 'value': {'level': 18}}
[EVT] {'sim_ts': 0.0, 'timestamp': '...', 'key': 'readings-2', 'task': 'reading', 'instance': 1, 'value': {'level': 11}}
[EVT] {'sim_ts': 0.0, 'timestamp': '...', 'key': 'readings-3', 'task': 'reading', 'instance': 2, 'value': {'level': 16}}
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'average': 15.0}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'average': 15.0}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'average': 15.0}}
```

## JSON Schema

The schema of `Aggregate`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`aggregate`: `mean`, `sum`, `min`, `max` or `count` over the last `last` rows of\nthe task `task` that `where` keeps. `where` is an expression over the row's own\nfields, such as `lead_days == 3 and precipitation > 0`; without it every row counts.\n`count` counts rows and takes no `path`; the others read the number at `path`. A\nreading looks back through the rows the task keeps (its `history`) until `last` rows\nare kept, and takes the rows there are when `where` keeps fewer.",
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
          "const": "aggregate",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "task": {
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
    },
    "last": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "function": {
      "anyOf": [
        {
          "enum": [
            "mean",
            "sum",
            "min",
            "max",
            "count"
          ],
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "where": {
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
    "task",
    "last",
    "function"
  ],
  "title": "Aggregate",
  "type": "object"
}
```
