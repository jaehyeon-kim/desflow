# call

`call`: what a `!python` function returns, called as `function(context, **kwargs)` with the kwargs evaluated first.

Use it for a value that your own Python function computes.

## Keys

| Key        | Type                      | Default  |
| ---------- | ------------------------- | -------- |
| `gen`      | `call`                    | required |
| `function` | !python reference         | required |
| `kwargs`   | mapping of names to Value |          |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`total` is a price from 10 to 50 plus a tax of 10 per cent, added by `with_tax` in `call_helpers.py`. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Call: what a Python function returns.
# An order starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start. The function is in `call_helpers.py` in this folder.
#
# Run it with: desflow run examples/generators/call.yaml

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
      total:
        gen: call
        function: !python call_helpers.with_tax
        kwargs:
          price: {gen: integer, bounds: [10, 50]}

run: {until: 200}
```

```python
"""Call: what a Python function returns, builder API.

The builder version of `call.yaml`. The function is passed as it is, where a blueprint
names it with `!python`.

Run it with: python examples/generators/call_builder.py
"""

import logging
from datetime import datetime, timezone

from call_helpers import with_tax

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
            "total": {
                "gen": "call",
                "function": with_tax,
                "kwargs": {"price": {"gen": "integer", "bounds": [10, 50]}},
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
"""Call: what a Python function returns, low-level API.

The low-level version of `call.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. The function is the one a blueprint names with
`!python`. It gets no context, because the script has none and passes None.

Needs: nothing.
Run it with: python examples/generators/call_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from call_helpers import with_tax

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""
    sampler = Sampler(seed=42)
    price = sampler.generator("tasks.order.payload.total.kwargs.price")

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            total = with_tax(None, int(price.integers(10, 50 + 1)))
            env.publish_event(
                f"orders-{number}", {"total": total}, task="order", instance=number - 1
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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'total': 35.2}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'total': 55.0}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'total': 49.5}}
```

## JSON Schema

The schema of `Call`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`call`: what a `!python` function returns, called as `function(context,\n**kwargs)` with the kwargs evaluated first.",
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
          "const": "call",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "function": {
      "$ref": "#/$defs/PythonReference"
    },
    "kwargs": {
      "additionalProperties": {
        "$ref": "#/$defs/Value"
      },
      "type": "object"
    }
  },
  "required": [
    "gen",
    "function"
  ],
  "title": "Call",
  "type": "object"
}
```
