# param

`param`: the live registry value at `path` (a full registry path, such as `Shop.variables.error_rate`) at the moment of the call, or the `field` key of a mapping value. A scenario step or an ingress update shows in the next call.

Use it for a live setting of the simulation, which a scenario step or an ingress update can change.

## Keys

| Key     | Type    | Default  |
| ------- | ------- | -------- |
| `gen`   | `param` | required |
| `path`  | text    | required |
| `field` | Value   | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`tax_rate` is the variable `tax_rate` of the simulation `Gen`, read when each order is published. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Param: the live value of a variable.
# An order starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/param.yaml

simulation:
  sim_id: Gen
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

variables: {tax_rate: 0.2}

arrivals:
  orders: {dist: constant, value: 60, spawn: order}

tasks:
  order:
    payload:
      tax_rate: {gen: param, path: Gen.variables.tax_rate}

run: {until: 200}
```

```python
"""Param: the live value of a variable, builder API.

The builder version of `param.yaml`.

Run it with: python examples/generators/param_builder.py
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
    app.add_variable("tax_rate", 0.2)
    app.add_arrival("orders", "constant", value=60, spawn="order")
    payload = Template(
        {"tax_rate": {"gen": "param", "path": "Gen.variables.tax_rate"}},
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
"""Param: the live value of a variable, low-level API.

The low-level version of `param.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. The variable is registered in the
environment's registry, and the script reads its value when each order is published, as
`param` does.

Needs: nothing.
Run it with: python examples/generators/param_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, SimParameter

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""
    env.registry.register_sim_parameter(
        SimParameter(sim_id="Gen", variables={"tax_rate": 0.2})
    )

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            rate = env.registry.get("Gen.variables.tax_rate").value
            env.publish_event(
                f"orders-{number}",
                {"tax_rate": rate},
                task="order",
                instance=number - 1,
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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'tax_rate': 0.2}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'tax_rate': 0.2}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'tax_rate': 0.2}}
```

## JSON Schema

The schema of `Param`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`param`: the live registry value at `path` (a full registry path, such as\n`Shop.variables.error_rate`) at the moment of the call, or the `field` key of a\nmapping value. A scenario step or an ingress update shows in the next call.",
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
          "const": "param",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "path": {
      "type": "string"
    },
    "field": {
      "$ref": "#/$defs/Value",
      "default": null
    }
  },
  "required": [
    "gen",
    "path"
  ],
  "title": "Param",
  "type": "object"
}
```
