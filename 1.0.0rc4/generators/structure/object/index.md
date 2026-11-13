# object

`object`: the mapping `fields`, each evaluated as a plain mapping is. It lets a modifier such as `null_rate` or `omit_rate` apply to the whole mapping.

Use it for a nested record that is sometimes null or left out as a whole.

## Keys

| Key      | Type                      | Default  |
| -------- | ------------------------- | -------- |
| `gen`    | `object`                  | required |
| `fields` | mapping of names to Value | required |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`address` is a mapping of a city and a postcode. With `null_rate: 0.5`, about half of the orders have no address (`null`). An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Object: a mapping that a modifier can apply to as a whole.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/object.yaml

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
      address:
        gen: object
        null_rate: 0.5
        fields:
          city: {gen: one_of, choices: [Perth, Hobart]}
          postcode: {gen: integer, bounds: [1000, 9999]}

run: {until: 200}
```

```python
"""Object: a mapping that a modifier can apply to as a whole, builder API.

The builder version of `object.yaml`: the same task, written with `Template`, which
gives the payload's generators their place and so their random streams.

Run it with: python examples/generators/object_builder.py
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
            "address": {
                "gen": "object",
                "null_rate": 0.5,
                "fields": {
                    "city": {"gen": "one_of", "choices": ["Perth", "Hobart"]},
                    "postcode": {"gen": "integer", "bounds": [1000, 9999]},
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
"""Object: a mapping that a modifier can apply to as a whole, low-level API.

The low-level version of `object.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. `null_rate` draws from the place with `#null`
added to its name: the value is null when `random()` is below the rate. The `fields` are
drawn from the places `fields.<name>` below the generator.

Needs: nothing.
Run it with: python examples/generators/object_low_level.py
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
    missing = sampler.generator("tasks.order.payload.address#null")
    city = sampler.generator("tasks.order.payload.address.fields.city")
    postcode = sampler.generator("tasks.order.payload.address.fields.postcode")
    cities = ["Perth", "Hobart"]

    def make(number):
        """Draws the payload of order `number`."""
        if missing.random() < 0.5:
            return {"address": None}
        return {
            "address": {
                "city": cities[int(city.integers(len(cities)))],
                "postcode": int(postcode.integers(1000, 9999 + 1)),
            }
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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'address': None}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'address': None}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'address': {'city': 'Perth', 'postcode': 9495}}}
```

## JSON Schema

The schema of `Object`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`object`: the mapping `fields`, each evaluated as a plain mapping is. It lets\na modifier such as `null_rate` or `omit_rate` apply to the whole mapping.",
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
          "const": "object",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "fields": {
      "additionalProperties": {
        "$ref": "#/$defs/Value"
      },
      "type": "object"
    }
  },
  "required": [
    "gen",
    "fields"
  ],
  "title": "Object",
  "type": "object"
}
```
