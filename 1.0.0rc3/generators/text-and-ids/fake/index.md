# fake

`fake`: a value from the Faker library. `method` names any method of Faker's providers, such as `name`, `email`, `isbn13` or `credit_card_number`, and `args` and `kwargs` are passed to it. `locale` picks the language and country, `en_US` by default. It needs the `fake` extra.

Each `fake` generator has a Faker of its own, seeded from the run's seed and its place in the file, so the same seed gives the same values and adding a field changes no other. With no seed, the values change on every run. A date, a time or a decimal result is written as text or a float.

Use it for names, addresses and other realistic text, when invented values are enough.

## Keys

| Key      | Type                      | Default   |
| -------- | ------------------------- | --------- |
| `gen`    | `fake`                    | required  |
| `method` | text                      | required  |
| `args`   | list of Value             |           |
| `kwargs` | mapping of names to Value |           |
| `locale` | text                      | `"en_US"` |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`customer` is a made-up person's name. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Fake: a made-up name from Faker.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/fake.yaml

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
      customer: {gen: fake, method: name}

run: {until: 200}
```

```python
"""Fake: a made-up name from Faker, builder API.

The builder version of `fake.yaml`: the same task, written with `Template`, which gives
the payload's generators their place and so their random streams.

Run it with: python examples/generators/fake_builder.py
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
        {"customer": {"gen": "fake", "method": "name"}},
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
"""Fake: a made-up name from Faker, low-level API.

The low-level version of `fake.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. `fake` calls a Faker that is seeded from the
random sequence of the place with `#fake` added to its name, so adding another `fake`
changes no other.

Needs: the fake extra.
Run it with: python examples/generators/fake_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from faker import Faker

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the order process on `env`."""
    sampler = Sampler(seed=42)
    customer = sampler.generator("tasks.order.payload.customer#fake")
    faker = Faker("en_US")
    faker.seed_instance(int(customer.integers(2**32)))

    def make(number):
        """Draws the payload of order `number`."""
        return {"customer": faker.name()}

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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'customer': 'Sara Mitchell'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'customer': 'Julie Bryant'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'customer': 'Matthew Haas'}}
```

## JSON Schema

The schema of `Fake`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`fake`: a value from the Faker library. `method` names any method of Faker's\nproviders, such as `name`, `email`, `isbn13` or `credit_card_number`, and `args`\nand `kwargs` are passed to it. `locale` picks the language and country, `en_US`\nby default. It needs the `fake` extra.\n\nEach `fake` generator has a Faker of its own, seeded from the run's seed and its\nplace in the file, so the same seed gives the same values and adding a field\nchanges no other. With no seed, the values change on every run. A date, a time\nor a decimal result is written as text or a float.",
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
          "const": "fake",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "method": {
      "type": "string"
    },
    "args": {
      "items": {
        "$ref": "#/$defs/Value"
      },
      "type": "array"
    },
    "kwargs": {
      "additionalProperties": {
        "$ref": "#/$defs/Value"
      },
      "type": "object"
    },
    "locale": {
      "default": "en_US",
      "type": "string"
    }
  },
  "required": [
    "gen",
    "method"
  ],
  "title": "Fake",
  "type": "object"
}
```
