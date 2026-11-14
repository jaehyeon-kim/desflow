# upstream

`upstream`: the finished payload of the task that routed to this one with `next`, or the part of it at `path`. A start by an arrival has no upstream, and a task that `on_fail` or `on_interrupt_next` reaches is handed a failure or an interrupted record, so a blueprint refuses `upstream` there.

Use it for a value from the record of the task that routed to this one.

## Keys

| Key    | Type       | Default  |
| ------ | ---------- | -------- |
| `gen`  | `upstream` | required |
| `path` | text       | none     |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

The task `order` routes to the task `invoice`, which publishes the amount of the order. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Upstream: a value from the record of the task that routed here.
# An order starts every 60 seconds and routes to an invoice. The run lasts 200 seconds,
# so three orders start.
#
# Run it with: desflow run examples/generators/upstream.yaml

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
      amount: {gen: integer, bounds: [10, 20]}
    next: invoice
  invoice:
    payload:
      amount: {gen: upstream, path: amount}

run: {until: 200}
```

```python
"""Upstream: a value from the record of the task that routed here, builder API.

The builder version of `upstream.yaml`. The order routes to the invoice with `next`.

Run it with: python examples/generators/upstream_builder.py
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
    order_payload = Template(
        {"amount": {"gen": "integer", "bounds": [10, 20]}}, name="tasks.order"
    )
    invoice_payload = Template(
        {"amount": {"gen": "upstream", "path": "amount"}}, name="tasks.invoice"
    )

    @app.task(name="order", next="invoice")
    def order(task_id):
        """Gives one order."""
        return order_payload

    @app.task(name="invoice")
    def invoice(task_id):
        """Gives the invoice of one order."""
        return invoice_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Upstream: a value from the record of the task that routed here, low-level API.

The low-level version of `upstream.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. The order publishes its record, then the
invoice takes the amount from it, as `upstream` reads the record of the task that routed
to it.

Needs: nothing.
Run it with: python examples/generators/upstream_low_level.py
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
    amount = sampler.generator("tasks.order.payload.amount")

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            key, instance = f"orders-{number}", number - 1
            order = {"amount": int(amount.integers(10, 20 + 1))}
            env.publish_event(key, order, task="order", instance=instance)
            env.publish_event(
                key, {"amount": order["amount"]}, task="invoice", instance=instance
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
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'amount': 18}}
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'invoice', 'instance': 0, 'value': {'amount': 18}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'amount': 15}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'invoice', 'instance': 1, 'value': {'amount': 15}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'amount': 12}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'invoice', 'instance': 2, 'value': {'amount': 12}}
```

## JSON Schema

The schema of `Upstream`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`upstream`: the finished payload of the task that routed to this one with\n`next`, or the part of it at `path`. A start by an arrival has no upstream, and a\ntask that `on_fail` or `on_interrupt_next` reaches is handed a failure or an\ninterrupted record, so a blueprint refuses `upstream` there.",
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
          "const": "upstream",
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
    }
  },
  "required": [
    "gen"
  ],
  "title": "Upstream",
  "type": "object"
}
```
