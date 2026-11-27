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

It prints these records:

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
