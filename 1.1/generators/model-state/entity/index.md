# entity

`entity`: the key of the entity the task runs for, such as `sensor-1`. It is given only inside a task, so an arrival's `attributes`, `vars` and `key` refuse it.

Use it for a payload field that names the entity the record belongs to.

## Keys

| Key   | Type     | Default  |
| ----- | -------- | -------- |
| `gen` | `entity` | required |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`order_key` is the key of the order, such as `orders-1`. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Entity: the key of the entity.
# An entity `orders-N` starts every 60 seconds, and the run lasts 200 seconds, so three
# orders start.
#
# Run it with: desflow run examples/generators/entity.yaml

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
      order_key: {gen: entity}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'order_key': 'orders-1'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'order_key': 'orders-2'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'order_key': 'orders-3'}}
```

## JSON Schema

The schema of `Entity`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`entity`: the key of the entity the task runs for, such as `sensor-1`. It is\ngiven only inside a task, so an arrival's `attributes`, `vars` and `key` refuse it.",
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
          "const": "entity",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    }
  },
  "required": [
    "gen"
  ],
  "title": "Entity",
  "type": "object"
}
```
