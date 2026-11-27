# sequence

`sequence`: the next integer of a counter, `start` plus `step` times the count of calls so far, restarting after `loop_after` calls. With `format`, the text `format.format(n)`, such as `"sensor-{}"`. There is one counter for the run, shared by every entity, so ids stay unique.

Use it for an order number or a label that counts up.

## Keys

| Key          | Type       | Default  |
| ------------ | ---------- | -------- |
| `gen`        | `sequence` | required |
| `start`      | integer    | `0`      |
| `step`       | integer    | `1`      |
| `loop_after` | integer    | none     |
| `format`     | text       | none     |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`order_id` counts up from 1000 and is written as `ord-1000`, `ord-1001` and so on. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Sequence: the next number of a counter, as text.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/sequence.yaml

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
      order_id: {gen: sequence, start: 1000, format: "ord-{}"}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'order_id': 'ord-1000'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'order_id': 'ord-1001'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'order_id': 'ord-1002'}}
```

## JSON Schema

The schema of `Sequence`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`sequence`: the next integer of a counter, `start` plus `step` times the\ncount of calls so far, restarting after `loop_after` calls. With `format`, the\ntext `format.format(n)`, such as `\"sensor-{}\"`. There is one counter for the run,\nshared by every entity, so ids stay unique.",
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
          "const": "sequence",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "start": {
      "default": 0,
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "$ref": "#/$defs/IntegerText"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "step": {
      "default": 1,
      "anyOf": [
        {
          "type": "integer"
        },
        {
          "$ref": "#/$defs/IntegerText"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "loop_after": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        },
        {
          "$ref": "#/$defs/PositiveIntegerText"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ],
      "default": null
    },
    "format": {
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
  "title": "Sequence",
  "type": "object"
}
```
