# repeat

`repeat`: a list of `times` values, each evaluated on its own.

Use it for the items of an order: a list in which each item is drawn on its own.

## Keys

| Key     | Type     | Default  |
| ------- | -------- | -------- |
| `gen`   | `repeat` | required |
| `times` | Value    | required |
| `value` | Value    | required |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`scores` is a list of 3 whole numbers from 1 to 5. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Repeat: a list of values, each drawn on its own.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/repeat.yaml

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
      scores:
        gen: repeat
        times: 3
        value: {gen: integer, bounds: [1, 5]}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'scores': [3, 2, 1]}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'scores': [1, 5, 5]}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'scores': [1, 1, 4]}}
```

## JSON Schema

The schema of `Repeat`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`repeat`: a list of `times` values, each evaluated on its own.",
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
          "const": "repeat",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "times": {
      "$ref": "#/$defs/Value"
    },
    "value": {
      "$ref": "#/$defs/Value"
    }
  },
  "required": [
    "gen",
    "times",
    "value"
  ],
  "title": "Repeat",
  "type": "object"
}
```
