# integer

`integer`: a whole number between `bounds[0]` and `bounds[1]`, both included.

Use it for a count or a quantity in which every whole number in the range is as likely as any other.

## Keys

| Key      | Type          | Default  |
| -------- | ------------- | -------- |
| `gen`    | `integer`     | required |
| `bounds` | list of Value | required |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`quantity` is a whole number from 1 to 50, and 1 and 50 are both possible. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Integer: a whole number between two bounds.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/integer.yaml

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
      quantity: {gen: integer, bounds: [1, 50]}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'quantity': 25}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'quantity': 43}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'quantity': 26}}
```

## JSON Schema

The schema of `Integer`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`integer`: a whole number between `bounds[0]` and `bounds[1]`, both included.",
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
          "const": "integer",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "bounds": {
      "items": {
        "$ref": "#/$defs/Value"
      },
      "maxItems": 2,
      "minItems": 2,
      "type": "array"
    }
  },
  "required": [
    "gen",
    "bounds"
  ],
  "title": "Integer",
  "type": "object"
}
```
