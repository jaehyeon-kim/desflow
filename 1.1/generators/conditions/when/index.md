# when

`when`: `then` when the `condition` (an expression) holds, else `else`. Only the branch taken is evaluated.

Use it for a value that changes with one condition, such as a discount for a large order.

## Keys

| Key         | Type   | Default  |
| ----------- | ------ | -------- |
| `gen`       | `when` | required |
| `condition` | text   | required |
| `then`      | Value  | required |
| `else`      | Value  | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`quantity` is a var, a whole number from 1 to 6. `size` is large when `quantity` is above 3, and small otherwise. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# When: one of two values, by a condition.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/when.yaml

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
    vars:
      quantity: {gen: integer, bounds: [1, 6]}
    payload:
      quantity: {gen: var, name: quantity}
      size: {gen: when, condition: "quantity > 3", then: large, else: small}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'quantity': 4, 'size': 'large'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'quantity': 4, 'size': 'large'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'quantity': 4, 'size': 'large'}}
```

## JSON Schema

The schema of `When`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`when`: `then` when the `condition` (an expression) holds, else `else`.\nOnly the branch taken is evaluated.",
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
          "const": "when",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "condition": {
      "type": "string"
    },
    "then": {
      "$ref": "#/$defs/Value"
    },
    "else": {
      "$ref": "#/$defs/Value",
      "default": null
    }
  },
  "required": [
    "gen",
    "condition",
    "then"
  ],
  "title": "When",
  "type": "object"
}
```
