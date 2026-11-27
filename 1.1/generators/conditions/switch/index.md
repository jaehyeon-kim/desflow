# switch

`switch`: the value of the first case that matches `on`, evaluated only when picked. `cases` is a mapping from a value to its result, or a list of ranges; `default` is used when none matches (null when it is left out). In a blueprint, a key written `yes`, `no`, `on` or `off` is that text, not a boolean.

Use it for one value out of several, chosen by an exact match or by a range.

## Keys

| Key       | Type                                                             | Default  |
| --------- | ---------------------------------------------------------------- | -------- |
| `gen`     | `switch`                                                         | required |
| `on`      | Value                                                            | required |
| `cases`   | mapping of names to Value or list of [SwitchRange](#switchrange) | required |
| `default` | Value                                                            | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

### SwitchRange

One range case of `switch`: the first case whose `min` is at or below `on` and whose `max`, when given, is above it.

| Key     | Type              | Default  |
| ------- | ----------------- | -------- |
| `min`   | integer or number | none     |
| `max`   | integer or number | none     |
| `value` | Value             | required |

## Example

`band` is small below 3, medium from 3 up to, but not including, 5, and large otherwise. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Switch: the value of the first case that matches.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/switch.yaml

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
      band:
        gen: switch
        on: {gen: var, name: quantity}
        cases:
          - {max: 3, value: small}
          - {min: 3, max: 5, value: medium}
        default: large

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'quantity': 4, 'band': 'medium'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'quantity': 4, 'band': 'medium'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'quantity': 4, 'band': 'medium'}}
```

## JSON Schema

The schema of `Switch`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`switch`: the value of the first case that matches `on`, evaluated only when\npicked. `cases` is a mapping from a value to its result, or a list of ranges;\n`default` is used when none matches (null when it is left out). In a blueprint,\na key written `yes`, `no`, `on` or `off` is that text, not a boolean.",
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
          "const": "switch",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "on": {
      "$ref": "#/$defs/Value"
    },
    "cases": {
      "anyOf": [
        {
          "additionalProperties": {
            "$ref": "#/$defs/Value"
          },
          "type": "object"
        },
        {
          "items": {
            "$ref": "#/$defs/SwitchRange"
          },
          "type": "array"
        }
      ]
    },
    "default": {
      "$ref": "#/$defs/Value",
      "default": null
    }
  },
  "required": [
    "gen",
    "on",
    "cases"
  ],
  "title": "Switch",
  "type": "object"
}
```
