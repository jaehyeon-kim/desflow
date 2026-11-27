# literal

`literal`: `value` as written, not evaluated, so a mapping that has a `gen` key can be a payload field.

Use it for a value that must be kept as written, even when it looks like a generator.

## Keys

| Key     | Type      | Default  |
| ------- | --------- | -------- |
| `gen`   | `literal` | required |
| `value` | None      | none     |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`template` is the mapping `{gen: not_evaluated}`, published as it is. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Literal: a value kept as written, not evaluated.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/literal.yaml

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
      template:
        gen: literal
        value: {gen: not_evaluated}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'template': {'gen': 'not_evaluated'}}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'template': {'gen': 'not_evaluated'}}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'template': {'gen': 'not_evaluated'}}}
```

## JSON Schema

The schema of `Literal_`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`literal`: `value` as written, not evaluated, so a mapping that has a `gen`\nkey can be a payload field.",
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
          "const": "literal",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "value": {
      "default": null
    }
  },
  "required": [
    "gen"
  ],
  "title": "Literal_",
  "type": "object"
}
```
