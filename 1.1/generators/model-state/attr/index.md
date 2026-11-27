# attr

`attr`: the entity's attribute `name`, or the part of it at `path`.

Use it for a value the entity has carried since it started, such as a size the arrival gave it.

## Keys

| Key    | Type   | Default  |
| ------ | ------ | -------- |
| `gen`  | `attr` | required |
| `name` | text   | required |
| `path` | text   | none     |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

Each order starts with a `size` attribute from its arrival, and the payload publishes it. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Attr: an attribute of the entity.
# An entity `orders-N` starts every 60 seconds with a `size`, and the run lasts 200
# seconds, so three orders start.
#
# Run it with: desflow run examples/generators/attr.yaml

simulation:
  sim_id: Gen
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  orders:
    dist: constant
    value: 60
    spawn: order
    attributes:
      size: {gen: integer, bounds: [1, 50]}

tasks:
  order:
    payload:
      size: {gen: attr, name: size}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'size': 7}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'size': 21}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'size': 29}}
```

## JSON Schema

The schema of `Attr`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`attr`: the entity's attribute `name`, or the part of it at `path`.",
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
          "const": "attr",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "name": {
      "type": "string"
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
    "gen",
    "name"
  ],
  "title": "Attr",
  "type": "object"
}
```
