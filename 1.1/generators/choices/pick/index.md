# pick

`pick`: a list of `count` different `choices`, drawn without replacement, in the order drawn. Only the choices picked are evaluated.

Use it for several different values out of a list, such as optional extras.

## Keys

| Key       | Type          | Default  |
| --------- | ------------- | -------- |
| `gen`     | `pick`        | required |
| `choices` | list of Value | required |
| `count`   | Value         | `1`      |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`extras` is a list of 2 different extras out of 4, in the order drawn. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Pick: several different choices out of a list.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/pick.yaml

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
      extras: {gen: pick, choices: [card, ribbon, box, tag], count: 2}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'extras': ['card', 'ribbon']}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'extras': ['tag', 'box']}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'extras': ['tag', 'box']}}
```

## JSON Schema

The schema of `Pick`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`pick`: a list of `count` different `choices`, drawn without replacement, in\nthe order drawn. Only the choices picked are evaluated.",
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
          "const": "pick",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "choices": {
      "items": {
        "$ref": "#/$defs/Value"
      },
      "minItems": 1,
      "type": "array"
    },
    "count": {
      "$ref": "#/$defs/Value",
      "default": 1
    }
  },
  "required": [
    "gen",
    "choices"
  ],
  "title": "Pick",
  "type": "object"
}
```
