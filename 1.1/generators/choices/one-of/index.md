# one_of

`one_of`: one of `choices`, each equally likely. Only the choice picked is evaluated.

Use it for one value out of a list, each equally likely, such as a colour.

## Keys

| Key       | Type          | Default  |
| --------- | ------------- | -------- |
| `gen`     | `one_of`      | required |
| `choices` | list of Value | required |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`colour` is red, green or blue, each with a chance of one in three. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# One Of: one choice out of a list, each equally likely.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/one_of.yaml

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
      colour: {gen: one_of, choices: [red, green, blue]}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'colour': 'blue'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'colour': 'red'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'colour': 'green'}}
```

## JSON Schema

The schema of `OneOf`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`one_of`: one of `choices`, each equally likely. Only the choice picked is\nevaluated.",
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
          "const": "one_of",
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
    }
  },
  "required": [
    "gen",
    "choices"
  ],
  "title": "OneOf",
  "type": "object"
}
```
