# object

`object`: the mapping `fields`, each evaluated as a plain mapping is. It lets a modifier such as `null_rate` or `omit_rate` apply to the whole mapping.

Use it for a nested record that is sometimes null or left out as a whole.

## Keys

| Key      | Type                      | Default  |
| -------- | ------------------------- | -------- |
| `gen`    | `object`                  | required |
| `fields` | mapping of names to Value | required |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`address` is a mapping of a city and a postcode. With `null_rate: 0.5`, about half of the orders have no address (`null`). An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Object: a mapping that a modifier can apply to as a whole.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/object.yaml

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
      address:
        gen: object
        null_rate: 0.4
        fields:
          city: {gen: one_of, choices: [Perth, Hobart]}
          postcode: {gen: integer, bounds: [1000, 9999]}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'address': None}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'address': {'city': 'Perth', 'postcode': 6403}}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'address': None}}
```

## JSON Schema

The schema of `Object`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`object`: the mapping `fields`, each evaluated as a plain mapping is. It lets\na modifier such as `null_rate` or `omit_rate` apply to the whole mapping.",
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
          "const": "object",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "fields": {
      "additionalProperties": {
        "$ref": "#/$defs/Value"
      },
      "type": "object"
    }
  },
  "required": [
    "gen",
    "fields"
  ],
  "title": "Object",
  "type": "object"
}
```
