# param

`param`: the live registry value at `path` (a full registry path, such as `Shop.variables.error_rate`) at the moment of the call, or the `field` key of a mapping value. A scenario step or an ingress update shows in the next call.

Use it for a live setting of the simulation, which a scenario step or an ingress update can change.

## Keys

| Key     | Type    | Default  |
| ------- | ------- | -------- |
| `gen`   | `param` | required |
| `path`  | text    | required |
| `field` | Value   | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`tax_rate` is the variable `tax_rate` of the simulation `Gen`, read when each order is published. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Param: the live value of a variable.
# An order starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/param.yaml

simulation:
  sim_id: Gen
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

variables: {tax_rate: 0.2}

arrivals:
  orders: {dist: constant, value: 60, spawn: order}

tasks:
  order:
    payload:
      tax_rate: {gen: param, path: Gen.variables.tax_rate}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'tax_rate': 0.2}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'tax_rate': 0.2}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'tax_rate': 0.2}}
```

## JSON Schema

The schema of `Param`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`param`: the live registry value at `path` (a full registry path, such as\n`Shop.variables.error_rate`) at the moment of the call, or the `field` key of a\nmapping value. A scenario step or an ingress update shows in the next call.",
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
          "const": "param",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "path": {
      "type": "string"
    },
    "field": {
      "$ref": "#/$defs/Value",
      "default": null
    }
  },
  "required": [
    "gen",
    "path"
  ],
  "title": "Param",
  "type": "object"
}
```
