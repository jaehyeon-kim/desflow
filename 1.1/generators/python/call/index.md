# call

`call`: what a `!python` function returns, called as `function(context, **kwargs)` with the kwargs evaluated first.

Use it for a value that your own Python function computes.

## Keys

| Key        | Type                      | Default  |
| ---------- | ------------------------- | -------- |
| `gen`      | `call`                    | required |
| `function` | !python reference         | required |
| `kwargs`   | mapping of names to Value |          |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`total` is a price from 10 to 50 plus a tax of 10 per cent, added by `with_tax` in `call_helpers.py`. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Call: what a Python function returns.
# An order starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start. The function is in `call_helpers.py` in this folder.
#
# Run it with: desflow run examples/generators/call.yaml

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
      total:
        gen: call
        function: !python call_helpers.with_tax
        kwargs:
          price: {gen: integer, bounds: [10, 50]}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'total': 41.8}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'total': 26.4}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'total': 53.9}}
```

## JSON Schema

The schema of `Call`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`call`: what a `!python` function returns, called as `function(context,\n**kwargs)` with the kwargs evaluated first.",
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
          "const": "call",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "function": {
      "$ref": "#/$defs/PythonReference"
    },
    "kwargs": {
      "additionalProperties": {
        "$ref": "#/$defs/Value"
      },
      "type": "object"
    }
  },
  "required": [
    "gen",
    "function"
  ],
  "title": "Call",
  "type": "object"
}
```
