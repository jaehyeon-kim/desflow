# previous

`previous`: the last finished payload of the same task for the same stream, or the part of it at `path`. The stream is the arrival, so all the entities it starts share one, except for an arrival with `count` or `keys`, where each entity has its own. `default` is evaluated only when the stream has no finished payload yet, and without one the first reading is null.

Use it for a value from the last finished record of the same task, such as the previous reading.

## Keys

| Key       | Type       | Default  |
| --------- | ---------- | -------- |
| `gen`     | `previous` | required |
| `path`    | text       | none     |
| `default` | Value      | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`before` is the `reading` of the order before, and 0 for the first order. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Previous: a value from the last finished record of the same task.
# An order starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/previous.yaml

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
      reading: {gen: integer, bounds: [10, 20]}
      before: {gen: previous, path: reading, default: 0}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'reading': 14, 'before': 0}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'reading': 17, 'before': 14}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'reading': 10, 'before': 17}}
```

## JSON Schema

The schema of `Previous`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`previous`: the last finished payload of the same task for the same stream, or\nthe part of it at `path`. The stream is the arrival, so all the entities it starts\nshare one, except for an arrival with `count` or `keys`, where each entity has its\nown. `default` is evaluated only when the stream has no finished payload yet, and\nwithout one the first reading is null.",
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
          "const": "previous",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
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
    },
    "default": {
      "$ref": "#/$defs/Value",
      "default": null
    }
  },
  "required": [
    "gen"
  ],
  "title": "Previous",
  "type": "object"
}
```
