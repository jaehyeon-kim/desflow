# now

`now`: the logical time plus `offset` (seconds or a duration, signed), as the ISO text a record's `timestamp` uses. It reads the simulation clock, never the wall clock.

Use it for a time field of a record, with an offset when it is dated before or after the clock.

## Keys

| Key      | Type  | Default  |
| -------- | ----- | -------- |
| `gen`    | `now` | required |
| `offset` | Value | `0`      |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`placed_at` is the time of the order, and `due_at` is one hour later. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Now: the simulation time, with an offset.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/now.yaml

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
      placed_at: {gen: now}
      due_at: {gen: now, offset: 1 hour}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'placed_at': '2026-01-01T00:01:00.000+00:00', 'due_at': '2026-01-01T01:01:00.000+00:00'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'placed_at': '2026-01-01T00:02:00.000+00:00', 'due_at': '2026-01-01T01:02:00.000+00:00'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'placed_at': '2026-01-01T00:03:00.000+00:00', 'due_at': '2026-01-01T01:03:00.000+00:00'}}
```

## JSON Schema

The schema of `Now`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`now`: the logical time plus `offset` (seconds or a duration, signed), as the\nISO text a record's `timestamp` uses. It reads the simulation clock, never the\nwall clock.",
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
          "const": "now",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "offset": {
      "$ref": "#/$defs/Value",
      "default": 0
    }
  },
  "required": [
    "gen"
  ],
  "title": "Now",
  "type": "object"
}
```
