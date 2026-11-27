# format_time

`format_time`: a time as text or a number. `time` is the text `{gen: now}` gives, or seconds of simulation time (default: now). `format` is a `strftime` pattern, `iso`, `iso_utc` (milliseconds and a `Z`), `epoch_s` or `epoch_ms` (integers). `tz` is an IANA name and defaults to the run's time zone.

Use it for a time in the form another system expects, such as epoch milliseconds or a local time zone.

## Keys

| Key      | Type          | Default  |
| -------- | ------------- | -------- |
| `gen`    | `format_time` | required |
| `time`   | Value         | none     |
| `format` | text          | `"iso"`  |
| `tz`     | text          | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`day` is the time of the order as year, month, day, hour and minute. `epoch_s` is the same time as seconds since 1 January 1970. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Format Time: the simulation time in a chosen format.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/format_time.yaml

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
      day: {gen: format_time, format: "%Y-%m-%d %H:%M"}
      epoch_s: {gen: format_time, format: epoch_s}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'day': '2026-01-01 00:01', 'epoch_s': 1767225660}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'day': '2026-01-01 00:02', 'epoch_s': 1767225720}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'day': '2026-01-01 00:03', 'epoch_s': 1767225780}}
```

## JSON Schema

The schema of `FormatTime`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`format_time`: a time as text or a number. `time` is the text `{gen: now}`\ngives, or seconds of simulation time (default: now). `format` is a `strftime`\npattern, `iso`, `iso_utc` (milliseconds and a `Z`), `epoch_s` or `epoch_ms`\n(integers). `tz` is an IANA name and defaults to the run's time zone.",
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
          "const": "format_time",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "time": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "format": {
      "default": "iso",
      "type": "string"
    },
    "tz": {
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
    "gen"
  ],
  "title": "FormatTime",
  "type": "object"
}
```
