# aggregate

`aggregate`: `mean`, `sum`, `min`, `max` or `count` over the last `last` rows of the task `task` that `where` keeps. `where` is an expression over the row's own fields, such as `lead_days == 3 and precipitation > 0`; without it every row counts. `count` counts rows and takes no `path`; the others read the number at `path`. A reading looks back through the rows the task keeps (its `history`) until `last` rows are kept, and takes the rows there are when `where` keeps fewer.

Use it for a mean, sum, minimum, maximum or count over earlier records of another task.

## Keys

| Key        | Type                                         | Default  |
| ---------- | -------------------------------------------- | -------- |
| `gen`      | `aggregate`                                  | required |
| `task`     | text                                         | required |
| `path`     | text                                         | none     |
| `last`     | integer                                      | required |
| `function` | `mean` or `sum` or `min` or `max` or `count` | required |
| `where`    | text                                         | none     |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

Three readings start at time 0. Each order publishes the mean of the last 3 levels. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Aggregate: a mean, sum, minimum, maximum or count over the finished records of another
# task. Three readings start at time 0, and an order starts every 60 seconds. The run
# lasts 200 seconds, so three orders start.
#
# Run it with: desflow run examples/generators/aggregate.yaml

simulation:
  sim_id: Gen
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  readings: {initial: 3, dist: constant, value: 3600, spawn: reading}
  orders: {dist: constant, value: 60, spawn: order}

tasks:
  reading:
    payload:
      level: {gen: integer, bounds: [10, 20]}
  order:
    payload:
      average: {gen: aggregate, task: reading, path: level, last: 3, function: mean}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 0.0, 'timestamp': '...', 'key': 'readings-1', 'task': 'reading', 'instance': 0, 'value': {'level': 15}}
[EVT] {'sim_ts': 0.0, 'timestamp': '...', 'key': 'readings-2', 'task': 'reading', 'instance': 1, 'value': {'level': 18}}
[EVT] {'sim_ts': 0.0, 'timestamp': '...', 'key': 'readings-3', 'task': 'reading', 'instance': 2, 'value': {'level': 13}}
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'average': 15.333333333333334}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'average': 15.333333333333334}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'average': 15.333333333333334}}
```

## JSON Schema

The schema of `Aggregate`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`aggregate`: `mean`, `sum`, `min`, `max` or `count` over the last `last` rows of\nthe task `task` that `where` keeps. `where` is an expression over the row's own\nfields, such as `lead_days == 3 and precipitation > 0`; without it every row counts.\n`count` counts rows and takes no `path`; the others read the number at `path`. A\nreading looks back through the rows the task keeps (its `history`) until `last` rows\nare kept, and takes the rows there are when `where` keeps fewer.",
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
          "const": "aggregate",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "task": {
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
    },
    "last": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "function": {
      "anyOf": [
        {
          "enum": [
            "mean",
            "sum",
            "min",
            "max",
            "count"
          ],
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "where": {
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
    "task",
    "last",
    "function"
  ],
  "title": "Aggregate",
  "type": "object"
}
```
