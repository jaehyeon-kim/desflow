# sample

`sample`: one draw from a distribution family with its parameters, or from the live distribution of a service.

Use it for a duration or a size whose shape you know, such as exponential gaps, or one that follows the live distribution of a service.

## Keys

| Key        | Type                              | Default  |
| ---------- | --------------------------------- | -------- |
| `gen`      | `sample`                          | required |
| `dist`     | text                              | none     |
| `service`  | text                              | none     |
| `rate`     | Value                             | none     |
| `mean`     | Value                             | none     |
| `std`      | Value                             | none     |
| `low`      | Value                             | none     |
| `value`    | Value                             | none     |
| `high`     | Value                             | none     |
| `mode`     | Value                             | none     |
| `shape`    | Value                             | none     |
| `scale`    | Value                             | none     |
| `p`        | Value                             | none     |
| `values`   | list of integer or number or text | none     |
| `weights`  | list of number                    | none     |
| `edges`    | list of integer or number or text | none     |
| `counts`   | list of number                    | none     |
| `decimals` | integer                           | none     |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`wait_s` is a draw from an exponential distribution with a rate of 0.5, so its mean is 2 seconds, rounded to 2 decimals. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Sample: a draw from a distribution family.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/sample.yaml

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
      wait_s: {gen: sample, dist: exponential, rate: 0.5, decimals: 2}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'wait_s': 0.37}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'wait_s': 0.72}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'wait_s': 2.37}}
```

## JSON Schema

The schema of `Sample`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`sample`: one draw from a distribution family with its parameters, or from\nthe live distribution of a service.",
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
          "const": "sample",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "dist": {
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
    "service": {
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
    "rate": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "mean": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "std": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "low": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "value": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "high": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "mode": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "shape": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "scale": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "p": {
      "$ref": "#/$defs/Value",
      "default": null
    },
    "values": {
      "anyOf": [
        {
          "items": {
            "anyOf": [
              {
                "type": "integer"
              },
              {
                "type": "number"
              },
              {
                "type": "string"
              },
              {
                "$ref": "#/$defs/NumberText"
              },
              {
                "$ref": "#/$defs/Placeholder"
              }
            ]
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "weights": {
      "anyOf": [
        {
          "items": {
            "anyOf": [
              {
                "type": "number"
              },
              {
                "$ref": "#/$defs/NumberText"
              },
              {
                "$ref": "#/$defs/Placeholder"
              }
            ]
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "edges": {
      "anyOf": [
        {
          "items": {
            "anyOf": [
              {
                "type": "integer"
              },
              {
                "type": "number"
              },
              {
                "type": "string"
              },
              {
                "$ref": "#/$defs/NumberText"
              },
              {
                "$ref": "#/$defs/Placeholder"
              }
            ]
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "counts": {
      "anyOf": [
        {
          "items": {
            "anyOf": [
              {
                "type": "number"
              },
              {
                "$ref": "#/$defs/NumberText"
              },
              {
                "$ref": "#/$defs/Placeholder"
              }
            ]
          },
          "type": "array"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "decimals": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        },
        {
          "$ref": "#/$defs/NonNegativeIntegerText"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ],
      "default": null
    }
  },
  "required": [
    "gen"
  ],
  "title": "Sample",
  "type": "object"
}
```
