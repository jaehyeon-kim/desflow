# uuid

`uuid`: a version 4 UUID from the generator's random sequence, so it repeats with the seed, or a version 7 UUID whose 48-bit millisecond field comes from the logical time.

Use it for a unique identifier, such as an order id.

## Keys

| Key       | Type       | Default  |
| --------- | ---------- | -------- |
| `gen`     | `uuid`     | required |
| `version` | `4` or `7` | `4`      |

Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`order_id` is a version 4 UUID, and the same seed gives the same UUIDs. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# UUID: a version 4 UUID that repeats with the seed.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/uuid.yaml

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
      order_id: {gen: uuid}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'order_id': 'e770f19e-f419-4749-9cf9-9551739f102d'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'order_id': 'a3b69289-469e-4be2-b8ec-41e32b1d0fed'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'order_id': '97215c05-1e3e-4ff7-b54a-b0380a902655'}}
```

## JSON Schema

The schema of `Uuid`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`uuid`: a version 4 UUID from the generator's random sequence, so it repeats\nwith the seed, or a version 7 UUID whose 48-bit millisecond field comes from the\nlogical time.",
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
          "const": "uuid",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "version": {
      "default": 4,
      "anyOf": [
        {
          "enum": [
            4,
            7
          ],
          "type": "integer"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    }
  },
  "required": [
    "gen"
  ],
  "title": "Uuid",
  "type": "object"
}
```
