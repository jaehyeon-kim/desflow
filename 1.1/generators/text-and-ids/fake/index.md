# fake

`fake`: a value from the Faker library. `method` names any method of Faker's providers, such as `name`, `email`, `isbn13` or `credit_card_number`, and `args` and `kwargs` are passed to it. `locale` picks the language and country, `en_US` by default. It needs the `fake` extra.

Each `fake` generator has a Faker of its own, seeded from the run's seed and its place in the file, so the same seed gives the same values and adding a field changes no other. With no seed, the values change on every run. A date, a time or a decimal result is written as text or a float.

Use it for names, addresses and other realistic text, when invented values are enough.

## Keys

| Key      | Type                      | Default   |
| -------- | ------------------------- | --------- |
| `gen`    | `fake`                    | required  |
| `method` | text                      | required  |
| `args`   | list of Value             |           |
| `kwargs` | mapping of names to Value |           |
| `locale` | text                      | `"en_US"` |

A `Value` is a constant or a generator, as [What a Generator Is](https://jaehyeon.me/desflow/latest/generators/#what-a-generator-is) explains. Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, which [Generators](https://jaehyeon.me/desflow/latest/generators/#keys-every-kind-accepts) describes.

## Example

`customer` is a made-up person's name. An order starts every 60 seconds for 200 seconds, with the seed 42, so three orders start.

```yaml
# Fake: a made-up name from Faker.
# A task `order` starts every 60 seconds, and the run lasts 200 seconds, so three orders
# start.
#
# Run it with: desflow run examples/generators/fake.yaml

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
      customer: {gen: fake, method: name}

run: {until: 200}
```

It prints these records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'orders-1', 'task': 'order', 'instance': 0, 'value': {'customer': 'Amanda Rose'}}
[EVT] {'sim_ts': 120.0, 'timestamp': '...', 'key': 'orders-2', 'task': 'order', 'instance': 1, 'value': {'customer': 'Noah Boone'}}
[EVT] {'sim_ts': 180.0, 'timestamp': '...', 'key': 'orders-3', 'task': 'order', 'instance': 2, 'value': {'customer': 'James Walker'}}
```

## JSON Schema

The schema of `Fake`, from `desflow schema`.

```json
{
  "additionalProperties": false,
  "description": "`fake`: a value from the Faker library. `method` names any method of Faker's\nproviders, such as `name`, `email`, `isbn13` or `credit_card_number`, and `args`\nand `kwargs` are passed to it. `locale` picks the language and country, `en_US`\nby default. It needs the `fake` extra.\n\nEach `fake` generator has a Faker of its own, seeded from the run's seed and its\nplace in the file, so the same seed gives the same values and adding a field\nchanges no other. With no seed, the values change on every run. A date, a time\nor a decimal result is written as text or a float.",
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
          "const": "fake",
          "type": "string"
        },
        {
          "$ref": "#/$defs/Placeholder"
        }
      ]
    },
    "method": {
      "type": "string"
    },
    "args": {
      "items": {
        "$ref": "#/$defs/Value"
      },
      "type": "array"
    },
    "kwargs": {
      "additionalProperties": {
        "$ref": "#/$defs/Value"
      },
      "type": "object"
    },
    "locale": {
      "default": "en_US",
      "type": "string"
    }
  },
  "required": [
    "gen",
    "method"
  ],
  "title": "Fake",
  "type": "object"
}
```
