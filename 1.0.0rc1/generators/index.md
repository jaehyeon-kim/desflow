# Generators

A generator draws a value each time a blueprint makes a record. This section has one page for each kind, with its keys, an example in YAML, Builder and Low-level, the records the example prints, and its JSON Schema.

## What a Generator Is

A place that takes a value, such as a field of a task's `payload` or of an arrival's `attributes`, takes a constant or a generator. A generator is a mapping with a `gen` key that names its kind, such as `{gen: integer, bounds: [1, 6]}`. The tables of this section call such a place `Value`. [YAML Keys](https://jaehyeon.me/desflow/latest/reference/yaml-keys/index.md) calls the generator part of such a place `Generator`: a key that takes a generator, an integer or text is written `Generator or integer or text`.

Each generator draws from a random sequence of its own, named for the place it is written at. The same seed gives the same values. Adding a field to a mapping moves no other draw. Inserting an item into a list moves the draws of the items after it. The keys of every other part of a blueprint are on [YAML Keys](https://jaehyeon.me/desflow/latest/reference/yaml-keys/index.md).

## Choose a Kind

The sidebar groups the kinds by the sort of value they give:

- **Random Numbers:** Numbers and flags drawn from a range or a distribution.
- **Choices:** A value picked from a list you write.
- **Text and IDs:** Names, identifiers and text.
- **Time:** The simulation clock as text or numbers.
- **Maths:** Arithmetic on other values.
- **Conditions:** A value that depends on a condition or a case, or that is kept as written.
- **Structure:** Mappings and lists built from other generators.
- **Model State:** Values the model already holds: the entity, the task's vars and the registry.
- **Related Records:** Values from the records other tasks or earlier orders published, and from files.
- **Python:** A value from your own Python function, the one kind that runs `!python`.

| Group           | Kind                                                                                            | Use it for                                                                                                                   |
| --------------- | ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Random Numbers  | [`integer`](https://jaehyeon.me/desflow/latest/generators/random-numbers/integer/index.md)      | A count or a quantity in which every whole number in the range is as likely as any other.                                    |
| Random Numbers  | [`uniform`](https://jaehyeon.me/desflow/latest/generators/random-numbers/uniform/index.md)      | A price or a size in which every value in the range is as likely as any other.                                               |
| Random Numbers  | [`normal`](https://jaehyeon.me/desflow/latest/generators/random-numbers/normal/index.md)        | A measurement that clusters around a mean, such as a weight.                                                                 |
| Random Numbers  | [`sample`](https://jaehyeon.me/desflow/latest/generators/random-numbers/sample/index.md)        | A duration or a size whose shape you know, such as exponential gaps, or one that follows the live distribution of a service. |
| Random Numbers  | [`boolean`](https://jaehyeon.me/desflow/latest/generators/random-numbers/boolean/index.md)      | A flag that is true for a share of the orders, such as a gift.                                                               |
| Choices         | [`one_of`](https://jaehyeon.me/desflow/latest/generators/choices/one-of/index.md)               | One value out of a list, each equally likely, such as a colour.                                                              |
| Choices         | [`weighted`](https://jaehyeon.me/desflow/latest/generators/choices/weighted/index.md)           | One value out of a list in which some values are more likely, such as a region.                                              |
| Choices         | [`pick`](https://jaehyeon.me/desflow/latest/generators/choices/pick/index.md)                   | Several different values out of a list, such as optional extras.                                                             |
| Text and IDs    | [`fake`](https://jaehyeon.me/desflow/latest/generators/text-and-ids/fake/index.md)              | Names, addresses and other realistic text, when invented values are enough.                                                  |
| Text and IDs    | [`uuid`](https://jaehyeon.me/desflow/latest/generators/text-and-ids/uuid/index.md)              | A unique identifier, such as an order id.                                                                                    |
| Text and IDs    | [`sequence`](https://jaehyeon.me/desflow/latest/generators/text-and-ids/sequence/index.md)      | An order number or a label that counts up.                                                                                   |
| Text and IDs    | [`format`](https://jaehyeon.me/desflow/latest/generators/text-and-ids/format/index.md)          | Text built from parts, such as a label made of a region and a number.                                                        |
| Text and IDs    | [`lower`](https://jaehyeon.me/desflow/latest/generators/text-and-ids/lower/index.md)            | A code or a label that must be in one standard case, whatever case it was drawn in.                                          |
| Text and IDs    | [`upper`](https://jaehyeon.me/desflow/latest/generators/text-and-ids/upper/index.md)            | A code or a label that must be in one standard case, whatever case it was drawn in.                                          |
| Time            | [`now`](https://jaehyeon.me/desflow/latest/generators/time/now/index.md)                        | A time field of a record, with an offset when it is dated before or after the clock.                                         |
| Time            | [`format_time`](https://jaehyeon.me/desflow/latest/generators/time/format-time/index.md)        | A time in the form another system expects, such as epoch milliseconds or a local time zone.                                  |
| Maths           | [`add`](https://jaehyeon.me/desflow/latest/generators/maths/add/index.md)                       | A total, or a time that falls some seconds after another time.                                                               |
| Maths           | [`subtract`](https://jaehyeon.me/desflow/latest/generators/maths/subtract/index.md)             | A difference, or a time that falls some seconds before another time.                                                         |
| Maths           | [`multiply`](https://jaehyeon.me/desflow/latest/generators/maths/multiply/index.md)             | A cost, such as a quantity times a price.                                                                                    |
| Maths           | [`divide`](https://jaehyeon.me/desflow/latest/generators/maths/divide/index.md)                 | A ratio, such as a total divided by a count.                                                                                 |
| Maths           | [`min`](https://jaehyeon.me/desflow/latest/generators/maths/min/index.md)                       | A cap: the smaller of a drawn value and a limit.                                                                             |
| Maths           | [`max`](https://jaehyeon.me/desflow/latest/generators/maths/max/index.md)                       | A floor: the larger of a drawn value and a minimum.                                                                          |
| Conditions      | [`when`](https://jaehyeon.me/desflow/latest/generators/conditions/when/index.md)                | A value that changes with one condition, such as a discount for a large order.                                               |
| Conditions      | [`switch`](https://jaehyeon.me/desflow/latest/generators/conditions/switch/index.md)            | One value out of several, chosen by an exact match or by a range.                                                            |
| Conditions      | [`expr`](https://jaehyeon.me/desflow/latest/generators/conditions/expr/index.md)                | A value computed from vars and numbers by an expression.                                                                     |
| Conditions      | [`literal`](https://jaehyeon.me/desflow/latest/generators/conditions/literal/index.md)          | A value that must be kept as written, even when it looks like a generator.                                                   |
| Structure       | [`object`](https://jaehyeon.me/desflow/latest/generators/structure/object/index.md)             | A nested record that is sometimes null or left out as a whole.                                                               |
| Structure       | [`repeat`](https://jaehyeon.me/desflow/latest/generators/structure/repeat/index.md)             | The items of an order: a list in which each item is drawn on its own.                                                        |
| Model State     | [`attr`](https://jaehyeon.me/desflow/latest/generators/model-state/attr/index.md)               | A value the entity has carried since it started, such as a size the arrival gave it.                                         |
| Model State     | [`var`](https://jaehyeon.me/desflow/latest/generators/model-state/var/index.md)                 | A value drawn once for the task and used in several places.                                                                  |
| Model State     | [`param`](https://jaehyeon.me/desflow/latest/generators/model-state/param/index.md)             | A live setting of the simulation, which a scenario step or an ingress update can change.                                     |
| Model State     | [`entity`](https://jaehyeon.me/desflow/latest/generators/model-state/entity/index.md)           | A payload field that names the entity the record belongs to.                                                                 |
| Related Records | [`lookup`](https://jaehyeon.me/desflow/latest/generators/related-records/lookup/index.md)       | A value from a record that another task already published, such as an existing customer.                                     |
| Related Records | [`previous`](https://jaehyeon.me/desflow/latest/generators/related-records/previous/index.md)   | A value from the last finished record of the same task, such as the previous reading.                                        |
| Related Records | [`upstream`](https://jaehyeon.me/desflow/latest/generators/related-records/upstream/index.md)   | A value from the record of the task that routed to this one.                                                                 |
| Related Records | [`aggregate`](https://jaehyeon.me/desflow/latest/generators/related-records/aggregate/index.md) | A mean, sum, minimum, maximum or count over earlier records of another task.                                                 |
| Related Records | [`row`](https://jaehyeon.me/desflow/latest/generators/related-records/row/index.md)             | A whole row of a CSV or Parquet file, such as a product from a price list.                                                   |
| Python          | [`call`](https://jaehyeon.me/desflow/latest/generators/python/call/index.md)                    | A value that your own Python function computes.                                                                              |

## Keys Every Kind Accepts

These four keys go on any kind.

| Key         | Type    | Default | What it does                                                                                                                                                  |
| ----------- | ------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `null_rate` | number  | none    | The chance, from 0 to 1, that the value is `null` instead of a draw.                                                                                          |
| `unique`    | boolean | `false` | When true, a draw that repeats an earlier value of this generator is drawn again, up to 1000 times, and the run stops with an error if no new value is found. |
| `omit_rate` | number  | none    | The chance, from 0 to 1, that the field of a mapping is left out.                                                                                             |
| `omit_when` | text    | none    | An expression. The field of a mapping is left out when it holds.                                                                                              |

`omit_rate` and `omit_when` apply to a field of a mapping, such as a field of a `payload`. A blueprint that writes them on any other value is refused when it loads.

## Run the Examples

Every example is a model in which an order starts every 60 seconds and the run lasts 200 seconds, so three orders start. The seed is 42 and `factor` is 0, so a run takes no wall-clock time and prints the same records each time. No example needs a service.

Each kind has three files in `examples/generators/` of the repository: `<kind>.yaml`, `<kind>_builder.py` and `<kind>_low_level.py`. Run them from the repository's root folder:

```bash
desflow run examples/generators/integer.yaml
python examples/generators/integer_builder.py
python examples/generators/integer_low_level.py
```

`fake` needs the `fake` extra, which `pip install "desflow[fake]"` adds. `row` reads `products.csv` and `call` imports `call_helpers.py`, both in the same folder.
