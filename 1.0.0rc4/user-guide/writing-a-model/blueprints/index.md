# YAML Blueprints

A YAML blueprint is a simulation written as a file. This page shows how to run one, how a file is read and checked, how to change settings per environment, and where an error points. [YAML Keys](https://jaehyeon.me/desflow/latest/reference/yaml-keys/index.md) lists every key a blueprint accepts.

______________________________________________________________________

## What It Is

A blueprint holds, as data, the builder calls that a Python script would make. Each section becomes the builder call of the same name: `resources` is `add_resource`, `tasks` is `@app.task`, `stats` is `with_stats`, and so on. There is no second way of building: `SimulationContext.from_yaml` makes those calls, in the order a script would, and returns an ordinary `SimulationContext`. A blueprint and a script that make the same calls produce the same simulation, and the tests check that the YAML, builder and low-level versions of every example publish the same records.

Use Python for logic that YAML cannot express. Reference it with `!python`, as [Call Python from YAML](https://jaehyeon.me/desflow/latest/how-to/tools/yaml-advanced/index.md) shows. An editor completes keys and marks a misspelt key or a value of the wrong kind as you type, from the JSON Schema that `desflow schema` prints and this site publishes. [Set Up Your Editor](https://jaehyeon.me/desflow/latest/how-to/tools/set-up-your-editor/index.md) shows the setting.

______________________________________________________________________

## How to Use It

### Run a Blueprint

Run it from a shell with the `desflow` command, which the package installs. These lines use the local example:

```bash
desflow examples get local --api yaml    # downloads local/yaml/local.yaml
cd local/yaml
desflow run local.yaml                   # runs until run.until
desflow run local.yaml --until 10min     # overrides run.until
desflow run local.yaml --seed 7          # overrides simulation.random_seed
desflow examples                         # lists the other examples
desflow --version
```

`--until` takes seconds or a duration such as `10 min`, `2 hours` or `1 week`. The command logs at INFO level. `ConsoleEgress`, in its default `log` format, writes each batch at that level, so its output is visible. A run with no seed logs the seed it drew, with the command that repeats it. An invalid file prints its errors and exits with code 1.

Or run it from Python:

```python
from desflow import SimulationContext

app = SimulationContext.from_yaml("local.yaml")
app.run()  # calls run.before, then runs until run.until
```

`run()` calls the blueprint's `run.before` functions first, and uses `run.until` when no `until` is passed. The builder methods and decorators still work on the context `from_yaml` returns.

`load_context` loads a blueprint or a builder Python file by the same path, so a script that takes either kind of file needs no check of its own:

```python
from desflow import load_context

# A blueprint, or a builder Python file that defines a SimulationContext at
# module level, from its folder: the one loader desflow run and explain use
app = load_context(".", "local.yaml")
```

### Preview the Records

`desflow run FILE` builds the blueprint and runs it with its own sinks until `--until`, `run.until` or Ctrl+C. It takes a YAML file, and with `--sample` also a Python builder file, which prints its first events the same way. A builder file otherwise runs with `python FILE`. Two options print the records instead of sending them to the file's sinks:

| Command                       | Behaviour                                                                                                                                                                                                                                                                                                                                                                                                        |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `desflow run FILE --sample N` | Prints the first `N` events, then stops. `N` is 1 or more. The run is unpaced: `factor` is set to 0 and `go_live_at` is cleared. `logical_start_time` and `random_seed` are kept, so timestamps and values look as they would in the real run. Ingress entries and `run.before` are skipped. Telemetry records print between the events but are not counted, and `system.simulation.lag_seconds` is not printed. |
| `desflow run FILE --stdout`   | Prints every record of a normal run except the `system.simulation.lag_seconds` readings, which describe the wall clock, not the model. `factor`, `go_live_at`, `logical_start_time` and ingress are kept; `run.before` is skipped.                                                                                                                                                                               |

Both replace every egress entry with one `Console` of `format: json`, and a second one for labels when the file has a `when: labels` entry. A JSON console writes each record as one JSON line on stdout. The run's log stays on stderr, so `desflow run FILE --sample 10 | jq .value` reads only the records.

A preview checks the whole file, but not the connectors it leaves out:

- The structure, keys and names of the whole file are validated.
- `run.before` is skipped, because its functions prepare the real sinks, such as creating topics. A warning on stderr says so when the file has any.
- The connectors a preview replaces or skips are not built, so only their `type` is checked. Their `config` is checked only by a run that builds them, such as `desflow run FILE` without these options.

| Combination                                           | Result                                                                                                                                                                                                                                                           |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--sample` with `--stdout`                            | Allowed. `--stdout` adds nothing.                                                                                                                                                                                                                                |
| `--sample` with `--until`, or a file with `run.until` | The run ends at whichever comes first. When `until` comes first, stderr says `sample ended at until with k of N events`. A model that runs out of events first ends the sample too, with exit code 0 and `sample ended with no events left, with k of N events`. |
| `--sample` on a file with `go_live_at`                | A backfill-then-live file prints its first history events. `when: history` and `when: live` have no effect, because their entries are replaced.                                                                                                                  |
| `--sample` on a file that publishes no events         | It runs until `until` or Ctrl+C, printing telemetry.                                                                                                                                                                                                             |
| `--stdout` on a file with several egress entries      | One console prints each record once. Per-entry `batch_size` and `flush_interval` are dropped with the entries, and the `batching` section applies.                                                                                                               |
| `--sample` or `--stdout` with `--replications`        | Usage error, exit code 2: both print one run.                                                                                                                                                                                                                    |
| `--sample 0`                                          | Usage error, exit code 2.                                                                                                                                                                                                                                        |

[Command Line](https://jaehyeon.me/desflow/latest/cli/index.md) lists every command and option.

### Times and Durations

A time in a blueprint is a number of seconds or a duration such as `90 s`, `10 min`, `2 hours` or `1 week`: `run.until`, a scenario step's `at`, `simulation.warmup`, a stats `interval`, and every time-valued distribution parameter. `rate` is the exception: it is a number per second. A text that is a plain number, such as `5` or `1e-3`, is that many seconds. A time place refuses NaN, `true` and a negative number, with the place named in the error. Infinity is refused where a time must end, such as `factor`, `initial_time`, a scenario `at` and a segment `every`, and elsewhere it means no limit. `factor` is a number, not a time, so `"2 min"` is refused.

`logical_start_time` and `go_live_at` are instants: `now`, a signed duration from now such as `-1d` or `-10m`, or an ISO datetime such as `2026-01-01T00:00:00`. Both are read against one moment, when the file is loaded, so `-10m` and `now` are exactly ten minutes apart. `now`, the relative forms and an ISO datetime without an offset are read in `simulation.timezone`, UTC by default, as [Clocks and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#time-zone) describes.

### Settings per Environment

`${VAR}` in a value is replaced with the environment variable, so one file can serve several environments. `${VAR:-default}` uses the default when the variable is unset or empty, as in a shell, and `$${` writes a literal `${`. An unquoted value is read as YAML reads it after the replacement, so `factor: ${SIM_FACTOR:-0}` is the number 0. A quoted value stays a string, so `"${LIMIT:-5}"` is the text `5`: write a number or a boolean unquoted. An unset variable with no default stops the load, and the error names its line.

```yaml
# Settings that change per environment, read from environment variables.
# Run it with: SIM_FACTOR=1 desflow run environment.yaml
simulation:
  sim_id: ${SIM_ID:-Shop}
  # An unquoted value is read after the replacement, so this is the number 0
  factor: ${SIM_FACTOR:-0}
  # Read against the moment the file is loaded: a day of history up to now
  logical_start_time: -1d
  # Under simulation: timestamps written in Melbourne time, with its offset; the
  # start and now are read in it. UTC by default
  timezone: ${SIM_TIMEZONE:-Australia/Melbourne}
  # Under simulation: how far the run is behind the wall clock, every 10 s; by
  # default off at factor 0 and every second otherwise, and 0 turns it off
  lag_interval: 10 s
  # Under simulation: true by default for a paced run and false at factor 0; false
  # lets a live run go on when a slow sink makes it fall behind the wall clock
  strict: false

egress:
  - {type: Console}

resources:
  # Quoted, because ${...} inside {...} would end the mapping early; the check
  # turns the text into a number
  packers: {current_cap: "${PACKERS:-2}", max_cap: 5}

run:
  # A duration, such as 1 week, or a number of seconds
  until: ${RUN_FOR:-1 hour}
```

The [Parquet example](https://jaehyeon.me/desflow/latest/gallery/simulation-models/parquet/index.md) switches from the local disk to S3 with five variables, and [Queues and Capacity](https://jaehyeon.me/desflow/latest/learn/first-model/queues-and-capacity/index.md) runs one file with several numbers of packers.

### Include a File

`!include path` puts a mapping from another YAML file in place, such as the settings `desflow fit` writes: `packing: !include fitted/packing.yaml`. The path is relative to the file that names it. `!include {file: path, spawn: pack}` adds keys to the file's mapping, such as the task an arrival spawns. The file is read with the same loader, so `${VAR}`, `!python` and `!include` work in it. A file that includes itself, directly or through another file, is refused. [Input Fitting](https://jaehyeon.me/desflow/latest/user-guide/model-parts/input-fitting/#load-the-settings) shows it.

### Generated Values

A value place takes a constant or a generator, a mapping with a `gen` key. The places are `payload`, `attributes`, `set`, a store's `items`, the `value` of an `on:` entry, a `fire`, a timeout and a scenario step, an interrupt's `cause`, and a scenario `record`. [Generators](https://jaehyeon.me/desflow/latest/generators/index.md) lists every kind, and [Generate Data](https://jaehyeon.me/desflow/latest/how-to/generate-data/choices-and-numbers/index.md) shows them in tasks. This blueprint builds an order record from most of them. The task's own keys come first:

```yaml
# Named values, drawn in order once for each instance of the task when it starts,
# because on_start is given. A generator reads one as {gen: var, name: ...}
vars:
  quantity: {gen: integer, bounds: [1, 6]}
  unit_price: {gen: uniform, low: 5, high: 40, decimals: 2}
  ship_to:
    gen: object
    fields:
      city: {gen: one_of, choices: [Perth, Hobart]}
      postcode: {gen: integer, bounds: [1000, 9999]}
# Drawn once for each entity and task, at its first use
vars_once:
  region: {gen: one_of, choices: [north, south]}
# Published as one event record when the task starts
on_start:
  opened_at: {gen: now}
# Drawn after the service, and may read the vars
finish_vars:
  closed_at: {gen: now, offset: 1 min}
# The key of the task's records, drawn at the first record
key: {gen: sequence, start: 1, format: "order-{}"}
# Rows kept for a lookup or aggregate of this task by other tasks
history: 500
```

The rules for where a generator counts:

- **Depth.** In a `payload`, `set`, `attributes` or scenario `record`, a mapping with a `gen` key is a generator at any depth. A `gen` key directly under `set`, `attributes` or a `record` is a field name. A nested mapping keeps any key other than `gen`, such as a number or a date.
- **Data.** `{gen: literal, value: ...}` keeps a mapping as data. A mapping with no `gen` key in an `on:` or `fire` value is data, and a generator nested in it, or a nested `gen` inside an `on:` value, is stored as written. A mapping or list in a `put.item` is read as a payload is, so a generator nested at any depth is evaluated as the put is made.
- **Text.** Text in a value place stays text. `"attr.k * 3"` is that string, and `{gen: expr, expr: "attr.k * 3"}` evaluates it.
- **Files.** A `row` reads its file from the blueprint's folder when the file loads. A cell of digits is a number, so leading zeros are lost: write them with `{gen: format, template: "{n:05d}"}`.
- **Seeds.** Each generator draws from a random stream of its own, made from the run's seed and the generator's place in the file. Adding a field to a mapping moves no other generator's draws. Inserting an item into a list, a scenario step or an `on:` entry moves the draws of the items after it. Each `fake` has its own Faker, and a `sample` of a service draws from the generator's own stream, so the service's draws stay as they were.

The payload of the same task uses the other kinds. Each block below is a part of the same file.

Arithmetic takes a list of arguments, each a constant or a generator: `add`, `subtract`, `multiply`, `divide`, `min` and `max`. `decimals` rounds the result. `expr` evaluates an expression whose `names` are generators.

```yaml
total:
  gen: add
  args: [{gen: var, name: unit_price}, 10]
  decimals: 1
discount:
  gen: subtract
  args: [100, {gen: var, name: quantity}]
weight:
  gen: multiply
  args: [{gen: var, name: quantity}, 2.5]
  decimals: 1
share:
  gen: divide
  args: [{gen: var, name: quantity}, 4]
  decimals: 2
least:
  gen: min
  args: [{gen: var, name: quantity}, 5]
most:
  gen: max
  args: [{gen: var, name: quantity}, 5]
```

```yaml
with_tax:
  gen: expr
  expr: "quantity * price * (1 + tax)"
  names:
    price: {gen: var, name: unit_price}
    tax: {gen: param, path: Shop.variables.tax_rate}
  decimals: 2
```

`lower`, `upper` and `literal` give text. `literal` keeps a value as data, even a mapping that has a `gen` key.

```yaml
region:
  gen: upper
  value: {gen: var, name: region}
source: {gen: lower, value: WEB}
note: {gen: literal, value: "n/a"}
```

`uniform` and `normal` draw numbers, and `sample` draws from a distribution or from a service of the file, with the service's own parameters.

```yaml
price: {gen: uniform, low: 5, high: 40, decimals: 2}
```

```yaml
delivery_minutes: {gen: normal, mean: 120, std: 15, decimals: 1}
```

```yaml
wait: {gen: sample, service: packing, decimals: 1}
retries: {gen: sample, dist: geometric, p: 0.5}
fixed: {gen: sample, dist: constant, value: 3}
```

`one_of` picks one value and `pick` picks `count` different values.

```yaml
colour: {gen: one_of, choices: [red, green, blue]}
```

```yaml
extras: {gen: pick, choices: [card, ribbon, box, tag], count: 2}
```

`weighted` picks by weight, from `options` or from a list of `value` and `weight`.

```yaml
paid_by:
  gen: weighted
  options: {card: 60, cash: 40}
priority:
  gen: weighted
  choices:
    - {value: normal, weight: 9}
    - {value: rush, weight: 1}
```

`sequence` counts from `start` in steps of `step`, starts again after `loop_after` values, and writes each with `format`.

```yaml
number: {gen: sequence, start: 1000, step: 5, loop_after: 1000, format: "ord-{}"}
```

`uuid` gives version 4 or 7.

```yaml
reference: {gen: uuid, version: 7}
```

`boolean` is true with probability `p`.

```yaml
gift: {gen: boolean, p: 0.1}
```

`now` gives the logical time, and `offset` moves it.

```yaml
due_at: {gen: now, offset: 1 hour}
```

`format_time` writes a time in a zone.

```yaml
placed_at:
  gen: format_time
  time: {gen: now, offset: -1 h}
  format: "%H:%M"
  tz: Australia/Melbourne
```

`fake` calls a method of Faker, with `args` and `kwargs`, for a `locale`.

```yaml
customer: {gen: fake, method: name, locale: en_AU}
phone: {gen: fake, method: numerify, args: ["04## ### ###"]}
email:
  gen: fake
  method: email
  kwargs: {domain: example.com}
```

`format` fills a `template` from `names`.

```yaml
code:
  gen: format
  template: "{region}-{n}"
  names:
    region: {gen: var, name: region}
    n: {gen: integer, bounds: [10, 99]}
```

`switch` and `when` choose a value from another.

```yaml
band:
  gen: switch
  on: {gen: var, name: quantity}
  cases:
    - {max: 3, value: small}
    - {min: 3, max: 5, value: medium}
  default: large
```

```yaml
size_label: {gen: when, condition: "quantity > 3", then: large, else: small}
```

`repeat` makes a list.

```yaml
scores:
  gen: repeat
  times: 3
  value: {gen: integer, bounds: [1, 5]}
```

`var` and `attr` read a var and an attribute, or one `path` of it. `param` reads a live registry value, or one `field` of a mapping. `object` builds a mapping.

```yaml
city: {gen: var, name: ship_to, path: city}
size: {gen: attr, name: size}
slot: {gen: attr, name: delivery, path: slot}
```

```yaml
tax_rate: {gen: param, path: Shop.variables.tax_rate}
high: {gen: param, path: Shop.variables.limits, field: high}
```

```yaml
address:
  gen: object
  fields:
    street: {gen: fake, method: street_name}
    postcode: {gen: integer, bounds: [1000, 9999]}
```

`call` runs a function of your own, with `kwargs` worked out first.

```yaml
gross:
  gen: call
  function: !python generated_values_logic.with_tax
  kwargs:
    price: {gen: var, name: unit_price}
```

`row` takes a row of a CSV or Parquet file, kept by `where`, chosen by the column `weight`, and ending as `on_end` says:

```yaml
stocked:
  gen: row
  file: generated_values_products.csv
  where: stock > 0
  weight: share
  on_end: loop
```

Any generator can be null, left out, or made to give each value once:

```yaml
coupon: {gen: uuid, null_rate: 0.3}
gift_card: {gen: integer, bounds: [1000, 9999], omit_rate: 0.5}
rush_fee: {gen: integer, bounds: [1, 9], omit_when: "quantity < 4"}
ticket: {gen: integer, bounds: [1, 1000000], unique: true}
```

The first records:

```text
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'order-1', 'task': 'order', 'instance': 0, 'value': {'opened_at': '2026-01-01T00:01:00.000+00:00'}}
[EVT] {'sim_ts': 60.0, 'timestamp': '...', 'key': 'order-1', 'task': 'order', 'instance': 0, 'value': {'total': 21.1, 'discount': 99, 'weight': 2.5, 'share': 0.25, 'least': 1, 'most': 5, 'with_tax': 13.28, 'region': 'SOUTH', 'source': 'web', 'note': 'n/a', 'price': 9.17, 'delivery_minutes': 124.0, 'wait': 41.4, 'retries': 2, 'fixed': 3, 'colour': 'red', 'extras': ['box', 'card'], 'paid_by': 'card', 'priority': 'normal', 'number': 'ord-1000', 'reference': '019b76db-9260-735c-8c03-5157b422dbdd', 'gift': False, 'due_at': '2026-01-01T01:01:00.000+00:00', 'placed_at': '10:01', 'customer': 'Sara Mitchell', 'phone': '0403 164 579', 'email': 'kevinellis@example.com', 'code': 'south-67', 'band': 'small', 'size_label': 'small', 'scores': [3, 4, 5], 'city': 'Perth', 'size': 30, 'slot': 12, 'tax_rate': 0.2, 'high': 9, 'address': {'street': 'Paul Route', 'postcode': 9495}, 'gross': 13.28, 'stocked': {'sku': 'B-300', 'name': 'Bowl', 'stock': 7, 'share': 2}, 'coupon': '6ef12d5b-d11e-4079-a284-bcb0d3fbe634', 'gift_card': 3256, 'ticket': 940200}}
```

### Names an Expression Reads

An expression reads names, calls and operators, and nothing else. The names depend on where the expression sits, and [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/index.md) shows them in a task:

- **Run:** `t` (the clock), `registry["..."]`, `level.<name>`, `users.<name>`, `queue.<name>`, `signal.<name>.count` and `.processed`, `done_count` in a `when` group, and `handle.<n>.triggered`, `.processed`, `.ok`, `.value` and `.alive` for an opened event.
- **Entity and flow:** `attr.<name>`, `payload.<field>`, `process.key`, `process.flow_name`, `item` and `item_priority` after a `get`, `arg.<name>` from the `args` of a `wait` `task` branch or a `spawn`, `cause.<name>` in an interrupt, and `wait.<branch>.done`. `wait.suspend.value` reads in the task that was suspended, and `cause.<name>` in a task that `on_interrupt_run` reaches.
- **Task that routed here:** `upstream.<field>`, and `upstream.status` and `upstream.reason` in a task that `on_fail`, `on_renege` or `on_balk` reaches.
- **Calls on the model:** `held(r)`, `waiting(r)`, `putting(x)` and `getting(x)` give lists of mappings with an `entity`. A resource entry also has `rank`, `time`, `preempt`, `key` and `since`, a store put has `item`, and a container has `amount`. `running(task[, cond])` is a count, and `peek()` is the time of the next event.
- **Forms:** `[...]`, `{...}`, a list comprehension with one `for`, subscripts and slices, `is None`, `in`, and dotted reads at any depth. `payload['total']` is refused because `payload` is not a name by itself: write `payload.total`. `attr["class"]` reads an attribute whose name Python keeps as a word.
- **Functions:** `len`, `sum`, `sorted`, `range` (at most 1,000,000 items), `any`, `all`, `str`, `int`, `float`, `bool`, `abs`, one-argument and many-argument `min` and `max`, `round`, `floor`, `ceil`, `sqrt`, `exp`, `log`, `log10`, `sin`, `cos`, `tan`, `has(attr.x)`, and `get_key(mapping, key, default)`, `set_key` and `drop_key`.

The limits are these. One reading that makes more than 1,000,000 items in all is a named run failure. `%` on text and `**` with an integer power above 10000 are refused, and so is `**` with a negative base and a fractional exponent, `round(x, -n)` with a very large `n`, and an expression nested more than 200 levels. Text is allowed in comparisons, indexes, list and mapping entries, `+` and the key functions. `prod` and `sumprod` are reserved for a later release, and a var, loop name or generator `names` entry cannot take them or any other name an expression already uses.

### Run on Your Own Environment

`simulation.environment` runs the model on your own subclass of `DynamicRealtimeEnvironment`, named with `!python`, as `SimulationContext(environment=...)` does in Python, for example `environment: !python my_model.TracingEnvironment`.

A subclass that overrides `__init__` takes `**kwargs` and passes them on, because desflow builds it with the arguments it builds its own environment with. One that overrides `schedule` or `step` calls `super()`. When a script steps outside `run()`, `step` runs once per model event. Inside `run()`, it runs once per event, desflow's own events included.

In a blueprint, the class is named under `simulation`. `CountingEnvironment` in `endings_logic.py` counts the events the run processes:

```yaml
# Run on a subclass of DynamicRealtimeEnvironment, from endings_logic.py beside the
# file
environment: !python endings_logic.CountingEnvironment
```

______________________________________________________________________

## How It Works

`from_yaml` makes the builder calls for you, so a blueprint gets everything [the builder does](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/builder-api/#how-it-works), and it checks the whole file before anything runs. A file is read and checked in five steps, and any step that fails stops the load with a `BlueprintError` before anything runs:

1. **Read the file.** `read_document` reads it with a subclass of `yaml.SafeLoader`. `${VAR}` is replaced while each scalar is read, `!python module.attribute` imports the module and returns the attribute, `!include path` reads a mapping from another YAML file relative to this one, and every key's line is kept in a source map. Every other node is built as `yaml.safe_load` would build it, except that the key `on` of a task, a signal, an experiment check and a `switch` is read as text, and so are the keys `yes`, `no`, `on` and `off` of a `switch`'s `cases` and a `weighted`'s `options`. A key written twice in one mapping is refused at its second line, and the error names both lines. Elsewhere, two keys that YAML reads as one value, such as `yes` and `true`, are refused the same way.
1. **Check every field.** The document is validated against the `Blueprint` model. Unknown keys are refused, so a misspelt key is reported rather than ignored. Every distribution is built as the builder would build it, so a parameter out of range, `true`, NaN, or one the family does not take, is refused here. Every expression place is checked at its line, a capacity below 0 is refused, and a refusal under a field that takes a name or a mapping reports only the member that matches, with no type or class name. Every relative time is read against one `now`. A `spawn` `after` and a `fire` `after` must be above 0. `simulation.environment` must be a subclass of `DynamicRealtimeEnvironment`. All validation errors are reported at once.
1. **Check the names.** Each name in the file must exist and fit where it is used:
   - Every service, resource, store, container and next task that a task names exists.
   - Every task's `spawn` and `on:` spawn names a task that does not share an arrival's name.
   - A `subscribe` names a task or a signal.
   - An `on:` fire names a signal, and an `on:` interrupt names a task.
   - An `on:` `fire ... from` names a signal or a task. The task must run on the same entity as the `on:` list. For a task's list, that is the entity of the task that holds the list. For a signal's list, it is the entity of a task that fires the signal.
   - Every `level.<name>` in an expression names a store or a container, and every `users.<name>` and `queue.<name>` names a resource.
   - A name is used in one of resources, stores, containers and signals, and holds no dot or space.
   - A task's `priority` needs a resource with `queue: priority`.
   - Resources and stores have whole-number capacities.
   - An `on:` fire with `urgent`, `after` or `priority` names a signal with `once: true`. A `priority` is a whole number and is refused with `urgent`.
   - A `fail` is refused with `from`, and `defused` needs `fail`. A `fail_reason` needs a `fail` condition.
   - A `listen` names one of `signal` and `handle`, an `index` needs a `handle`, and the signal or handle is declared.
   - A `when` or a generator `value` in a signal's `on:` that reads `wait.<branch>` is refused, because a signal's entries run with no wait in scope. So is an `each.index` or `each.value` read outside the branch of an `each`.
   - A scenario step's interrupt `where` reads `wait.<branch>` only for a branch of the wait of the task it interrupts.
   - `simulation.environment` names a subclass of `DynamicRealtimeEnvironment`.
1. **Build the context.** The builder calls are made in the order a script would make them: the simulation, ingress, egress, batching, stats, resources, stores, containers, variables, services, arrivals, tasks, arrival loops, processes, telemetry and the scenario.
1. **Check the paths.** The parameters are registered into a scratch registry, with the code that the run uses. Every scenario `path`, and every `registry["..."]` that an expression reads, is then checked against the paths the run will have. Every scenario `value` must convert to the type its path holds.

`!python module.attribute` imports `module` and returns its `attribute`: a function, a class, a dictionary or a constant. The longest prefix that is a module is imported, and the rest is read as attributes. The folder of the YAML file is added to the front of `sys.path`, so a module beside the blueprint is found from any working directory, and two blueprints in different folders that each have a `logic.py` use their own, also when one command loads both. The exception is a module that the process imported before it loaded its first blueprint, such as `random` or another standard library module: every blueprint uses that module, not a file of the same name beside it. Give a module beside a blueprint a name that no standard library module or installed package has. `${VAR}` is replaced inside a reference too. Every reference is resolved when the file is loaded, so a wrong one fails before the run starts, with the file and line:

______________________________________________________________________

## Stability and Limits

An error at any step is mapped back to its line through the source map. A time in a message reads `5` for a whole number, and a number in `{now:g}` form has six significant digits, so 1000000 reads `1e+06`:

```yaml
# A blueprint with a mistake, for the YAML Blueprints page: pack_order has a priority,
# but its resource serves its queue in the default order, fifo.
simulation: {sim_id: Shop, factor: 0}

resources:
  packers: {current_cap: 1, max_cap: 2}

services:
  packing: {dist: constant, value: 1.5 s}

arrivals:
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    priority: 0
    payload: {event_type: order_packed}
```

```python
try:
    SimulationContext.from_yaml("blueprint_error.yaml")
except BlueprintError as error:
    print(f"Error: {error}")  # what desflow run prints, before exiting with code 1
```

```text
Error: blueprint_error.yaml:18: task 'pack_order' has a priority, but resource 'packers' does not set queue: priority; set it, or remove the priority
```

A blueprint covers configuration, including several resources per task (`resource: [picker, bench]` and `keep`), preemption (`preempt: true`) and breakdowns (`breakdown:`). These stay in Python:

- **Values computed per entity.** A mapping payload is a constant, apart from three values added for each entity: the task id that `id_field` adds, the attributes that `attributes` adds, and the item a `get` took, which `item_field` adds. Random choices, derived fields and state carried between events are a Python payload or process, as [Call Python from YAML](https://jaehyeon.me/desflow/latest/how-to/tools/yaml-advanced/index.md) shows. A task's `priority` and a `get` or `put` amount can be an expression. `next` rules read attributes, payload fields, levels, registry values, the item a `get` took (`item.<field>`), the wait's branches (`wait.<branch>.done`) and signals (`signal.<name>.count`), as [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/index.md) describes.
- **Keys described where their subject is.** `seize`, `release`, `open`, `cancel` and handles are in [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/index.md) and [Tasks and Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/index.md). `batch` is in [Serve Entities Together](https://jaehyeon.me/desflow/latest/how-to/modelling/serve-entities-together/index.md). `family` and `members` are in [Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/index.md). `{of, key}` and `{of, lowest}` references, `fire` with `fail`, and `listen` are in [Signals and Waiting](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/index.md). `lookup`, `previous`, `upstream` and `aggregate` are in [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/index.md). `breakdown.clock` is in [Priority and Preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/index.md). The arrival keys are in [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/index.md), and `run.segments`, `steps` and `sync` in [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#run-in-segments).
- **Entity keys.** An arrival's `key` and a task's `key` take text, a number or a generator, and the default is `<arrival>-<n>`. Only a key written as a Python function is builder only.
- **One simulation per file.** A blueprint has one `sim_id`. `!include` puts a mapping from another file in place, such as fitted settings, but not a second simulation.

### Security

A blueprint with no `!python` references is read entirely by `yaml.SafeLoader`, and loading it imports only the connector modules its `type` entries name. An `!include` reads another YAML file the same way and imports nothing itself. A file that references Python, directly or in a file it includes, runs the imports it names. Such a file needs the same trust as a Python script: run blueprints only from people whose scripts you would run. Every other node is built by `yaml.SafeLoader`, and only `!python` and `!include` are added to it.
