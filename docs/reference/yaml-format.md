# YAML Format

Every key a blueprint accepts, section by section. [YAML Blueprints](../user-guide/blueprints.md) describes how a file is read and where an error points, and each User Guide page shows its keys in use.

Every section is optional except `simulation`. Unknown keys are refused, so a misspelt key is reported with its line rather than ignored. A time is a number of seconds or a duration such as `10 min`. An instant is `now`, a signed duration such as `-1d`, or an ISO datetime. `${VAR}` and `${VAR:-default}` read environment variables anywhere in the file. [YAML Keys](yaml-keys.md) lists the same keys with their types and defaults. It is generated from the JSON Schema at `https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json`, which [Set Up Your Editor](../guides/set-up-your-editor.md) uses.

Each section becomes one builder call:

![simulation becomes SimulationContext, resources add_resource, stores and containers add_store and add_container, services add_service, arrivals add_arrival and spawn, tasks @app.task with get, put, next and set, stats with_stats, ingress and egress add_ingress and add_egress, scenario registry updates and labels at set times, and run run(until)](../assets/diagrams/blueprint-sections.png)

A blueprint with logic in Python beside it, which the sections below refer to:

```yaml title="docs/snippets/yaml/hybrid.yaml"
# The shop with its logic in Python. YAML keeps the blueprint and the
# experiment; shop_logic.py beside this file holds what YAML cannot express.
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 1

egress:
  - type: Console
    # Only events reach the terminal; telemetry is dropped.
    when: !python shop_logic.events_only

resources:
  packers: {current_cap: 1, max_cap: 3}

services:
  packing: {dist: normal, mean: 4.0, std: 1.0}

arrivals:
  # An order every 5 s on average (rate 0.2)
  orders: {dist: exponential, rate: 0.2, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    # Called as choose_delivery(task_id, context) for every packed order.
    payload: !python shop_logic.choose_delivery

processes:
  # Called as breaks(context, resource="packers", every=50, duration=5).
  - function: !python shop_logic.breaks
    kwargs: {resource: packers, every: 50, duration: 5}

scenario:
  - {at: 1 min, path: Shop.arrival.orders.rate, value: 0.4}

run:
  until: 3 min
```

---

## simulation

The arguments of `SimulationContext`. [Time and Pacing](../user-guide/time-and-pacing/index.md) describes the clock.

| Key | Value |
|---|---|
| `sim_id` | required; the first part of every registry path and statistic name |
| `factor` | 1.0 follows the wall clock, and 0 does not wait; 1.0 by default |
| `random_seed` | a whole number; without one, the run draws a seed and logs it |
| `logical_start_time` | the instant simulation time 0 maps to; the load time by default |
| `go_live_at` | the instant from which the run is paced in real time |
| `warmup` | a time, 0 or more; no sink receives a record before it, and the statistics restart at it |
| `initial_time` | the simulation clock's reading when the run starts, in seconds or a duration; 0 by default |
| `timezone` | an IANA name, such as `Australia/Melbourne`. Timestamps are written in this zone, with their offset. A `logical_start_time` or `go_live_at` without an offset, and `now`, are read in it. Without `timezone`, the run uses the zone of `logical_start_time`, or UTC |
| `lag_interval` | simulation seconds or a duration between `system.simulation.lag_seconds` readings; 0 turns them off; by default off at `factor: 0` and every second otherwise |
| `strict` | `true` raises `RuntimeError` when a paced run falls behind the wall clock; `false` by default, as [Strict Pacing](../user-guide/time-and-pacing/clocks-and-pacing.md#strict-pacing) explains |
| `environment` | a `!python` reference to a subclass of `DynamicRealtimeEnvironment` the model runs on; the subclass's `__init__` takes `**kwargs` and passes them on |

## resources, stores and containers

`name: {...}` entries. [Resources](../user-guide/resources/index.md) and [Stores and Containers](../user-guide/stores-and-containers.md) describe them.

| Section | Keys |
|---|---|
| `resources` | `current_cap` and `max_cap`: whole numbers, or `.inf` for no limit. Both are optional. `current_cap` is 1 by default and `max_cap` is `current_cap`, so an entry `{}` is one unit. `queue`: `fifo` (the default), `lifo` or `priority`. `queue_limit`: the most requests that may wait, 0 or more (no limit by default). `type: !python module.Class`: a subclass of `ResourceBase` to build the resource from. `preempt: true`: with `queue: priority`, a request with a lower priority value takes a busy unit. `breakdown: {time_to_failure, repair, work}` makes the resource fail and be repaired; `time_to_failure` and `repair` are distributions, and `work` is `interrupt` (the default) or `finish` |
| `stores` | `current_cap` and `max_cap`: whole numbers of items, or `.inf` for no limit. Both are optional. Without `current_cap`, it is `max_cap` if that is given, and no limit if not; without `max_cap`, it is `current_cap`. `priority: true` returns the item with the lowest priority first; a priority is any value that can be ordered, such as a number, text, a date or a list. `items`: the items held at the start, as `[priority, item]` pairs for a priority store. `type: !python module.Class`: a subclass of `StoreBase` to build the store from; it cannot be combined with `priority` |
| `containers` | `current_cap` and `max_cap`, the largest amount or `.inf` for no limit, both optional as for a store; `level`, the amount at the start, 0 by default; `type: !python module.Class`, a subclass of `ContainerBase` to build the container from |

A store and a container cannot share a name.

## signals

A list of names, each a signal that tasks fire with `signal` and wait for with `wait: {signal: name}`. [Signals and Waiting](../user-guide/tasks-and-processes/signals-and-waiting.md#wait-signals-and-child-flows) describes them.

A signal may also be written as a mapping, `{name: {once, on}}`, with the list form still accepted:

| Key | Value |
|---|---|
| `once` | `true` makes the signal fire once, as SimPy's `Event` does: it does not re-arm, a wait after the firing resumes at once, and a second firing is refused |
| `on` | a list of actions run when the signal fires, in the same form as a task's `on` |

## variables

`name: value` entries, any YAML value, each at the registry path `<sim_id>.variables.<name>`. [Live Parameters](../user-guide/live-parameters.md) describes them.

## services and arrivals

`name: {dist, ...}` entries: a distribution family and the parameters it takes. Time-valued parameters take durations, and `rate` is per second. [Arrivals and Services](../user-guide/arrivals-and-services.md) shows each family, and [Reference, Distributions](distributions.md) lists their parameters and rules.

An arrival also takes:

| Key | Value |
|---|---|
| `spawn` | the task each arrival starts |
| `attributes` | a mapping each new entity starts with, read as `attr.<name>` |
| `replay` | in place of a distribution: `{path, time, unit, fields, on_invalid}`. `path` is a CSV or Parquet file, relative to the blueprint, whose rows arrive at their times. `unit` is `ms`, `s` (the default), `min`, `h` or `timestamp`. `fields` is a list of columns or `all` (the default). `on_invalid` is `error` (the default) or `skip`. [Arrivals and Services](../user-guide/arrivals-and-services.md#replay) describes it |

## tasks

`name: {...}` entries, each built with `@app.task`. [Tasks and Processes](../user-guide/tasks-and-processes/index.md) and [Task Flow](../user-guide/task-flow.md) describe them.

| Key | Value |
|---|---|
| `service` | the service whose time the task takes; needed with a `resource` |
| `resource` | the resource the task holds while it works, or a list of them, acquired in the order listed |
| `keep` | resources in `resource` handed to the next task, which releases them once it starts |
| `payload` | required: the finished event's value, a mapping, or a `!python` function called as `function(task_id, context)` |
| `id_field` | adds the task id to a mapping payload under this key |
| `item_field` | adds the item the task's store `get` took to a mapping payload under this key |
| `attributes` | a list of entity attributes added to a mapping payload |
| `priority` | a number or an expression such as `attr.tier`, for a resource with `queue: priority` |
| `patience` | seconds, a duration or an expression: how long the task may wait for its `wait`, its resource and its `get`, all together, before it leaves and publishes `reneged`. Patience counts waiting time only and pauses while the task is served |
| `balk` | a condition checked before the task joins a queue; when it holds, the task publishes `balked` |
| `wait` | what the task waits for before its resource. A leaf is one of `{signal: name}`, `{task: name}`, `{timeout: ...}` (seconds, a duration, or `{after, value}` where `value` reaches the waiter), `{condition: ...}`, `{request: name}` (a resource unit), `{get: {...}}` or `{put: {...}}` (a store or container), or `{running: {task, where}}` (the running instances of a task). A group is `{any: {name: branch, ...}}`, `{all: {...}}` or `{at_least: n, of: {...}}`, and groups nest. After the wait, read each branch as `wait.<branch>.done`, `.ok` and `.value` |
| `signal` | a signal under `signals`. The task fires the signal with its payload when the task finishes, and fails the signal when the task fails |
| `spawn` | `{task, after}`: on success, the task starts `task` as a new entity keyed `<task>-n`, after an optional `after` delay. The new entity's `attr.parent` is the spawning entity's key |
| `subscribe` | a wait branch such as `{task: name}` or `{running: {...}}`: the task is interrupted when that event happens, with the cause `{event, value}` |
| `on` | a list of actions run on every end of the task: each `{fire: {signal, value, from}}`, `{spawn: {task, after}}`, `{interrupt: {task, where, cause}}`, or a `{path, value}` registry change. A fire action takes either a literal `value` or `from` (the field `from_`), a signal or task name whose last outcome it copies. For a signal with `once: true`, it also takes `urgent: true`, which resumes the waiters before the other events at their time, and `after` (above 0, a number or a duration), which fires the signal that long from now |
| `fail` | a condition checked after the service, reading the task's own payload; when it holds the task publishes `failed` |
| `fail_reason` | the `reason` a failure publishes; the `fail` condition's text by default |
| `on_fail` | the task a failed instance goes to; without it the flow ends |
| `on_interrupt` | `resume` (the default), `restart` or `abandon`: what an interrupt or a preemption does |
| `on_interrupt_next` | the task an abandoned instance goes to |
| `preempt` | `false` keeps the task's requests from preempting; `true` by default |
| `interrupt` | `{task, where, cause}`: when the task finishes, interrupts every running instance of `task` whose `where` holds |
| `get` | `{container: name, amount: ...}` or `{store: name}`, taken after the resource and before the work. A store `get` takes an optional `filter` condition, which reads the item offered as `item.<field>`. With an optional `wait: false`, the task makes the request and goes on at once; the request is granted when it can be, first in first out, and the task does not get its item |
| `put` | the same form, without a filter, added after the work. A store receives the payload, with an optional `priority` for a priority store. With an optional `wait: false`, the task makes the request and goes on at once; the request is granted when it can be, first in first out |
| `next` | a task name; `{when: [{condition, task}, ...], default: task}`; or `{weights: {task: number, ...}}` |
| `set` | a mapping applied to the entity's attributes when the task finishes |

An `amount` is a number or an expression, 1 when left out. A task with a `service` and no `resource` is a timed step. A task with neither, and no `get`, takes no time.

## processes

A list of `!python` generator functions, each written as `{function: ..., kwargs: {...}}` or as the bare reference. Each is started as `function(context, **kwargs)`. [Call Python from YAML](../guides/yaml-advanced.md) shows them.

## telemetry

A list of custom readings, each `{interval, function}`. Every `interval` simulation seconds, `function(context)` is called and publishes with `context.publish`. `interval` must be greater than 0. The built-in readings of resources, stores and containers come from `stats` instead.

```yaml title="docs/snippets/yaml/telemetry.yaml:telemetry"
telemetry:
  # Every 5 simulation seconds, calls packers_report(context), which publishes
  # Shop.waiting_per_packer and Shop.arrival_rate with context.publish
  - interval: 5
    function: !python shop_logic.packers_report
```

## stats

The built-in statistics, as a level, `stats: basic`, or as a mapping, `stats: {level: all, interval: 1 min}`. The level is `none`, `minimal`, `basic`, `typical` or `all`. The interval is a time greater than 0, and 10 seconds by default. [Statistics](../user-guide/statistics.md) describes them.

## scenario

A list of steps, applied in time order on the simulation clock:

| Key | Value |
|---|---|
| `at` | the time of the step |
| `path` | a registry path, checked when the file loads; needs a `value` |
| `value` | the new value, which must convert to the type the path holds |
| `record` | a mapping, published as a ground-truth label to the `when: labels` sinks |
| `interrupt` | `{task, where, cause}`: interrupts every running instance of `task` whose `where` holds |

A step needs a `path` and `value`, a `record`, an `interrupt`, or more than one. A step applies before every other event at its time. Steps at the same time apply in the order written. A step at or after `until` never applies. [Live Parameters](../user-guide/live-parameters.md) shows scenarios and labels, and [Vary Arrivals by Time](../guides/time-varying-arrivals.md#boundaries) the boundaries.

## ingress and egress

Lists of connectors, each with a `type` and a `config` of the constructor's keyword arguments. `type` is one of the short names below, or a `!python` class from another package. A connector's module is imported only when a blueprint names it, so a blueprint that uses Kafka does not need the PostgreSQL driver. A missing package stops the load with its line and the `pip install` command. Parquet and JSON Lines are the exception: they import PyArrow when they start writing.

| Direction | `type` | Class | Extra |
|---|---|---|---|
| ingress | `Local` | `LocalIngress` | none |
| ingress | `Kafka` | `KafkaIngress` | `kafka` |
| ingress | `Postgres` | `PostgresIngress` | `postgres` |
| ingress | `Redis` | `RedisIngress` | `redis` |
| egress | `Console` | `ConsoleEgress` | none |
| egress | `Kafka` | `KafkaEgress` | `kafka` |
| egress | `Parquet` | `ParquetStorageEgress` | `parquet` |
| egress | `Jsonl` | `JsonlStorageEgress` | `parquet` |
| egress | `Iceberg` | `IcebergStorageEgress` | `iceberg` |
| egress | `Postgres` | `PostgresEgress` | `postgres` |
| egress | `Redis` | `RedisEgress` | `redis` |

An egress entry also takes `when`, `batch_size` and `flush_interval`. `when` is `history`, `live`, `labels` or a `!python` function of one record. `history` and `live` need `simulation.go_live_at`. `batch_size` and `flush_interval` are both greater than 0. `Console` takes `format`: `log` (the default) writes each record as a log line, and `json` writes it as one JSON line on stdout. [Ingress](../user-guide/ingress.md) and [Egress](../user-guide/egress.md) describe them.

Settings that the Python API takes as objects are plain values:

| `type` | Setting | Plain value |
|---|---|---|
| `Kafka` | topics | Without a `topic_router`, `event_topic` and `telemetry_topic` are created when the run starts, with one partition each, if they do not exist. |
| `Parquet`, `Jsonl` | `filesystem` | A mapping. `type` is `local` (the default) or `s3`, and the other keys are passed to PyArrow's `S3FileSystem`, such as `endpoint_override`, `access_key`, `secret_key` and `region`. An `endpoint_override` starting with `http://` sets the scheme. A key whose value is empty is left out. |
| `Parquet`, `Jsonl` | `default_path` | Without a `path_router`, each event is written there as one flat row, with its `value` mapping unpacked into columns, and telemetry is left out. It is a file name prefix, not a folder: each batch is a file of its own, named as [Egress](../user-guide/egress.md#parquet-and-json-lines) explains. |
| `Iceberg` | `catalog` | A mapping of PyIceberg catalog properties, such as `type`, `uri`, `warehouse` and `s3.endpoint`, passed to `load_catalog` on the first write. An optional `name` key names the catalog. |
| `Iceberg` | `default_table`, `schemas` | Without a `table_router`, each event is written to `default_table` as one flat row. `schemas` maps a table to its columns, each with a type: `string`, `int`, `long`, `float`, `double`, `boolean`, `timestamp`, `timestamptz`, `date` or `binary`. |
| `Postgres` | `tables` | Tables created when the run starts, if they do not exist. Each maps to `columns`, a mapping of column name to SQL type, and an optional `primary_key`. `table_name` defaults to the table when `tables` names one. |

An ISO time string bound for a timestamp or date column of Iceberg or PostgreSQL is converted before the write.

## batching

The arguments of `with_batching`, which are the defaults for every sink: `batch_size` and `flush_interval`, and optionally `max_queued_batches` and `drain_stall_seconds`. All four are greater than 0. [Egress](../user-guide/egress.md#batch-size-and-flush-interval) describes them.

## run

| Key | Value |
|---|---|
| `until` | a time greater than `initial_time`; `desflow run --until` overrides it |
| `stop` | ends the run before `until` when it happens: `{signal: name}`, the next firing of a signal, or `{condition: ...}`, the first moment a condition holds |
| `drain` | `true` stops the arrivals at `until` and runs on until no entity is left. `{limit: 1 h}` also stops the run at most that long after `until`. `desflow run --drain` sets it |
| `on_unhandled_failure` | what a failure of a task, child flow or one-shot signal does when nothing handles it. `continue` (the default) defuses it, so the run goes on. `stop` lets it stop the run, as plain SimPy does |
| `before` | a list of `!python` functions called with no arguments before the run, for setup such as creating topics |

---

## Running a Blueprint

`desflow run FILE` builds the blueprint and runs it with its own sinks until `--until`, `run.until` or Ctrl+C. It takes a YAML file only. A Python builder file runs with `python FILE`. Two options print the records instead of sending them to the file's sinks:

| Command | Behaviour |
|---|---|
| `desflow run FILE --sample N` | Prints the first `N` events, then stops. `N` is 1 or more. The run is unpaced: `factor` is set to 0 and `go_live_at` is cleared. `logical_start_time` and `random_seed` are kept, so timestamps and values look as they would in the real run. Ingress entries and `run.before` are skipped. Telemetry records print between the events but are not counted, and `system.simulation.lag_seconds` is not printed. |
| `desflow run FILE --stdout` | Prints every record of a normal run except the `system.simulation.lag_seconds` readings, which describe the wall clock, not the model. `factor`, `go_live_at`, `logical_start_time` and ingress are kept; `run.before` is skipped. |

Both replace every egress entry with one `Console` of `format: json`, and a second one for labels when the file has a `when: labels` entry. A JSON console writes each record as one JSON line on stdout. The run's log stays on stderr, so `desflow run FILE --sample 10 | jq .value` reads only the records.

A preview checks the whole file, but not the connectors it leaves out:

- The structure, keys and names of the whole file are validated.
- `run.before` is skipped, because its functions prepare the real sinks, such as creating topics. A warning on stderr says so when the file has any.
- The connectors a preview replaces or skips are not built, so only their `type` is checked. Their `config` is checked only by a run that builds them, such as `desflow run FILE` without these options.

| Combination | Result |
|---|---|
| `--sample` with `--stdout` | Allowed. `--stdout` adds nothing. |
| `--sample` with `--until`, or a file with `run.until` | The run ends at whichever comes first. When `until` comes first, stderr says `sample ended at until with k of N events`. |
| `--sample` on a file with `go_live_at` | A backfill-then-live file prints its first history events. `when: history` and `when: live` have no effect, because their entries are replaced. |
| `--sample` on a file that publishes no events | It runs until `until` or Ctrl+C, printing telemetry. |
| `--stdout` on a file with several egress entries | One console prints each record once. Per-entry `batch_size` and `flush_interval` are dropped with the entries, and the `batching` section applies. |
| `--sample` or `--stdout` with `--replications` | Usage error, exit code 2: both print one run. |
| `--sample 0` | Usage error, exit code 2. |

[CLI](cli/index.md) lists every command and option.

---

## Include a File

`!include path` puts a mapping from another YAML file in place, such as the settings `desflow fit` writes: `packing: !include fitted/packing.yaml`. The path is relative to the file that names it. `!include {file: path, spawn: pack}` adds keys to the file's mapping, such as the task an arrival spawns. The file is read with the same loader, so `${VAR}`, `!python` and `!include` work in it. A file that includes itself, directly or through another file, is refused. [Input Fitting](../user-guide/input-fitting.md#load-the-settings) shows it.

---

## Security

A blueprint with no `!python` references is read entirely by `yaml.SafeLoader`, and loading it imports only the connector modules its `type` entries name. An `!include` reads another YAML file the same way and imports nothing itself. A file that references Python, directly or in a file it includes, runs the imports it names. Such a file needs the same trust as a Python script: run blueprints only from people whose scripts you would run. [Call Python from YAML](../guides/yaml-advanced.md#security) explains.
