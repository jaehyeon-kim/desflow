# YAML Blueprints, from First File to Connectors

A blueprint puts the configuration of a simulation in a YAML file: the resources, the distributions, the connectors and the experiment. This guide starts with a first file, then adds a scripted experiment, connectors, and settings that change from one environment to the next. Every file on this page is plain YAML.

The [YAML Blueprints reference](https://jaehyeon.me/desflow/latest/architecture/yaml/index.md) lists every section and field. Every file on this page is in the repository under [`docs/snippets/`](https://github.com/jaehyeon-kim/desflow/tree/main/docs/snippets), and the tests build each one.

______________________________________________________________________

## When to use a blueprint

Use a blueprint when the configuration is what changes between runs: a new arrival rate, a second packer, a different broker, a capacity experiment. A YAML file is easier to review and to diff than a builder chain, and a run is one command.

Stay in Python when most of the program is logic. A blueprint can also reference Python objects with `!python` for the logic YAML cannot express, as [Python in a Blueprint](https://jaehyeon.me/desflow/latest/guides/yaml-advanced/index.md) explains.

______________________________________________________________________

## Write a first blueprint

The smallest useful blueprint declares a packer, how long packing takes, how often orders arrive, and what happens to each order:

docs/snippets/yaml/first.yaml

```yaml
# A first blueprint: one packer, one kind of order, printed to the terminal.
simulation:
  sim_id: Shop
  # 0 runs as fast as the machine allows, so the run ends at once.
  factor: 0
  random_seed: 1

egress:
  - type: Console

resources:
  packers: {current_cap: 1, max_cap: 3}

services:
  packing: {dist: normal, mean: 4.0, std: 1.0}

arrivals:
  orders: {dist: exponential, rate: 0.2, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {status: finished}
    id_field: order_id

run:
  until: 60
```

Run it with the command the package installs:

```bash
desflow run first.yaml
```

Every section of a blueprint becomes a builder call:

Each section maps to one builder call. `resources` is `add_resource`, `services` is `add_service`, and `arrivals` is `add_arrival`. `spawn: pack_order` starts the `pack_order` task on every arrival, which is the arrival loop a Python script writes by hand. The task requests a packer, waits a sampled packing time and publishes its `payload`:

```text
[EVT] {'sim_ts': 5.365, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Shop.service.packing', 'status': 'queued'}}
[EVT] {'sim_ts': 5.365, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Shop.service.packing', 'status': 'started'}}
[EVT] {'sim_ts': 8.062, 'timestamp': '...', 'key': 'task-0', 'value': {'status': 'finished', 'order_id': 0}}
```

`factor: 0` runs as fast as the machine allows, so 60 simulation seconds end at once. Set `factor: 1` to pace the run in real time. A mistake stops the load with the file and the line, before anything runs:

```text
Error: first.yaml:22: task 'pack_order' uses service 'packers', which is not defined under services
```

______________________________________________________________________

## Script an experiment

A `scenario` changes registry values at set simulation times. Here a second packer starts at 30 seconds, orders arrive twice as fast from one minute, and the extra packer leaves at two minutes:

docs/snippets/yaml/scenario.yaml

```yaml
# The first blueprint with a scripted experiment: a second packer starts at 30 s,
# orders arrive twice as fast from 1 min, and the extra packer leaves at
# 2 min. Every change lands at exactly that simulation time, on every run.
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 1

egress:
  - type: Console

resources:
  packers: {current_cap: 1, max_cap: 3}

services:
  packing: {dist: normal, mean: 4.0, std: 1.0}

arrivals:
  orders: {dist: exponential, rate: 0.2, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {status: finished}
    id_field: order_id

telemetry:
  # Shows each change as it lands.
  - interval: 10
    publish:
      packers.capacity: packers.capacity
      packers.queue_length: packers.queue_length

scenario:
  - {at: 30, path: Shop.resources.packers.current_cap, value: 2}
  - {at: 1 min, path: Shop.arrival.orders.rate, value: 0.4}
  - {at: 2 min, path: Shop.resources.packers.current_cap, value: 1}

run:
  until: 3 min
```

The telemetry entry shows the change land:

```text
[TEL] {'sim_ts': 20.0, 'timestamp': '...', 'path_id': 'Shop.packers.capacity', 'value': 1}
[TEL] {'sim_ts': 60.0, 'timestamp': '...', 'path_id': 'Shop.packers.capacity', 'value': 2}
```

The steps run on the simulation clock, so the experiment repeats exactly on every run and works at `factor: 0`. Every path is checked when the file is loaded. A misspelt path such as `Shop.resources.packer.current_cap` stops the load with its line, rather than being ignored at the moment it is due.

A path names one value in the registry: `<sim_id>.resources.<name>.current_cap`, `<sim_id>.arrival.<name>.rate` for an exponential arrival, `<sim_id>.service.<name>.mean`, or `<sim_id>.variables.<name>`. A scenario can run beside an ingress connector, so a scripted baseline can run while an operator steers over Kafka. [Scenarios versus `LocalIngress`](https://jaehyeon.me/desflow/latest/architecture/registry/#scenarios-versus-localingress) explains why a scenario is not a `LocalIngress` schedule.

______________________________________________________________________

## Connect to the outside

Connectors are listed under `ingress` and `egress`. `type` is a short name, and `config` holds the constructor's arguments. This is the Redis example, which writes to a stream and takes updates from a channel:

examples/yaml/redis.yaml

```yaml
# Redis Streams output with live parameter updates, in YAML.
#
# The YAML version of examples/builder/redis_example.py: an online shop that records
# each order as it is placed. Each order spawns record_order, a task with no service
# or resource, which publishes its payload at once. RedisEgress writes it to the
# order_events stream that the __stream__ key names, while RedisIngress subscribes to
# the simulation_params channel.
#
# Needs Valkey: odctl up valkey. Runs until interrupted with Ctrl + C.
# Run it with: desflow run examples/yaml/redis.yaml

simulation:
  sim_id: Shop
  factor: 1.0
  random_seed: 42

ingress:
  - type: Redis
    config:
      # The odctl valkey profile disables the unauthenticated default user, so the
      # URL carries credentials.
      url: redis://user:password@localhost:6379/0
      channel_name: simulation_params

egress:
  - type: Redis
    config:
      url: redis://user:password@localhost:6379/0
      stream_name: events

arrivals:
  orders: {dist: exponential, rate: 2.0, spawn: record_order}

tasks:
  record_order:
    # id_field adds the task id as order_id.
    payload: {__stream__: order_events, channel: web, status: placed}
    id_field: order_id
```

The other options of `add_egress` sit beside `config`: `when` routes records, and `batch_size` and `flush_interval` give one sink its own cadence. `when: history` sends the records stamped before `simulation.go_live_at`, and `when: live` sends the rest. `batching` sets the defaults, as `with_batching` does. The [YAML examples](https://jaehyeon.me/desflow/latest/examples/local/index.md) cover every connector the builder examples use: Console, Kafka, Parquet, Iceberg, Postgres and Redis, and the backfill-then-live run that sends history to Parquet and the live tail to Kafka.

Settings that the Python API takes as objects are plain values in `config`:

- A Parquet or JSONL `filesystem` is a mapping. `type` is `local` or `s3`, and the other keys are passed to PyArrow's `S3FileSystem`. A key whose value is empty is left out. The destination folder is created on the first write.
- An Iceberg `catalog` is a mapping of PyIceberg catalog properties, and `schemas` names a type for each column, such as `string`, `double` or `timestamp`.
- Without a router, Parquet, JSONL and Iceberg write each event to `default_path` or `default_table` as one flat row, and leave telemetry out.
- Kafka creates its event and telemetry topics, and Postgres creates the tables listed under `tables`, when the run starts.

______________________________________________________________________

## Change settings per environment

`${VAR}` in a value is replaced with the environment variable, and `${VAR:-default}` uses the default when the variable is unset or empty. An unquoted value is read as YAML reads it after the replacement, so `port: ${PG_PORT:-5432}` is the number 5432. `$${` writes a literal `${`. The Kafka example reads its broker this way, as `bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}`, and the [Parquet example](https://jaehyeon.me/desflow/latest/examples/parquet/index.md) switches from the local disk to S3 with five variables.

`logical_start_time` and `go_live_at` take `now`, a signed duration from now such as `-1d` or `-10m`, or an ISO datetime. Both are read against one moment, when the file is loaded, so `-10m` and `now` are exactly ten minutes apart. The [backfill-then-live example](https://jaehyeon.me/desflow/latest/examples/backfill-live/index.md) uses both, with `when: history` and `when: live`.

docs/snippets/yaml/environment.yaml

```yaml
# Settings that change per environment, read from environment variables.
# Run it with: SIM_FACTOR=1 desflow run environment.yaml
simulation:
  sim_id: ${SIM_ID:-Shop}
  # An unquoted value is read after the replacement, so this is the number 0
  factor: ${SIM_FACTOR:-0}
  # Read against the moment the file is loaded: a day of history up to now
  logical_start_time: -1d

egress:
  - type: Console

resources:
  # Quoted, because ${...} inside {...} would end the mapping early; the check
  # turns the text into a number
  packers: {current_cap: "${PACKERS:-2}", max_cap: 5}

run:
  until: ${RUN_FOR:-1 hour}
```

______________________________________________________________________

## Limitations

A blueprint is configuration. These stay in Python:

- **Field values with logic.** A mapping payload is a constant, apart from the task id that `id_field` adds. Random or weighted choices, derived fields and state carried between events are a Python payload or process, as the [advanced guide](https://jaehyeon.me/desflow/latest/guides/yaml-advanced/index.md) shows.
- **Containers and stores as SimPy objects.** `containers` registers capacities in the registry only, as `add_container` does. A process that needs a SimPy container builds a `DynamicContainer`, or reads and writes the registry path as the hot rolling twin does. Stores have no builder method, so neither API registers them.
- **Anything beyond one task on one resource.** Multi-stage routing, several resources per task, priorities and preemption are processes.
- **Telemetry other than resource statistics.** Container levels, variables and derived metrics need a telemetry `function`.

The [reference](https://jaehyeon.me/desflow/latest/architecture/yaml/#limitations) lists the full set.
