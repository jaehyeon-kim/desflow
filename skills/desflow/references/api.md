# Python API

Every name `from desflow import ...` gives, with the extra it needs and the first line of its docstring. A name with an extra is there only when the extra is installed, with `pip install "desflow[<extra>]"`, except `IcebergStorageEgress`, `JsonlStorageEgress` and `ParquetStorageEgress`: they are always there, and building one without its extra raises an `ImportError` that names the extra. https://jaehyeon.me/desflow/latest/reference/api/ has each one in full.

| Name | Extra | What it is |
|---|---|---|
| `BreakdownConfig` | none | How a resource breaks down: the time to each failure, the repair time, and what happens to the work in progress. |
| `CapacityConfig` | none | Configuration for the capacity of a simulated resource, container, or store. |
| `CheckFailed` | none | An experiment's check failed. The artifacts are written before it is raised. |
| `ConsoleEgress` | none | Local egress provider that outputs simulation data to the console. |
| `ContainerBase` | none | A SimPy `Container` that tells `DynamicContainer` about every change, the class to subclass for a container of your own. |
| `ContainerConfig` | none | Configuration for a container: its capacity and the level it starts at. |
| `DistributionConfig` | none | Configuration for a statistical distribution used in the simulation. |
| `DynamicContainer` | none | A wrapper for SimPy Container with dynamic capacity updates. |
| `DynamicRealtimeEnvironment` | none | The core simulation engine for `desflow`. |
| `DynamicResource` | none | A SimPy Resource with a dynamically adjustable capacity. |
| `DynamicStore` | none | A wrapper for SimPy FilterStore and PriorityStore with dynamic capacity updates. |
| `DynamicValue` | none | The atom of the registry. Holds a value and a SimPy event that triggers whenever the value changes. |
| `Entity` | none | One thing moving through the model, such as an order, a slab or a session. |
| `EventPayload` | none | Schema for high-volume discrete simulation events. |
| `ExperimentResult` | none | What an experiment found, as `run_experiment` returns it and its artifacts hold it. |
| `ExperimentSpec` | none | A whole experiment file, or the same settings given in Python. |
| `Expression` | none | A parsed and checked expression, ready to evaluate against a `Scope`. |
| `FileIngress` | none | Ingress provider that applies the entries of a local YAML file when you edit it. |
| `FitResult` | none | The outcome of fitting one column: every candidate, the best and the data. |
| `IcebergStorageEgress` | `iceberg` | Apache Iceberg table writer for lakehouse ingestion patterns. |
| `JsonlStorageEgress` | `parquet` | High-throughput JSONL batch writer for local file systems or object storage. |
| `KafkaAdminConnector` | `kafka` | Unified Kafka Admin and Monitoring Connector. |
| `KafkaEgress` | `kafka` | High-throughput Kafka producer for simulation telemetry and events. |
| `KafkaIngress` | `kafka` | Resilient Kafka consumer for dynamic simulation configuration updates. |
| `LabelPayload` | none | Schema for ground-truth records: what a scenario step changed, and why. |
| `LiveScope` | none | What every scope of a run reads, in one place: the run's clock and registry, its stores, containers, resources, signals and running instances, and the model's families. |
| `LocalIngress` | none | Ingress provider for deterministic, time-scheduled parameter updates. |
| `LowLevelModel` | none | A model written with the low-level API, for `replicate` and `run_experiment`. |
| `NO_RECORD` | none | What a task function returns to finish with no record: the Builder form of a blueprint's `payload: null`. |
| `ParamChange` | none | One accepted change to a registry path. |
| `ParamRejected` | none | Raised by `SimulationRegistry.change` when the registry refuses a value. |
| `ParquetStorageEgress` | `parquet` | Columnar Parquet writer for local disks or object storage. |
| `PostgresEgress` | `postgres` | Asynchronous egress provider for persisting simulation data to PostgreSQL. |
| `PostgresIngress` | `postgres` | Asynchronous ingress provider for polling updates from a PostgreSQL database. |
| `RedisEgress` | `redis` | High-throughput asynchronous egress provider for streaming simulation data to Redis. |
| `RedisIngress` | `redis` | Asynchronous ingress provider for dynamic parameter updates via Redis Pub/Sub. |
| `Replications` | none | The result of `replicate`: every value with its seed, and a summary per measure. |
| `ResourceBase` | none | A resource of units that tasks request, with a capacity that follows the registry. |
| `Router` | none | Chooses the task that follows a finished one. |
| `Row` | none | One arrival of a trace, as `read_trace` gives it (the `row` generator is `desflow.values.Row`). |
| `Sampler` | none | Generates random numbers based on live `DistributionConfig` objects. |
| `Scope` | none | What the names in an expression read. |
| `Signal` | none | A named event that fires once, or many times with `once=False`, each firing waking every waiter. |
| `SignalFailed` | none | The exception a waiter of a failed signal receives. |
| `SimParameter` | none | The master schema representing the state of a specific simulation entity (like a production line). |
| `SimulationContext` | none | Builder and Orchestrator for Event-Driven Digital Twins. |
| `SimulationRegistry` | none | A central registry that maps dot-notation paths to dynamic simulation parameters. |
| `StoreBase` | none | A SimPy `Store` that tells `DynamicStore` about every change, the class to subclass for a store of your own. |
| `TaskFailed` | none | A task instance failed, and no `on_fail` handled it. |
| `TelemetryPayload` | none | Schema for low-volume, single-metric telemetry updates. |
| `Transfer` | none | What a task takes from, or adds to, a store or a container. |
| `Upstream` | none | What a finished task hands the task routed after it. |
| `fit_column` | none | Fits a column of measured times to desflow's distribution families. |
| `load_context` | none | Builds the simulation that `entry` in `folder` defines. |
| `read_trace` | none | Reads and checks the rows of a trace, in their order. |
| `replicate` | none | Runs a model `n` times with spawned seeds and summarises its statistics. |
| `run_breakdowns` | none | Breaks a resource down and repairs it, for as long as the run lasts. |
| `run_experiment` | none | Runs an experiment: its variants and sweep, replications, comparisons and checks. |
| `run_scenario` | none | Applies each step at its time, in time order and, at equal times, in the order given. A step's `at` counts from when this process starts, the start of the run when it is started with the model, so it does not depend on `initial_time`. |
| `time_to_seconds` | none | Converts a human-readable duration into seconds. |

## Values

`from desflow.values import ...` gives these. A Builder task whose payload holds generators, such as `{gen: var, name: n}`, returns a `Template`.

| Name | What it is |
|---|---|
| `Aggregate` | `aggregate`: `mean`, `sum`, `min`, `max` or `count` over the last `last` rows of the task `task` that `where` keeps. `where` is an expression over the row's own fields, such as `lead_days == 3 and precipitation > 0`; without it every row counts. `count` counts rows and takes no `path`; the others read the number at `path`. A reading looks back through the rows the task keeps (its `history`) until `last` rows are kept, and takes the rows there are when `where` keeps fewer. |
| `BoundPayload` | A `Template` bound to an entity and to fields that overwrite generated ones, such as the `id_field` of a task. A task renders it where it would publish a plain payload. |
| `Fake` | `fake`: `getattr(faker, method)(*args, **kwargs)` for a Faker of `locale` (`en_US` by default). It needs the `fake` extra. Each `fake` generator has a Faker of its own, seeded from the run's seed, so adding one changes no other. A date, a time or a decimal result is written as text or a float. |
| `HistogramBin` | One bin of a `lookup` histogram: `bin` is the share of the rows it covers, in the order they were written, and `frequency` is the chance that a draw falls in it. Both are scaled to add up to 1, so `{bin: 2, frequency: 8}, {bin: 8, frequency: 2}` means the same as 0.2 with 0.8, and 0.8 with 0.2. |
| `Lookup` | `lookup`: a row of the finished payloads of the task `task`, or the part of it at `path` (dotted; an integer part indexes a list). |
| `Previous` | `previous`: the last finished payload of the same task for the same stream, or the part of it at `path`. The stream is the arrival, so all the entities it starts share one, except for an arrival with `count` or `keys`, where each entity has its own. `default` is evaluated only when the stream has no finished payload yet, and without one the first reading is null. |
| `Row` | `row`: one row of a CSV or Parquet file as a mapping, read once at load. A cell of digits is read as a number, so a postal code with leading zeros loses them; write them with `{gen: format, template: "{n:05d}"}`. `where` is an expression over the row's columns that keeps some rows; `weight` names a column of weights; `order` is `random` (the default), `sequential`, `shuffle` (every row once, in a random order) or `circular` (in file order, again from the first); `on_end` says what a `sequential` or `shuffle` order does when the rows run out: `error` (the default), `loop` or `stop`, which ends the run. |
| `Template` | A payload of generated values, for a builder or low-level script. |
| `ValueGenerationError` | A generator could not give its value: a bad argument, a missing var, a draw from an empty choice. The message names the place in the file. |
| `ValueRuntime` | The state the generators of one run share, and the means to evaluate them. |
| `generator` | Validates one generator written as a mapping, such as `{"gen": "uniform", "low": 2, "high": 200}`, and gives the typed object. |
