# Python API

Every name `from desflow import ...` gives, with the extra it needs and the first line of its docstring. A name with an extra is there only when the extra is installed, with `pip install "desflow[<extra>]"`. https://jaehyeon.me/desflow/latest/reference/api/ has each one in full.

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
| `Entity` | none | One thing moving through the model, such as an order, a slab or a session. |
| `EventPayload` | none | Schema for high-volume discrete simulation events. |
| `ExperimentResult` | none | What an experiment found, as `run_experiment` returns it and its artifacts hold it. |
| `ExperimentSpec` | none | A whole experiment file, or the same settings given in Python. |
| `FileIngress` | none | Ingress provider that applies the entries of a local YAML file when you edit it. |
| `FitResult` | none | The outcome of fitting one column: every candidate, the best and the data. |
| `IcebergStorageEgress` | `iceberg` | Apache Iceberg table writer for lakehouse ingestion patterns. |
| `JsonlStorageEgress` | `parquet` | High-throughput JSONL batch writer for local file systems or object storage. |
| `KafkaAdminConnector` | `kafka` | Unified Kafka Admin and Monitoring Connector. |
| `KafkaEgress` | `kafka` | High-throughput Kafka producer for simulation telemetry and events. |
| `KafkaIngress` | `kafka` | Resilient Kafka consumer for dynamic simulation configuration updates. |
| `LabelPayload` | none | Schema for ground-truth records: what a scenario step changed, and why. |
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
| `Sampler` | none | Generates random numbers based on live `DistributionConfig` objects. |
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
| `BoundPayload` | A `Template` bound to an entity and to fields that overwrite generated ones, such as the `id_field` of a task. A task renders it where it would publish a plain payload. |
| `Template` | A payload of generated values, for a builder or low-level script. |
| `generator` | Validates one generator written as a mapping, such as `{"gen": "uniform", "low": 2, "high": 200}`, and gives the typed object. |
