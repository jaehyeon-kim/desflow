# Python API

Every name `from desflow import ...` gives, with the first line of its docstring. https://jaehyeon.me/desflow/latest/api/ has each one in full.

| Name | What it is |
|---|---|
| `BreakdownConfig` | How a resource breaks down: the time to each failure, the repair time, and what happens to the work in progress. |
| `CapacityConfig` | Configuration for the capacity of a simulated resource, container, or store. |
| `CheckFailed` | An experiment's check failed. The artifacts are written before it is raised. |
| `ConsoleEgress` | Local egress provider that outputs simulation data to the console. |
| `ContainerBase` | A SimPy `Container` that tells `DynamicContainer` about every change, the class to subclass for a container of your own. |
| `ContainerConfig` | Configuration for a container: its capacity and the level it starts at. |
| `DistributionConfig` | Configuration for a statistical distribution used in the simulation. |
| `DynamicContainer` | A wrapper for SimPy Container with dynamic capacity updates. |
| `DynamicRealtimeEnvironment` | The core simulation engine for `desflow`. |
| `DynamicResource` | A SimPy Resource with a dynamically adjustable capacity. |
| `DynamicStore` | A wrapper for SimPy FilterStore and PriorityStore with dynamic capacity updates. |
| `Entity` | One thing moving through the model, such as an order, a slab or a session. |
| `EventPayload` | Schema for high-volume discrete simulation events. |
| `ExperimentResult` | What an experiment found, as `run_experiment` returns it and its artifacts hold it. |
| `ExperimentSpec` | A whole experiment file, or the same settings given in Python. |
| `FitResult` | The outcome of fitting one column: every candidate, the best and the data. |
| `IcebergStorageEgress` | Apache Iceberg table writer for lakehouse ingestion patterns. |
| `JsonlStorageEgress` | High-throughput JSONL batch writer for local file systems or object storage. |
| `KafkaAdminConnector` | Unified Kafka Admin and Monitoring Connector. |
| `KafkaEgress` | High-throughput Kafka producer for simulation telemetry and events. |
| `KafkaIngress` | Resilient Kafka consumer for dynamic simulation configuration updates. |
| `LabelPayload` | Schema for ground-truth records: what a scenario step changed, and why. |
| `LocalIngress` | Ingress provider for deterministic, time-scheduled parameter updates. |
| `LowLevelModel` | A model written with the low-level API, for `replicate` and `run_experiment`. |
| `ParquetStorageEgress` | Columnar Parquet writer for local disks or object storage. |
| `PostgresEgress` | Asynchronous egress provider for persisting simulation data to PostgreSQL. |
| `PostgresIngress` | Asynchronous ingress provider for polling updates from a PostgreSQL database. |
| `RedisEgress` | High-throughput asynchronous egress provider for streaming simulation data to Redis. |
| `RedisIngress` | Asynchronous ingress provider for dynamic parameter updates via Redis Pub/Sub. |
| `Replications` | The result of `replicate`: every value with its seed, and a summary per measure. |
| `ResourceBase` | A resource of units that tasks request, with a capacity that follows the registry. |
| `Router` | Chooses the task that follows a finished one. |
| `Sampler` | Generates random numbers based on live `DistributionConfig` objects. |
| `Signal` | A named event that can fire many times, each firing waking every waiter. |
| `SignalFailed` | The exception a waiter of a failed signal receives. |
| `SimParameter` | The master schema representing the state of a specific simulation entity (like a production line). |
| `SimulationContext` | Builder and Orchestrator for Event-Driven Digital Twins. |
| `SimulationRegistry` | A central registry that maps dot-notation paths to dynamic simulation parameters. |
| `StoreBase` | A SimPy `Store` that tells `DynamicStore` about every change, the class to subclass for a store of your own. |
| `TaskFailed` | A task instance failed, and no `on_fail` handled it. |
| `TelemetryPayload` | Schema for low-volume, single-metric telemetry updates. |
| `Transfer` | What a task takes from, or adds to, a store or a container. |
| `Upstream` | What a finished task hands the task routed after it. |
| `fit_column` | Fits a column of measured times to desflow's distribution families. |
| `load_context` | Builds the simulation that `entry` in `folder` defines. |
| `read_trace` | Reads and checks the rows of a trace, in their order. |
| `replicate` | Runs a model `n` times with spawned seeds and summarises its statistics. |
| `run_breakdowns` | Breaks a resource down and repairs it, for as long as the run lasts. |
| `run_experiment` | Runs an experiment: its variants and sweep, replications, comparisons and checks. |
| `run_scenario` | Applies each step at its time, in time order and, at equal times, in the order given. A step's `at` counts from when this process starts, the start of the run when it is started with the model, so it does not depend on `initial_time`. |
| `time_to_seconds` | Converts a human-readable duration into seconds. |
