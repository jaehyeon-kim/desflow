# Roadmap

Releases come in the order shown, without dates: a release ships when it is ready. Each item links to its GitHub issue.

## Before 1.0

::timeline::
- title: '0.27.0: Schema registry formats'
  content: 'JSON Schema and Protobuf serializers and deserializers for the Confluent and Glue schema registries, next to Avro. desflow writes the Glue wire format itself and no longer depends on `aws-glue-schema-registry`, so the `kafka` extra also drops kafka-python and creates topics through aiokafka. [#82](https://github.com/jaehyeon-kim/desflow/issues/82)'
  icon: ':material-play:'
  key: now
- title: '0.28.0: Library hooks for the server'
  content: 'Parameter changes that report who made them and why: `change` returns a `ParamChange` or raises `ParamRejected`, an `on_change` list of callbacks receives each accepted change, and `registry_paths()` lists the paths a simulation will have without running it. The server after 1.0 then adds to the library without changing it. `FileIngress` changes parameters during a run when you edit a local file, with no broker. [#70](https://github.com/jaehyeon-kim/desflow/issues/70) [#86](https://github.com/jaehyeon-kim/desflow/issues/86)'
  icon: ':material-clock-outline:'
  key: planned
- title: '0.29.0: Reserved gen key, SimPy''s examples and record contract freeze'
  content: 'After 1.0, a `gen` key marks a generated value. To keep the key free, a blueprint refuses a mapping with a `gen` key in any of these places: a payload, a scenario value, a `record` field, an action or timeout value, a task''s `set` or an arrival''s `attributes`. The Pydantic floor rises to 2.6.1. The seven examples of the SimPy docs, such as Bank Renege and Machine Shop, are written in YAML, the builder and the low-level API, each beside SimPy''s own. Every record field is checked, then the format is marked frozen: no 1.x release removes a field, renames it or changes its type. [#53](https://github.com/jaehyeon-kim/desflow/issues/53), [#95](https://github.com/jaehyeon-kim/desflow/issues/95), [#51](https://github.com/jaehyeon-kim/desflow/issues/51)'
  icon: ':material-clock-outline:'
  key: planned
- title: '0.30.0: Preparation for 1.0'
  content: 'The names kept for compatibility are removed, with the removals listed in Upgrading to 1.0. The docs gain a Compatibility Policy page and a warning on the docs of older versions. This release changes no format, so 1.0.0 follows a release with no format change. [#84](https://github.com/jaehyeon-kim/desflow/issues/84), [#54](https://github.com/jaehyeon-kim/desflow/issues/54)'
  icon: ':material-clock-outline:'
  key: planned
- title: '1.0.0: Stable'
  content: '1.0.0 sets the version number and the Production/Stable classifier, and adds its Changelog section. From here the format, the command, the public API and the record format stay compatible across 1.x. [#54](https://github.com/jaehyeon-kim/desflow/issues/54)'
  icon: ':material-clock-outline:'
  key: planned
::/timeline::

## What 1.0 means

From 1.0, the YAML blueprint format, the `desflow` command, the public Python API (`SimulationContext`, `DynamicRealtimeEnvironment` and the connectors) and the published record format stay compatible across every 1.x release. The package status moves from Beta to Production/Stable. The names kept for compatibility, `dynamic_des`, `ddes` and the old registry settings, are removed in 0.30.0, and Upgrading to 1.0 lists each.

## After 1.0

### Simulation model

- **Maintenance, schedules and incidents:** planned maintenance, a failure clock that runs only while a resource works, schedules that repeat by time of day, and incidents that change several parameters at once. 0.22.0 ships random breakdowns with repair and finite schedules.
- **More flow and resource features:** batching, matching and splitting, setup times, and resources chosen by skill.
- **Reuse, costs and continuous flow:** parts of a model defined once and used several times, costs and energy per resource state, and levels that rise and fall continuously.
- **Transport on maps:** vehicles that move between locations on real roads and publish their positions.
- **Data-driven inputs:** arrivals replayed from a database, or taken live from a Kafka topic.
- **Decisions from outside:** a dispatcher or an agent answers for one order or session, and a run can repeat exactly from a log of those answers.

### Analysis

- **Experiments:** experiment designs, optimisation, and calibration against observed data.
- **Fitting and comparison:** distribution fitting from more data sources, automatic fault-impact experiments, and fuller run comparison and fidelity scoring.

### Simulation server

The server ships after 1.0, as 1.x releases installed with the `server` extra.

- **Server API and MCP tools:** `desflow serve` runs simulations through a REST API: upload, start, stop, change parameters during a run and follow its events. MCP tools, which let an AI agent do the same, are experimental. ([#48](https://github.com/jaehyeon-kim/desflow/issues/48))
- **Browser UI:** the same from a browser, including each simulation's graph from `desflow explain`. ([#49](https://github.com/jaehyeon-kim/desflow/issues/49))
- **Schedules:** run simulations on cron schedules. ([#50](https://github.com/jaehyeon-kim/desflow/issues/50))
- **Server image and odctl profile:** a published image and an [odctl](https://github.com/jaehyeon-kim/odctl) profile run the server locally with nothing installed on the host. ([#52](https://github.com/jaehyeon-kim/desflow/issues/52))
- **Visualisation:** a graph image from `desflow explain`, animation of a running or recorded simulation, and a map view.
- **Distributed runs:** workers on several machines.

### Writing simulations

- **Generated values in blueprints:** random values, IDs and timestamps in YAML, then entities, relations between records, states and ramps.
- **Preview loop:** run a blueprint again each time the file changes, and check a file without running it.
- **Tests in blueprints:** results a model must keep at a fixed seed, checked by `desflow test`, which can also write the values from a run for you to review.
- **Variables from the command line:** `--var NAME=VALUE` fills `${NAME}` in a blueprint, as an environment variable does today.
- **Example gallery and visual builder.**
- **Conversion:** turn a YAML blueprint into builder code, and builder code into YAML with no Python, with an error that names each part that cannot be converted and how to rewrite it.

### Connectors

- **More sinks:** a webhook sink, a generic SQL sink and more.
- **Error sink:** records a sink could not write go to another sink instead of being lost, and a run can fail when too many do.
- **Delivery faults:** records delivered late, twice, damaged or not at all, to test how a pipeline handles them.
- **Lineage:** each run reported to OpenLineage and OpenMetadata.

### Platform

- **More speed:** cheaper record publishing, which takes about 40 percent of a run's time, with the same published records ([#69](https://github.com/jaehyeon-kim/desflow/issues/69)), and a faster runtime for YAML-only blueprints if measurements show the need.

### Docs

- **Reference:** a page for each generator and connector, with examples and the records they produce, and a command-line cheat sheet.
- **How-To:** generating data at scale, and testing pipelines with simulated data.

## Released

??? note "0.26.0: Storage, schema registry code and Python 3.15"

    - **Storage:** Parquet and JSON Lines files on Google Cloud Storage and Azure Blob Storage, and Iceberg tables through SQL catalogs on SQLite or PostgreSQL and the Glue catalog. [#32](https://github.com/jaehyeon-kim/desflow/issues/32)
    - **Schema registry code:** the Confluent schema registry client moves into desflow, so the `confluent` extra no longer installs confluent-kafka. [#83](https://github.com/jaehyeon-kim/desflow/issues/83)
    - **Python 3.15** is supported. [#40](https://github.com/jaehyeon-kim/desflow/issues/40)
    - **Fix:** `GlueAvroSerializer` serializes a record. [#85](https://github.com/jaehyeon-kim/desflow/issues/85)
    - **Docs:** page addresses follow the sidebar, and each page kind has one shape.

??? note "0.25.0: desflow explain"

    - **desflow explain:** `desflow explain` shows a simulation as a graph, marks each resource OK, congested or unstable (its queue grows without limit), names the bottleneck and suggests changes. [#47](https://github.com/jaehyeon-kim/desflow/issues/47)
    - **Commands:** `desflow dist` converts between a distribution's parameters and its mean and standard deviation. `desflow run --sample` prints a few records from a blueprint, and `--stdout` sends every record except the `system.simulation.lag_seconds` readings to the standard output as JSON lines in place of the real sinks.
    - **Library:** `load_context` loads a simulation folder from Python. Builder arrivals can name the task they start.
    - **Docs:** a generated reference page for the `desflow` command, and in Learn, Complete Model, the `explain` section of Checking a Model and Check Against Theory. [#60](https://github.com/jaehyeon-kim/desflow/issues/60)

??? note "0.24.0: Blueprint schema and AI skill"

    - **Blueprint schema and AI skill:** a JSON Schema for blueprints, shipped in the package and on the docs site, so editors complete keys and flag mistakes as you type, with a YAML Keys reference page generated from it. A skill that teaches an AI assistant to write and check blueprints. [#41](https://github.com/jaehyeon-kim/desflow/issues/41)
    - **Public API test:** a test that fails when a public name or signature of the library or the `desflow` command is removed or changed. [#67](https://github.com/jaehyeon-kim/desflow/issues/67)
    - **Removal:** `resource.queue.items`, which warns since 0.23.0, is removed: use `list(resource.queue)`.
    - **Learn section:** the concept pages, Checking a Model, Live Changes, Streaming Out and the How-To pages that do not need `explain`. [#60](https://github.com/jaehyeon-kim/desflow/issues/60)

??? note "0.23.0: SimPy parity"

    - **SimPy parity:** every SimPy 4.1.2 feature natively in YAML, the builder and the low-level API, each checked against SimPy, such as nested conditions, event callbacks, unlimited stores and custom resource types. The SimPy Compatibility page lists each one, and the three differences kept by design. [#71](https://github.com/jaehyeon-kim/desflow/issues/71)

??? note "0.22.0: Native core, experiments and replications"

    - **Native core parity:** the operations on the SimPy Compatibility page work natively in YAML, the builder and the low-level API, and each is checked against SimPy. They include signals, interrupts, tasks needing several resources, filtered store gets, stopping and stepping a run, and customers who give up on a queue or never join it. [#64](https://github.com/jaehyeon-kim/desflow/issues/64)
    - **Breakdowns, changing demand and replay:** a resource that fails and is repaired, a schedule of times at which an arrival rate or a capacity changes, and arrivals read from a local CSV or Parquet file. [#65](https://github.com/jaehyeon-kim/desflow/issues/65)
    - **Replications:** run a model with many seeds and get each statistic with a [95% interval](../learn/concepts/output-analysis.md#interval-and-half-width), so a result is never read from one run. [#59](https://github.com/jaehyeon-kim/desflow/issues/59)
    - **Experiments:** compare designs and sweep parameters over [replications](../learn/concepts/output-analysis.md#replications), fail an experiment when a result breaks a limit, fit a distribution to measured data, and write a Markdown report, all on one machine. [#66](https://github.com/jaehyeon-kim/desflow/issues/66)
    - **Speed:** faster runs that publish the same records. [#24](https://github.com/jaehyeon-kim/desflow/issues/24)
    - **Record contract:** every record field documented with its type and format, payload keys such as `__table__` reserved for desflow, a `timezone` setting for timestamps, and no `lag_seconds` from a run at full speed. [#51](https://github.com/jaehyeon-kim/desflow/issues/51)
    - **Learn section:** the shop tutorial extends one shop model page by page and answers a question with a number on each page. It has 20 pages, and the new pages cover replications, comparing designs, sweeps, fitting, patience, interruptions, end conditions and more. [#60](https://github.com/jaehyeon-kim/desflow/issues/60)

??? note "0.21.0: Distributions, flows and statistics"

    - **Distributions and seeds:** more distributions, including `empirical`, which draws from values measured in a real system. Each distribution draws from its own random stream, so adding an arrival leaves the other draws unchanged. desflow needs Python 3.11 or later. [#42](https://github.com/jaehyeon-kim/desflow/issues/42)
    - **Ingress messages:** every ingress connector, Kafka, Redis and PostgreSQL, takes a parameter change as `path` and `value`, the names a scenario step already uses. [#51](https://github.com/jaehyeon-kim/desflow/issues/51)
    - **Stores, containers and priority:** tasks take items from stores and amounts from containers, and a resource serves its queue oldest first, newest first or by task priority. [#43](https://github.com/jaehyeon-kim/desflow/issues/43)
    - **Flows:** routing between tasks by a condition or by probability, entity state that an order keeps from task to task, and delays without a resource. [#44](https://github.com/jaehyeon-kim/desflow/issues/44)
    - **Statistics and ground truth:** wait and service times, throughput, [utilisation](../learn/concepts/queueing-basics.md#utilisation) and queue length as telemetry, with a [warm-up](../learn/concepts/output-analysis.md#one-run-starts-empty) period; and ground-truth records of what a scenario changed, so a detector or an agent can be scored against them. [#45](https://github.com/jaehyeon-kim/desflow/issues/45)

??? note "0.20.0: Rename to desflow"

    - **Rename to desflow:** the package, its import name and its command are all `desflow`. The old names, `dynamic-des`, `dynamic_des` and `ddes`, still work with a warning until 0.30.0.

The [Changelog](changelog.md) lists every release.
