# Roadmap

Releases come in the order shown, without dates: a release ships when it is ready. The timeline lists released work, marked with a tick, then the work planned for the coming releases, in order, marked with a clock. Each item links to its GitHub issue.

## Now and next

Rename to desflow

Done in 0.20.0: the package, its import name and its command are all `desflow`. The old `dynamic-des` names still work, with a warning, until a later release.

Distributions and seeds

Done in 0.21.0: more distributions, including `empirical`, which draws from values measured in a real system. Each distribution draws from its own random stream, so adding an arrival leaves the other draws unchanged. desflow now needs Python 3.11 or later. [#42](https://github.com/jaehyeon-kim/desflow/issues/42)

Ingress messages

Done in 0.21.0: every ingress connector, Kafka, Redis and PostgreSQL, takes a parameter change as `path` and `value`, the names a scenario step already uses. [#51](https://github.com/jaehyeon-kim/desflow/issues/51)

Stores, containers and priority

Done in 0.21.0: tasks take items from stores and amounts from containers, and a resource serves its queue oldest first, newest first or by task priority. [#43](https://github.com/jaehyeon-kim/desflow/issues/43)

Flows

Done in 0.21.0: routing between tasks by a condition or by probability, entity state that an order keeps from task to task, and delays without a resource. [#44](https://github.com/jaehyeon-kim/desflow/issues/44)

Statistics and ground truth

Done in 0.21.0: wait and service times, throughput, utilisation and queue length as telemetry, with a warm-up period; and ground-truth records of what a scenario changed, so a detector or an agent can be scored against them. [#45](https://github.com/jaehyeon-kim/desflow/issues/45)

Native core parity

Done in 0.22.0: the operations on the SimPy Compatibility page, natively in YAML, the builder and the low-level API, each checked against SimPy: signals, interrupts, tasks needing several resources, filtered store gets, stopping and stepping a run, and customers who give up on a queue or never join it. [#64](https://github.com/jaehyeon-kim/desflow/issues/64)

Breakdowns, changing demand and replay

Done in 0.22.0: a resource that fails and is repaired, a schedule of times at which an arrival rate or a capacity changes, and arrivals read from a local CSV or Parquet file. [#65](https://github.com/jaehyeon-kim/desflow/issues/65)

Replications

Done in 0.22.0: run a model with many seeds and get each statistic with a confidence interval, so a result is never read from one run. [#59](https://github.com/jaehyeon-kim/desflow/issues/59)

Experiments

Done in 0.22.0: compare designs and sweep parameters over replications, fail an experiment when a result breaks a limit, fit a distribution to measured data, and write a Markdown report, all on one machine. [#66](https://github.com/jaehyeon-kim/desflow/issues/66)

Speed

Done in 0.22.0: faster runs that publish the same records. [#24](https://github.com/jaehyeon-kim/desflow/issues/24)

Record contract

Done in 0.22.0: every record field documented with its type and format, payload keys such as `__table__` reserved for desflow, a `timezone` setting for timestamps, and no `lag_seconds` from a run at full speed. The format is frozen before 1.0. [#51](https://github.com/jaehyeon-kim/desflow/issues/51)

SimPy parity

In 0.23.0: every SimPy 4.1.2 feature natively in YAML, the builder and the low-level API, each checked against SimPy, such as nested conditions, event callbacks, unlimited stores and custom resource types. The SimPy Compatibility page lists each one, and the two differences kept by design. [#71](https://github.com/jaehyeon-kim/desflow/issues/71)

Blueprint schema and AI skill

A JSON Schema for blueprints, shipped in the package, so editors complete keys and flag mistakes as you type, and a skill that teaches an AI assistant to write and check blueprints. [#41](https://github.com/jaehyeon-kim/desflow/issues/41)

Downstream checks

With the blueprint schema: before a release, the tests of projects that use desflow, the benchtop data generators, run against the new version, and a test fails when a public class, function or argument is removed or changed. [#67](https://github.com/jaehyeon-kim/desflow/issues/67)

desflow explain

Shows a simulation as a graph, marks each resource OK, congested or unstable (its queue grows without limit), names the bottleneck and suggests changes. `desflow run --sample` prints a few records from a blueprint. [#47](https://github.com/jaehyeon-kim/desflow/issues/47)

Learn section

The shop tutorial, which extends one shop model page by page and answers a question with a number on each page, has 20 pages since 0.22.0, which added replications, comparing designs, sweeps, fitting, patience, interruptions, end conditions and more. Left: concept pages on a simulation study, checking and validating, queueing and output analysis, a final study of the whole shop and an independent problem. [#60](https://github.com/jaehyeon-kim/desflow/issues/60)

## Later

The work after the Learn section, on the way to 1.0, in the order it is planned.

- **Storage and Python 3.15**

  ______________________________________________________________________

  Parquet and JSONL on more cloud storage, more Iceberg catalogs, and Python 3.15 support.

  [#32](https://github.com/jaehyeon-kim/desflow/issues/32), [#40](https://github.com/jaehyeon-kim/desflow/issues/40)

- **Library hooks for the server**

  ______________________________________________________________________

  Changes the server will need, made before 1.0 so the server adds to the library without changing it: parameter changes that report who made them and why, and a way to load and inspect a simulation folder.

  [#70](https://github.com/jaehyeon-kim/desflow/issues/70)

- **Reserve the gen key**

  ______________________________________________________________________

  Before 1.0, a blueprint refuses a mapping with a `gen` key in a payload, a scenario value, a `record` field, a task's `set` or an arrival's `attributes`. After 1.0, `gen` marks a generated value, such as `{gen: uniform, ...}`, so no 1.0 blueprint can already use the key for something else.

  [#53](https://github.com/jaehyeon-kim/desflow/issues/53)

- **Record contract freeze**

  ______________________________________________________________________

  A final check of every record field, then the format is marked frozen: no 1.x release removes a field, renames it or changes its type.

  [#51](https://github.com/jaehyeon-kim/desflow/issues/51)

- **1.0**

  ______________________________________________________________________

  The generated YAML key reference, a command-line page, a compatibility policy and upgrade notes.

  [#54](https://github.com/jaehyeon-kim/desflow/issues/54)

## What 1.0 means

From 1.0, the YAML blueprint format, the `desflow` command, the public Python API (`SimulationContext`, `DynamicRealtimeEnvironment` and the connectors) and the published record format stay compatible across every 1.x release. The package status moves from Beta to Production/Stable.

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
- **Visualisation:** animation of a running or recorded simulation, and a map view.
- **Distributed runs:** workers on several machines.

### Writing simulations

- **Generated values in blueprints:** random values, IDs and timestamps in YAML, then entities, relations between records, states and ramps.
- **Preview loop:** run a blueprint again each time the file changes, and check a file without running it.
- **Example gallery and visual builder.**
- **Conversion:** turn a YAML blueprint into builder code, and builder code into YAML with no Python, with an error that names each part that cannot be converted and how to rewrite it.

### Connectors

- **More sinks:** a webhook sink, a generic SQL sink and more.
- **Delivery faults:** records delivered late, twice, damaged or not at all, to test how a pipeline handles them.
- **Lineage:** each run reported to OpenLineage and OpenMetadata.

### Platform

- **More speed:** cheaper record publishing, which takes about 40 percent of a run, with the same published records ([#69](https://github.com/jaehyeon-kim/desflow/issues/69)), and a faster runtime for YAML-only blueprints if measurements show the need.

### Docs

- **Reference:** a page for each generator and connector, with examples and the records they produce, and a command-line cheat sheet.
- **How-To:** generating data at scale, and testing pipelines with simulated data.
