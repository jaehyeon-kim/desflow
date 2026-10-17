# Roadmap

Releases come in the order shown, without dates: a release ships when it is ready. The timeline marks the release that is done, the work that is done in code and ships with the next release, the releases that follow, and the Learn section of the docs. Each links to its GitHub issue.

## Now and next

::timeline::
- title: Rename to desflow
  content: 'Done in 0.20.0: install `desflow`, import `desflow` and run `desflow`.'
  icon: ':material-check:'
  key: done
- title: Distributions and seeds
  content: 'Done in 0.21.0: more distributions, including one built from values taken from real data, each drawing from its own random stream, so adding an arrival leaves the other draws unchanged. desflow now needs Python 3.11 or later. [#42](https://github.com/jaehyeon-kim/desflow/issues/42)'
  icon: ':material-check:'
  key: done
- title: Ingress messages
  content: 'Done in 0.21.0: every ingress connector, Kafka, Redis and PostgreSQL, takes a parameter change as `path` and `value`, the names a scenario step already uses. [#51](https://github.com/jaehyeon-kim/desflow/issues/51)'
  icon: ':material-check:'
  key: done
- title: Stores, containers and priority
  content: 'Done in 0.21.0: stores and containers used by tasks, with a choice of queue order and task priority. [#43](https://github.com/jaehyeon-kim/desflow/issues/43)'
  icon: ':material-check:'
  key: done
- title: Flows
  content: 'Done in 0.21.0: routing between tasks by a condition or by probability, entity state that an order keeps from task to task, and delays without a resource. [#44](https://github.com/jaehyeon-kim/desflow/issues/44)'
  icon: ':material-check:'
  key: done
- title: Statistics and ground truth
  content: 'Done in 0.21.0: wait and service times, throughput, utilisation and queue length as telemetry, with a warm-up period; and ground-truth records of what a scenario changed, so a detector or an agent can be scored against them. [#45](https://github.com/jaehyeon-kim/desflow/issues/45)'
  icon: ':material-check:'
  key: done
- title: Native core parity
  content: 'In 0.22.0: SimPy-equivalent modelling in all three ways to write a model, including waiting on events and processes, interrupts and preemption, competing resources, filtered inventory, run control, and reneging and balking. [#64](https://github.com/jaehyeon-kim/desflow/issues/64)'
  icon: ':material-clock-outline:'
  key: planned
- title: Breakdowns, changing demand and replay
  content: 'In 0.22.0: a resource that fails and is repaired, a finite schedule that changes an arrival rate or a capacity, and replay of a local file as arrivals. [#65](https://github.com/jaehyeon-kim/desflow/issues/65)'
  icon: ':material-clock-outline:'
  key: planned
- title: Replications
  content: 'In 0.22.0: run a model with many seeds and get each statistic with a confidence interval, so a result is never read from one run. [#59](https://github.com/jaehyeon-kim/desflow/issues/59)'
  icon: ':material-clock-outline:'
  key: planned
- title: Local studies
  content: 'In 0.22.0: parameter sweeps, paired comparison, distribution fitting and a report, without a server. [#66](https://github.com/jaehyeon-kim/desflow/issues/66)'
  icon: ':material-clock-outline:'
  key: planned
- title: Speed
  content: 'In 0.22.0: faster runs without changing the published records. [#24](https://github.com/jaehyeon-kim/desflow/issues/24)'
  icon: ':material-clock-outline:'
  key: planned
- title: Record contract
  content: 'In 0.22.0: the record format that 1.0 keeps stable, including a `timezone` setting for timestamps. [#51](https://github.com/jaehyeon-kim/desflow/issues/51)'
  icon: ':material-clock-outline:'
  key: planned
- title: desflow explain
  content: 'Shows a simulation as a graph, says whether each part is stable, points to the bottleneck and suggests changes; and `desflow run --sample` prints a few records from a blueprint. [#47](https://github.com/jaehyeon-kim/desflow/issues/47)'
  icon: ':material-clock-outline:'
  key: planned
- title: Learn section
  content: 'Finishes the shop tutorial, which extends one shop model page by page and answers a question with a number on each page, and adds concept pages on studies, verification and queueing. 0.21.0 opens the Learn section with the first nine tutorial pages. [#60](https://github.com/jaehyeon-kim/desflow/issues/60)'
  icon: ':material-clock-outline:'
  key: planned
::/timeline::

## Later

The work after the Learn section, on the way to 1.0, in the order it is planned.

<div class="grid cards" markdown>

-   __Blueprint schema and AI skill__

    ---

    A JSON Schema for blueprints, shipped in the package, and a skill that teaches an AI assistant to write and check blueprints.

    [#41](https://github.com/jaehyeon-kim/desflow/issues/41)

-   __Server: API and MCP tools__

    ---

    `desflow serve` runs simulations through a REST API: upload, start, stop, change parameters while a run is going and follow its events. Tools for AI agents (MCP) ship with it, or with the next server release, as an experimental part.

    [#48](https://github.com/jaehyeon-kim/desflow/issues/48)

-   __Server: browser UI__

    ---

    The same from a browser, including each simulation's graph from `desflow explain`.

    [#49](https://github.com/jaehyeon-kim/desflow/issues/49)

-   __Server: schedules__

    ---

    Run simulations on cron schedules.

    [#50](https://github.com/jaehyeon-kim/desflow/issues/50)

-   __Server image and odctl profile__

    ---

    A published image and an [odctl](https://github.com/jaehyeon-kim/odctl) profile run the server locally with nothing installed on the host.

    [#52](https://github.com/jaehyeon-kim/desflow/issues/52)

-   __Storage and Python 3.15__

    ---

    Parquet and JSONL on more cloud storage, more Iceberg catalogs, and Python 3.15 support.

    [#32](https://github.com/jaehyeon-kim/desflow/issues/32), [#40](https://github.com/jaehyeon-kim/desflow/issues/40)

-   __Reserve the gen key__

    ---

    A blueprint mapping with a `gen` key is refused, so the value generators planned after 1.0 do not change what a 1.0 blueprint means.

    [#53](https://github.com/jaehyeon-kim/desflow/issues/53)

-   __1.0__

    ---

    The generated YAML key reference, a command-line page, the record format marked frozen, a compatibility policy and upgrade notes.

    [#54](https://github.com/jaehyeon-kim/desflow/issues/54)

</div>

## What 1.0 means

From 1.0, the YAML blueprint format, the `desflow` command, the public Python API (`SimulationContext`, `DynamicRealtimeEnvironment` and the connectors) and the published record format stay compatible across every 1.x release. The package status moves from Beta to Production/Stable.

## After 1.0

### Simulation model

- **Breakdowns, schedules and incidents:** planned maintenance, repeating schedules by time of day, and faults that start at random times.
- **More flow and resource features:** batching, matching and splitting, setup times, and resources chosen by skill.
- **Reuse, costs and continuous flow:** parts of a model defined once and used several times, costs and energy per resource state, and levels that rise and fall continuously.
- **Transport on maps:** vehicles that move between locations on real roads and publish their positions.
- **Data-driven inputs:** arrivals replayed from a database, or taken live from a Kafka topic.
- **Decisions from outside:** a dispatcher or an agent answers for one order or session, and a run can repeat exactly from a log of those answers.

### Analysis

- **Experiments:** experiment designs, optimisation, and calibration against observed data.
- **Fitting and comparison:** distribution fitting from more data sources, automatic fault-impact studies, and fuller run comparison and fidelity scoring.

### Simulation server

- **Visualisation:** animation of a running or recorded simulation, and a map view.
- **Distributed runs:** workers on several machines.

### Writing simulations

- **Generated values in blueprints:** random values, IDs and timestamps in YAML, then entities, relations between records, states and ramps.
- **Preview loop:** run a blueprint again each time the file changes, and check a file without running it.
- **Example gallery and visual builder.**

### Connectors

- **More sinks:** a webhook sink, a generic SQL sink and more.
- **Delivery faults:** records delivered late, twice, damaged or not at all, to test how a pipeline handles them.
- **Lineage:** each run reported to OpenLineage and OpenMetadata.

### Platform

- **More speed:** larger changes for faster runs, and a faster runtime for YAML-only blueprints if measurements show the need.

### Docs

- **Reference:** a page for each generator and connector, with examples and the records they produce, and a command-line cheat sheet.
- **How-To:** generating data at scale, testing pipelines with simulated data, and performance tuning.
