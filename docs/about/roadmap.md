# Roadmap

The items planned after 1.0 have no versions or dates yet: a release ships when it is ready. Each released item links to its GitHub issue.

## What 1.0 means

From 1.0, the YAML blueprint format, the `desflow` command, the public Python API and the published record format stay compatible across every 1.x release. The public Python API is the set of promised import paths, which the [Compatibility Policy](compatibility.md) lists. The package status moves from Beta to Production/Stable. The names kept for compatibility, `dynamic_des`, `ddes` and the old registry settings, are removed in 1.0, and [Upgrading to 1.0](upgrading-to-1.0.md#removed-names) lists each.

## After 1.0

The work after 1.0 has four aims, and each item below serves one of them.

### Easier to Create Simulations

- **Models from history:** a model discovered from an event log, calibrated against observed data, compared with that log, and updated from new logs.
- **Payloads from your data:** record fields generated from a schema or a few sample records, with the real field names, types and value ranges.

### Easier to Access desflow

- **Orchestrators:** Airflow, Dagster and similar tools.
- **AI assistants:** the desflow skill for Claude Code, Codex and other tools that read the Agent Skills format.

### Easier to Manage Simulations

- **Simulation server:** run, change and follow simulations through an API, a browser UI and schedules, with a published image and an odctl profile.
- **Experiments:** experiment designs, optimisation, and comparison and scoring of runs.
- **Lineage:** each run reported to OpenLineage and OpenMetadata.
- **Faster runs:** cheaper record publishing, with the same published records.

### More Realism

- **Process features:** planned maintenance, schedules by time of day, incidents, matching and splitting, setup times, resources chosen by skill, costs and energy, and continuous levels.
- **Transport on maps:** vehicles that move between locations on real roads.
- **Real inputs:** arrivals replayed from a database or a Kafka topic, a trained model read from a store during a run, and decisions from an outside dispatcher or agent.
- **Sinks:** more sinks, such as a webhook and a generic SQL sink, and an error sink for records a sink could not write.
- **Delivery faults:** records delivered late, twice, damaged or not at all.

## Released

??? note "1.0.0: Stable"

    - **Stable:** the YAML blueprint format, the `desflow` command, the public Python API and the record format stay compatible across 1.x, as the [Compatibility Policy](compatibility.md) sets out. The package status is Production/Stable. [#54](https://github.com/jaehyeon-kim/desflow/issues/54)
    - **SimPy's rules:** a run ends when no event is left, as in SimPy, and `strict` and `on_unhandled_failure` default to SimPy's values. The SimPy Compatibility page shows each SimPy feature in YAML, Builder and Low-level with its status.
    - **SimPy's step order at a tie:** a task with one `wait` branch, the task after `next`, the actions of `on:` and a resumed job go on in the same simulation step as SimPy's process, so records at one instant can change order. [#105](https://github.com/jaehyeon-kim/desflow/issues/105)
    - **YAML with no Python:** every known case runs in YAML alone, with `--var`, `--watch`, generated values, `desflow check` and `desflow test`.
    - **SimPy's examples:** the seven examples of SimPy's docs in YAML, Builder and Low-level. [#95](https://github.com/jaehyeon-kim/desflow/issues/95)
    - **Record contract frozen:** the records 1.x keeps. [#51](https://github.com/jaehyeon-kim/desflow/issues/51)
    - **Old names removed:** `dynamic_des`, `ddes` and the other names kept for compatibility. [#84](https://github.com/jaehyeon-kim/desflow/issues/84)
    - **Pydantic 2.7.0 or later.** [#104](https://github.com/jaehyeon-kim/desflow/issues/104)
    - **Rewritten docs.** The Gallery, the Generate Data group and the Agent Skill section are new, and every page is rewritten for 1.0.
    - **Fixes from reviews of the whole code:** the release candidates 1.0.0rc1 to 1.0.0rc4 and two full reads of the code fixed models that ran wrong, hung or failed in the middle of a run. [Upgrading to 1.0](upgrading-to-1.0.md) lists each change that alters records or refuses input.

??? note "0.28.0: Library hooks for the server"

    - **Parameter changes that report back:** `change` returns a `ParamChange` or raises `ParamRejected`, `on_change` callbacks receive every accepted change with its source, and `registry_paths()` lists the paths a simulation will have without running it. [#70](https://github.com/jaehyeon-kim/desflow/issues/70)
    - **File ingress:** `FileIngress` changes parameters during a run when you edit a local file, with no broker. [#86](https://github.com/jaehyeon-kim/desflow/issues/86)
    - **Scenario values checked:** a scenario step whose value the registry refuses stops the load or the start, instead of being logged and ignored. [#93](https://github.com/jaehyeon-kim/desflow/issues/93)
    - **Drafting from a description:** the AI skill teaches an assistant to draft a model from a description of a process. [#94](https://github.com/jaehyeon-kim/desflow/issues/94)

??? note "0.27.0: Schema registry formats"

    - **JSON Schema and Protobuf:** serializers and deserializers for both formats, through the Confluent and Glue schema registries, next to Avro. [#82](https://github.com/jaehyeon-kim/desflow/issues/82)
    - **Glue wire format:** desflow writes and reads it itself, so the `glue` extra no longer installs aws-glue-schema-registry.
    - **Kafka:** the `kafka` extra no longer installs kafka-python, and topics are created through aiokafka.

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

    - **Rename to desflow:** the package, its import name and its command are all `desflow`. The old names, `dynamic-des`, `dynamic_des` and `ddes`, were removed in 1.0.

The [Changelog](changelog.md) lists every release.
