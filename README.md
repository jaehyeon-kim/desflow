# desflow

[![CI Pipeline](https://github.com/jaehyeon-kim/desflow/actions/workflows/pipeline.yml/badge.svg)](https://github.com/jaehyeon-kim/desflow/actions/workflows/pipeline.yml)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://jaehyeon.me/desflow/)
[![PyPI version](https://badge.fury.io/py/desflow.svg)](https://badge.fury.io/py/desflow)
[![Python Versions](https://img.shields.io/pypi/pyversions/desflow.svg)](https://pypi.org/project/desflow/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Realistic event streams from a simulation you can steer while it runs.**

> **dynamic-des is now desflow.** The package was renamed in 0.20.0: install `desflow`, import `desflow` and run `desflow`. Code that imports `dynamic_des` and scripts that run `ddes` keep working with a warning until a later release, and the [Changelog](https://jaehyeon.me/desflow/latest/about/changelog/) says what to change.

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/desflow/main/docs/assets/architecture.png" alt="desflow architecture" width="900" />
</div>

Most test data generators write random rows. desflow writes the events of a working process: orders queue for packers, packing slows down at busy times, and a backlog builds when arrivals outpace capacity. The data has the cause and effect of a real system, because it comes from a [SimPy](https://simpy.readthedocs.io/) discrete-event simulation.

Change an arrival rate or a capacity while the simulation runs, from **Kafka**, **Redis**, **PostgreSQL** or a timed scenario, and the pipeline downstream sees the effect. One run can write a week of history at full speed, then keep streaming in real time.

Events and telemetry go to **Kafka**, **Redis**, **PostgreSQL**, **Parquet** or **JSONL** files (on local disk or S3-compatible storage), or **Apache Iceberg** tables. A model is a YAML file, a Python builder, or SimPy code.

**Used for:** testing streaming pipelines with data that behaves like production, demos and workshops, digital twins that follow live data, and training data for models that predict delays or load.

The control dashboard changes simulation parameters while a run is going, and the telemetry reacts without a restart:

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/desflow/main/docs/assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="800" />
</div>

## Key Features

- **Real time or full speed**: `DynamicRealtimeEnvironment` runs SimPy in step with the system clock, or unpaced, and reports how far the simulation lags behind real time.
- **Live parameters**: arrival rates, service times and capacities change mid-run through registry paths such as `Shop.arrival.standard.rate`. A resource grows at once, and shrinks only as busy units are released, so no work in progress is lost.
- **Several sinks per run**: each sink added with `add_egress` has its own `when` filter, `batch_size` and `flush_interval`, so one run can feed a stream and a data lake together.
- **Backfill then go live**: one run generates backdated history at full speed, then switches to real time at `go_live_at`.
- **Three ways to write a simulation**: the low-level `DynamicRealtimeEnvironment`, the `SimulationContext` builder, or a YAML blueprint run with `desflow run`.
- **Serialisation**: JSON through `orjson` by default, Avro through the Confluent or AWS Glue Schema Registry for chosen topics, and Pydantic models published as they are.
- **Kafka security**: extra keyword arguments go to the Kafka client, so SASL, mTLS, OAuth and AWS IAM clusters work.

## Installation

Install the core library with uv or pip.

With uv:

```bash
# The desflow command
uv tool install desflow

# Or the library, in a uv project
uv add desflow
```

With pip:

```bash
pip install desflow
```

The connectors are optional extras, for example `uv add "desflow[kafka,parquet]"` or `pip install "desflow[kafka,parquet]"`:

| Extra | Adds |
|---|---|
| `kafka` | Kafka ingress and egress |
| `confluent` | Avro with the Confluent Schema Registry (includes `kafka`) |
| `glue` | Avro with the AWS Glue Schema Registry (includes `kafka`) |
| `redis` | Redis ingress and egress |
| `postgres` | PostgreSQL ingress and egress |
| `parquet` | Parquet and JSONL files on local disk or S3-compatible storage |
| `iceberg` | Apache Iceberg tables through a REST catalog |
| `all` | Every extra above |

## Run an Example

Every example is written three ways: as a YAML blueprint, with the builder API and with the low-level API. The local example needs no container. Here is its YAML version:

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

**With uv**

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local --api yaml
cd local/yaml

# 3. Run it
desflow run local.yaml
```

**With pip**

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local --api yaml
cd local/yaml

# 3. Run it
desflow run local.yaml
```

For the builder or low-level version, use `--api builder` or `--api low-level`; the [Installation page](https://jaehyeon.me/desflow/latest/getting-started/) shows all three in tabs. `desflow examples` lists the other examples, and each [example page](https://jaehyeon.me/desflow/latest/examples/) gives the same steps, including the [odctl](https://github.com/jaehyeon-kim/odctl) service it needs.

## Three Ways to Write a Simulation

A simulation can be a YAML blueprint, a script using the builder API (`SimulationContext`), or a script using the low-level API (`DynamicRealtimeEnvironment` and SimPy processes). All three build the same parameters and run on the same environment, so the registry paths, the records and the connectors are the same whichever way you choose.

This blueprint is a small online shop: one packer, an order about every 2 seconds and 1.5 seconds of packing, with every record printed to the terminal:

```yaml title="docs/snippets/yaml/first_shop.yaml"
# The First Simulation tutorial: the shop as a blueprint.
# Run it with: desflow run first_shop.yaml
simulation:
  sim_id: Shop
  factor: 1.0  # 1 simulated second takes 1 real second

resources:
  packers: {current_cap: 1, max_cap: 1}

services:
  # With no std, a normal distribution returns its mean every time
  packing: {dist: normal, mean: 1.5}

arrivals:
  # One order every 2 seconds on average; each arrival spawns pack_order
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    # The finished event's value; id_field adds the task id as order_id
    payload: {}
    id_field: order_id

egress:
  - type: Console

run:
  until: 10
```

[First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/) builds this shop step by step, with the YAML, builder and low-level code side by side at each step, and the [comparison of the three ways](https://jaehyeon.me/desflow/latest/architecture/overview/) explains which to choose.

## Roadmap

- **Planned for 1.0:** routing between tasks, entity state, more distributions, stores and containers, built-in statistics, replications and `desflow explain`; a blueprint JSON Schema and a skill for AI assistants; a simulation server (`desflow serve`) with a REST API, a browser UI and schedules, run locally through [odctl](https://github.com/jaehyeon-kim/odctl); a page in the docs for each new building block as it ships.
- **Future work:** breakdowns and schedules, transport on maps, data-driven inputs, experiments and analysis, animation, generated values in YAML, more sinks and delivery faults.

The [roadmap page](https://jaehyeon.me/desflow/latest/about/roadmap/) has the details.

## Learn More

The documentation is at [jaehyeon.me/desflow](https://jaehyeon.me/desflow/).

- **Getting Started**: [installation](https://jaehyeon.me/desflow/latest/getting-started/), and [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/), which builds one online shop step by step with a tab for YAML, the builder API and the low-level API at each step.
- **How Simulation Works**: [what discrete-event simulation is](https://jaehyeon.me/desflow/latest/des/how-it-works/), [what SimPy does and what desflow adds](https://jaehyeon.me/desflow/latest/des/simpy-and-desflow/), the [building blocks](https://jaehyeon.me/desflow/latest/building-blocks/) of a model (arrivals and services, tasks and processes, resources), and [queues and capacity](https://jaehyeon.me/desflow/latest/des/queues-and-capacity/).
- **Core Architecture**: the [three ways to write a simulation](https://jaehyeon.me/desflow/latest/architecture/overview/), each in detail ([low-level](https://jaehyeon.me/desflow/latest/architecture/low-level/), [builder](https://jaehyeon.me/desflow/latest/architecture/context/), [YAML](https://jaehyeon.me/desflow/latest/architecture/yaml/)), and the runtime: the [environment](https://jaehyeon.me/desflow/latest/architecture/environment/), [live parameters](https://jaehyeon.me/desflow/latest/architecture/registry/), [time and pacing](https://jaehyeon.me/desflow/latest/architecture/time/), [connectors](https://jaehyeon.me/desflow/latest/architecture/connectors/), [records and telemetry](https://jaehyeon.me/desflow/latest/architecture/records/), and [batching and delivery](https://jaehyeon.me/desflow/latest/architecture/batching/).
- **Guides**: [record routing](https://jaehyeon.me/desflow/latest/guides/complex-routing/), [Avro and Pydantic](https://jaehyeon.me/desflow/latest/guides/avro-and-pydantic/), [secure Kafka](https://jaehyeon.me/desflow/latest/guides/kafka-security/), [backfill then go live](https://jaehyeon.me/desflow/latest/guides/backfill-then-live/), [changing parameters](https://jaehyeon.me/desflow/latest/guides/live-parameters/), [writing blueprints](https://jaehyeon.me/desflow/latest/guides/yaml-blueprints/), and the custom logic guides: [Python in a blueprint](https://jaehyeon.me/desflow/latest/guides/yaml-advanced/), [multi-resource handoffs](https://jaehyeon.me/desflow/latest/guides/multi-resource-handoffs/), [preemptive breakdowns](https://jaehyeon.me/desflow/latest/guides/preemptive-breakdowns/) and [dynamic topology](https://jaehyeon.me/desflow/latest/guides/dynamic-topology/).
- **Examples**: one page per example, with the YAML, builder and low-level versions side by side, starting with the [overview](https://jaehyeon.me/desflow/latest/examples/).
- **API Reference**: [every public class](https://jaehyeon.me/desflow/latest/api/).
- **About**: the [roadmap](https://jaehyeon.me/desflow/latest/about/roadmap/) and the [Changelog](https://jaehyeon.me/desflow/latest/about/changelog/).

## Related reading

Blog posts about desflow and the projects built on it are tagged [desflow](https://jaehyeon.me/tags/desflow/) on jaehyeon.me.

Projects that use desflow:

- [benchtop](https://github.com/jaehyeon-kim/benchtop): hands-on data engineering and machine learning demos that run locally, most of them with data produced by desflow.
- [agentic-analytics-system](https://github.com/jaehyeon-kim/agentic-analytics-system): conversational analytics over an Iceberg lakehouse, with the lakehouse data generated by desflow.
- [oml-digital-twin-hotrolling](https://github.com/jaehyeon-kim/oml-digital-twin-hotrolling): a streaming digital twin of a steel hot rolling mill, with online machine learning on Kafka and Flink.

## License

MIT
