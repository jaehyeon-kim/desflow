# Dynamic DES

[![CI Pipeline](https://github.com/jaehyeon-kim/dynamic-des/actions/workflows/pipeline.yml/badge.svg)](https://github.com/jaehyeon-kim/dynamic-des/actions/workflows/pipeline.yml)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://jaehyeon.me/dynamic-des/)
[![PyPI version](https://badge.fury.io/py/dynamic-des.svg)](https://badge.fury.io/py/dynamic-des)
[![Python Versions](https://img.shields.io/pypi/pyversions/dynamic-des.svg)](https://pypi.org/project/dynamic-des/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Realistic event streams from a simulation you can steer while it runs.**

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/docs/assets/architecture.png" alt="Dynamic DES architecture" width="900" />
</div>

Most test data generators write random rows. Dynamic DES writes the events of a working process: orders queue for packers, packing slows down at busy times, and a backlog builds when arrivals outpace capacity. The data has the cause and effect of a real system, because it comes from a [SimPy](https://simpy.readthedocs.io/) discrete-event simulation.

Change an arrival rate or a capacity while the simulation runs, from **Kafka**, **Redis**, **PostgreSQL** or a timed scenario, and the pipeline downstream sees the effect. One run can write a week of history at full speed, then keep streaming in real time.

Events and telemetry go to **Kafka**, **Redis**, **PostgreSQL**, **Parquet** or **JSONL** files (on local disk or S3-compatible storage), or **Apache Iceberg** tables. A model is a YAML file, a Python builder, or SimPy code.

**Used for:** testing streaming pipelines with data that behaves like production, demos and workshops, digital twins that follow live data, and training data for models that predict delays or load.

The control dashboard changes simulation parameters while a run is going, and the telemetry reacts without a restart:

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/dynamic-des/main/docs/assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="800" />
</div>

## Key Features

- **Real time or full speed**: `DynamicRealtimeEnvironment` runs SimPy in step with the system clock, or unpaced, and reports how far the simulation lags behind real time.
- **Live parameters**: arrival rates, service times and capacities change mid-run through registry paths such as `Shop.arrival.standard.rate`. A resource grows at once, and shrinks only as busy units are released, so no work in progress is lost.
- **Several sinks per run**: each sink added with `add_egress` has its own `when` filter, `batch_size` and `flush_interval`, so one run can feed a stream and a data lake together.
- **Backfill then go live**: one run generates backdated history at full speed, then switches to real time at `go_live_at`.
- **Three ways to write a simulation**: the low-level `DynamicRealtimeEnvironment`, the `SimulationContext` builder, or a YAML blueprint run with `ddes run`.
- **Serialisation**: JSON through `orjson` by default, Avro through the Confluent or AWS Glue Schema Registry for chosen topics, and Pydantic models published as they are.
- **Kafka security**: extra keyword arguments go to the Kafka client, so SASL, mTLS, OAuth and AWS IAM clusters work.

## Installation

Install the core library with uv or pip.

With uv:

```bash
# The ddes command
uv tool install dynamic-des

# Or the library, in a uv project
uv add dynamic-des
```

With pip:

```bash
pip install dynamic-des
```

The connectors are optional extras, for example `uv add "dynamic-des[kafka,parquet]"` or `pip install "dynamic-des[kafka,parquet]"`:

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

`ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

**With uv**

```bash
# 1. Install
uv tool install dynamic-des

# 2. Download the example
ddes examples get local --api yaml
cd local/yaml

# 3. Run it
ddes run local.yaml
```

**With pip**

```bash
# 1. Install
pip install dynamic-des

# 2. Download the example
ddes examples get local --api yaml
cd local/yaml

# 3. Run it
ddes run local.yaml
```

For the builder or low-level version, use `--api builder` or `--api low-level`; the [Installation page](https://jaehyeon.me/dynamic-des/latest/getting-started/) shows all three in tabs. `ddes examples` lists the other examples, and each [example page](https://jaehyeon.me/dynamic-des/latest/examples/) gives the same steps, including the [odctl](https://github.com/jaehyeon-kim/odctl) service it needs.

## Three Ways to Write a Simulation

A simulation can be a YAML blueprint, a script using the builder API (`SimulationContext`), or a script using the low-level API (`DynamicRealtimeEnvironment` and SimPy processes). All three build the same parameters and run on the same environment, so the registry paths, the records and the connectors are the same whichever way you choose.

This blueprint is a small online shop: one packer, an order about every 2 seconds and 1.5 seconds of packing, with every record printed to the terminal:

```yaml title="docs/snippets/yaml/first_shop.yaml"
# The First Simulation tutorial: the shop as a blueprint.
# Run it with: ddes run first_shop.yaml
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

[First Simulation](https://jaehyeon.me/dynamic-des/latest/tutorials/first-simulation/) builds this shop step by step, with the YAML, builder and low-level code side by side at each step, and the [comparison of the three ways](https://jaehyeon.me/dynamic-des/latest/architecture/overview/) explains which to choose.

## Roadmap

- **Planned for 1.0:** routing between tasks, entity state, more distributions, stores and containers, built-in statistics and `ddes explain`; a simulation server (`ddes serve`) with a REST API, a browser UI and schedules, run locally through [odctl](https://github.com/jaehyeon-kim/odctl); a page in the docs for each new building block as it ships.
- **Future work:** breakdowns and schedules, transport on maps, data-driven inputs, experiments and analysis, animation, generated values in YAML, more sinks and delivery faults.

The [roadmap page](https://jaehyeon.me/dynamic-des/latest/about/roadmap/) has the details.

## Learn More

The documentation is at [jaehyeon.me/dynamic-des](https://jaehyeon.me/dynamic-des/).

- **Getting Started**: [installation](https://jaehyeon.me/dynamic-des/latest/getting-started/), and [First Simulation](https://jaehyeon.me/dynamic-des/latest/tutorials/first-simulation/), which builds one online shop step by step with a tab for YAML, the builder API and the low-level API at each step.
- **How Simulation Works**: [what discrete-event simulation is](https://jaehyeon.me/dynamic-des/latest/des/how-it-works/), [what SimPy does and what Dynamic DES adds](https://jaehyeon.me/dynamic-des/latest/des/simpy-and-dynamic-des/), the [building blocks](https://jaehyeon.me/dynamic-des/latest/building-blocks/) of a model (arrivals and services, tasks and processes, resources), and [queues and capacity](https://jaehyeon.me/dynamic-des/latest/des/queues-and-capacity/).
- **Core Architecture**: the [three ways to write a simulation](https://jaehyeon.me/dynamic-des/latest/architecture/overview/), each in detail ([low-level](https://jaehyeon.me/dynamic-des/latest/architecture/low-level/), [builder](https://jaehyeon.me/dynamic-des/latest/architecture/context/), [YAML](https://jaehyeon.me/dynamic-des/latest/architecture/yaml/)), and the runtime: the [environment](https://jaehyeon.me/dynamic-des/latest/architecture/environment/), [live parameters](https://jaehyeon.me/dynamic-des/latest/architecture/registry/), [time and pacing](https://jaehyeon.me/dynamic-des/latest/architecture/time/), [connectors](https://jaehyeon.me/dynamic-des/latest/architecture/connectors/), [records and telemetry](https://jaehyeon.me/dynamic-des/latest/architecture/records/), and [batching and delivery](https://jaehyeon.me/dynamic-des/latest/architecture/batching/).
- **Guides**: [record routing](https://jaehyeon.me/dynamic-des/latest/guides/complex-routing/), [Avro and Pydantic](https://jaehyeon.me/dynamic-des/latest/guides/avro-and-pydantic/), [secure Kafka](https://jaehyeon.me/dynamic-des/latest/guides/kafka-security/), [backfill then go live](https://jaehyeon.me/dynamic-des/latest/guides/backfill-then-live/), [changing parameters](https://jaehyeon.me/dynamic-des/latest/guides/live-parameters/), [writing blueprints](https://jaehyeon.me/dynamic-des/latest/guides/yaml-blueprints/), and the custom logic guides: [Python in a blueprint](https://jaehyeon.me/dynamic-des/latest/guides/yaml-advanced/), [multi-resource handoffs](https://jaehyeon.me/dynamic-des/latest/guides/multi-resource-handoffs/), [preemptive breakdowns](https://jaehyeon.me/dynamic-des/latest/guides/preemptive-breakdowns/) and [dynamic topology](https://jaehyeon.me/dynamic-des/latest/guides/dynamic-topology/).
- **Examples**: one page per example, with the YAML, builder and low-level versions side by side, starting with the [overview](https://jaehyeon.me/dynamic-des/latest/examples/).
- **API Reference**: [every public class](https://jaehyeon.me/dynamic-des/latest/api/).
- **About**: the [roadmap](https://jaehyeon.me/dynamic-des/latest/about/roadmap/).

## Related reading

Blog posts about dynamic-des and the projects built on it are tagged [dynamic-des](https://jaehyeon.me/tags/dynamic-des/) on jaehyeon.me.

Projects that use dynamic-des:

- [benchtop](https://github.com/jaehyeon-kim/benchtop): hands-on data engineering and machine learning demos that run locally, most of them with data produced by dynamic-des.
- [agentic-analytics-system](https://github.com/jaehyeon-kim/agentic-analytics-system): conversational analytics over an Iceberg lakehouse, with the lakehouse data generated by dynamic-des.
- [oml-digital-twin-hotrolling](https://github.com/jaehyeon-kim/oml-digital-twin-hotrolling): a streaming digital twin of a steel hot rolling mill, with online machine learning on Kafka and Flink.

## License

MIT
