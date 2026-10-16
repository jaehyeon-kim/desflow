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

**Used for:** testing streaming pipelines with data that behaves like production, training data for ML models that predict delays or load, data for testing AI agents, digital twins that follow live data, and demos and workshops.

The control dashboard changes simulation parameters while a run is going, and the telemetry reacts without a restart:

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/desflow/main/docs/assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="800" />
</div>

## Key Features

- **Real time or full speed**: `DynamicRealtimeEnvironment` runs SimPy in step with the system clock, or unpaced, and reports how far the simulation lags behind real time.
- **Live parameters**: arrival rates, service times and capacities change mid-run through registry paths such as `Shop.arrival.standard.rate`. A resource grows at once, and shrinks only as busy units are released, so no work in progress is lost.
- **Several sinks per run**: each sink added with `add_egress` has its own `when` filter, `batch_size` and `flush_interval`, so one run can feed a stream and a data lake together.
- **Backfill then go live**: one run generates backdated history at full speed, then switches to real time at `go_live_at`.
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

The local example needs no container. With desflow installed as above, `desflow examples get` downloads the example at the release you installed, into a folder of its own, and `desflow run` runs it:

```bash
desflow examples get local --api yaml
cd local/yaml
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

The next releases complete the simulation model (distributions, stores, flows, statistics and replications) and add `desflow explain` and a Learn DES tutorial, followed by a simulation server with tools for AI agents. The [roadmap page](https://jaehyeon.me/desflow/latest/about/roadmap/) shows them in order, with what is in progress.

## Learn More

The documentation is at [jaehyeon.me/desflow](https://jaehyeon.me/desflow/). Good places to start:

- [Installation](https://jaehyeon.me/desflow/latest/getting-started/) and [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/), which builds one online shop step by step.
- [Examples](https://jaehyeon.me/desflow/latest/examples/), each in YAML, the builder API and the low-level API.
- [API Reference](https://jaehyeon.me/desflow/latest/api/), the [roadmap](https://jaehyeon.me/desflow/latest/about/roadmap/) and the [Changelog](https://jaehyeon.me/desflow/latest/about/changelog/).

## Related Reading

Blog posts about desflow and the projects built on it are tagged [desflow](https://jaehyeon.me/tags/desflow/) on jaehyeon.me.

Projects that use desflow:

- [benchtop](https://github.com/jaehyeon-kim/benchtop): hands-on data engineering and machine learning demos that run locally, most of them with data produced by desflow.
- [agentic-analytics-system](https://github.com/jaehyeon-kim/agentic-analytics-system): conversational analytics over an Iceberg lakehouse, with the lakehouse data generated by desflow.
- [oml-digital-twin-hotrolling](https://github.com/jaehyeon-kim/oml-digital-twin-hotrolling): a streaming digital twin of a steel hot rolling mill, with online machine learning on Kafka and Flink.

## License

MIT
