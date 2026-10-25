# desflow

[![CI Pipeline](https://github.com/jaehyeon-kim/desflow/actions/workflows/pipeline.yml/badge.svg)](https://github.com/jaehyeon-kim/desflow/actions/workflows/pipeline.yml) [![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://jaehyeon.me/desflow/) [![PyPI version](https://badge.fury.io/py/desflow.svg)](https://badge.fury.io/py/desflow) [![Python Versions](https://img.shields.io/pypi/pyversions/desflow.svg)](https://pypi.org/project/desflow/) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Realistic event streams from a simulation you can steer while it runs.**

> **dynamic-des is now desflow.** The package was renamed: install `desflow`, import `desflow` and run `desflow`. Code that imports `dynamic_des` and scripts that run `ddes` keep working with a warning until a release before 1.0 removes them, and the [Changelog](https://jaehyeon.me/desflow/latest/about/changelog/) says what to change.

The three ways to write a model build one real-time run. Ingress connectors change its live parameters while it runs, and egress connectors write its events and telemetry:

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/desflow/main/docs/assets/diagrams/architecture.png" alt="desflow architecture" width="900" />
</div>

Most test data generators write random rows. desflow writes the events of a working process: orders queue for packers, waits grow at busy times, and a backlog builds when orders arrive faster than the packers can pack them. The data comes from a [SimPy](https://simpy.readthedocs.io/) discrete-event simulation, so each change in it has a cause in the model, as in a real system.

You can change an arrival rate or a capacity while the simulation runs, from **Kafka**, **Redis**, **PostgreSQL**, a local schedule or a timed scenario, and the pipeline downstream sees the effect. One run can write a week of history at full speed, then keep streaming in real time.

Events and telemetry go to **Kafka**, **Redis**, **PostgreSQL**, **Parquet** or **JSONL** files (on local disk or S3-compatible storage), or **Apache Iceberg** tables. A model is a YAML file, a Python builder, or SimPy code.

**Used for:** testing streaming pipelines with realistic data, running demos and workshops, building digital twins that follow live data, generating training data for models that predict delays or load, and evaluating agents and detectors against ground truth. A seeded run can plant a fault and record what it planted as a label record. A detector's or an agent's answer is then scored against that known cause.

To see why it exists, read [Why desflow](https://jaehyeon.me/desflow/latest/about/why-desflow/). To check what desflow does against plain SimPy, see [SimPy Compatibility](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/).

The control dashboard of the [Kafka Digital Twin](https://jaehyeon.me/desflow/latest/examples/kafka/) example changes simulation parameters during a run, and the telemetry shows the effect without a restart. The example's Quick Start runs the dashboard beside the simulation:

<div align="center">
  <img src="https://raw.githubusercontent.com/jaehyeon-kim/desflow/main/docs/assets/dashboard-preview.gif" alt="Live parameter updates from the control dashboard" width="700" />
</div>

## Key Features

- **Real time or full speed**: `DynamicRealtimeEnvironment` runs SimPy in step with the system clock, or as fast as the machine allows, and reports how far the simulation lags behind real time.
- **Live parameters**: arrival rates, service times and capacities change mid-run through named parameters (registry paths) such as `Shop.arrival.orders.rate`. A resource grows at once. It shrinks only as busy units are released, so no work in progress is lost.
- **Several sinks per run**: each sink added with `add_egress` has its own `when` filter, `batch_size` and `flush_interval`, so one run can feed a stream and a data lake together.
- **Backfill then go live**: one run generates backdated history at full speed, then switches to real time at `go_live_at`.
- **Three ways to write a simulation**: the low-level `DynamicRealtimeEnvironment`, the `SimulationContext` builder, or a YAML blueprint run with `desflow run`.
- **Check before a long run**: `desflow explain` works out each resource's utilisation and waits from the model, without running it, and `--verify` compares them with runs.
- **Replications and experiments**: `desflow run --replications` gives each statistic with a 95% interval, and `desflow experiment` compares designs seed by seed and writes a report.
- **Inputs from data**: `desflow fit` fits every distribution family to measured times, and `desflow dist` turns a mean and a standard deviation into a family's parameters.
- **Serialisation**: records are JSON through `orjson` by default, or Avro through the Confluent or AWS Glue Schema Registry for chosen topics. A Pydantic model can be published as it is.
- **Kafka security**: extra keyword arguments go to the Kafka client, so SASL, mTLS, OAuth and AWS IAM clusters work.

## Commands

`desflow` is the command that comes with the package. It runs a blueprint, and checks, compares and fits models without a line of Python. [Command Line](https://jaehyeon.me/desflow/latest/user-guide/command-line/) uses every command on one shop.

| Command | What you use it for | How to use it | Reference |
|---|---|---|---|
| `desflow examples` | list the examples, and download one with `get` | [Get an Example](https://jaehyeon.me/desflow/latest/user-guide/command-line/#get-an-example) | [`desflow examples`](https://jaehyeon.me/desflow/latest/reference/cli/examples/), [`desflow examples get`](https://jaehyeon.me/desflow/latest/reference/cli/examples-get/) |
| `desflow explain` | check a model for bottlenecks before a long run | [Check It Before a Long Run](https://jaehyeon.me/desflow/latest/user-guide/command-line/#check-it-before-a-long-run) | [`desflow explain`](https://jaehyeon.me/desflow/latest/reference/cli/explain/) |
| `desflow run` | run a blueprint, or its replications with a 95% interval for each statistic | [Run It](https://jaehyeon.me/desflow/latest/user-guide/command-line/#run-it) | [`desflow run`](https://jaehyeon.me/desflow/latest/reference/cli/run/) |
| `desflow experiment` | compare designs of a model and check them against bounds | [Compare Designs](https://jaehyeon.me/desflow/latest/user-guide/command-line/#compare-designs) | [`desflow experiment`](https://jaehyeon.me/desflow/latest/reference/cli/experiment/) |
| `desflow report` | build an experiment's report again from its files | [Compare Designs](https://jaehyeon.me/desflow/latest/user-guide/command-line/#compare-designs) | [`desflow report`](https://jaehyeon.me/desflow/latest/reference/cli/report/) |
| `desflow fit` | fit every distribution family to measured times | [Fit Measured Times](https://jaehyeon.me/desflow/latest/user-guide/command-line/#fit-measured-times) | [`desflow fit`](https://jaehyeon.me/desflow/latest/reference/cli/fit/) |
| `desflow dist` | turn a mean and a standard deviation into a family's parameters, and back | [Convert Parameters](https://jaehyeon.me/desflow/latest/user-guide/command-line/#convert-parameters) | [`desflow dist`](https://jaehyeon.me/desflow/latest/reference/cli/dist/) |
| `desflow schema` | print the blueprint JSON Schema, for an editor | [Set Up an Editor](https://jaehyeon.me/desflow/latest/user-guide/command-line/#set-up-an-editor) | [`desflow schema`](https://jaehyeon.me/desflow/latest/reference/cli/schema/) |

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

The connectors are optional extras, for example `uv add "desflow[kafka,parquet]"` or `pip install "desflow[kafka,parquet]"`, and `uv tool install "desflow[kafka,parquet]"` for the `desflow` command:

| Extra | Adds |
|---|---|
| `kafka` | Kafka ingress and egress |
| `confluent` | Avro with the Confluent Schema Registry (includes `kafka`) |
| `glue` | Avro with the AWS Glue Schema Registry (includes `kafka`) |
| `redis` | Redis ingress and egress |
| `postgres` | PostgreSQL ingress and egress |
| `parquet` | Parquet and JSONL files on local disk or S3-compatible storage |
| `iceberg` | Apache Iceberg tables through a REST catalog |
| `fit` | scipy and matplotlib, for [input fitting](https://jaehyeon.me/desflow/latest/user-guide/input-fitting/) with `desflow fit` |
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

This blueprint models a shop that receives and packs customer orders: one packer, an order about every 2 seconds and 1.5 seconds of packing, with every record printed to the terminal:

```yaml title="docs/snippets/yaml/first_shop.yaml"
# The First Simulation tutorial: the shop as a blueprint.
# Run it with: desflow run first_shop.yaml
simulation:
  sim_id: Shop
  factor: 1.0  # 1 simulated second takes 1 real second
  random_seed: 42  # the same arrival times on every run

resources:
  packers: {current_cap: 1, max_cap: 1}

services:
  # Every order takes exactly 1.5 seconds to pack
  packing: {dist: constant, value: 1.5}

arrivals:
  # One order every 2 seconds on average; each arrival spawns pack_order
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    # The value of the finished event
    payload: {event_type: order_packed}

egress:
  - type: Console

run:
  until: 20
```

[First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/) builds this shop model step by step, with the YAML, builder and low-level code side by side at each step, and the [comparison of the three ways](https://jaehyeon.me/desflow/latest/user-guide/ways-to-write/) explains which to choose.

## Roadmap

desflow reached feature parity with SimPy in YAML, the builder and the low-level API. Before 1.0, it adds more storage backends, JSON Schema and Protobuf for the schema registries, library hooks for a simulation server and a record format that 1.0 keeps stable. A simulation server with tools for AI agents follows 1.0. The [roadmap page](https://jaehyeon.me/desflow/latest/about/roadmap/) shows each step in order, with what is in progress.

## Learn More

The documentation is at [jaehyeon.me/desflow](https://jaehyeon.me/desflow/). Good places to start:

- [Installation](https://jaehyeon.me/desflow/latest/getting-started/) and [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/), which builds a shop model step by step.
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
