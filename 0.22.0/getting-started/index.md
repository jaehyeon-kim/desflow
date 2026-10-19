# Installation

This page installs desflow, runs a first simulation that needs no containers, and sets up the services the other examples use.

## Install the Package

Install the core library with uv or pip:

```bash
# The desflow command
uv tool install desflow

# Or the library, in a uv project
uv add desflow
```

```bash
pip install desflow
```

desflow was called dynamic-des before 0.20.0. Code that imports `dynamic_des` and scripts that run `ddes` keep working with a warning until a later release, and the [Changelog](https://jaehyeon.me/desflow/latest/about/changelog/index.md) says what to change.

The connectors are optional extras, for example `uv add "desflow[kafka,parquet]"` or `pip install "desflow[kafka,parquet]"`:

| Extra       | Adds                                                                                                                               |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `kafka`     | Kafka ingress and egress                                                                                                           |
| `confluent` | Avro with the Confluent Schema Registry (includes `kafka`)                                                                         |
| `glue`      | Avro with the AWS Glue Schema Registry (includes `kafka`)                                                                          |
| `redis`     | Redis ingress and egress                                                                                                           |
| `postgres`  | PostgreSQL ingress and egress                                                                                                      |
| `parquet`   | Parquet and JSONL files on local disk or S3-compatible storage                                                                     |
| `iceberg`   | Apache Iceberg tables through a REST catalog                                                                                       |
| `fit`       | scipy and matplotlib, for [input fitting](https://jaehyeon.me/desflow/latest/user-guide/input-fitting/index.md) with `desflow fit` |
| `all`       | Every extra above                                                                                                                  |

______________________________________________________________________

## Run an Example

The local example models a shop that receives and packs customer orders. The simulation prints events and telemetry to the terminal. It runs in real time and ends on its own after 30 seconds, in all three versions. Pick the way you write, then follow uv or pip:

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local --api yaml
cd local/yaml

# 3. Run it
desflow run local.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local --api yaml
cd local/yaml

# 3. Run it
desflow run local.yaml
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local --api builder
cd local/builder

# 3. Run it
uv run --no-project --with desflow local_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local --api builder
cd local/builder

# 3. Run it
python local_example.py
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local --api low-level
cd local/low-level

# 3. Run it
uv run --no-project --with desflow local_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local --api low-level
cd local/low-level

# 3. Run it
python local_example.py
```

`desflow examples` lists every example.

______________________________________________________________________

## Services for the Examples

Every example that needs a broker, a database or an object store gets it from [odctl](https://github.com/jaehyeon-kim/odctl), a separate CLI that manages curated Docker Compose stacks. Install it once:

```bash
# With uv
uv tool install "odctl>=1.0,<2"

# Or with pip
pip install "odctl>=1.0,<2"
```

`odctl list -d` shows every profile and the ports it publishes. The examples here use five of them: `kafka-lite`, `postgres`, `valkey`, `storage` and `catalog`.

### Starting a Profile

Each example needs at most one odctl profile, started before you run it and torn down after:

| Example                                | Start                 | Stop                              |
| -------------------------------------- | --------------------- | --------------------------------- |
| local, parquet, stock, flow, measuring | nothing needed        |                                   |
| kafka, backfill-live                   | `odctl up kafka-lite` | `odctl down kafka-lite --volumes` |
| postgres, related-records              | `odctl up postgres`   | `odctl down postgres --volumes`   |
| redis                                  | `odctl up valkey`     | `odctl down valkey --volumes`     |
| parquet with `PARQUET_FILESYSTEM=s3`   | `odctl up storage`    | `odctl down storage --volumes`    |
| iceberg                                | `odctl up catalog`    | `odctl down catalog --volumes`    |

The YAML blueprints in `examples/yaml/` need the same profile as their builder twin.

Kafka and Redis are the two whose profile names are not what you would guess, because odctl ships a one-broker Kafka as `kafka-lite` and a three-broker cluster as `kafka-full`, and uses Valkey rather than Redis. The Kafka examples run on either, since both publish a broker at `localhost:9092`.

### Endpoints These Profiles Publish

These are the addresses the examples use. Every profile publishes on your machine only (`localhost`).

| Profile      | Service                                              | Address the examples use                                                                |
| ------------ | ---------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `kafka-lite` | Kafka, one broker, with the Karapace schema registry | `localhost:9092`; schema registry `http://localhost:8081`                               |
| `kafka-full` | Kafka, three brokers, with the same schema registry  | `localhost:9092,localhost:9093,localhost:9094`; schema registry `http://localhost:8081` |
| `postgres`   | PostgreSQL, database `odctl`                         | `postgresql://user:password@localhost:5432/odctl`                                       |
| `valkey`     | Valkey (Redis protocol)                              | `redis://user:password@localhost:6379/0`                                                |
| `storage`    | S3-compatible object store, bucket `odctl-dev`       | `http://localhost:8333`, access key `user`, secret key `password`                       |
| `catalog`    | Iceberg REST catalog, warehouse `s3://warehouse/`    | `http://localhost:8181`                                                                 |

`odctl list -d` shows every port a profile publishes.

______________________________________________________________________

## Next Steps

- [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/index.md), the first page of the shop tutorial in the Learn section, builds a shop model step by step. The following pages extend the model one idea at a time.
- The [examples](https://jaehyeon.me/desflow/latest/examples/index.md) show each connector, written three ways, with the profile each one needs.
