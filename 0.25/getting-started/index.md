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

desflow was called dynamic-des before 0.20.0. Install the package `desflow`, import the module `desflow` and run the command `desflow`. Code that imports `dynamic_des` and scripts that run `ddes` keep working with a warning until 0.30.0, and the [Changelog](https://jaehyeon.me/desflow/latest/about/changelog/index.md) says what to change.

The connectors are optional extras, for example `uv add "desflow[kafka,parquet]"` or `pip install "desflow[kafka,parquet]"`, and `uv tool install "desflow[kafka,parquet]"` for the `desflow` command:

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

The package installs the `desflow` command, and `desflow` on its own lists its commands. [Command Line](https://jaehyeon.me/desflow/latest/user-guide/command-line/index.md) uses each one on the shop, and [CLI](https://jaehyeon.me/desflow/latest/reference/cli/index.md) gives the help of each.

______________________________________________________________________

## Run an Example

The local example models a shop that receives and packs customer orders. The simulation prints events and telemetry to the terminal. The example is written three ways: as a YAML blueprint, with the builder and with the low-level API. Each version runs in real time and ends on its own after 30 seconds. Pick the way you write models, then follow uv or pip:

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

Each example needs at most one odctl profile. Start the profile before you run the example, and stop it afterwards:

| Example                                                                                      | Start                 | Stop                              |
| -------------------------------------------------------------------------------------------- | --------------------- | --------------------------------- |
| every example that `desflow examples` marks `(no service)`, such as local, parquet and stock | nothing needed        |                                   |
| kafka, backfill-live                                                                         | `odctl up kafka-lite` | `odctl down kafka-lite --volumes` |
| postgres, related-records                                                                    | `odctl up postgres`   | `odctl down postgres --volumes`   |
| redis                                                                                        | `odctl up valkey`     | `odctl down valkey --volumes`     |
| parquet with `PARQUET_FILESYSTEM=s3`                                                         | `odctl up storage`    | `odctl down storage --volumes`    |
| iceberg                                                                                      | `odctl up catalog`    | `odctl down catalog --volumes`    |

An example's YAML version needs the same profile as its builder version: `desflow examples get <name>` prints the profile with the run command.

The Kafka and Redis profiles are not named after their service. odctl's one-broker Kafka profile is `kafka-lite` and its three-broker Kafka profile is `kafka-full`. odctl runs Valkey in place of Redis, so its Redis profile is `valkey`. The Kafka examples run on either Kafka profile, because both publish a broker at `127.0.0.1:9092`.

### Endpoints These Profiles Publish

These are the addresses the examples use. Every profile publishes on your machine only, on `127.0.0.1`. Use that address rather than `localhost`, which macOS resolves to the IPv6 address `::1` first, where nothing listens.

| Profile      | Service                                              | Address the examples use                                                                |
| ------------ | ---------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `kafka-lite` | Kafka, one broker, with the Karapace schema registry | `127.0.0.1:9092`; schema registry `http://127.0.0.1:8081`                               |
| `kafka-full` | Kafka, three brokers, with the same schema registry  | `127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094`; schema registry `http://127.0.0.1:8081` |
| `postgres`   | PostgreSQL, database `odctl`                         | `postgresql://user:password@127.0.0.1:5432/odctl`                                       |
| `valkey`     | Valkey (Redis protocol)                              | `redis://user:password@127.0.0.1:6379/0`                                                |
| `storage`    | S3-compatible object store, bucket `odctl-dev`       | `http://127.0.0.1:8333`, access key `user`, secret key `password`                       |
| `catalog`    | Iceberg REST catalog, warehouse `s3://warehouse/`    | `http://127.0.0.1:8181`                                                                 |

`odctl list -d` shows every port a profile publishes.

______________________________________________________________________

## Next Steps

- [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/index.md) builds a shop model step by step. It is the first page of the shop tutorial in the Learn section, and each following page adds one idea to the model.
- The [examples](https://jaehyeon.me/desflow/latest/examples/index.md) show each connector, written three ways, with the profile each one needs.
