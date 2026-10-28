# Installation

This page installs desflow and runs a first simulation that needs no containers.

## Install the Package

Install the core library with uv or pip:

=== "uv"

    ```bash
    # The desflow command
    uv tool install desflow

    # Or the library, in a uv project
    uv add desflow
    ```

=== "pip"

    ```bash
    pip install desflow
    ```

desflow was called dynamic-des before 0.20.0. The package, the Python module and the command are now all named `desflow`. Code that imports `dynamic_des` and scripts that run `ddes` keep working with a warning until 0.30.0, and the [Changelog](about/changelog.md) says what to change.

Connectors and input fitting are optional extras. For example, `uv add "desflow[kafka,parquet]"` or `pip install "desflow[kafka,parquet]"`, and `uv tool install "desflow[kafka,parquet]"` for the `desflow` command:

| Extra | Adds |
|---|---|
| `kafka` | Kafka ingress and egress |
| `confluent` | Avro with the Confluent Schema Registry (includes `kafka`) |
| `glue` | Avro with the AWS Glue Schema Registry (includes `kafka`) |
| `redis` | Redis ingress and egress |
| `postgres` | PostgreSQL ingress and egress |
| `parquet` | Parquet and JSONL files on local disk, S3-compatible storage, Google Cloud Storage or Azure Blob Storage |
| `iceberg` | Apache Iceberg tables through a REST catalog, a SQL catalog on SQLite or PostgreSQL, or the Glue catalog |
| `fit` | scipy and matplotlib, for [input fitting](user-guide/input-fitting.md) with `desflow fit` |
| `all` | Every extra above |

On Python 3.15, the `kafka`, `confluent` and `glue` extras build aiokafka, and the `iceberg` extra builds pyiceberg and zstandard, from source, because those packages publish no wheel for 3.15 yet. A build from source needs a C compiler.

Run `desflow` on its own to list its commands. [Command Line](user-guide/command-line.md) uses each command on one shop, the model of the `experiment` example, and [CLI](reference/cli/index.md) gives the help of each.

---

## Run an Example

The local example models a shop that receives and packs customer orders. The simulation prints events and telemetry to the terminal. The example is written three ways: as a YAML blueprint, with the builder and with the low-level API. Each version runs in real time and ends on its own after 30 seconds. Pick the way you write models, then follow uv or pip:

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get local --api yaml
        cd local/yaml

        # 3. Run it
        desflow run local.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get local --api yaml
        cd local/yaml

        # 3. Run it
        desflow run local.yaml
        ```

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get local --api builder
        cd local/builder

        # 3. Run it
        uv run --no-project --with desflow local_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get local --api builder
        cd local/builder

        # 3. Run it
        python local_example.py
        ```

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get local --api low-level
        cd local/low-level

        # 3. Run it
        uv run --no-project --with desflow local_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get local --api low-level
        cd local/low-level

        # 3. Run it
        python local_example.py
        ```

`desflow examples` lists every example. Examples that need Kafka, a database or an object store start it with [odctl](https://github.com/jaehyeon-kim/odctl), and each example page shows how.

---

## Next Steps

- [First Simulation](tutorials/first-simulation.md) builds a shop model step by step. It is the first page of the shop tutorial in the Learn section, and each following page adds one idea to the model.
- The [examples](examples/index.md) show each connector, written three ways, with the profile each one needs.
