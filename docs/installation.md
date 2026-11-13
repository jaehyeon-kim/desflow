# Installation

This page installs desflow and runs a first simulation that needs no containers.

## Install the Package

Install the core library with uv or pip:

=== "uv"

    ```bash
    # desflow command
    uv tool install desflow

    # Or the library, in a uv project
    uv add desflow
    ```

=== "pip"

    ```bash
    pip install desflow
    ```

desflow was called dynamic-des before 0.20.0. The package, the Python module and the command are now all named `desflow`. Version 1.0 removed the `dynamic_des` module and the `ddes` command. Code that imports `dynamic_des` and scripts that run `ddes` must change to `desflow`, and [Upgrading to 1.0](about/upgrading-to-1.0.md#removed-names) says what to change.

Connectors and input fitting are optional extras. For example, `uv add "desflow[kafka,parquet]"` or `pip install "desflow[kafka,parquet]"`, and `uv tool install "desflow[kafka,parquet]"` for the `desflow` command:

| Extra | Adds |
|---|---|
| `kafka` | Kafka ingress and egress |
| `confluent` | Avro, JSON Schema and Protobuf with the Confluent Schema Registry (includes `kafka`) |
| `glue` | Avro, JSON Schema and Protobuf with the AWS Glue Schema Registry (includes `kafka`) |
| `redis` | Redis ingress and egress |
| `postgres` | PostgreSQL ingress and egress |
| `parquet` | Parquet and JSONL files on local disk, S3-compatible storage, Google Cloud Storage or Azure Blob Storage |
| `iceberg` | Apache Iceberg tables through a REST catalog, a SQL catalog on SQLite or PostgreSQL, or the Glue catalog |
| `fit` | scipy and matplotlib, for [input fitting](user-guide/model-parts/input-fitting.md) with `desflow fit` |
| `fake` | Faker, for the `fake` generator of realistic names, addresses and dates |
| `watch` | watchfiles, for `desflow run --watch`, which runs the file again each time it changes |
| `all` | Every extra above |

Two cautions apply to the `fake` extra. A seeded `fake` field gives the same values again only with the same version of Faker, because 1.0 does not record the version. A Faker method that reads the date, such as `date_between`, reads the wall clock and not the simulation clock, so it gives other values on another day.

On Python 3.15, the `kafka`, `confluent` and `glue` extras build aiokafka, and the `iceberg` extra builds pyiceberg and zstandard, from source, because those packages publish no wheel for 3.15 yet. A build from source needs a C compiler.

Run `desflow` on its own to list its commands. [Command Line](cli/index.md) uses each command on one shop, the model of the `experiment` example, and links the full help of each.

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

- **[Learn](learn/index.md):** teaches discrete-event simulation and desflow from the start, through a tutorial that builds one shop model. Follow the tutorial in order. Read the Simulation Concepts pages for the theory after the tutorial, or when a tutorial page links to one.
- **[User Guide](user-guide/index.md):** describes each part of desflow, what it does and all its options. Read a page when you need that part.
- **[How-To](how-to/index.md):** does one task per page, step by step, from modelling to connectors and analysis. It also holds complete examples to download and run.
