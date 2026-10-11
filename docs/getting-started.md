# Installation

This page installs Dynamic DES, runs a first simulation that needs no containers, and sets up the services the other examples use.

## Install the Package

Install the core library with uv or pip:

=== "uv"

    ```bash
    # The ddes command
    uv tool install dynamic-des

    # Or the library, in a uv project
    uv add dynamic-des
    ```

=== "pip"

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

---

## Run an Example

The local example is a production line that prints its events and telemetry to the terminal. It runs in real time and ends on its own: after 60 seconds in the YAML and declarative versions, 30 in the low-level one. Pick the way you write, then follow uv or pip:

=== "YAML"

    **Quick Start**

    `ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install dynamic-des

        # 2. Download the example
        ddes examples get local --api yaml
        cd local/yaml

        # 3. Run it
        ddes run local.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install dynamic-des

        # 2. Download the example
        ddes examples get local --api yaml
        cd local/yaml

        # 3. Run it
        ddes run local.yaml
        ```

=== "Declarative"

    **Quick Start**

    `ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install dynamic-des

        # 2. Download the example
        ddes examples get local --api declarative
        cd local/declarative

        # 3. Run it
        uv run --no-project --with dynamic-des local_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install dynamic-des

        # 2. Download the example
        ddes examples get local --api declarative
        cd local/declarative

        # 3. Run it
        python local_example.py
        ```

=== "Low-level"

    **Quick Start**

    `ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install dynamic-des

        # 2. Download the example
        ddes examples get local --api low-level
        cd local/low-level

        # 3. Run it
        uv run --no-project --with dynamic-des local_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install dynamic-des

        # 2. Download the example
        ddes examples get local --api low-level
        cd local/low-level

        # 3. Run it
        python local_example.py
        ```

`ddes examples` lists every example.

---

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

![The local and parquet examples need nothing started; kafka and backfill-live need odctl up kafka-lite; postgres and related-records need postgres; redis needs valkey; iceberg needs catalog; parquet written to S3 needs storage](assets/diagrams/example-infrastructure.png)

| Example | Start | Stop |
|---|---|---|
| local | nothing needed | |
| kafka, backfill-live, dashboard | `odctl up kafka-lite` | `odctl down kafka-lite --volumes` |
| postgres | `odctl up postgres` | `odctl down postgres --volumes` |
| redis | `odctl up valkey` | `odctl down valkey --volumes` |
| parquet with `PARQUET_FILESYSTEM=s3` | `odctl up storage` | `odctl down storage --volumes` |
| iceberg | `odctl up catalog` | `odctl down catalog --volumes` |

The YAML blueprints in `examples/yaml/` need the same profile as their declarative twin.

Kafka and Redis are the two whose profile names are not what you would guess, because odctl ships a one-broker Kafka as `kafka-lite` and uses Valkey rather than Redis.

### Endpoints These Profiles Publish

The Postgres database is `odctl`. The object store bucket is `odctl-dev`. Valkey requires the `user` / `password` credentials, so the connection URL is `redis://user:password@localhost:6379/0`.

---

## Next Steps

- [First Simulation](tutorials/first-simulation.md) builds a factory step by step, then adds random times, capacity changes and Kafka.
- The [examples](examples/index.md) show each connector, written three ways, with the profile each one needs.
