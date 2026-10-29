# Connectors

The short names a connector `type` takes in `ingress` and `egress`. A name is written with its capital letter. `type` also takes a `!python` reference to your own class. A connector that needs an extra is installed with `pip install "desflow[<extra>]"`.

## Ingress

| Short name | Class | Extra |
|---|---|---|
| `Local` | `desflow.connectors.ingress.local.LocalIngress` | none |
| `Kafka` | `desflow.connectors.ingress.kafka.KafkaIngress` | `kafka` |
| `Postgres` | `desflow.connectors.ingress.postgres.PostgresIngress` | `postgres` |
| `Redis` | `desflow.connectors.ingress.redis.RedisIngress` | `redis` |

## Egress

| Short name | Class | Extra |
|---|---|---|
| `Console` | `desflow.connectors.egress.local.ConsoleEgress` | none |
| `Kafka` | `desflow.connectors.egress.kafka.KafkaEgress` | `kafka` |
| `Parquet` | `desflow.connectors.egress.storage.ParquetStorageEgress` | `parquet` |
| `Jsonl` | `desflow.connectors.egress.storage.JsonlStorageEgress` | `parquet` |
| `Iceberg` | `desflow.connectors.egress.iceberg.IcebergStorageEgress` | `iceberg` |
| `Postgres` | `desflow.connectors.egress.postgres.PostgresEgress` | `postgres` |
| `Redis` | `desflow.connectors.egress.redis.RedisEgress` | `redis` |

## Storage

A `Parquet` or `Jsonl` egress writes through a `filesystem` mapping. Its `type` is one of the PyArrow classes below, and its other keys are the arguments of that class.

| `filesystem` type | PyArrow class |
|---|---|
| `local` | `LocalFileSystem` |
| `s3` | `S3FileSystem` |
| `gcs` | `GcsFileSystem` |
| `azure` | `AzureFileSystem` |

An `Iceberg` egress writes through a `catalog` mapping of PyIceberg properties. Its `type` is `rest` for an Iceberg REST catalog, `sql` for a SQL catalog on SQLite or PostgreSQL, or `glue` for AWS Glue, and the `iceberg` extra installs all three.

## Avro Serializers

A `KafkaEgress` serializer of a topic, given in Python. Each needs its own extra.

| Class | Schema registry | Extra |
|---|---|---|
| `desflow.connectors.egress.kafka.ConfluentAvroSerializer` | Confluent Schema Registry | `confluent` |
| `desflow.connectors.egress.kafka.GlueAvroSerializer` | AWS Glue Schema Registry | `glue` |

`ConfluentAvroSerializer` and `ConfluentAvroDeserializer` take `auth`, `verify`, `cert`, `timeout`, `headers`, `proxy` and `max_retries` for a secured registry. Each step of a request to the registry times out after 10 seconds by default; `timeout=None` waits without a limit.
