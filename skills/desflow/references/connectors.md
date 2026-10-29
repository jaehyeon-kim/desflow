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

## Serializers and Deserializers

The serializer of a `KafkaEgress` topic and the deserializer of a `KafkaIngress` topic, given in Python. Each needs its own extra.

| Format | Schema registry | Extra | Serializer | Deserializer |
|---|---|---|---|---|
| Avro | Confluent Schema Registry | `confluent` | `desflow.connectors.egress.kafka.ConfluentAvroSerializer` | `desflow.connectors.ingress.kafka.ConfluentAvroDeserializer` |
| Avro | AWS Glue Schema Registry | `glue` | `desflow.connectors.egress.kafka.GlueAvroSerializer` | `desflow.connectors.ingress.kafka.GlueAvroDeserializer` |
| JSON Schema | Confluent Schema Registry | `confluent` | `desflow.connectors.egress.kafka.ConfluentJsonSchemaSerializer` | `desflow.connectors.ingress.kafka.ConfluentJsonSchemaDeserializer` |
| JSON Schema | AWS Glue Schema Registry | `glue` | `desflow.connectors.egress.kafka.GlueJsonSchemaSerializer` | `desflow.connectors.ingress.kafka.GlueJsonSchemaDeserializer` |
| Protobuf | Confluent Schema Registry | `confluent` | `desflow.connectors.egress.kafka.ConfluentProtobufSerializer` | `desflow.connectors.ingress.kafka.ConfluentProtobufDeserializer` |
| Protobuf | AWS Glue Schema Registry | `glue` | `desflow.connectors.egress.kafka.GlueProtobufSerializer` | `desflow.connectors.ingress.kafka.GlueProtobufDeserializer` |

The Confluent classes take `auth`, `verify`, `cert`, `timeout`, `headers`, `proxy` and `max_retries` for a secured registry. Each step of a request to the registry times out after 10 seconds by default; `timeout=None` waits without a limit.

The JSON Schema serializers take the schema as the JSON string `schema_str`. The Protobuf classes take `message_type`, the class that `protoc` generated. `GlueProtobufSerializer` also takes `schema_str`, the text of the `.proto` file. A deserializer returns a 64 bit integer field of a Protobuf message as a string.
