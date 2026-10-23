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
