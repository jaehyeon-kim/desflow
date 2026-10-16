"""Each connector built and attached to a context, as Ingress and Egress show them.

The addresses are those of the odctl profiles the examples use. Building a connector
opens no connection: it connects when the run starts, so tests/docs run this file
without any service.
"""

from pyarrow import fs
from pyiceberg.catalog.rest import RestCatalog

from desflow import (
    ConsoleEgress,
    IcebergStorageEgress,
    JsonlStorageEgress,
    KafkaEgress,
    KafkaIngress,
    LocalIngress,
    ParquetStorageEgress,
    PostgresEgress,
    PostgresIngress,
    RedisEgress,
    RedisIngress,
    SimulationContext,
)

app = SimulationContext(sim_id="Shop")

# --8<-- [start:local-ingress]
# 10 wall-clock seconds after the start, set the packers capacity to 3
app.add_ingress(
    LocalIngress(schedule=[(10.0, "Shop.resources.packers.current_cap", 3)])
)
# --8<-- [end:local-ingress]

# --8<-- [start:kafka-ingress]
# Reads {"path": ..., "value": ...} messages from the sim-config topic
app.add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092"))
# --8<-- [end:kafka-ingress]

# --8<-- [start:redis-ingress]
# Reads {"path": ..., "value": ...} messages from a Pub/Sub channel
app.add_ingress(
    RedisIngress(
        "redis://user:password@localhost:6379/0", channel_name="simulation_params"
    )
)
# --8<-- [end:redis-ingress]

# --8<-- [start:postgres-ingress]
# Applies new rows (path, value) of simulation_params, checking every 2 seconds
app.add_ingress(
    PostgresIngress(
        "postgresql://user:password@localhost:5432/odctl",
        table_name="simulation_params",
        poll_interval=2.0,
    )
)
# --8<-- [end:postgres-ingress]

# --8<-- [start:console-egress]
app.add_egress(ConsoleEgress())
# --8<-- [end:console-egress]

# --8<-- [start:kafka-egress]
app.add_egress(
    KafkaEgress(
        bootstrap_servers="localhost:9092",
        event_topic="sim-events",
        telemetry_topic="sim-telemetry",
    )
)
# --8<-- [end:kafka-egress]

# --8<-- [start:storage-egress]
# Parquet files on the local disk
app.add_egress(ParquetStorageEgress(default_path="output/events"))

# JSON Lines files on S3-compatible storage, here SeaweedFS from odctl
seaweedfs = fs.S3FileSystem(
    access_key="user",
    secret_key="password",
    endpoint_override="127.0.0.1:8333",
    scheme="http",
)
app.add_egress(
    JsonlStorageEgress(default_path="warehouse/events", filesystem=seaweedfs)
)
# --8<-- [end:storage-egress]

# --8<-- [start:redis-egress]
# XADD to the events stream, unless a record's value names another in __stream__
app.add_egress(
    RedisEgress("redis://user:password@localhost:6379/0", stream_name="events")
)
# --8<-- [end:redis-egress]

# --8<-- [start:postgres-egress]
dsn = "postgresql://user:password@localhost:5432/odctl"
app.add_egress(PostgresEgress(dsn, table_name="orders"))
# --8<-- [end:postgres-egress]

# --8<-- [start:postgres-upsert]
app.add_egress(PostgresEgress(dsn, table_name="orders", upsert_keys=["order_id"]))
# --8<-- [end:postgres-upsert]

# --8<-- [start:iceberg-egress]
catalog = RestCatalog(
    "odctl",
    uri="http://localhost:8181",
    warehouse="s3://warehouse/",
    **{
        "s3.endpoint": "http://localhost:8333",
        "s3.access-key-id": "user",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    },
)
# One commit per flush, so this sink gets a large batch of its own
app.add_egress(
    IcebergStorageEgress(catalog=catalog, default_table="sim.events"),
    batch_size=200_000,
)
# --8<-- [end:iceberg-egress]

# --8<-- [start:iceberg-upsert]
app.add_egress(
    IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.orders",
        upsert_keys={"sim.orders": ["order_id"]},
    ),
    batch_size=200_000,
)
# --8<-- [end:iceberg-upsert]
