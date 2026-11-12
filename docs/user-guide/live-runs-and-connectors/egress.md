# Egress, Batching and Delivery

An egress connector, or sink, writes the records a run publishes: to the console, Kafka, Redis, PostgreSQL, Parquet or JSON Lines files, or Iceberg tables. This page shows each sink, how to choose which records each one gets, and how records travel from a task to a sink.

---

## What It Is

A run publishes three kinds of record in the ordinary case: **events**, such as a task's `queued`, `started` and finished; **telemetry**, single values such as the statistics; and **labels**, the ground-truth records of what a scenario changed. Every sink receives every event and every telemetry record, unless its `when` says otherwise. Labels go only to sinks that ask for them. A run can publish a fourth kind, **trace**, one record for each event the environment processes, only when `stats.trace` is set, and only to a sink with `when: trace`. [Records and Telemetry](../../reference/records-and-telemetry.md) shows each record's shape and what each sink writes.

![Parameter changes from outside reach the registry through LocalIngress, FileIngress, KafkaIngress, RedisIngress or PostgresIngress; records from tasks and processes leave through ConsoleEgress, KafkaEgress, ParquetStorageEgress, JsonlStorageEgress, RedisEgress, PostgresEgress or IcebergStorageEgress](../../assets/diagrams/connectors-overview.png)

The tutorial page [Streaming Out](../../learn/running-live/streaming-out.md) teaches these features step by step.

---

## How to Use It

Each sink is listed under `egress` in a blueprint with its `type` and `config`, attached with `add_egress` in the builder, or passed to `env.setup_egress` in the low-level API. In the builder snippets below, `app` is a `SimulationContext`.

In a blueprint, `type` is one of the short names below, or a `!python` class from another package. A connector's module is imported only when a blueprint names it, so a blueprint that uses Kafka does not need the PostgreSQL driver. Parquet and JSON Lines are the exception: they import PyArrow when they start writing. A missing package stops the load with its line and the `pip install` command.

| `type` | Class | Extra |
|---|---|---|
| `Console` | `ConsoleEgress` | none |
| `Kafka` | `KafkaEgress` | `kafka` |
| `Parquet` | `ParquetStorageEgress` | `parquet` |
| `Jsonl` | `JsonlStorageEgress` | `parquet` |
| `Iceberg` | `IcebergStorageEgress` | `iceberg` |
| `Postgres` | `PostgresEgress` | `postgres` |
| `Redis` | `RedisEgress` | `redis` |

`config` holds the constructor's arguments. Settings that Python takes as objects are plain values there: a Parquet or JSON Lines `filesystem` is a mapping with `type: local`, `s3`, `gcs` or `azure`, and an Iceberg `catalog` is a mapping of PyIceberg properties. A Postgres `tables` mapping creates its tables when the run starts. Each table maps to `columns`, a mapping of column name to SQL type, and an optional `primary_key`. An ISO time string bound for a timestamp or date column of Iceberg or PostgreSQL is converted before the write. [YAML Keys](../../reference/yaml-keys.md#egress) lists the keys of an egress entry.

### Console

`ConsoleEgress` has two formats. The default, `log`, logs each batch at INFO level with one call, one line per record:

- Each line starts with `[EVT]` for an event, `[TEL]` for telemetry or `[LBL]` for a label. A trace record, which only a `when: trace` sink receives, starts with `[EVT]` too.
- `desflow run` prints these lines as they are. Its other log lines start with the time, level and logger name.
- In a script, the logging format the script sets, such as the time and level, starts only the first line of each batch.

One call per batch is much faster than one call per record. In the measurements on [Tune Performance](../../how-to/tools/performance-tuning.md), a run that logged each record with its own call took about 50 percent longer in total.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:console-egress"
    - {type: Console}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/connectors.py:console-egress"
    app.add_egress(ConsoleEgress())
    ```

=== "Low-level"

    The low-level API passes the same `ConsoleEgress()` to `env.setup_egress`, in its list of sinks, as [Low-level API](../writing-a-model/low-level-api.md#attach-the-connectors) shows.

With `format="json"`, `ConsoleEgress` writes each record to stdout as one JSON line instead: the record as published, with its `stream_type`. Another program can then read the records. It writes at any log level, because the records are output, not a log. Python's logging writes to stderr by default, so the log stays apart from the records, and a pipe such as `| jq .key` reads only the records. In a blueprint it is `type: Console` with `config: {format: json}`. `desflow run --sample` and `--stdout` print through it in place of the file's sinks, as [YAML Blueprints](../writing-a-model/blueprints.md#preview-the-records) describes.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:console-json"
    # One JSON line per record on stdout, stream_type included
    - type: Console
      config: {format: json}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/connectors.py:console-json"
    # One JSON line per record on stdout, stream_type included
    app.add_egress(ConsoleEgress(format="json"))
    ```

=== "Low-level"

    The low-level API passes the same `ConsoleEgress(format="json")` to `env.setup_egress`, in its list of sinks, as [Low-level API](../writing-a-model/low-level-api.md#attach-the-connectors) shows.

A consumer can check each line against the record schema: `EventPayload`, `TelemetryPayload` and `LabelPayload` are the Pydantic models of the three stream types, as [Records and Telemetry](../../reference/records-and-telemetry.md) describes:

```python title="docs/snippets/pages/record_schemas.py:validate"
# The model of each stream type; a record with a missing or mistyped field raises
SCHEMAS = {"event": EventPayload, "telemetry": TelemetryPayload, "label": LabelPayload}
for line in printed.getvalue().splitlines():
    record = json.loads(line)
    SCHEMAS[record["stream_type"]].model_validate(record)
```

The `trace` record has no model in `SCHEMAS`. A consumer reads it as a record whose `value` is a mapping with the keys `priority`, `event_id` and `event`.

### Kafka

`KafkaEgress` sends events to `event_topic` and telemetry to `telemetry_topic`, keyed by the record's `key` or `path_id`. Kafka sends all records with one key to one partition, so all events of one entity stay in order. `KafkaEgress` creates both topics when the run starts, with one partition each, if they do not exist. With a `topic_router`, it creates neither of them, and only the topics named in `topics`. [Route Records to Topics](../../how-to/records-and-kafka/complex-routing.md) sends records to other topics, and [Connect to Secure Kafka](../../how-to/records-and-kafka/kafka-security.md) connects to a cluster that uses SASL, mTLS, OAuth or AWS IAM.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:kafka-egress"
    - type: Kafka
      config:
        bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}
        event_topic: sim-events
        telemetry_topic: sim-telemetry
    ```

=== "Builder"

    ```python title="docs/snippets/pages/connectors.py:kafka-egress"
    app.add_egress(
        KafkaEgress(
            bootstrap_servers="127.0.0.1:9092",
            event_topic="sim-events",
            telemetry_topic="sim-telemetry",
        )
    )
    ```

=== "Low-level"

    The low-level API passes the same `KafkaEgress` to `env.setup_egress`, in its list of sinks, as [Low-level API](../writing-a-model/low-level-api.md#attach-the-connectors) shows.

**Topics.** `topic_policy` says what the sink does with its topics when the run starts. It is a word, or a mapping of `mode`, `partitions`, `replication_factor` and `topic_configs`:

- `create`, the default, creates a missing topic and leaves an existing one as it is, with one partition, a replication factor of 1 and no settings of its own.
- `drop_and_create` deletes the topic and creates it again, so each run starts with an empty topic.
- `manual` creates nothing.

The policy covers the event topic and the telemetry topic. `topics` gives settings to other topics by name, each with the keys of `topic_policy` on top of the policy. A topic named there is created by the sink, also when a record chooses it with `__topic__` or a `topic_router`. A `topics` value must be a mapping, and a key that is not a policy key is refused when the blueprint loads.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:kafka-topics"
    # The policy covers the event and telemetry topics: three partitions each, with a
    # retention setting. sim-audit is created too, with one partition of its own
    - type: Kafka
      config:
        bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}
        event_topic: sim-events
        telemetry_topic: sim-telemetry
        topic_policy:
          mode: create
          partitions: 3
          topic_configs: {retention.ms: 3600000}
        topics:
          sim-audit: {partitions: 1}
    ```

=== "Builder"

    `KafkaEgress` takes the same `topic_policy` and `topics` as arguments.

=== "Low-level"

    The low-level API passes the same `KafkaEgress(topic_policy=..., topics=...)` to `env.setup_egress`.

An event whose `value` has a `__topic__` key goes to the topic it names, and the key is removed from the message. A `topic_router` wins over `__topic__`, and telemetry always goes to `telemetry_topic`. The sink also takes a `serializer` (a mapping with a `type` of `confluent_avro`, `confluent_json_schema`, `confluent_protobuf`, `glue_avro`, `glue_json_schema` or `glue_protobuf`, with the options of that type, such as `schema`, `schema_file`, `descriptor_file` and `message`), `ssl` and `sasl_oauth_token_provider` (`{type: aws_msk_iam, region: ...}`), and `serializer` can also be set for one topic under `topics`. In a blueprint, a `schema_file`, a `descriptor_file`, the `verify` and `cert` files of a serializer and the files of `ssl` are read from the blueprint's folder, as an `!include` is, so `desflow run sub/shop.yaml` and `cd sub && desflow run shop.yaml` read the same files. The Builder reads them from the working directory. [Use Schema Serializers](../../how-to/records-and-kafka/json-schema-and-protobuf.md) and [Connect to Secure Kafka](../../how-to/records-and-kafka/kafka-security.md) use these options. The registry options of the Avro serializers (`auth`, `verify`, `cert`, `headers`, `proxy`, `timeout` and `max_retries`) are in the step [Connect to a Secured Schema Registry](../../how-to/records-and-kafka/kafka-security.md#connect-to-a-secured-schema-registry).

### Parquet and JSON Lines

`ParquetStorageEgress` and `JsonlStorageEgress` write each batch as a file of its own, on a local disk, S3-compatible storage, Google Cloud Storage or Azure Blob Storage, through PyArrow. Without a `path_router`, each event is one flat row in `default_path`, with its `value` mapping unpacked into columns, and telemetry is left out.

A Parquet file has a column for every key of any row in its batch, and a mapping value becomes a struct column. A later batch can change the columns:

- A key that first appears in a later batch adds a column to the later files. A reader that combines the columns of all the files reads it as null in the earlier rows.
- An integer column that a later batch gives a float becomes a double in the later files.
- A nested key that first appears later is dropped, because the first batch with a mapping value fixes the keys of its struct column.

The first batch sets the type of each column. A number in the first batch is read as a double for the `value` of a telemetry record, for the `now` and `peek` of a `run.steps` or `run.segments` reading, and for `usage_since`, so a later fraction is kept. A statistic (a number) and an event or a reading (a mapping) cannot share a file, because the `value` column has one type. Send them to different files with a `path_router`, because the default route writes no telemetry.

A batch that cannot be written is logged at ERROR, and the sink goes on with the next one.

**Routing by `__path__`.** An event whose `value` has a `__path__` key is written to the file path it names, and the key is removed from the row. A `path_router` wins over `__path__`, and a record with no `__path__` goes to `default_path`. `__path__` is relative to the working directory or to the sink's `filesystem`, and it is not placed under the folder of `default_path`. A path built with `{gen: format_time, format: "folder/%Y-%m-%d.jsonl"}` gives each simulated day its own files, `folder/2026-01-01_<id>.jsonl`, with one file for each flush. [Write to Cloud Storage](../../how-to/storage-and-tables/cloud-storage.md) runs it.

**File names.** `default_path`, and any path a `path_router` returns or `__path__` names, is a file name prefix, not a folder. Each batch is written as a file of its own, `<stem>_<8 hex digits><extension>`, in the path's folder, so `output/events.parquet` gives files such as `output/events_1a2b3c4d.parquet`. Give the extension in the path: `output/events` gives files with none. The folder is created on the first write.

**Columns of a finished event.** A task's `queued` and `started` events carry `path_id` and `status`. Its finished event's value is the task's payload only. So in a finished event's row, `path_id` and `status` are null unless the payload has them. The [Parquet example](../../gallery/simulation-models/parquet.md) adds them to its payload for that reason.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:storage-egress"
    # Parquet files on the local disk, output/events_<8 hex digits>.parquet
    - type: Parquet
      config: {default_path: output/events.parquet}
    # JSON Lines files on S3-compatible storage, here SeaweedFS from odctl; a mapping
    # becomes PyArrow's S3FileSystem
    - type: Jsonl
      config:
        default_path: warehouse/events.jsonl
        filesystem:
          type: s3
          access_key: user
          secret_key: password
          endpoint_override: 127.0.0.1:8333
          scheme: http
    ```

=== "Builder"

    ```python title="docs/snippets/pages/connectors.py:storage-egress"
    # Parquet files on the local disk, output/events_<8 hex digits>.parquet
    app.add_egress(ParquetStorageEgress(default_path="output/events.parquet"))

    # JSON Lines files on S3-compatible storage, here SeaweedFS from odctl
    seaweedfs = fs.S3FileSystem(
        access_key="user",
        secret_key="password",
        endpoint_override="127.0.0.1:8333",
        scheme="http",
    )
    app.add_egress(
        JsonlStorageEgress(default_path="warehouse/events.jsonl", filesystem=seaweedfs)
    )
    ```

=== "Low-level"

    The low-level API passes the same sinks to `env.setup_egress`, in its list of sinks, as [Low-level API](../writing-a-model/low-level-api.md#attach-the-connectors) shows.

Read the files back as one table with `pyarrow.dataset`, which reads every file in the folder:

```python title="docs/snippets/pages/file_sink.py:read"
def read_events(folder="output"):
    """Every file the sink wrote under `folder`, as one table."""
    import pyarrow.dataset as ds

    # format="json" reads the files of a JsonlStorageEgress the same way
    return ds.dataset(folder, format="parquet").to_table()
```

A second run adds its files beside the first run's, so reading the folder then counts both runs. Give each run a folder of its own, or clear the folder before a run.

**Filesystems.** `filesystem` is a PyArrow `FileSystem` object, or a mapping that desflow builds into one when the run starts. The mapping's `type` picks the class, and its other keys are the arguments of that class:

| `type` | PyArrow class | Credentials |
|---|---|---|
| `local` (the default) | `LocalFileSystem` | none |
| `s3` | `S3FileSystem` | `access_key` and `secret_key`; without them, the standard AWS settings |
| `gcs` | `GcsFileSystem` | `anonymous: true`; or `access_token` with `credential_token_expiration`; without them, Google's default credentials |
| `azure` | `AzureFileSystem` | `account_name`, always; with it `account_key`, or `sas_token` (pyarrow 20.0.0 or later), or `client_id`, `client_secret` and `tenant_id` (pyarrow 21.0.0 or later); with `account_name` alone, Azure's default credentials |

An `endpoint_override` that starts with `http://` or `https://` also sets `scheme`, for `s3` and `gcs`, unless `scheme` is given. `azure` has no `endpoint_override`: `blob_storage_authority` and `blob_storage_scheme` point it at an emulator such as Azurite. For `gcs`, `credential_token_expiration` is an ISO 8601 time, quoted or not, which desflow converts to UTC, and `retry_time_limit` is a number of seconds, because YAML has no duration type. A key whose value is empty is left out. PyArrow has `AzureFileSystem` from 16.0.0, the release the `parquet` extra requires. `sas_token` and the client credentials need the later releases the table names. PyPI's pyarrow wheels for Windows include it only from 24.0.0.

The same two sinks write to Google Cloud Storage and Azure Blob Storage when `filesystem` is a mapping. [Write to Cloud Storage](../../how-to/storage-and-tables/cloud-storage.md) runs a shop against each store, from start to end:

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/storage_backends.yaml:cloud"
    # Parquet files on Google Cloud Storage, with Google's default credentials
    - type: Parquet
      config:
        default_path: my-bucket/events.parquet
        # type: gcs makes PyArrow's GcsFileSystem; without credentials it uses Google's
        # default ones
        filesystem: {type: gcs}
    # JSON Lines files on Azure Blob Storage, with Azure's default credentials
    - type: Jsonl
      config:
        default_path: my-container/events.jsonl
        # type: azure makes PyArrow's AzureFileSystem; without a key it uses Azure's
        # default credentials
        filesystem: {type: azure, account_name: myaccount}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/storage_backends.py:cloud"
    # Parquet files on Google Cloud Storage, with Google's default credentials
    cloud.add_egress(
        ParquetStorageEgress(
            default_path="my-bucket/events.parquet",
            filesystem={"type": "gcs"},
        )
    )
    # JSON Lines files on Azure Blob Storage, with Azure's default credentials
    cloud.add_egress(
        JsonlStorageEgress(
            default_path="my-container/events.jsonl",
            filesystem={"type": "azure", "account_name": "myaccount"},
        )
    )
    ```

=== "Low-level"

    The low-level API passes the same sinks to `env.setup_egress`, in its list of sinks, as [Low-level API](../writing-a-model/low-level-api.md#attach-the-connectors) shows.

### Redis

`RedisEgress` appends each record to a Redis Stream with `XADD`, as one `payload` field holding the record as JSON. A `__stream__` key in the record's value picks the stream, and is removed before writing.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:redis-egress"
    # XADD to the events stream, unless a record's value names another in __stream__
    - type: Redis
      config: {url: redis://user:password@127.0.0.1:6379/0, stream_name: events}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/connectors.py:redis-egress"
    # XADD to the events stream, unless a record's value names another in __stream__
    app.add_egress(
        RedisEgress("redis://user:password@127.0.0.1:6379/0", stream_name="events")
    )
    ```

=== "Low-level"

    The low-level API passes the same `RedisEgress` to `env.setup_egress`, in its list of sinks, as [Low-level API](../writing-a-model/low-level-api.md#attach-the-connectors) shows.

### PostgreSQL

`PostgresEgress` inserts records into a table in batches. It writes only the records whose value is a mapping, and the labels on a labels sink, each label as `sim_ts`, `timestamp`, `path`, `old`, `new` and `label`. A record's row is its value, changed in four ways:

- The record's `sim_ts`, `timestamp`, `key` (as `event_id`) and `path_id` are added, and replace fields of the same name.
- Its `task` and `instance` are added too, but a value field of the same name keeps its own value.
- Keys with no matching column are dropped.
- A `__table__` key sends the record only to the sink whose `table_name` matches.

The columns are those of the one table that the name resolves to, as PostgreSQL reads it in the `CREATE TABLE` and `INSERT` statements. A name with capitals reads the table with the lower-case name, and a name with no schema reads the first table of that name on the `search_path`. `table_policy` says what the sink does about the tables in `tables` when the run starts: `create`, the default, creates the schema of a qualified name and the table when they do not exist, `drop_and_create` also drops the table first, and `manual` creates nothing.

The connection string names `odctl`, the database that the odctl `postgres` profile creates, with the user `user` and the password `password`.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:postgres-egress"
    - type: Postgres
      config:
        connection_dsn: postgresql://user:password@127.0.0.1:5432/odctl
        table_name: orders
    ```

=== "Builder"

    ```python title="docs/snippets/pages/connectors.py:postgres-egress"
    dsn = "postgresql://user:password@127.0.0.1:5432/odctl"
    app.add_egress(PostgresEgress(dsn, table_name="orders"))
    ```

=== "Low-level"

    The low-level API passes the same `PostgresEgress` to `env.setup_egress`, in its list of sinks, as [Low-level API](../writing-a-model/low-level-api.md#attach-the-connectors) shows.

By default a record whose key already exists is skipped. `upsert_keys` updates that row instead, for a model that changes rows it wrote earlier, such as an order moving from packed to shipped. The key columns, a list or one column name, need a unique index. An update sets only the columns the record carries, and within one flush the last record for a key wins. A record that has no value for a key column, or a null there, is skipped, and one warning at the end of each flush says how many were skipped and why.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:postgres-upsert"
    - type: Postgres
      config:
        connection_dsn: postgresql://user:password@127.0.0.1:5432/odctl
        table_name: orders
        upsert_keys: [order_id]
    ```

=== "Builder"

    ```python title="docs/snippets/pages/connectors.py:postgres-upsert"
    app.add_egress(PostgresEgress(dsn, table_name="orders", upsert_keys=["order_id"]))
    ```

=== "Low-level"

    The low-level API passes the same `PostgresEgress` to `env.setup_egress`, in its list of sinks, as [Low-level API](../writing-a-model/low-level-api.md#attach-the-connectors) shows.

### Iceberg

`IcebergStorageEgress` appends records to an Apache Iceberg table through a catalog. One flush is one commit, so this sink needs a large `batch_size` of its own: every commit writes new metadata, and many small commits slow down queries.

A table the sink creates takes its columns from the records:

- The first batch gives a column for every key that has a value in some row.
- A key that is null in every row waits for a batch that gives it a value, because Iceberg has no null type. An example is `task` in a low-level event published without one.
- A key that first appears in a later batch is added as an optional column, unless the table is named in `schemas`, which keeps the columns it gives. A table with `upsert_keys` refuses that batch instead, so name every column of an upsert table in `schemas`.

The first batch sets the type of each column. A whole number in the first batch gives an integer column, except that a telemetry `value`, the `now` and `peek` of a `run.steps` or `run.segments` reading and `usage_since` are read as doubles. A batch with a fraction for an integer column is logged at ERROR and its rows are not written. No value is truncated. Iceberg cannot change a `long` column to a double later, so a column that takes fractions is named in `schemas` before the table is created, such as `schemas: {shop.events: {key: string, v: double}}`: a table named there is created with exactly the columns listed, and every batch is cast to them. A statistic (a number) and an event or a reading (a mapping) need different tables, because the `value` column has one type. Send them apart with a `table_router`, because the default route writes no telemetry. [Write to Iceberg Tables](../../how-to/storage-and-tables/iceberg-tables.md) runs a batch that is lost and the fix.

An event whose `value` has a `__table__` key, a `namespace.table` name, is written to that table, and the key is removed. A `table_router` wins over `__table__`, and a record with no `__table__` goes to `default_table`. An event that Postgres and Iceberg both receive and that has a `__table__` without a namespace is refused by Iceberg, with "names no namespace": the error is logged and that batch is lost, where Iceberg used to drop the key and write to `default_table`. Give Iceberg a `table_router`, or write `namespace.table` in the key.

`table_policy` says what the sink does about a table at its first write, and every run of a context applies it again: `create`, the default, creates the namespace and the table when they do not exist, `drop_and_create` drops the table and creates it again once for each run, and `manual` creates nothing and fails the write if the table is missing. Each run also makes its own connection pool or client, in its own event loop, and makes the sink's folders again.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:table-policy"
    # Each run starts with an empty table: dropped and created again at the first write
    - type: Iceberg
      config:
        default_table: sim.events
        table_policy: drop_and_create
        catalog:
          name: odctl
          type: rest
          uri: http://127.0.0.1:8181
          warehouse: s3://warehouse/
      batch_size: 200000
    ```

=== "Builder"

    `IcebergStorageEgress` takes the same `table_policy` as an argument.

=== "Low-level"

    The low-level API passes the same `IcebergStorageEgress(table_policy=...)` to `env.setup_egress`.

**Catalogs.** `catalog` is a PyIceberg `Catalog` object, or a mapping of PyIceberg catalog properties that desflow passes to `load_catalog` on the first write. The mapping's optional `name` key names the catalog. Each catalog below has a test that writes through it:

| Catalog | `type` | Tested against |
|---|---|---|
| REST | `rest` | the odctl `catalog` profile |
| SQL on SQLite | `sql` | the unit tests, with a database file |
| SQL on PostgreSQL | `sql` | the odctl `postgres` profile |
| Glue | `glue` | Floci, an AWS emulator, not AWS itself |

The properties of each catalog are listed in [PyIceberg's configuration](https://py.iceberg.apache.org/configuration/). [Write to Iceberg Tables](../../how-to/storage-and-tables/iceberg-tables.md) runs a shop through each catalog, from start to end.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:iceberg-egress"
    # A mapping of PyIceberg catalog properties, here the REST catalog of odctl.
    # One commit per flush, so this sink gets a large batch of its own
    - type: Iceberg
      config:
        default_table: sim.events
        catalog:
          name: odctl
          type: rest
          uri: http://127.0.0.1:8181
          warehouse: s3://warehouse/
          s3.endpoint: http://127.0.0.1:8333
          s3.access-key-id: user
          s3.secret-access-key: password
          s3.region: us-east-1
      batch_size: 200000
    ```

=== "Builder"

    ```python title="docs/snippets/pages/connectors.py:iceberg-egress"
    catalog = RestCatalog(
        "odctl",
        uri="http://127.0.0.1:8181",
        warehouse="s3://warehouse/",
        **{
            "s3.endpoint": "http://127.0.0.1:8333",
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
    ```

=== "Low-level"

    The low-level API passes the same `IcebergStorageEgress` to `env.setup_egress`, in its list of sinks, and gives its batch size in `batch_sizes`, as [Batch Size and Flush Interval](#batch-size-and-flush-interval) shows.

A SQL catalog on SQLite needs no service, so it suits a first run. Its database file and the table files are under one folder:

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/storage_backends.yaml:sqlite"
    # The catalog's database and the table files are under the folder iceberg/
    - type: Iceberg
      config:
        default_table: sim.events
        # The folder iceberg/ must exist before the run
        catalog:
          name: local
          type: sql
          uri: sqlite:///iceberg/catalog.db
          warehouse: file://${PWD}/iceberg/warehouse
    ```

=== "Builder"

    ```python title="docs/snippets/pages/storage_backends.py:sqlite"
    # The catalog's database and the table files are under the folder iceberg/
    folder = Path("iceberg").resolve()
    folder.mkdir(exist_ok=True)
    catalog = {
        "name": "local",
        "type": "sql",
        "uri": f"sqlite:///{folder}/catalog.db",
        "warehouse": f"file://{folder}/warehouse",
    }
    app.add_egress(IcebergStorageEgress(catalog=catalog, default_table="sim.events"))
    ```

=== "Low-level"

    The low-level API passes the same `IcebergStorageEgress` to `env.setup_egress`, in its list of sinks, as [Low-level API](../writing-a-model/low-level-api.md#attach-the-connectors) shows.

`upsert_keys` upserts on key columns instead of appending, and each table takes a list of columns or one column name. A seeded model rerun over the same window writes rows with the same keys, so the upsert replaces those rows instead of duplicating them. An upsert reads the matching rows first, replaces whole rows, never deletes, and needs pyiceberg 0.9.0 or later. A table with `upsert_keys` cannot gain a column, because the matching rows in the old data files lack it. A batch that brings a column the table lacks is logged as an error that names the table and the columns, and is not written, so name every column in `schemas` before the table is created. A record that lacks a key column, or holds null in it, is skipped, and one warning at the end of each flush says how many were skipped and why. Before, `IcebergStorageEgress` raised `ValueError` for such a record. An entry that names its tables in `schemas`, `locations` or `upsert_keys` needs no `default_table` and no router.

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:iceberg-upsert"
    - type: Iceberg
      config:
        default_table: sim.orders
        upsert_keys: {sim.orders: [order_id]}
        catalog:
          name: odctl
          type: rest
          uri: http://127.0.0.1:8181
          warehouse: s3://warehouse/
      batch_size: 200000
    ```

=== "Builder"

    ```python title="docs/snippets/pages/connectors.py:iceberg-upsert"
    app.add_egress(
        IcebergStorageEgress(
            catalog=catalog,
            default_table="sim.orders",
            upsert_keys={"sim.orders": ["order_id"]},
        ),
        batch_size=200_000,
    )
    ```

=== "Low-level"

    The low-level API passes the same `IcebergStorageEgress` to `env.setup_egress`, in its list of sinks, and gives its batch size in `batch_sizes`, as [Batch Size and Flush Interval](#batch-size-and-flush-interval) shows.

### Choose What Each Sink Gets

Every sink receives every event and telemetry record. `when` narrows that down:

- **A function** taking one record and returning True to send it to this sink, in the builder or as `!python` in a blueprint.
- **`events` and `telemetry`**, in a blueprint: only the event records, or only the telemetry records.
- **`{expr: ...}`**, in a blueprint: an expression over the record, written with `record.stream_type` (`event` or `telemetry`), `record.key`, `record.task` and `record.value.<field>`. The sink receives the record when the expression is true. A name the record lacks reads as null, so an equality with it is false. An ordering comparison such as `record.value.amount > 100` fails the run on the first record that has no `amount`, so write `record.stream_type == 'event' and ...` first. `t` is refused in a `when` expression.
- **`history` and `live`**, in a blueprint: the records stamped before `simulation.go_live_at`, and the rest, as [Clocks and Pacing](../time-and-pacing/clocks-and-pacing.md#go-live) describes.
- **`trace`**: the event trace of `stats.trace`, and nothing else. [Built-in Statistics](../results/statistics.md#trace-the-events) sets it up. A run with a `trace` sink and no `stats.trace` is refused when it loads.
- **`labels`**: the ground-truth labels, and nothing else. No other sink receives a label, so data and labels always reach different sinks. [Live Parameters](live-parameters.md#record-what-a-change-was-for) describes them.

=== "YAML"

    ```yaml title="docs/snippets/yaml/batching.yaml:egress"
    egress:
      # Small and often for this sink; the other uses the defaults
      - {type: Console, batch_size: 50, flush_interval: 0.5}
      # Only the records stamped before go_live_at
      - {type: Console, when: history}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/batching.py:every-sink"
    app.add_egress(stream).add_egress(lake)  # both receive every record
    ```

    ```python title="docs/snippets/pages/batching.py:when"
    # A record's timestamp is an ISO string with its zone offset, so read it as a time
    hot_from = datetime.now(timezone.utc)


    def is_hot(record) -> bool:
        return datetime.fromisoformat(record["timestamp"]) >= hot_from


    app.add_egress(stream, when=is_hot)  # hot tail
    app.add_egress(lake, when=lambda r: not is_hot(r))  # cold history
    app.add_egress(audit)  # no predicate: everything
    ```

=== "Low-level"

    `setup_egress` takes the functions in `predicates`, one for each sink by position, as [Batch Size and Flush Interval](#batch-size-and-flush-interval) shows.

The `events`, `telemetry` and `{expr}` forms are YAML forms:

=== "YAML"

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:when-forms"
    # Only the event records
    - {type: Console, when: events}
    # Only the telemetry records
    - {type: Console, when: telemetry}
    # Only the events that carry an amount over 100
    - type: Console
      when: {expr: "record.stream_type == 'event' and record.value.amount > 100"}
    ```

=== "Builder"

    `add_egress(sink, when=...)` takes a function, as above. A function can test `record["stream_type"]` and `record["value"]` for the same result.

=== "Low-level"

    `predicates` in `setup_egress` takes the same functions.

A labels sink gets the ground-truth labels and nothing else:

=== "YAML"

    ```yaml title="docs/snippets/yaml/ground_truth.yaml:sinks"
    egress:
      # The data under test: events and telemetry, and no label
      - {type: Console}
      # The labels only, for a scorer that checks what a detector found
      - type: Jsonl
        config: {default_path: labels/labels.jsonl}
        when: labels
    ```

=== "Builder"

    ```python title="docs/snippets/pages/batching.py:labels"
    app.add_egress(stream)  # events and telemetry, never a label
    app.add_egress(audit, when="labels")  # ground-truth labels, and nothing else
    ```

=== "Low-level"

    `labels` in `setup_egress` marks each labels sink with True, by position, as [Batch Size and Flush Interval](#batch-size-and-flush-interval) shows.

A file sink such as this one writes one file per batch under `labels/`, such as `labels/labels_1a2b3c4d.jsonl`. A run's `warmup` applies to every sink: none receives a record or telemetry stamped before it. [Run Length](../time-and-pacing/run-length.md#warm-up) describes it.

### Write the Value Alone

A sink normally writes each record with its envelope: `stream_type`, `sim_ts`, `timestamp`, `key`, `task` and `instance` around the event's `value`. With `envelope: false` on the entry, the sink writes each event's `value` alone and receives no telemetry record and no `queued` or `started` record of a task. A label record of a `when: labels` entry is not changed. The Console `json` format writes the value alone too.

- A Kafka sink sends the value as the message, with the record's key as the message key. A null value is sent as a tombstone, a message with the key and a null value, which a compacted topic reads as a delete of that key. A `topic_router` still receives the whole record.
- A Redis sink sends a value that is not a mapping as JSON, a null included.
- A storage, Iceberg or PostgreSQL sink skips a value that is not a mapping, with one warning.

A task's `key_field` names the payload field whose value becomes the key of its finished event, and so the Kafka message key. The `queued` and `started` records keep the entity's key, and a payload without the field fails the run. [Task Flow](../model-parts/task-flow.md#record-key-and-fields) describes the key.

=== "YAML"

    ```yaml title="docs/snippets/yaml/egress_envelope.yaml:envelope"
    egress:
      # Each event's value alone is written: no telemetry, and no queued or started record
      - {type: Console, envelope: false}
    ```

    A Kafka sink takes the key in the same way. The value alone is the message, and a null value is sent as a tombstone:

    ```yaml title="docs/snippets/yaml_build/connectors.yaml:envelope-false"
    # The value of each event alone is the message: no telemetry, no queued or started
    # record, and a null value is sent as a tombstone
    - type: Kafka
      config:
        bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}
        event_topic: sim-orders
      envelope: false
    ```

=== "Builder"

    `app.add_egress(sink, envelope=False)` does the same.

=== "Low-level"

    `setup_egress` takes `envelopes`, a list with True or False for each sink, by position.

### Batch Size and Flush Interval

A sink receives records in batches. A batch is handed over when it holds `batch_size` records, or when `flush_interval` simulation seconds have passed, whichever comes first. At `factor: 0`, the timer does not start until a `go_live_at` switches the run to real time, because simulation time is not tied to the wall clock before then.

| Setting | Default | Set with |
|---|---|---|
| `batch_size` | 500 | `with_batching`, or per sink on `add_egress` |
| `flush_interval` | 1.0 simulation seconds | `with_batching`, or per sink on `add_egress` |
| `max_queued_batches` | 2000 | `with_batching` |
| `drain_stall_seconds` | 30.0 seconds | `with_batching` |

A size is a whole number of 1 or more and an interval is above 0. The Builder refuses `true`, 0, a negative number and NaN, as YAML does, and YAML refuses `true`. `with_batching` sets the defaults, and each sink can have its own size and interval. A stream sink needs small, frequent batches to keep latency down. A file or lake sink needs large ones, because each batch becomes one file, and for Iceberg one commit.

=== "YAML"

    ```yaml title="docs/snippets/yaml/batching.yaml"
    # The delivery settings of the Egress page, as a blueprint.
    # Run it with: desflow run batching.yaml
    simulation:
      sim_id: Shop
      factor: 0
      # A day of history at factor 0, then 1 simulated second per real second from now;
      # when: history and when: live split the records at this instant
      logical_start_time: -1d
      go_live_at: now

    # Defaults for every egress, the arguments of with_batching
    batching:
      batch_size: 500
      flush_interval: 1.0
      max_queued_batches: 2000
      drain_stall_seconds: 30

    egress:
      # Small and often for this sink; the other uses the defaults
      - {type: Console, batch_size: 50, flush_interval: 0.5}
      # Only the records stamped before go_live_at
      - {type: Console, when: history}

    resources:
      packers: {current_cap: 1, max_cap: 1}

    run: {until: 60}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/batching.py:defaults"
    app.with_batching(
        batch_size=5_000,  # records per batch
        flush_interval=1.0,  # simulation seconds; not used at factor=0
        max_queued_batches=2_000,  # a full queue makes the simulation wait
        drain_stall_seconds=30.0,  # how long teardown waits for a queue that stopped
    )
    ```

    ```python title="docs/snippets/pages/batching.py:per-sink"
    app.with_batching(batch_size=500, flush_interval=1.0)  # the default for every sink
    app.add_egress(stream, batch_size=50, flush_interval=0.5)  # small and often
    app.add_egress(lake, batch_size=200_000)  # large files, the default interval
    ```

=== "Low-level"

    The lists are matched to the sinks by position, and `None` means the default:

    ```python title="docs/snippets/pages/batching.py:low-level"
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.setup_egress(
        [stream, lake],
        batch_size=500,  # defaults for every provider
        flush_interval=1.0,
        predicates=[None, lambda r: r["stream_type"] == "event"],  # by position
        batch_sizes=[50, None],  # None: the default
        flush_intervals=[0.5, None],
        labels=[False, False],  # True makes a provider a labels sink
        warmup=None,  # simulation seconds before which no provider gets a record
    )
    ```

`setup_egress` also takes `lag_monitor_interval`, the simulation seconds between `system.simulation.lag_seconds` records. Its default, `"auto"`, is off at `factor` 0 and 1.0 simulation seconds otherwise. A run that goes live starts it at the go-live instant. `None` or 0 turns it off. The builder and YAML set it with `lag_interval`, as [Clocks and Pacing](../time-and-pacing/clocks-and-pacing.md#lag) describes.

---

## How It Works

A published record goes through the same steps for each sink:

![A record from publish_event, publish_telemetry or publish_label is offered to every egress. Each checks whether it takes it: events and telemetry for the data sinks and labels only for a labels sink, after the warm-up, and its when. A record it takes goes into its buffer until batch_size or flush_interval, then into its queue of at most max_queued_batches, and the egress thread writes it](../../assets/diagrams/egress-path.png)

1. `publish_event`, `publish_telemetry` or `publish_label` builds the record as a plain dictionary.
2. Each sink tests it once. A labels sink takes only labels, and only a labels sink takes a label. No sink takes a record stamped before the [warm-up](../../learn/concepts/output-analysis.md#one-run-starts-empty). Then the sink applies its `when`.
3. The record goes into that sink's buffer. When the buffer reaches `batch_size`, or the sink's timer fires, the buffer is handed to the sink's queue as one batch.
4. The egress thread takes batches from each queue and writes them with the sink's connector.

A connector is a class with one method, `async run(queue)`, so a sink desflow does not have takes one class to write: [Write a Connector](../../how-to/records-and-kafka/write-a-connector.md) shows how.

Each sink has its own buffer and its own queue. All sinks run as tasks on one thread, so a sink that waits with `await` does not hold up the others. A sink that blocks the thread holds up every sink. Memory holds each sink's buffer and the batches waiting in its queue. A record that several sinks take is stored once. A queue holds at most `max_queued_batches` batches. When it is full, the simulation waits for room, so a sink that cannot keep up slows the run instead of building a backlog. When a queue stays full for `drain_stall_seconds`, the sink has stopped consuming, and the run raises `RuntimeError` rather than losing records.

**Teardown** flushes every buffer, then waits for the queues to drain. It keeps waiting while they shrink, and gives up only when they have not shrunk for `drain_stall_seconds`. Records still queued then are lost, and a warning names the sinks that held them.

---

## Stability and Limits

- **A slow sink slows the run.** The run slows on purpose: otherwise a backlog would build that teardown would have to throw away.
- **Size or interval, whichever comes first.** The effective batch is the smaller of `batch_size` and what arrives in one `flush_interval`, so a large size with a short interval never fills. A warning says so once, after one sink has flushed on its timer 20 times in a row with less than a quarter of its `batch_size`.
- **A labels sink receives only labels,** and a data sink never receives one. A label sent to a Kafka sink goes to its `event_topic` with no key.
- **Records matching no sink are not written anywhere,** which is how `when` drops them.
- **At `factor: 0`**, records leave a buffer only when it is full and at teardown.
