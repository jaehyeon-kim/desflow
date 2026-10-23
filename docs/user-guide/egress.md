# Egress, Batching and Delivery

An egress connector, or sink, writes the records a run publishes: to the console, Kafka, Redis, PostgreSQL, Parquet or JSON Lines files, or Iceberg tables. This page shows each sink, how to choose which records each one gets, and how records travel from a task to a sink.

---

## What It Is

A run publishes three kinds of record: **events**, such as a task's `queued`, `started` and finished; **telemetry**, single values such as the statistics; and **labels**, the ground-truth records of what a scenario changed. Every sink receives every event and every telemetry record, unless its `when` says otherwise, and labels go only to sinks that ask for them. [Records and Telemetry](../reference/records-and-telemetry.md) shows each record's shape and what each sink writes.

![Parameter changes from outside reach the registry through LocalIngress, KafkaIngress, RedisIngress or PostgresIngress; records from tasks and processes leave through ConsoleEgress, KafkaEgress, ParquetStorageEgress, JsonlStorageEgress, RedisEgress, PostgresEgress or IcebergStorageEgress](../assets/diagrams/connectors-overview.png)

---

## How to Use It

Each sink is attached with `add_egress` in the builder, passed to `env.setup_egress` in the low-level API, or listed under `egress` in a blueprint with its `type` and `config`. In the builder snippets below, `app` is a `SimulationContext`.

### Console

`ConsoleEgress` has two formats. The default, `log`, logs each record at INFO level, one line per record: `[EVT]` for an event, `[TEL]` for telemetry and `[LBL]` for a label. It logs a whole batch with one call, so the logging format's prefix, such as the time and level, starts each batch, and the lines after it start with `[EVT]`, `[TEL]` or `[LBL]`. One call per record cost far more: in the measurements on [Performance Tuning](../guides/performance-tuning.md), a run printing every record took about 50 percent longer.

```python title="docs/snippets/pages/connectors.py:console-egress"
app.add_egress(ConsoleEgress())
```

With `format="json"`, `ConsoleEgress` writes each record to stdout as one JSON line instead, the record as published with its `stream_type`, so another program can read them. It writes at any log level, because the records are output, not a log. Python's logging writes to stderr by default, so the log stays apart from the records, and a pipe such as `| jq .key` reads only the records. In a blueprint it is `type: Console` with `config: {format: json}`, and `desflow run --sample` and `--stdout` print through it in place of the file's sinks, as [YAML Format](../reference/yaml-format.md#running-a-blueprint) describes.

```python title="docs/snippets/pages/connectors.py:console-json"
# One JSON line per record on stdout, stream_type included
app.add_egress(ConsoleEgress(format="json"))
```

### Kafka

`KafkaEgress` sends events to `event_topic` and telemetry to `telemetry_topic`, keyed by the record's `key` or `path_id`, so all events of one entity land on one partition, in order. `KafkaEgress` creates both topics when the run starts, if they do not exist; with a `topic_router` it creates none. [Complex Routing](../guides/complex-routing.md) sends records to other topics.

```python title="docs/snippets/pages/connectors.py:kafka-egress"
app.add_egress(
    KafkaEgress(
        bootstrap_servers="localhost:9092",
        event_topic="sim-events",
        telemetry_topic="sim-telemetry",
    )
)
```

### Parquet and JSON Lines

`ParquetStorageEgress` and `JsonlStorageEgress` write each batch as a file of its own, on a local disk or S3-compatible storage, through PyArrow. Without a `path_router`, each event is one flat row in `default_path`, with its `value` mapping unpacked into columns, and telemetry is left out. A Parquet file has a column for every key of any row in its batch. A key that first appears in a later batch adds a column to the later files, and a reader that unifies the files' schemas reads it as null in the earlier rows. An integer column that a later batch gives a float becomes a double in the later files. The keys inside a mapping value, a struct column, are fixed by the first batch that has it: a nested key that first appears later is dropped. A batch that cannot be written is logged at ERROR, and the sink goes on with the next one.

**File names.** `default_path`, and any path a `path_router` returns, is a file name prefix, not a folder. Each batch is written as a file of its own, `<stem>_<8 hex digits><extension>`, in the path's folder, so `output/events.parquet` gives files such as `output/events_1a2b3c4d.parquet`. Give the extension in the path: `output/events` gives files with none. The folder is created on the first write.

**Columns of a finished event.** A task's `queued` and `started` events carry `path_id` and `status`, but its finished event's value is the task's payload only, so in a finished event's row `path_id` and `status` are null unless the payload has them. The [Parquet example](../examples/parquet.md) adds them to its payload for that reason.

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

Read the files back as one table with `pyarrow.dataset`, which reads every file in the folder:

```python title="docs/snippets/pages/file_sink.py:read"
def read_events(folder="output"):
    """Every file the sink wrote under `folder`, as one table."""
    import pyarrow.dataset as ds

    # format="json" reads the files of a JsonlStorageEgress the same way
    return ds.dataset(folder, format="parquet").to_table()
```

A second run adds its files beside the first run's, so reading the folder then counts both runs. Give each run a folder of its own, or clear the folder before a run.

### Redis

`RedisEgress` appends each record to a Redis Stream with `XADD`, as one `payload` field holding the record as JSON. A `__stream__` key in the record's value picks the stream, and is removed before writing.

```python title="docs/snippets/pages/connectors.py:redis-egress"
# XADD to the events stream, unless a record's value names another in __stream__
app.add_egress(
    RedisEgress("redis://user:password@localhost:6379/0", stream_name="events")
)
```

### PostgreSQL

`PostgresEgress` inserts records into a table in batches. Only records whose value is a mapping are written, and the labels on a labels sink. A label is written as `sim_ts`, `timestamp`, `path`, `old`, `new` and `label`. The record's `sim_ts`, `timestamp`, `key` (as `event_id`) and `path_id` are added to the value and replace fields of the same name. Its `task` and `instance` are added too, but a value field of the same name keeps its own value. Keys with no matching column are dropped. A `__table__` key in the value sends the record only to the sink whose `table_name` matches.

```python title="docs/snippets/pages/connectors.py:postgres-egress"
dsn = "postgresql://user:password@localhost:5432/odctl"
app.add_egress(PostgresEgress(dsn, table_name="orders"))
```

By default a record whose key already exists is skipped. `upsert_keys` updates that row instead, for a model that changes rows it wrote earlier, such as an order moving from packed to shipped. The key columns need a unique index; an update sets only the columns the record carries, and within one flush the last record for a key wins.

```python title="docs/snippets/pages/connectors.py:postgres-upsert"
app.add_egress(PostgresEgress(dsn, table_name="orders", upsert_keys=["order_id"]))
```

### Iceberg

`IcebergStorageEgress` appends records to an Apache Iceberg table through a REST catalog. One flush is one commit, so this sink wants a large `batch_size` of its own: every commit writes new metadata, and many small commits slow down queries. A table the sink creates has a column for every key of its first batch that has a value in some row; a key null in every row, such as `task` in a low-level event published without one, waits for a batch that gives it a value, as Iceberg has no null type. A key that first appears in a later batch is added to the table as an optional column, unless the table is named in `schemas`, which keeps the columns it gives. A batch the table refuses is logged at ERROR.

```python title="docs/snippets/pages/connectors.py:iceberg-egress"
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
```

`upsert_keys` upserts on key columns instead of appending, so rerunning a seeded model over the same window does not duplicate rows. An upsert reads the matching rows first, replaces whole rows, never deletes, and needs pyiceberg 0.9.0 or later.

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

### Connectors in a Blueprint

In a blueprint, `config` holds the constructor's arguments, and settings that Python takes as objects are plain values: a Parquet or JSON Lines `filesystem` is a mapping with `type: local` or `type: s3`, an Iceberg `catalog` is a mapping of PyIceberg properties, and a Postgres `tables` mapping creates its tables when the run starts. [YAML Format](../reference/yaml-format.md#ingress-and-egress) lists them all.

```yaml title="docs/snippets/yaml_build/connectors.yaml:egress"
egress:
  - type: Kafka
    config:
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
      event_topic: sim-events
      telemetry_topic: sim-telemetry
    batch_size: 50
    flush_interval: 0.5
  - type: Parquet
    config:
      default_path: warehouse/events.parquet
      # A mapping becomes PyArrow's S3FileSystem; type: local is the local disk
      filesystem:
        type: s3
        endpoint_override: 127.0.0.1:8333
        scheme: http
        access_key: user
        secret_key: password
    batch_size: 200000
  - type: Jsonl
    config:
      path_router: !python routing_logic.daily_path
  - type: Postgres
    config:
      connection_dsn: postgresql://user:password@localhost:5432/odctl
      table_name: orders
      upsert_keys: [order_id]
  - type: Redis
    config:
      url: redis://user:password@localhost:6379/0
      stream_name: events
```

### Choose What Each Sink Gets

Every sink receives every event and telemetry record. `when` narrows that down:

- **A function** taking one record and returning True to send it to this sink, in the builder or as `!python` in a blueprint.
- **`history` and `live`**, in a blueprint: the records stamped before `simulation.go_live_at`, and the rest, as [Clocks and Pacing](time-and-pacing/clocks-and-pacing.md#go-live) describes.
- **`labels`**: the ground-truth labels, and nothing else. No other sink receives a label, so data and labels always reach different sinks; [Live Parameters](live-parameters.md#record-what-a-change-was-for) describes them.

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

```python title="docs/snippets/pages/batching.py:labels"
app.add_egress(stream)  # events and telemetry, never a label
app.add_egress(audit, when="labels")  # ground-truth labels, and nothing else
```

```yaml title="docs/snippets/yaml/ground_truth.yaml:sinks"
egress:
  # The data under test: events and telemetry, and no label
  - type: Console
  # The labels only, for a scorer that checks what a detector found
  - type: Jsonl
    config: {default_path: labels/labels.jsonl}
    when: labels
```

A file sink such as this one writes one file per batch under `labels/`, such as `labels/labels_1a2b3c4d.jsonl`. A run's `warmup` applies to every sink: none receives a record or telemetry stamped before it. [Run Length](time-and-pacing/run-length.md#warm-up) describes it.

### Batch Size and Flush Interval

A sink receives records in batches. A batch is handed over when it holds `batch_size` records, or when `flush_interval` simulation seconds have passed, whichever comes first. At `factor: 0` the timer is not started, because simulation time is not tied to the wall clock, until a `go_live_at` switches the run to real time.

| Setting | Default | Set with |
|---|---|---|
| `batch_size` | 500 | `with_batching`, or per sink on `add_egress` |
| `flush_interval` | 1.0 simulation seconds | `with_batching`, or per sink on `add_egress` |
| `max_queued_batches` | 2000 | `with_batching` |
| `drain_stall_seconds` | 30.0 seconds | `with_batching` |

`with_batching` sets the defaults, and each sink can have its own size and interval: a stream sink wants small, frequent batches to keep latency down, and a file or lake sink wants large ones, because the batch is the file, and for Iceberg the commit.

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

The same settings in a blueprint, and in the low-level API, where the lists are matched to the sinks by position and `None` means the default:

```yaml title="docs/snippets/yaml/batching.yaml"
# The delivery settings of the Egress page, as a blueprint.
# Run it with: desflow run batching.yaml
simulation:
  sim_id: Shop
  factor: 0
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
  - type: Console
    batch_size: 50
    flush_interval: 0.5
  # Only the records stamped before go_live_at
  - type: Console
    when: history

resources:
  packers: {current_cap: 1, max_cap: 1}

run:
  until: 60
```

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

`setup_egress` also takes `lag_monitor_interval`, the simulation seconds between `system.simulation.lag_seconds` records. Its default, `"auto"`, is off at `factor` 0 and 1.0 otherwise, and a run that goes live starts it at the go-live instant; `None` or 0 turns it off. The builder and YAML set it with `lag_interval`, as [Clocks and Pacing](time-and-pacing/clocks-and-pacing.md#lag) describes.

---

## How It Works

A published record goes through the same steps for each sink:

![A record from publish_event, publish_telemetry or publish_label is offered to every egress. Each checks whether it takes it: events and telemetry for the data sinks and labels only for a labels sink, after the warm-up, and its when. A record it takes goes into its buffer until batch_size or flush_interval, then into its queue of at most max_queued_batches, and the egress thread writes it](../assets/diagrams/egress-path.png)

1. `publish_event`, `publish_telemetry` or `publish_label` builds the record as a plain dictionary.
2. Each sink tests it once: a label only for a labels sink, and nothing else for one; nothing stamped before the warm-up; then its `when`.
3. The record goes into that sink's buffer. When the buffer reaches `batch_size`, or the sink's timer fires, the buffer is handed to the sink's queue as one batch.
4. The egress thread takes batches from each queue and writes them with the sink's connector.

Each sink has its own buffer and its own queue, so a slow sink does not hold up a fast one, and memory is the sum of the buffers. A queue holds at most `max_queued_batches` batches. When it is full, the simulation waits for room, so a sink that cannot keep up slows the run instead of building a backlog. When a queue stays full for `drain_stall_seconds`, the sink has stopped consuming, and the run raises `RuntimeError` rather than losing records.

**Teardown** flushes every buffer, then waits for the queues to drain. It keeps waiting while they shrink, and gives up only when they have not shrunk for `drain_stall_seconds`; records still queued then are lost, and a warning names the sinks that held them.

---

## Stability and Limits

- **A slow sink slows the run.** That is on purpose: the alternative is a backlog that teardown would have to throw away.
- **Size or interval, whichever comes first.** The effective batch is the smaller of `batch_size` and what arrives in one `flush_interval`, so a large size with a short interval never fills; a warning says so once, after one sink has flushed on its timer 20 times in a row with less than a quarter of its `batch_size`.
- **A labels sink receives only labels,** and a data sink never receives one. A label sent to a Kafka sink goes to its `event_topic` with no key.
- **Records matching no sink are not written anywhere,** which is how `when` drops them.
- **At `factor: 0`**, records leave a buffer only when it is full and at teardown.
