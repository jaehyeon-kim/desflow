# Records and Telemetry

This contract is frozen. Every 1.x release keeps each field below, with its name, type and position. A release may add a field after the last one of its kind, and the Changelog names it. A release may also add a record kind that reaches only the sinks that ask for it, as `trace` does.

Everything a simulation publishes is a record of one of three kinds: a **telemetry** record for a single value, an **event** record for something that happened to an entity, and a **label** record for the ground truth of a scenario step. The three kinds are frozen. A fourth kind, **trace**, is frozen the same way. It is one record for each model event the environment is about to process while `stats.trace` is on, and only a sink with `when: trace` receives it, as only a `when: labels` sink receives labels. The records have the shape of the `TelemetryPayload`, `EventPayload` and `LabelPayload` models, and the trace record has no model. They are the same whichever way the simulation was written.

All three share `stream_type`, `sim_ts` and `timestamp`. Telemetry names its value in `path_id`. An event names its entity in `key`, and its task in `task` and `instance`. A label carries what a step changed:

______________________________________________________________________

## Record Shape

Every sink, router and `when` predicate receives records as plain dictionaries, with these keys in this order. The type is the Python type a sink receives, and the JSON type a JSON sink writes:

| Field         | Type                   | Telemetry             | Event                                | Label                                         |
| ------------- | ---------------------- | --------------------- | ------------------------------------ | --------------------------------------------- |
| `stream_type` | string                 | `"telemetry"`         | `"event"`                            | `"label"`                                     |
| `sim_ts`      | float                  | simulation seconds    | simulation seconds                   | simulation seconds                            |
| `timestamp`   | string                 | logical time          | logical time                         | logical time                                  |
| `path_id`     | string                 | the name of the value |                                      |                                               |
| `key`         | string                 |                       | the entity's key, such as `orders-1` |                                               |
| `task`        | string or null         |                       | the task's name, or `None`           |                                               |
| `instance`    | any JSON value or null |                       | the task's instance id, or `None`    |                                               |
| `path`        | string or null         |                       |                                      | the registry path the step changed, or `None` |
| `old`         | any JSON value or null |                       |                                      | the value before the step, or `None`          |
| `new`         | any JSON value or null |                       |                                      | the value after the step, or `None`           |
| `label`       | object                 |                       |                                      | the step's `record` mapping                   |
| `value`       | any JSON value         | a single value        | the event's value                    |                                               |

This is how `ConsoleEgress` prints a statistic and the three events of one task:

```text
[TEL] {'sim_ts': 10.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.in_use', 'value': 0}
[EVT] {'sim_ts': 3.672097157355851, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'sim_id': 'Shop', 'service': 'packing', 'status': 'queued'}}
[EVT] {'sim_ts': 3.672097157355851, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'sim_id': 'Shop', 'service': 'packing', 'status': 'started'}}
[EVT] {'sim_ts': 5.369082605964606, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'event_type': 'order_packed'}}
```

This is how a JSON Lines sink writes two labels:

```text
{"stream_type":"label","sim_ts":30.0,"timestamp":"...","path":"Shop.arrival.orders.rate","old":0.5,"new":1.5,"label":{"fault":"order_surge","cause":"flash sale"}}
{"stream_type":"label","sim_ts":45.0,"timestamp":"...","path":null,"old":null,"new":null,"label":{"fault":"label_printer_offline"}}
```

`sim_ts` is simulation seconds. `timestamp` is `logical_start_time` plus the simulation time. It is an ISO 8601 string with milliseconds and the offset of the run's time zone, such as `2026-10-03T12:00:00.000+10:00`, or `+00:00` in UTC, the default. [Clocks and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#time-zone) describes the zone. `sim_ts` is not rounded, so it can differ from `timestamp` by less than a millisecond, because `timestamp` is cut to the millisecond.

______________________________________________________________________

## Field Values

- `sim_ts` is a float and is not rounded to three decimal places. A time written `5` reads `5.0`, and simulation time 0 is `0.0`. The `now` and `peek` of a `run.steps` or `run.segments` record, `env.now` and the capacity fields stay integers when they are whole.
- `key` and `path_id` are strings. `bytes` and `bytearray` are decoded to strings. Any other type, such as an `int`, raises a Pydantic `ValidationError` at the call.
- `task` is the name of a builder or YAML task. `instance` is its instance id: an integer from 0 for a builder or YAML task, or what a low-level process passes. `publish_event` and `publish_label` do not check the type of `task`, `instance` or `path`, so a low-level process that passes another type writes that type. The types in the table are what the builder and YAML write.
- A capacity with no limit reads as `null` in a record, because a JSON record cannot hold infinity. A value that reads the capacity of an unbounded container or store, such as `registry['T.containers.c.current_cap']` in an event's payload, is `null` in the record that publishes it.
- `value`, `old`, `new` and `label` are converted to plain JSON values before any sink sees them. A Pydantic V2 model becomes a dictionary, a datetime becomes an ISO string, and a NaN or an infinity becomes `None`. This holds at any depth, in a model as in a plain dictionary.
- A time in an error message reads `5` for a whole number, and a number in `{now:g}` form has six significant digits, so 1000000 reads `1e+06`.

```python
class Reading(BaseModel):
    sensor: str
    celsius: float
    taken_at: datetime


def readings(context: SimulationContext):
    while True:
        yield context.env.timeout(30.0)
        reading = Reading(
            sensor="spindle", celsius=float("nan"), taken_at=datetime.now()
        )
        # The model becomes a dict, taken_at an ISO string, and NaN becomes None
        context.env.publish_event("sensor-spindle", reading)
```

A trace record is published only while `stats.trace` is on, to a sink with `when: trace`:

```json
{"stream_type": "trace", "sim_ts": 0.0, "timestamp": "...", "path_id": "trace", "value": {"priority": 0, "event_id": 2, "event": "Initialize"}}
```

This is the first trace record of the run in [Built-in Statistics](https://jaehyeon.me/desflow/latest/user-guide/results/statistics/#trace-the-events). `priority` is the event's priority, `event_id` is SimPy's sequence number of the event, which desflow's own events also advance, and `event` is its type. [Built-in Statistics](https://jaehyeon.me/desflow/latest/user-guide/results/statistics/#trace-the-events) switches it on.

______________________________________________________________________

## Task Events

`@app.task` and a YAML task publish three events for each run that is served, all with the entity's key, the task's name and its instance id:

| Event    | `value`                                                                                                        |
| -------- | -------------------------------------------------------------------------------------------------------------- |
| queued   | `{"path_id": "<sim_id>.service.<service>", "sim_id": "<sim_id>", "service": "<service>", "status": "queued"}`  |
| started  | `{"path_id": "<sim_id>.service.<service>", "sim_id": "<sim_id>", "service": "<service>", "status": "started"}` |
| finished | exactly what the task returns, or the YAML `payload`                                                           |

A run that is not served, or fails, publishes one other event in place of its finished event:

| Event   | When                                                                                                                                                                                                                                                                                                                                                                | `value`                                                                                                                  |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| balked  | its `balk` condition held, so it never joined the queue and publishes no `queued`. Or a resource it asked for already held `queue_limit` requests; then it publishes no `queued` either, when that resource is its first and nothing is waited for before it. A task that was preempted and asks again when the queue is full balks after `started` and `preempted` | `{"path_id": ..., "sim_id": ..., "service": ..., "status": "balked"}`, with `"resource": ...` when a resource refused it |
| reneged | its `patience` ran out before it was served; it left the queue at that moment                                                                                                                                                                                                                                                                                       | `{"path_id": ..., "sim_id": ..., "service": ..., "status": "reneged"}`                                                   |
| failed  | its `fail` condition held, its wait failed, or its function raised                                                                                                                                                                                                                                                                                                  | `{"path_id": ..., "sim_id": ..., "service": ..., "status": "failed", "reason": ...}`                                     |

An interrupted run publishes these as well, and its finished event when it ends:

| Event       | When                                                                                                                    | `value`                                                                                                                            |
| ----------- | ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| interrupted | an `interrupt` reached it; its waits were given up, and a unit it held stays with it unless `on_interrupt` is `abandon` | `{"path_id": ..., "sim_id": ..., "service": ..., "status": "interrupted", "cause": ...}`                                           |
| preempted   | a request of a lower priority value took its unit                                                                       | `{"path_id": ..., "sim_id": ..., "service": ..., "status": "preempted", "resource": ..., "preempted_by": ..., "usage_since": ...}` |
| resumed     | after an interrupt or a preemption, it holds its units again, also when it never gave them back, and is served          | `{"path_id": ..., "sim_id": ..., "service": ..., "status": "resumed"}`                                                             |

The fields of these values are:

- `status`, `path_id`, `sim_id`, `service` and `reason`: strings. `sim_id` and `service` are the two names `path_id` joins with `.service.`. A reader takes them from these fields and does not split `path_id`, because a name may hold `.service.` itself.
- `cause`: any JSON value the interrupt was given, or `None`. When a `subscribe` interrupts the instance, the `cause` is `{"event": name, "value": value}`. `value` is what the event carried. `name` is the subscribed task's name for a task, the process's name for a process, and `None` for any other event.
- `resource`: the name of the resource that was taken or that refused the task.
- `preempted_by`, in a `preempted` record: the key of the entity whose request took the unit, or `None` when a process that is not a task took it.
- `usage_since`: the simulation time at which the preempted task was granted the unit.

With statistics at `basic` or above, entities that enter and leave are counted as `<sim_id>.stats.flows.<arrival>.started`, `.finished` and `.in_system`. From `minimal`, each task's `<sim_id>.stats.tasks.<task>.in_progress` is published. Each of the outcomes above is counted as `<sim_id>.stats.tasks.<task>.balked`, `.reneged`, `.failed`, `.interrupted`, `.preempted` and `.resumed`, published once the first one happens. A flow that ends in a failure is counted as `<sim_id>.stats.flows.<arrival>.failed`. [Patience](https://jaehyeon.me/desflow/latest/learn/shop-logic/patience/index.md) shows balking and reneging, [Failures](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/failures/#failures) shows failures, and [Interruptions](https://jaehyeon.me/desflow/latest/learn/shop-logic/interruptions/index.md) shows interrupts and preemption. A failed task publishes `started` before its failure when it got that far, and a task with a `wait` publishes `queued` when the wait starts.

A resource with a `breakdown` publishes an event at each failure and each repair. Its key is the resource's name, and it has no task or instance:

| Event    | When                                                    | `value`                                       |
| -------- | ------------------------------------------------------- | --------------------------------------------- |
| down     | the resource failed; its capacity is 0 until the repair | `{"status": "down", "reason": "failure"}`     |
| repaired | the repair ended; its capacity is `current_cap` again   | `{"status": "repaired", "reason": "failure"}` |

With statistics at `minimal` or above, such a resource publishes `<sim_id>.stats.resources.<name>.down`, which is 1 while it is down. From `basic`, it also publishes `.downtime`, the seconds it has been down, and `.failures`. [Model Breakdowns](https://jaehyeon.me/desflow/latest/how-to/modelling/breakdowns/index.md) checks them against the events.

A task with no service has `{"status": ...}` without `path_id`, `sim_id` or `service`. A timed step, with a service and no resource, publishes no `queued`. A task with no service, resource or `get` publishes only the finished event, as soon as it starts. The key is the arrival's name and a counter from 1, such as `orders-1`, on every event of every task the entity passes through. [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/#record-key-and-fields) describes it. A low-level process publishes whatever it passes to `env.publish_event(key, value, task=..., instance=...)`.

______________________________________________________________________

## Telemetry Names

| Published by                                                                                                                | `path_id`                                                                                                                                                                                                                                            |
| --------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| the built-in statistics                                                                                                     | `<sim_id>.stats.<kind>.<name>.<statistic>`, such as `Shop.stats.resources.packers.in_use`; [Reference, Statistics](https://jaehyeon.me/desflow/latest/reference/statistics/index.md) lists every name                                                |
| [replications](https://jaehyeon.me/desflow/latest/user-guide/results/replications/#what-the-sinks-receive), once at the end | `<sim_id>.stats.<measure>.replication.<i>` for each replication from 1, then `<sim_id>.stats.<measure>.mean`, `.half_width` and `.n`. The measure is a statistic's name without the sim id, such as `tasks.pack.wait_mean`                           |
| `context.publish(name, value)` and a YAML `telemetry` function                                                              | `<sim_id>.<name>`                                                                                                                                                                                                                                    |
| `env.publish_telemetry(path_id, value)`                                                                                     | `path_id`, as given                                                                                                                                                                                                                                  |
| the lag monitor                                                                                                             | `system.simulation.lag_seconds`, every `lag_interval`: by default every simulation second at a `factor` above 0, and none at `factor: 0` ([Clocks and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#lag)) |

```python
@app.telemetry_loop(interval=10.0)
def report(context: SimulationContext):
    # path_id is Shop.packers_free: the sim_id is added in front
    packers = context.get_resource("packers")
    context.publish("packers_free", packers.capacity - packers.in_use)
```

```python
env = DynamicRealtimeEnvironment(factor=0.0)
env.setup_egress([ConsoleEgress()])
env.publish_telemetry("plant.power_kw", 412.5)  # path_id exactly as given
# key, task, instance and value as given; task and instance are optional
env.publish_event("orders-17", {"status": "shipped"}, task="ship", instance=16)
# A ground-truth label, which only a when: labels sink receives
env.publish_label({"fault": "slow_packing"}, path="Shop.service.packing.scale")
```

______________________________________________________________________

## Reserved Payload Keys

Some keys of a record's `value` belong to desflow, not to the data. They are the keys that start and end with two underscores around a name of at least one character, such as `__table__` or `__stream__`, in an event's value or in a telemetry value that is a mapping. A sink handles them in one of two ways:

- A sink that routes by such a key uses it and removes it:

| Key          | Sink that routes by it                       | Where the record goes                                                                                                                                                                                                                                                                             |
| ------------ | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `__topic__`  | `KafkaEgress`                                | the topic the key names; a `topic_router` wins over the key, and telemetry always goes to `telemetry_topic`                                                                                                                                                                                       |
| `__table__`  | `IcebergStorageEgress`                       | the `namespace.table` the key names; a `table_router` wins over the key, and a record with no key goes to `default_table`                                                                                                                                                                         |
| `__table__`  | `PostgresEgress`                             | the table the key names, in the sink whose `table_name` matches                                                                                                                                                                                                                                   |
| `__path__`   | `ParquetStorageEgress`, `JsonlStorageEgress` | the base path of the chunk files the key names; a `path_router` wins over the key, and a record with no key goes to `default_path`. The path is relative to the working directory or the sink's `filesystem`, not under the folder of `default_path`, and each flush writes `<stem>_<id><suffix>` |
| `__stream__` | `RedisEgress`                                | the stream the key names                                                                                                                                                                                                                                                                          |

- Every other key of that kind is dropped, with one warning per key the first time a sink meets it, by `KafkaEgress`, the Parquet, JSON Lines and Iceberg sinks, and `ConsoleEgress`. Each of them keeps the key it routes by. `PostgresEgress` and `RedisEgress` remove only their own routing key, so any other such key reaches their rows as data.

The warning names the sink and the key:

```text
KafkaEgress drops the payload key '__table__': keys that start and end with two underscores are reserved for desflow, and only a sink that routes by one, such as PostgreSQL's __table__ or Redis's __stream__, uses it. Rename the key to keep it as data.
```

The record itself is not changed, so a PostgreSQL sink on the same run still finds its `__table__`. A sink drops the keys it does not route by before its router sees the record, so a router cannot route by a key its sink does not use. Rename a key such as `__source__` to `source` to keep it as data.

A `__table__` key that PostgreSQL and Iceberg share on one event needs the name to hold a namespace for Iceberg: a name with none is refused with "names no namespace", and Iceberg logs the error and loses that batch, where it used to drop the key and write to `default_table`. Give Iceberg a `table_router`, or write `namespace.table` in the key.

## What Each Sink Writes

Every sink receives the events and telemetry above. A `when: labels` sink receives the labels instead, and a `when: trace` sink receives the trace records instead. What each writes differs:

| Sink                                                                 | Writes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ConsoleEgress`                                                      | one line per record: `[TEL]`, `[EVT]` or `[LBL]`, followed by the record without `stream_type`. A trace record prints as `[EVT]`. Each batch is one INFO log call, so only a batch's first line carries the logging format's prefix                                                                                                                                                                                                                                                                             |
| `KafkaEgress`                                                        | the record as JSON, without `stream_type` unless `include_stream_type=True`; the message key is `path_id` for telemetry and `key` for an event. A label goes to `event_topic` with no key                                                                                                                                                                                                                                                                                                                       |
| `RedisEgress`                                                        | the record as JSON in one `payload` field, with any `__stream__` key removed                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `ParquetStorageEgress`, `JsonlStorageEgress`, `IcebergStorageEgress` | without a router, one flat row per event or label, with an event's `value` mapping unpacked into columns, and no telemetry; with a router, each record as the router leaves it                                                                                                                                                                                                                                                                                                                                  |
| `PostgresEgress`                                                     | an event or telemetry record whose value is a mapping, written to the table its `__table__` key names; the key is removed. The sink adds `sim_ts` and `timestamp` to the row, and for an event `key` as `event_id`, `task` and `instance`, or for telemetry `path_id`, as [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#postgresql) describes. A label is written as the columns `sim_ts`, `timestamp`, `path`, `old`, `new` and `label`, the last three as JSON text |

With `envelope: false` on the entry, a sink writes each event's `value` alone: no `stream_type`, `sim_ts`, `timestamp`, `key`, `task` or `instance`, and no telemetry and no `queued` or `started` record of a task. A label record of a `when: labels` entry is left whole. A Kafka sink sends a null value as a tombstone, a message with the record's key and a null value, and a value that is not a mapping as JSON. A Redis sink sends a value that is not a mapping as JSON. A file, Iceberg or PostgreSQL sink skips such a value with one warning. [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#write-the-value-alone) shows the key. 1.x keeps this shape.

In a flat row, a finished event's columns are its task's payload only. The `queued` and `started` rows of a task carry `path_id`, `sim_id`, `service` and `status`. Its finished row has them null unless the payload holds them. [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#parquet-and-json-lines) describes the file sinks.
