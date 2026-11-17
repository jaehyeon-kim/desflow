# Ingress

An ingress connector brings parameter changes into a running simulation from outside: from Kafka, Redis, PostgreSQL or a local schedule. This page shows each connector, the message it reads, how to send one, and when the change lands.

______________________________________________________________________

## What It Is

An ingress connector listens to a source while the run goes on. Every message it reads is one change: a registry `path` and its new `value`, the same two names a scenario step uses. The change reaches the registry, and the simulation follows it, as [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/index.md) describes. Four ingress connectors bring changes in. The seven connectors described on [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/index.md) write records out:

The tutorial page [Live Changes](https://jaehyeon.me/desflow/latest/learn/running-live/live-changes/index.md) teaches these features step by step.

______________________________________________________________________

## How to Use It

Each connector is attached with `add_ingress` in the builder, passed to `env.setup_ingress` in the low-level API, or listed under `ingress` in a blueprint with its `type` and `config`. In a blueprint, `type` is one of the short names below, or a `!python` class from another package. `config` holds the constructor's arguments. A connector's module is imported only when a blueprint names it. A missing package stops the load with its line and the `pip install` command. [YAML Keys](https://jaehyeon.me/desflow/latest/reference/yaml-keys/#ingress) lists the keys of an ingress entry.

| Connector         | `type`     | Source                 | Message                                                           | Extra      |
| ----------------- | ---------- | ---------------------- | ----------------------------------------------------------------- | ---------- |
| `LocalIngress`    | `Local`    | a schedule in the code | `(delay in wall-clock seconds, path, value)`                      | none       |
| `KafkaIngress`    | `Kafka`    | a topic                | a JSON object `{"path": ..., "value": ...}`                       | `kafka`    |
| `RedisIngress`    | `Redis`    | a Pub/Sub channel      | the same JSON object                                              | `redis`    |
| `PostgresIngress` | `Postgres` | a table, polled        | a row with the columns `path` and `value`, the value as JSON text | `postgres` |

### Local Schedule

`LocalIngress` applies changes after set delays in wall-clock seconds from the start of the run. It suits a local run. For a change at an exact simulation time, use a scenario step.

```yaml
# 10 wall-clock seconds after the start, set the packers capacity to 3
- type: Local
  config:
    schedule:
      - [10.0, Shop.resources.packers.current_cap, 3]
```

```python
# 10 wall-clock seconds after the start, set the packers capacity to 3
app.add_ingress(
    LocalIngress(schedule=[(10.0, "Shop.resources.packers.current_cap", 3)])
)
```

The low-level API passes the same `LocalIngress` to `env.setup_ingress`, in its list of connectors, as [Low-level API](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/low-level-api/#attach-the-connectors) shows.

### Kafka

`KafkaIngress` consumes a topic and reads each message as a JSON object with `path` and `value`, or through a deserializer of your own, such as Avro, as [Use Avro and Pydantic](https://jaehyeon.me/desflow/latest/how-to/records-and-kafka/avro-and-pydantic/index.md) shows. [Connect to Secure Kafka](https://jaehyeon.me/desflow/latest/how-to/records-and-kafka/kafka-security/index.md) connects it to a secured cluster.

```yaml
# Reads {"path": ..., "value": ...} messages from the sim-config topic
- type: Kafka
  config:
    topic: sim-config
    bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}
```

```python
# Reads {"path": ..., "value": ...} messages from the sim-config topic
app.add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers="127.0.0.1:9092"))
```

The low-level API passes the same `KafkaIngress` to `env.setup_ingress`, in its list of connectors, as [Low-level API](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/low-level-api/#attach-the-connectors) shows.

`KafkaAdminConnector.send_config(topic, path, value)` sends one such message. The [Kafka example](https://jaehyeon.me/desflow/latest/how-to/examples/kafka/index.md) listens on `sim-config`, and this sets its number of packers:

A blueprint has no sender of its own. Send the message from Python, as the Builder tab shows, or from any Kafka client, as below.

```python
"""Sets the packers capacity of the running Kafka example.

`KafkaAdminConnector.send_config` publishes `{"path": ..., "value": ...}` to the
topic `KafkaIngress` reads, `sim-config` in the example. Call `set_packer_capacity(3)`
while the example runs.
"""

import asyncio

from desflow import KafkaAdminConnector


def set_packer_capacity(value: int) -> None:
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    asyncio.run(
        admin.send_config("sim-config", "Shop.resources.packers.current_cap", value)
    )
```

The sender does not depend on how the model is written: use the function the Builder tab shows.

From any Kafka client, the message is the JSON object itself:

```python
async def send_with_kafka(rate: float) -> None:
    """A JSON object with path and value, to the topic KafkaIngress reads."""
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers="127.0.0.1:9092")
    await producer.start()
    try:
        message = {"path": "Shop.arrival.orders.rate", "value": rate}
        await producer.send_and_wait("sim-config", json.dumps(message).encode())
    finally:
        await producer.stop()
```

### Redis

`RedisIngress` subscribes to a Pub/Sub channel and reads each message as the same JSON object.

```yaml
# Reads {"path": ..., "value": ...} messages from a Pub/Sub channel
- type: Redis
  config:
    url: redis://user:password@127.0.0.1:6379/0
    channel_name: simulation_params
```

```python
# Reads {"path": ..., "value": ...} messages from a Pub/Sub channel
app.add_ingress(
    RedisIngress(
        "redis://user:password@127.0.0.1:6379/0", channel_name="simulation_params"
    )
)
```

The low-level API passes the same `RedisIngress` to `env.setup_ingress`, in its list of connectors, as [Low-level API](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/low-level-api/#attach-the-connectors) shows.

```python
async def send_with_redis(rate: float) -> None:
    """The same JSON object, published to the channel RedisIngress subscribes to."""
    import redis.asyncio as redis

    client = redis.from_url("redis://user:password@127.0.0.1:6379/0")
    message = {"path": "Shop.arrival.orders.rate", "value": rate}
    await client.publish("simulation_params", json.dumps(message))
    await client.aclose()
```

With the [Redis example](https://jaehyeon.me/desflow/latest/how-to/examples/redis/index.md) running, `valkey-cli` sends one from a shell:

```bash
# Change the order arrival rate of the running simulation
docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"path": "Shop.arrival.orders.rate", "value": 10.0}'
```

### PostgreSQL

`PostgresIngress` polls a table, `simulation_params` by default, every `poll_interval` seconds, 2 by default. It creates the table when it is missing, with the columns `id`, `path`, `value`, `is_applied` and `created_at`. It applies the rows in two steps:

1. At the start, it applies the latest applied value of each path, so a restarted run resumes from the last settings.
1. At each poll, it applies every row whose `is_applied` is false and marks it true, so the table keeps the history of every change.

`value` is read as JSON, and as plain text when it is not JSON.

The connection string names `odctl`, the database that the odctl `postgres` profile creates, with the user `user` and the password `password`.

```yaml
# Applies new rows (path, value) of simulation_params, checking every 2 seconds
- type: Postgres
  config:
    connection_dsn: postgresql://user:password@127.0.0.1:5432/odctl
    table_name: simulation_params
    poll_interval: 2.0
```

```python
# Applies new rows (path, value) of simulation_params, checking every 2 seconds
app.add_ingress(
    PostgresIngress(
        "postgresql://user:password@127.0.0.1:5432/odctl",
        table_name="simulation_params",
        poll_interval=2.0,
    )
)
```

The low-level API passes the same `PostgresIngress` to `env.setup_ingress`, in its list of connectors, as [Low-level API](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/low-level-api/#attach-the-connectors) shows.

```python
async def send_with_postgres(rate: float) -> None:
    """A row in the table PostgresIngress polls; value is JSON text."""
    import asyncpg

    conn = await asyncpg.connect("postgresql://user:password@127.0.0.1:5432/odctl")
    try:
        await conn.execute(
            "INSERT INTO simulation_params (path, value) VALUES ($1, $2)",
            "Shop.arrival.orders.rate",
            json.dumps(rate),
        )
    finally:
        await conn.close()
```

With the [PostgreSQL example](https://jaehyeon.me/desflow/latest/how-to/examples/postgres/index.md) running, `psql` adds one from a shell:

```bash
# Change the order arrival rate of the running simulation
docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (path, value) VALUES ('Shop.arrival.orders.rate', '5.0');"
```

______________________________________________________________________

## How It Works

The connectors run on the ingress thread, apart from the simulation. Each reads its source, turns every message into a `(path, value)` pair with one shared check, and puts it on a queue. A SimPy process, the ingress monitor, applies everything queued every 0.1 simulation seconds:

At `factor: 1.0`, 0.1 simulation seconds take 0.1 real seconds, so a change lands within 0.1 seconds of arriving. The simulation time it lands at depends on when it arrives, so two runs with the same seed and the same messages can differ. A change that must land at an exact simulation time needs a scenario step.

A connector is a class with one method, `async run(queue)`, so a source desflow does not have takes one class to write: [Write a Connector](https://jaehyeon.me/desflow/latest/how-to/records-and-kafka/write-a-connector/index.md) shows how.

______________________________________________________________________

## Stability and Limits

- **A bad message is skipped.** A message that cannot be decoded, that is not an object, or that lacks a non-empty `path` or a non-null `value` is logged as one WARNING line and skipped, and the run goes on. A PostgreSQL row like that is marked applied, so it is read once. A path the model does not have is a warning from the registry.

- **Old keys get a hint.** Before 0.21.0, Kafka read `path_id`, and Redis and PostgreSQL read `param_path` and `param_value`. A message with the old keys is skipped with a line that says so, such as this one, which `parse_change` gives for a Kafka message written for 0.20.0:

  ```text
  Skipped a Kafka ingress message: expected a non-empty path and a non-null value, got {'path_id': 'Shop.arrival.orders.rate', 'value': 1.0}; path_id, param_path and param_value were renamed to path and value in desflow 0.21.0
  ```

- **Old PostgreSQL columns.** A table created before 0.21.0 has the columns `param_path` and `param_value`. Reading it fails with an error line that gives the two statements to rename them:

  ```sql
  ALTER TABLE simulation_params RENAME COLUMN param_path TO path;
  ALTER TABLE simulation_params RENAME COLUMN param_value TO value;
  ```

- **`LocalIngress` follows the wall clock,** so at `factor: 0` an unpaced run can end before its first change.

- **Ingress needs an `until`.** The ingress monitor repeats for as long as the run lasts, so a run with ingress needs `until` or Ctrl+C, as [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#run-length_1) explains.
