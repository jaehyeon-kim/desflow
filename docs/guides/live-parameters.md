# Change Parameters While a Simulation Runs

Every parameter of a run has a path in the registry, and a new value sent to that path takes effect without restarting the run. This guide shows how to find the path, and how to send a value from each source. [Live Parameters](../architecture/registry.md) explains what happens to the value once it arrives.

Changing a parameter takes three steps:

![Find the registry path, send the new value from a scenario, LocalIngress, Kafka, Redis or PostgreSQL, and see it in the telemetry](../assets/diagrams/live-change-steps.png)

---

## Find the path

A path is the `sim_id`, the kind of parameter, its name and the field, such as `Line_A.resources.lathe.current_cap` or `Line_A.arrival.standard.rate`. [Registry paths](../architecture/registry.md#registry-paths) lists every form.

A path that does not exist is logged as a warning and ignored, so a misspelt path changes nothing. `compile_parameters()` returns the parameters a `SimulationContext` will register, which shows the names to use:

```python title="docs/snippets/pages/live_parameters.py:compile"
print(app.compile_parameters())
```

A YAML scenario is checked when the file is loaded, so a misspelt path there stops the load with its line.

---

## Send the value

Each source suits a different job.

| Source | Use it for | Lands at |
|---|---|---|
| A YAML `scenario` | An experiment that must repeat exactly | The exact simulation time given |
| `LocalIngress` | A local run with changes after set wall-clock delays | A wall-clock delay from the start |
| `KafkaIngress`, `RedisIngress`, `PostgresIngress` | An operator or another program steering a running twin | When the message arrives |
| A process calling `env.registry.update` | A change that depends on the state of the simulation | When the process makes it |

### Kafka

`KafkaIngress` reads JSON messages with `path_id` and `value` from its topic. The [Kafka example](../examples/kafka.md) listens on `sim-config`. `KafkaAdminConnector.send_config` publishes such a message, and the example's dashboard uses it. This function sets the lathe capacity of the running example:

```python title="docs/snippets/pages/live_parameters_kafka.py"
"""Sets the lathe capacity of the running Kafka example.

`KafkaAdminConnector.send_config` publishes `{"path_id": ..., "value": ...}` to the
topic `KafkaIngress` reads, `sim-config` in the example. Call `set_lathe_capacity(3)`
while the example runs.
"""

import asyncio

from dynamic_des import KafkaAdminConnector


def set_lathe_capacity(value: int) -> None:
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    asyncio.run(
        admin.send_config("sim-config", "Line_A.resources.lathe.current_cap", value)
    )
```

### Redis

`RedisIngress` reads JSON messages with `param_path` and `param_value` from a Pub/Sub channel. With the [Redis example](../examples/redis.md) running:

```bash
docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}'
```

### PostgreSQL

`PostgresIngress` polls a table, `simulation_params` by default, and applies each row not yet applied. With the [Postgres example](../examples/postgres.md) running:

```bash
docker exec -it postgres psql -U user -d odctl -c "INSERT INTO simulation_params (param_path, param_value) VALUES ('Store.arrival.customer_order.rate', '5.0');"
```

The row stays in the table, marked as applied, so the table keeps the history of every change. A restarted run applies the latest value of each path first.

### A scenario

A blueprint lists its changes under `scenario`, each with the simulation time it applies at. [Script an experiment](yaml-blueprints.md#script-an-experiment) shows a complete file. A scenario and an ingress connector can run together, so a scripted baseline can run while an operator steers over Kafka.

---

## Watch it land

A change is easiest to confirm in the telemetry. Publish the parameter you change, such as the lathe's capacity:

```python title="docs/snippets/pages/live_parameters.py:telemetry"
@app.telemetry_loop(interval=2.0)
def lathe_metrics(context):
    context.publish("lathe.capacity", context.get_resource("lathe").capacity)
```

In a blueprint, the same entry is `{interval: 2.0, publish: {lathe.capacity: lathe.capacity}}`. A change to an arrival rate shows as a change in how many events arrive in each interval rather than as a value of its own.
