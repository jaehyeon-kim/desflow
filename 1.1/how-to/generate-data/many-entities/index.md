# Many Entities

Start many entities from one arrival, each with its own key and its own values: ten tills that report their takings every minute, and three warehouses named in a list.

______________________________________________________________________

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

  ```bash
  git clone https://github.com/jaehyeon-kim/desflow.git
  cd desflow
  ```

- [First Simulation](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md) explains arrivals, tasks and the records a run writes.

- [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/index.md) lists the keys of an arrival, and [`entity`](https://jaehyeon.me/desflow/latest/generators/model-state/entity/index.md) lists the generator that reads an entity's key.

______________________________________________________________________

## Steps

### Start Entities with a Count

`count: 10` starts ten entities from the one arrival, and each reports every 60 seconds. `key` is a generator read once for each entity, and `sequence` with `format: "till-{}"` names them `till-1` to `till-10`.

```yaml
arrivals:
  tills:
    dist: constant
    value: 60
    spawn: report
    count: 10
    key: {gen: sequence, start: 1, format: "till-{}"}
```

```python
# A blueprint's `count: 10` is ten arrivals here, each with the key of its till
for number in range(1, 11):
    app.add_arrival(
        f"tills{number}",
        "constant",
        value=60,
        spawn="report",
        key=lambda n, number=number: f"till-{number}",
    )
```

A Builder arrival has no `count`, so the file adds one arrival for each till, each with its own key.

```python
sampler = Sampler(seed=42)
takings = sampler.generator("tasks.report.payload.takings")
tills_gap = DistributionConfig(dist="constant", value=60)
# The random sequence the blueprint's draw uses
tills_gap.stream = "arrival.tills"
sent = {"report": 0}
```

A Low-level script starts one process for each till.

### Read the Entity's Key

[`entity`](https://jaehyeon.me/desflow/latest/generators/model-state/entity/index.md) writes the entity's key into the payload, next to a takings figure that each till draws for itself. The key is also the record key, so a Kafka topic keeps all the records of one till in one partition, in order.

```yaml
tasks:
  report:
    payload:
      till: {gen: entity}
      takings: {gen: normal, mean: 400, std: 60, decimals: 2}
```

```python
report_payload = Template(
    {
        "till": {"gen": "entity"},
        "takings": {"gen": "normal", "mean": 400, "std": 60, "decimals": 2},
    },
    name="tasks.report",
)

@app.task(name="report")
def report(task_id):
    """Gives one report record."""
    return report_payload
```

```python
def till(key):
    while True:
        yield env.timeout(sampler.sample(tills_gap))
        payload = {
            "till": key,
            "takings": round(float(takings.normal(400, 60)), 2),
        }
        env.publish_event(key, payload, task="report", instance=sent["report"])
        sent["report"] += 1

for number in range(1, 11):
    env.process(till(f"till-{number}"))
```

### Start Entities from a List of Keys

`keys` is a list of names instead of a count, and it starts one entity for each name. Three warehouses report the orders waiting every 2 seconds under their own names.

```yaml
arrivals:
  warehouses: {dist: constant, value: 2, spawn: report, keys: [north, south, east]}
```

```yaml
tasks:
  report:
    payload:
      warehouse: {gen: entity}
      orders_waiting: {gen: integer, bounds: [0, 40]}
```

```python
# A blueprint's `keys` is one arrival for each name here, each with its own key
for name in ["north", "south", "east"]:
    app.add_arrival(
        f"warehouses-{name}",
        "constant",
        value=2,
        spawn="report",
        key=lambda n, name=name: name,
    )
```

As with `count`, a Builder arrival has no `keys`, so the file adds one arrival for each name.

```python
report_payload = Template(
    {
        "warehouse": {"gen": "entity"},
        "orders_waiting": {"gen": "integer", "bounds": [0, 40]},
    },
    name="tasks.report",
)

@app.task(name="report")
def report(task_id):
    """Gives one report record."""
    return report_payload
```

```python
sampler = Sampler(seed=42)
waiting = sampler.generator("tasks.report.payload.orders_waiting")
warehouses_gap = DistributionConfig(dist="constant", value=2)
# The random sequence the blueprint's draw uses
warehouses_gap.stream = "arrival.warehouses"
sent = {"report": 0}
```

```python
def warehouse(key):
    while True:
        yield env.timeout(sampler.sample(warehouses_gap))
        payload = {
            "warehouse": key,
            "orders_waiting": int(waiting.integers(0, 40 + 1)),
        }
        env.publish_event(key, payload, task="report", instance=sent["report"])
        sent["report"] += 1

for key in ["north", "south", "east"]:
    env.process(warehouse(key))
```

______________________________________________________________________

## Check the Result

Run the first model for 300 simulated seconds and print its first 3 records. All ten tills report at 60 seconds, each under its own key, and again at 120, 180 and 240 seconds, which makes 40 records. The run ends at 300 seconds before the reports due at that time.

```bash
desflow run docs/snippets/generate-data/many_entities.yaml --sample 3
```

```bash
python docs/snippets/generate-data/many_entities_builder.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```bash
python docs/snippets/generate-data/many_entities_low_level.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```text
{"stream_type":"event","sim_ts":60.0,"timestamp":"...","key":"till-1","task":"report","instance":0,"value":{"till":"till-1","takings":446.41}}
{"stream_type":"event","sim_ts":60.0,"timestamp":"...","key":"till-2","task":"report","instance":1,"value":{"till":"till-2","takings":297.08}}
{"stream_type":"event","sim_ts":60.0,"timestamp":"...","key":"till-3","task":"report","instance":2,"value":{"till":"till-3","takings":434.26}}
```

The model with `keys` reports in the order of the list, and the first 4 records are the three warehouses at 2 seconds and the first again at 4 seconds.

```bash
desflow run docs/snippets/generate-data/many_entities_named.yaml --sample 4
```

```bash
python docs/snippets/generate-data/many_entities_named_builder.py
```

```bash
python docs/snippets/generate-data/many_entities_named_low_level.py
```

```text
{"stream_type":"event","sim_ts":2.0,"timestamp":"...","key":"north","task":"report","instance":0,"value":{"warehouse":"north","orders_waiting":22}}
{"stream_type":"event","sim_ts":2.0,"timestamp":"...","key":"south","task":"report","instance":1,"value":{"warehouse":"south","orders_waiting":20}}
{"stream_type":"event","sim_ts":2.0,"timestamp":"...","key":"east","task":"report","instance":2,"value":{"warehouse":"east","orders_waiting":2}}
{"stream_type":"event","sim_ts":4.0,"timestamp":"...","key":"north","task":"report","instance":3,"value":{"warehouse":"north","orders_waiting":4}}
```

______________________________________________________________________

## Full Files

Full files

```yaml
# Many entities: ten tills, each with its own key, report their takings every minute.
#
# `count: 10` starts ten entities from the one arrival, and `key` gives each its own
# key, till-1 to till-10, which the task reads with `entity`. The key is the record key
# too, so a Kafka topic puts each till's records in one partition, in order.
#
# Needs: nothing.
# Run it with: desflow run docs/snippets/generate-data/many_entities.yaml

simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  tills:
    dist: constant
    value: 60
    spawn: report
    count: 10
    key: {gen: sequence, start: 1, format: "till-{}"}

tasks:
  report:
    payload:
      till: {gen: entity}
      takings: {gen: normal, mean: 400, std: 60, decimals: 2}

run: {until: 300}
```

```yaml
# Many entities, by name: one entity for each key in a list.
#
# `keys` starts one entity for each name, here three warehouses, and each reports the
# orders waiting every 2 seconds under its own name.
#
# Needs: nothing.
# Run it with: desflow run docs/snippets/generate-data/many_entities_named.yaml

simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  warehouses: {dist: constant, value: 2, spawn: report, keys: [north, south, east]}

tasks:
  report:
    payload:
      warehouse: {gen: entity}
      orders_waiting: {gen: integer, bounds: [0, 40]}

run: {until: 10}
```

```python
"""Many entities: ten tills, each with its own key, builder API.

The builder version of `many_entities.yaml`: ten tills, each with its own key, report
their takings every minute. Each till is its own arrival, because `add_arrival` has no
`count`.

Needs: nothing.
Run it with: python docs/snippets/generate-data/many_entities_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 300


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    # A blueprint's `count: 10` is ten arrivals here, each with the key of its till
    for number in range(1, 11):
        app.add_arrival(
            f"tills{number}",
            "constant",
            value=60,
            spawn="report",
            key=lambda n, number=number: f"till-{number}",
        )
    report_payload = Template(
        {
            "till": {"gen": "entity"},
            "takings": {"gen": "normal", "mean": 400, "std": 60, "decimals": 2},
        },
        name="tasks.report",
    )

    @app.task(name="report")
    def report(task_id):
        """Gives one report record."""
        return report_payload


    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Many entities, by name: one entity for each key in a list, builder API.

The builder version of `many_entities_named.yaml`: three warehouses, each with its own
name, report the orders waiting every 2 seconds. Each warehouse is its own arrival,
because `add_arrival` has no `keys`.

Needs: nothing.
Run it with: python docs/snippets/generate-data/many_entities_named_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 10


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    # A blueprint's `keys` is one arrival for each name here, each with its own key
    for name in ["north", "south", "east"]:
        app.add_arrival(
            f"warehouses-{name}",
            "constant",
            value=2,
            spawn="report",
            key=lambda n, name=name: name,
        )
    report_payload = Template(
        {
            "warehouse": {"gen": "entity"},
            "orders_waiting": {"gen": "integer", "bounds": [0, 40]},
        },
        name="tasks.report",
    )

    @app.task(name="report")
    def report(task_id):
        """Gives one report record."""
        return report_payload


    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Many entities: ten tills, each with its own key, low-level API.

The low-level version of `many_entities.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
till is one process with its own key. Each draw is made with `Sampler`, from the
random sequence the blueprint's generator at that place uses, so the draws are the
blueprint's, and the ten tills share that one sequence.

Needs: nothing.
Run it with: python docs/snippets/generate-data/many_entities_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 300
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the ten tills on `env`."""
    sampler = Sampler(seed=42)
    takings = sampler.generator("tasks.report.payload.takings")
    tills_gap = DistributionConfig(dist="constant", value=60)
    # The random sequence the blueprint's draw uses
    tills_gap.stream = "arrival.tills"
    sent = {"report": 0}

    def till(key):
        while True:
            yield env.timeout(sampler.sample(tills_gap))
            payload = {
                "till": key,
                "takings": round(float(takings.normal(400, 60)), 2),
            }
            env.publish_event(key, payload, task="report", instance=sent["report"])
            sent["report"] += 1

    for number in range(1, 11):
        env.process(till(f"till-{number}"))


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```

```python
"""Many entities, by name: one entity for each key in a list, low-level API.

The low-level version of `many_entities_named.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
warehouse is one process with its own name. Each draw is made with `Sampler`, from the
random sequence the blueprint's generator at that place uses, so the draws are the
blueprint's, and the three warehouses share that one sequence.

Needs: nothing.
Run it with: python docs/snippets/generate-data/many_entities_named_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 10
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the three warehouses on `env`."""
    sampler = Sampler(seed=42)
    waiting = sampler.generator("tasks.report.payload.orders_waiting")
    warehouses_gap = DistributionConfig(dist="constant", value=2)
    # The random sequence the blueprint's draw uses
    warehouses_gap.stream = "arrival.warehouses"
    sent = {"report": 0}

    def warehouse(key):
        while True:
            yield env.timeout(sampler.sample(warehouses_gap))
            payload = {
                "warehouse": key,
                "orders_waiting": int(waiting.integers(0, 40 + 1)),
            }
            env.publish_event(key, payload, task="report", instance=sent["report"])
            sent["report"] += 1

    for key in ["north", "south", "east"]:
        env.process(warehouse(key))


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```
