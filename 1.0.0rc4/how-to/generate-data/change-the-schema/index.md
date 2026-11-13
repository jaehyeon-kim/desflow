# Change the Schema During a Run

Add, rename and retype a field at known times, and write a label record for each change: an order gains a `currency` field at 60 seconds, `total` becomes `amount` at 120 seconds, and the amount becomes text at 180 seconds.

______________________________________________________________________

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

  ```bash
  git clone https://github.com/jaehyeon-kim/desflow.git
  cd desflow
  ```

- [First Simulation](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md) explains arrivals, tasks and the records a run writes.

- [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/index.md) explains scenario steps and label records.

______________________________________________________________________

## Steps

### Describe the Arrival

An order arrives every 10 seconds.

```yaml
arrivals:
  orders: {dist: constant, value: 10, spawn: order}
```

```python
app.add_arrival("orders", "constant", value=10, spawn="order")
```

```python
sampler = Sampler(seed=42)
value = sampler.generator("tasks.order.vars.value")
gap = DistributionConfig(dist="constant", value=10)
# The random sequence the blueprint's draw uses
gap.stream = "arrival.orders"
```

`Sampler(seed=42)` supplies every draw: a random generator for each payload field and for the gap. The `stream` line names the random sequence the blueprint uses for this arrival, so the three forms draw the same gaps.

### Change the Fields at Set Times

`omit_when` leaves a field out of a record while its condition is true, and `t` is the simulation time. `currency` is left out before 60 seconds. `total` is left out from 120 seconds, when `amount` replaces it, and `amount` is left out before then. From 180 seconds [`when`](https://jaehyeon.me/desflow/latest/generators/conditions/when/index.md) writes the amount as text with [`format`](https://jaehyeon.me/desflow/latest/generators/text-and-ids/format/index.md). A `vars` entry draws the value once for each order, so `total` and `amount` hold the same number.

```yaml
tasks:
  order:
    vars:
      value: {gen: uniform, low: 5, high: 200, decimals: 2}
    payload:
      order_id: {gen: sequence, start: 1}
      currency: {gen: literal, value: EUR, omit_when: "t < 60"}
      total: {gen: var, name: value, omit_when: "t >= 120"}
      amount:
        gen: when
        condition: "t >= 180"
        then: {gen: format, template: "{value}"}
        else: {gen: var, name: value}
        omit_when: "t < 120"
```

```python
order_payload = Template(
    {
        "order_id": {"gen": "sequence", "start": 1},
        "currency": {"gen": "literal", "value": "EUR", "omit_when": "t < 60"},
        "total": {"gen": "var", "name": "value", "omit_when": "t >= 120"},
        "amount": {
            "gen": "when",
            "condition": "t >= 180",
            "then": {"gen": "format", "template": "{value}"},
            "else": {"gen": "var", "name": "value"},
            "omit_when": "t < 120",
        },
    },
    name="tasks.order",
    vars={"value": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2}},
)

@app.task(name="order")
def order(task_id):
    """Gives one order record."""
    return order_payload
```

```python
def order(number):
    """The payload of one order, as it is at the time `t` it is written."""
    t = env.now
    total = round(float(value.uniform(5, 200)), 2)
    payload = {"order_id": number}
    if t >= 60:
        payload["currency"] = "EUR"
    if t < 120:
        payload["total"] = total
    else:
        payload["amount"] = f"{total}" if t >= 180 else total
    return payload

def source():
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.publish_event(
            f"orders-{number}",
            order(number),
            task="order",
            instance=number - 1,
        )

def changes():
    for at, record in CHANGES:
        yield env.timeout(at - env.now)
        env.publish_label(record)

env.process(source())
env.process(changes())
```

### Label Each Change

A step of the `scenario` with a `record` writes a label record at its time. The second egress has `when: labels`, so it receives only the labels, and the first receives only the orders. The labels say what changed and when without mixing into the data.

```yaml
scenario:
  - at: 60
    record: {change: field_added, field: currency}
  - at: 120
    record: {change: field_renamed, field: total, to: amount}
  - at: 180
    record: {change: field_retyped, field: amount, to: text}
```

```yaml
egress:
  - {type: Console}
  - {type: Console, when: labels}
```

```python
app.add_scenario(
    [
        {"at": 60, "record": {"change": "field_added", "field": "currency"}},
        {
            "at": 120,
            "record": {
                "change": "field_renamed",
                "field": "total",
                "to": "amount",
            },
        },
        {
            "at": 180,
            "record": {
                "change": "field_retyped",
                "field": "amount",
                "to": "text",
            },
        },
    ]
)
```

```python
app.add_egress(ConsoleEgress())
app.add_egress(ConsoleEgress(), when="labels")
```

```python
CHANGES = [  # (time in seconds, the label record)
    (60, {"change": "field_added", "field": "currency"}),
    (120, {"change": "field_renamed", "field": "total", "to": "amount"}),
    (180, {"change": "field_retyped", "field": "amount", "to": "text"}),
]
```

```python
env.setup_egress([ConsoleEgress(), ConsoleEgress()], labels=[False, True])
```

The Low-level file publishes each label with `env.publish_label` from a process of its own, and `labels=[False, True]` sends the second sink only the labels.

______________________________________________________________________

## Check the Result

Run the model for 240 simulated seconds and print its first 19 orders. The block keeps an order on each side of each change, and the three labels.

```bash
desflow run docs/snippets/generate-data/change_the_schema_during_a_run.yaml --sample 19
```

```bash
python docs/snippets/generate-data/change_the_schema_during_a_run_builder.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```bash
python docs/snippets/generate-data/change_the_schema_during_a_run_low_level.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```text
{"stream_type":"event","sim_ts":50.0,"timestamp":"...","key":"orders-5","task":"order","instance":4,"value":{"order_id":5,"total":44.44}}
{"stream_type":"event","sim_ts":60.0,"timestamp":"...","key":"orders-6","task":"order","instance":5,"value":{"order_id":6,"currency":"EUR","total":151.34}}
{"stream_type":"event","sim_ts":110.0,"timestamp":"...","key":"orders-11","task":"order","instance":10,"value":{"order_id":11,"currency":"EUR","total":22.01}}
{"stream_type":"event","sim_ts":120.0,"timestamp":"...","key":"orders-12","task":"order","instance":11,"value":{"order_id":12,"currency":"EUR","amount":103.07}}
{"stream_type":"event","sim_ts":170.0,"timestamp":"...","key":"orders-17","task":"order","instance":16,"value":{"order_id":17,"currency":"EUR","amount":143.54}}
{"stream_type":"event","sim_ts":180.0,"timestamp":"...","key":"orders-18","task":"order","instance":17,"value":{"order_id":18,"currency":"EUR","amount":"92.49"}}
{"stream_type":"label","sim_ts":60.0,"timestamp":"...","path":null,"old":null,"new":null,"label":{"change":"field_added","field":"currency"}}
{"stream_type":"label","sim_ts":120.0,"timestamp":"...","path":null,"old":null,"new":null,"label":{"change":"field_renamed","field":"total","to":"amount"}}
{"stream_type":"label","sim_ts":180.0,"timestamp":"...","path":null,"old":null,"new":null,"label":{"change":"field_retyped","field":"amount","to":"text"}}
```

Order 6, at 60 seconds, is the first with `currency`. Order 12, at 120 seconds, is the first with `amount` in place of `total`. Order 18, at 180 seconds, is the first with an amount that is text. Each label carries the `sim_ts` of its change. The docs test checks all three changes and that the labels reach only the labels sink.

______________________________________________________________________

## Full Files

Full files

```yaml
# Change the schema during a run: a field is added, renamed and retyped at known times.
#
# An order arrives every 10 seconds. From 60 s the record has a `currency` field. From
# 120 s `total` is written as `amount`, and from 180 s the amount is text, not a number.
# A step of the scenario at each change writes a label record, which only the second
# egress receives, so the labels say what changed and when without mixing into the data.
#
# Needs: nothing.
# Run it with: desflow run
# docs/snippets/generate-data/change_the_schema_during_a_run.yaml

simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}
  - {type: Console, when: labels}

arrivals:
  orders: {dist: constant, value: 10, spawn: order}

tasks:
  order:
    vars:
      value: {gen: uniform, low: 5, high: 200, decimals: 2}
    payload:
      order_id: {gen: sequence, start: 1}
      currency: {gen: literal, value: EUR, omit_when: "t < 60"}
      total: {gen: var, name: value, omit_when: "t >= 120"}
      amount:
        gen: when
        condition: "t >= 180"
        then: {gen: format, template: "{value}"}
        else: {gen: var, name: value}
        omit_when: "t < 120"

scenario:
  - at: 60
    record: {change: field_added, field: currency}
  - at: 120
    record: {change: field_renamed, field: total, to: amount}
  - at: 180
    record: {change: field_retyped, field: amount, to: text}

run: {until: 240}
```

```python
"""Change the schema during a run, builder API.

The builder version of `change_the_schema_during_a_run.yaml`: an order every 10 seconds
for 4 minutes. A field is added at 60 s, renamed at 120 s and retyped at 180 s, and a
scenario step at each change writes a label record to the labels sink.

Needs: nothing.
Run it with: python docs/snippets/generate-data/change_the_schema_during_a_run_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 240


def build() -> SimulationContext:
    """Builds the shop."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_egress(ConsoleEgress(), when="labels")
    app.add_arrival("orders", "constant", value=10, spawn="order")
    app.add_scenario(
        [
            {"at": 60, "record": {"change": "field_added", "field": "currency"}},
            {
                "at": 120,
                "record": {
                    "change": "field_renamed",
                    "field": "total",
                    "to": "amount",
                },
            },
            {
                "at": 180,
                "record": {
                    "change": "field_retyped",
                    "field": "amount",
                    "to": "text",
                },
            },
        ]
    )
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "currency": {"gen": "literal", "value": "EUR", "omit_when": "t < 60"},
            "total": {"gen": "var", "name": "value", "omit_when": "t >= 120"},
            "amount": {
                "gen": "when",
                "condition": "t >= 180",
                "then": {"gen": "format", "template": "{value}"},
                "else": {"gen": "var", "name": "value"},
                "omit_when": "t < 120",
            },
        },
        name="tasks.order",
        vars={"value": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2}},
    )

    @app.task(name="order")
    def order(task_id):
        """Gives one order record."""
        return order_payload


    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Change the schema during a run, low-level API.

The low-level version of `change_the_schema_during_a_run.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`. A process publishes each label record with
`publish_label` at the time of its change. The order is a dictionary. It gains
`currency` from 60 s and writes `total` as `amount` from 120 s, and from 180 s the
amount is text, as the blueprint's `omit_when` and `when` on `t` do. The value is drawn with
`Sampler`, from the random sequence the blueprint's generator at that place uses.

Needs: nothing.
Run it with: python docs/snippets/generate-data/change_the_schema_during_a_run_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 240
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}

CHANGES = [  # (time in seconds, the label record)
    (60, {"change": "field_added", "field": "currency"}),
    (120, {"change": "field_renamed", "field": "total", "to": "amount"}),
    (180, {"change": "field_retyped", "field": "amount", "to": "text"}),
]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the orders and the changes on `env`."""
    sampler = Sampler(seed=42)
    value = sampler.generator("tasks.order.vars.value")
    gap = DistributionConfig(dist="constant", value=10)
    # The random sequence the blueprint's draw uses
    gap.stream = "arrival.orders"

    def order(number):
        """The payload of one order, as it is at the time `t` it is written."""
        t = env.now
        total = round(float(value.uniform(5, 200)), 2)
        payload = {"order_id": number}
        if t >= 60:
            payload["currency"] = "EUR"
        if t < 120:
            payload["total"] = total
        else:
            payload["amount"] = f"{total}" if t >= 180 else total
        return payload

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.publish_event(
                f"orders-{number}",
                order(number),
                task="order",
                instance=number - 1,
            )

    def changes():
        for at, record in CHANGES:
            yield env.timeout(at - env.now)
            env.publish_label(record)

    env.process(source())
    env.process(changes())


def run() -> None:
    """Runs the model unpaced: the orders to one sink, the labels to another."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress(), ConsoleEgress()], labels=[False, True])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```
