# Stop After a Count

End a run after exactly 12 records, with no time limit: `max_events` stops the arrival, and the run ends when the last record is written.

______________________________________________________________________

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

  ```bash
  git clone https://github.com/jaehyeon-kim/desflow.git
  cd desflow
  ```

- [First Simulation](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md) explains arrivals, tasks and the records a run writes.

- [End Conditions](https://jaehyeon.me/desflow/latest/learn/first-model/end-conditions/index.md) lists every way a run ends, and [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/index.md) explains `until` and the other limits.

______________________________________________________________________

## Steps

### Limit the Arrival

`max_events: 12` is how many times the arrival starts its task in all. An order arrives every 2 seconds, so the 12th arrives at 24 seconds, and the arrival then stops.

```yaml
arrivals:
  orders: {dist: constant, value: 2, spawn: order, max_events: 12}
```

```python
app.add_arrival("orders", "constant", value=2, spawn="order", max_events=12)
```

```python
sampler = Sampler(seed=42)
orders_gap = DistributionConfig(dist="constant", value=2)
# The random sequence the blueprint's draw uses
orders_gap.stream = "arrival.orders"
```

A Low-level script has no `max_events`. Its source loops over `range(1, 13)` and ends after the 12th record.

### Leave Out the End Time

The task numbers each order with `sequence`, so the records show the count. The blueprint has no `run` section, and the Builder and Low-level files set no `until`, so the run ends when the last record is written.

```yaml
tasks:
  order:
    payload:
      order_id: {gen: sequence, start: 1}
```

```python
order_payload = Template(
    {"order_id": {"gen": "sequence", "start": 1}}, name="tasks.order"
)

@app.task(name="order")
def order(task_id):
    """Gives one order record."""
    return order_payload
```

```python
def orders_source():
    for number in range(1, 13):
        yield env.timeout(sampler.sample(orders_gap))
        env.publish_event(
            f"orders-{number}",
            {"order_id": number},
            task="order",
            instance=number - 1,
        )

env.process(orders_source())
```

______________________________________________________________________

## Check the Result

Run the model and print its first 5 records. The `order_id` runs from 1, and the records come every 2 seconds.

```bash
desflow run docs/snippets/generate-data/stop_after_a_count.yaml --sample 5
```

```bash
python docs/snippets/generate-data/stop_after_a_count_builder.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```bash
python docs/snippets/generate-data/stop_after_a_count_low_level.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```text
{"stream_type":"event","sim_ts":2.0,"timestamp":"...","key":"orders-1","task":"order","instance":0,"value":{"order_id":1}}
{"stream_type":"event","sim_ts":4.0,"timestamp":"...","key":"orders-2","task":"order","instance":1,"value":{"order_id":2}}
{"stream_type":"event","sim_ts":6.0,"timestamp":"...","key":"orders-3","task":"order","instance":2,"value":{"order_id":3}}
{"stream_type":"event","sim_ts":8.0,"timestamp":"...","key":"orders-4","task":"order","instance":3,"value":{"order_id":4}}
{"stream_type":"event","sim_ts":10.0,"timestamp":"...","key":"orders-5","task":"order","instance":4,"value":{"order_id":5}}
```

The run ends with order 12 at 24 seconds. The docs test also runs the model to 1,000 seconds and still gets 12 records, and the Builder file with no `until` gets 12 too.

______________________________________________________________________

## Full Files

Full files

```yaml
# Stop after a count: an arrival that ends after exactly 12 records.
#
# An order arrives every 2 seconds, and `max_events` ends the arrival after 12. The run
# has no `until`, so it ends when the last record is written.
#
# Needs: nothing.
# Run it with: desflow run docs/snippets/generate-data/stop_after_a_count.yaml

simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  orders: {dist: constant, value: 2, spawn: order, max_events: 12}

tasks:
  order:
    payload:
      order_id: {gen: sequence, start: 1}
```

```python
"""Stop after a count: an arrival that ends after exactly 12 records, builder API.

The builder version of `stop_after_a_count.yaml`: an order every 2 seconds, ended by
`max_events` after 12.

Needs: nothing.
Run it with: python docs/snippets/generate-data/stop_after_a_count_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = None


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("orders", "constant", value=2, spawn="order", max_events=12)
    order_payload = Template(
        {"order_id": {"gen": "sequence", "start": 1}}, name="tasks.order"
    )

    @app.task(name="order")
    def order(task_id):
        """Gives one order record."""
        return order_payload


    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run()
```

```python
"""Stop after a count: an arrival that ends after exactly 12 records, low-level API.

The low-level version of `stop_after_a_count.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. The
arrival loop runs 12 times and then ends, so the run ends when the last record is
written.

Needs: nothing.
Run it with: python docs/snippets/generate-data/stop_after_a_count_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = None
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrival on `env`."""
    sampler = Sampler(seed=42)
    orders_gap = DistributionConfig(dist="constant", value=2)
    # The random sequence the blueprint's draw uses
    orders_gap.stream = "arrival.orders"

    def orders_source():
        for number in range(1, 13):
            yield env.timeout(sampler.sample(orders_gap))
            env.publish_event(
                f"orders-{number}",
                {"order_id": number},
                task="order",
                instance=number - 1,
            )

    env.process(orders_source())


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
