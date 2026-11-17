# Write a Connector

Write a connector of your own when a model must read changes from, or write records to, a system that desflow has no connector for. This page writes an egress that writes each event as a row of a CSV file, and an ingress that follows a JSON Lines file of changes. Neither needs a service.

______________________________________________________________________

## Before You Start

- [Ingress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/ingress/index.md) and [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/index.md) describe the connectors desflow has and the threads they run on.

______________________________________________________________________

## Steps

### Write an Egress

An egress subclasses `BaseEgress` and has one method, `async run(queue)`. desflow runs it on the egress thread's asyncio loop, apart from the simulation. The queue holds batches: each batch is a list of records, in the [record shape](https://jaehyeon.me/desflow/latest/reference/records-and-telemetry/#record-shape) every sink receives. `run` takes a batch with `get_nowait`, and sleeps when the queue is empty so the other connectors on the loop can run:

```python
class CsvEgress(BaseEgress):
    """Writes one CSV row per event: its time, key and task, then the payload
    fields that `columns` names."""

    def __init__(self, path: str, columns: List[str]):
        self.path = Path(path)
        self.columns = list(columns)
        self.active_tasks = 0  # teardown waits while a write is in progress

    async def run(self, egress_queue: queue.Queue) -> None:
        with self.path.open("w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["sim_ts", "key", "task", *self.columns])
            while True:
                try:
                    batch = egress_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.1)  # let the other connectors run
                    continue
                self.active_tasks += 1
                try:
                    # A batch also holds telemetry; this sink writes only the events
                    rows = [
                        self._row(record)
                        for record in batch
                        if record["stream_type"] == "event"
                    ]
                    # A file write blocks, so it runs on a worker thread
                    await asyncio.to_thread(self._write, file, writer, rows)
                finally:
                    self.active_tasks -= 1

    def _row(self, record: dict) -> list:
        value = record["value"] if isinstance(record.get("value"), dict) else {}
        fields = [value.get(column) for column in self.columns]
        return [record["sim_ts"], record.get("key"), record.get("task"), *fields]

    @staticmethod
    def _write(file, writer, rows: list) -> None:
        writer.writerows(rows)
        file.flush()
```

Three rules apply to every egress:

- **Do not block the loop.** A write that blocks, such as a file write or a client without `async`, runs on a worker thread with `asyncio.to_thread`.
- **Count the writes in progress.** At the end of the run, teardown waits for every queue to empty, then waits while the sink's `active_tasks` is above 0. A batch taken from the queue but not yet written is therefore not lost.
- **Copy before you change a record.** Every sink receives the same dictionaries, so a sink that changes one in place changes what the others write. This sink builds new rows.

An error that ends `run` is logged at ERROR with the connector's name. The sink then takes no more batches, so once its queue is full the simulation waits, and after `drain_stall_seconds` the run raises `RuntimeError`, as [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#how-it-works) describes.

### Write an Ingress

An ingress subclasses `BaseIngress` and has one method too, `async run(queue)`, which desflow runs on the ingress thread. It puts each change on the queue as a `(path, value)` pair. `parse_change` turns a message such as `{"path": "Shop.resources.packers.current_cap", "value": 2}` into that pair, with the check the built-in connectors use. The ingress monitor applies what is queued every 0.1 simulation seconds, and the registry checks each value before it stores it ([Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/#how-it-works)):

```python
class JsonlIngress(BaseIngress):
    """Follows a JSON Lines file and applies each line it adds, such as
    `{"path": "Shop.resources.packers.current_cap", "value": 2}`."""

    def __init__(self, path: str, poll: float = 0.1):
        self.path = Path(path)
        self.poll = poll

    async def run(self, ingress_queue: queue.Queue) -> None:
        read = 0  # the lines already applied
        while True:
            if self.path.exists():
                # A file read blocks, so it runs on a worker thread
                text = await asyncio.to_thread(self.path.read_text)
                # Only whole lines: a writer may be partway through the last one
                lines = text[: text.rfind("\n") + 1].splitlines()
                for line in lines[read:]:
                    try:
                        ingress_queue.put(parse_change(json.loads(line)))
                    except ValueError as error:  # also a line that is not JSON
                        logger.warning("Skipped a line of %s: %s", self.path, error)
                read = len(lines)
            await asyncio.sleep(self.poll)
```

A line of the file that is not a change is logged as one WARNING and skipped, as the built-in connectors do, so one bad message does not stop the run. At the end of the run desflow cancels `run`, so it needs no way to stop.

### Use Them

A connector of your own goes where a built-in one goes. Each `config` key is an argument of the class's constructor:

```yaml
ingress:
  - type: !python write_a_connector_logic.JsonlIngress
    config: {path: changes.jsonl}

egress:
  - type: !python write_a_connector_logic.CsvEgress
    config: {path: events.csv, columns: [status, event_type]}
```

The classes are in `write_a_connector_logic.py`, beside the blueprint. `type: !python` names the class, and the loader builds it with `config` as its arguments.

```python
app = (
    SimulationContext(sim_id="Shop", factor=0.05, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=3)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="constant", value=1.0)
    .add_ingress(JsonlIngress("changes.jsonl"))
    .add_egress(CsvEgress("events.csv", columns=["status", "event_type"]))
)

@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return {"event_type": "order_packed"}

@app.arrival_loop("orders")
def order_generator(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

app.run(until=20)
```

```python
env = DynamicRealtimeEnvironment(factor=0.05)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop",
        service={"packing": DistributionConfig(dist="constant", value=1.5)},
        resources={"packers": CapacityConfig(current_cap=1, max_cap=3)},
    )
)
env.setup_ingress([JsonlIngress("changes.jsonl")])
env.setup_egress([CsvEgress("events.csv", columns=["status", "event_type"])])
packers = DynamicResource(env, "Shop", "packers")

def order(env, number):
    key, path_id = f"orders-{number}", "Shop.service.packing"
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", number - 1
    )
    with packers.request() as request:
        yield request
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", number - 1
        )
        yield env.timeout(1.5)
    env.publish_event(key, {"event_type": "order_packed"}, "pack_order", number - 1)

def arrivals(env):
    number = 0
    while True:
        yield env.timeout(1.0)
        number += 1
        env.process(order(env, number))

env.process(arrivals(env))
try:
    env.run(until=20)
finally:
    env.teardown()  # writes what is still buffered and stops the threads
```

The low-level API attaches the same objects with `setup_ingress` and `setup_egress`, and `teardown` waits for the CSV sink to write what is still queued.

The shop is paced at `factor: 0.05`, so 20 simulation seconds pass in each real second, and the run lasts one second of real time. An ingress reads its source while time passes, and a run at `factor: 0` can end before the source is read once.

______________________________________________________________________

## Check the Result

Before the run, `changes.jsonl` holds one line, which sets the packers to 2 units. An order arrives every second from 1 second, and each takes 1.5 seconds, so one packer would fall behind. The ingress applies the change in the first moments of the run, and from then on each order starts when it arrives. The first rows of `events.csv`:

```text
sim_ts,key,task,status,event_type
1.0,orders-1,pack_order,queued,
1.0,orders-1,pack_order,started,
2.0,orders-2,pack_order,queued,
2.0,orders-2,pack_order,started,
2.5,orders-1,pack_order,,order_packed
```

`orders-2` starts at 2 seconds while `orders-1` is still being packed, so the second unit is there. By 20 seconds, 19 orders have arrived and started, and the 18 that arrived by 18 seconds are packed. That is 19 `queued`, 19 `started` and 18 `order_packed` events: 56 rows. All three forms write the same rows.

Save the files that the tab of your form names from [Full Files](#full-files) in one folder, each under the last part of its title, and run the command of your form from that folder:

Save `write_a_connector.yaml` and `write_a_connector_logic.py`. The first line writes the change that the ingress reads. The blueprint does not write it:

```bash
echo '{"path": "Shop.resources.packers.current_cap", "value": 2}' > changes.jsonl
desflow run write_a_connector.yaml
head -n 6 events.csv
wc -l events.csv
```

```text
sim_ts,key,task,status,event_type
1.0,orders-1,pack_order,queued,
1.0,orders-1,pack_order,started,
2.0,orders-2,pack_order,queued,
2.0,orders-2,pack_order,started,
2.5,orders-1,pack_order,,order_packed
      57 events.csv
```

The 57 lines are the header and the 56 rows. The spaces before the count depend on your system.

Save `write_a_connector.py`, `write_a_connector_logic.py` and the blueprint `write_a_connector.yaml`, because the script runs the YAML form first. `python write_a_connector.py` writes `changes.jsonl`, runs the YAML form and then the builder form, and prints the packers' units at the end and the number of rows in `events.csv` for each:

```text
yaml_form       packers: 2, rows in events.csv: 56
builder_form    packers: 2, rows in events.csv: 56
```

Save `write_a_connector_low_level.py` and `write_a_connector_logic.py`. `python write_a_connector_low_level.py` does the same for the low-level form:

```text
low_level_form  packers: 2, rows in events.csv: 56
```

______________________________________________________________________

## Full Files

Every form uses the two connectors in `write_a_connector_logic.py` unchanged. Put it, the blueprint and the scripts in one folder, and run the scripts from there.

Full files

```python
"""Python for write_a_connector.yaml: an egress that writes events to a CSV file and
an ingress that follows a JSON Lines file of changes, as the Write a Connector guide
builds them."""

import asyncio
import csv
import json
import logging
import queue
from pathlib import Path
from typing import List

from desflow.connectors.egress.base import BaseEgress
from desflow.connectors.ingress.base import BaseIngress, parse_change

logger = logging.getLogger(__name__)


class CsvEgress(BaseEgress):
    """Writes one CSV row per event: its time, key and task, then the payload
    fields that `columns` names."""

    def __init__(self, path: str, columns: List[str]):
        self.path = Path(path)
        self.columns = list(columns)
        self.active_tasks = 0  # teardown waits while a write is in progress

    async def run(self, egress_queue: queue.Queue) -> None:
        with self.path.open("w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["sim_ts", "key", "task", *self.columns])
            while True:
                try:
                    batch = egress_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.1)  # let the other connectors run
                    continue
                self.active_tasks += 1
                try:
                    # A batch also holds telemetry; this sink writes only the events
                    rows = [
                        self._row(record)
                        for record in batch
                        if record["stream_type"] == "event"
                    ]
                    # A file write blocks, so it runs on a worker thread
                    await asyncio.to_thread(self._write, file, writer, rows)
                finally:
                    self.active_tasks -= 1

    def _row(self, record: dict) -> list:
        value = record["value"] if isinstance(record.get("value"), dict) else {}
        fields = [value.get(column) for column in self.columns]
        return [record["sim_ts"], record.get("key"), record.get("task"), *fields]

    @staticmethod
    def _write(file, writer, rows: list) -> None:
        writer.writerows(rows)
        file.flush()


class JsonlIngress(BaseIngress):
    """Follows a JSON Lines file and applies each line it adds, such as
    `{"path": "Shop.resources.packers.current_cap", "value": 2}`."""

    def __init__(self, path: str, poll: float = 0.1):
        self.path = Path(path)
        self.poll = poll

    async def run(self, ingress_queue: queue.Queue) -> None:
        read = 0  # the lines already applied
        while True:
            if self.path.exists():
                # A file read blocks, so it runs on a worker thread
                text = await asyncio.to_thread(self.path.read_text)
                # Only whole lines: a writer may be partway through the last one
                lines = text[: text.rfind("\n") + 1].splitlines()
                for line in lines[read:]:
                    try:
                        ingress_queue.put(parse_change(json.loads(line)))
                    except ValueError as error:  # also a line that is not JSON
                        logger.warning("Skipped a line of %s: %s", self.path, error)
                read = len(lines)
            await asyncio.sleep(self.poll)
```

```yaml
# The Write a Connector guide: a shop whose packers follow a file of changes and
# whose events go to a CSV file, through two connectors of its own.
# Write changes.jsonl first (the line is in the Check the Result section of the page),
# then run it with: desflow run write_a_connector.yaml
# In a folder with no changes.jsonl, it keeps one packer.
simulation:
  sim_id: Shop
  factor: 0.05  # 20 simulation seconds a second, so the file is read during the run
  random_seed: 42

resources:
  packers: {current_cap: 1, max_cap: 3}

services:
  packing: {dist: constant, value: 1.5 s}

arrivals:
  orders:
    dist: constant
    value: 1 s
    spawn: pack_order

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

ingress:
  - type: !python write_a_connector_logic.JsonlIngress
    config: {path: changes.jsonl}

egress:
  - type: !python write_a_connector_logic.CsvEgress
    config: {path: events.csv, columns: [status, event_type]}

run:
  until: 20 s
```

```python
"""Write a Connector: an egress that writes events to a CSV file and an ingress that
follows a JSON Lines file of changes.

An order arrives every second and each takes 1.5 seconds to pack, so one packer
falls behind. Before the run, changes.jsonl holds one change that sets the packers to
2. The ingress applies it at the start, and the events go to events.csv. Each
function runs the shop in the current folder, in YAML or in the builder, and
returns the packers' capacity at the end and the rows of events.csv.
write_a_connector_low_level.py is the low-level API.
"""

import csv
import json
import logging
import sys
from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # write_a_connector_logic.py is beside the blueprint
from write_a_connector_logic import CsvEgress, JsonlIngress  # noqa: E402

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)
CHANGE = {"path": "Shop.resources.packers.current_cap", "value": 2}


def _rows():
    with open("events.csv", newline="") as file:
        return list(csv.DictReader(file))


def yaml_form():
    """The blueprint beside the classes."""
    Path("changes.jsonl").write_text(json.dumps(CHANGE) + "\n")
    app = SimulationContext.from_yaml(HERE / "write_a_connector.yaml")
    app.run()
    return app.get_resource("packers").capacity, _rows()


def builder_form():
    """The same shop with the builder."""
    Path("changes.jsonl").write_text(json.dumps(CHANGE) + "\n")
    app = (
        SimulationContext(sim_id="Shop", factor=0.05, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=3)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="constant", value=1.0)
        .add_ingress(JsonlIngress("changes.jsonl"))
        .add_egress(CsvEgress("events.csv", columns=["status", "event_type"]))
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

    app.run(until=20)
    return app.get_resource("packers").capacity, _rows()


if __name__ == "__main__":
    for form in (yaml_form, builder_form):
        capacity, rows = form()
        print(
            f"{form.__name__:15} packers: {capacity}, rows in events.csv: {len(rows)}"
        )
```

```python
"""Write a Connector, low-level API: the shop with the CSV egress and the JSON Lines
ingress of write_a_connector_logic.py.

The low-level version of write_a_connector.py. Before the run, changes.jsonl holds one
change that sets the packers to 2. `low_level_form` runs the shop in the current
folder and returns the packers' capacity at the end and the rows of events.csv.
"""

import csv
import json
import logging
import sys
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # write_a_connector_logic.py is beside the blueprint
from write_a_connector_logic import CsvEgress, JsonlIngress  # noqa: E402

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)
CHANGE = {"path": "Shop.resources.packers.current_cap", "value": 2}


def _rows():
    with open("events.csv", newline="") as file:
        return list(csv.DictReader(file))


def low_level_form():
    """The same shop with the environment and the resource."""
    Path("changes.jsonl").write_text(json.dumps(CHANGE) + "\n")
    env = DynamicRealtimeEnvironment(factor=0.05)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            service={"packing": DistributionConfig(dist="constant", value=1.5)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=3)},
        )
    )
    env.setup_ingress([JsonlIngress("changes.jsonl")])
    env.setup_egress([CsvEgress("events.csv", columns=["status", "event_type"])])
    packers = DynamicResource(env, "Shop", "packers")

    def order(env, number):
        key, path_id = f"orders-{number}", "Shop.service.packing"
        env.publish_event(
            key, {"path_id": path_id, "status": "queued"}, "pack_order", number - 1
        )
        with packers.request() as request:
            yield request
            env.publish_event(
                key, {"path_id": path_id, "status": "started"}, "pack_order", number - 1
            )
            yield env.timeout(1.5)
        env.publish_event(key, {"event_type": "order_packed"}, "pack_order", number - 1)

    def arrivals(env):
        number = 0
        while True:
            yield env.timeout(1.0)
            number += 1
            env.process(order(env, number))

    env.process(arrivals(env))
    try:
        env.run(until=20)
    finally:
        env.teardown()  # writes what is still buffered and stops the threads
    return packers.capacity, _rows()


if __name__ == "__main__":
    capacity, rows = low_level_form()
    print(f"low_level_form  packers: {capacity}, rows in events.csv: {len(rows)}")
```
