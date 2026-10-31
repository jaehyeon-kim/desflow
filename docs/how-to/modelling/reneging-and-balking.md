# Add Reneging and Balking

Give a task a time limit on its wait, a condition that stops it joining at all, or both, and account for every entity afterwards.

---

## Before You Start

- [Patience](../../learn/shop-logic/patience.md) explains both ideas.
- [Resources](../../user-guide/resources/index.md#how-it-works) explains how a request is granted.

---

## Steps

### Leave After a Wait and Refuse to Join

Two settings let a task leave without being served:

- **Reneging:** a task's `patience` is how long it may wait before it leaves the queue and publishes `reneged`.
- **Balking:** a task's `balk` is a condition checked before it joins the queue. When the condition holds, the task publishes `balked` and never requests the resource.

Patience covers the task's `wait`, then its resource and then its `get`. It counts waiting time only and pauses while the task is served, so a task that is preempted and queues again goes on with the time it has left. A get that gives up is cancelled, so it takes nothing later.

Here each order brings its own patience as an attribute, which its arrival sets with `attributes:`, and no order joins after the courier's last collection at 100 seconds:

=== "YAML"

    ```yaml title="examples/yaml/patience.yaml:pack"
    arrivals:
      orders: {dist: exponential, rate: 0.5, spawn: pack, attributes: {patience: 20}}
      walk_ins: {dist: exponential, rate: 0.1, spawn: pack, attributes: {patience: 10}}

    tasks:
      pack:
        service: packing
        resource: packers
        # Each order's own patience: it leaves the queue when it runs out.
        patience: attr.patience
        # No new orders once the courier has made its last collection.
        balk: t >= 100
        payload: {event_type: order_packed}
    ```

=== "Builder"

    ```python title="examples/builder/patience_example.py:pack"
    @app.task(
        service_id="packing",
        resource_id="packers",
        # Each order's own patience: it leaves the queue when it runs out
        patience="attr.patience",
        # No new orders once the courier has made its last collection
        balk="t >= 100",
    )
    def pack(task_id, context):
        """Packs one order."""
        return {"event_type": "order_packed"}
    ```

=== "Low-level"

    ```python title="examples/low-level/patience_example.py:pack"
    def pack(env, sampler, packers, key, patience, task_id):
        """Balks after 100 s; otherwise waits at most `patience` for a packer."""
        path_id = "Shop.service.packing"

        def publish(value):
            env.publish_event(key, value, task="pack", instance=task_id)

        flow = key.rsplit("-", 1)[0]
        env.stats.flow_started(flow)  # the order enters the shop
        # No new orders once the courier has made its last collection
        if env.now >= 100:
            publish({"path_id": path_id, "status": "balked"})
            env.stats.task_balked("pack", task_id)
            env.stats.flow_ended(flow)
            return
        publish({"path_id": path_id, "status": "queued"})
        env.stats.task_queued("pack", task_id)
        with packers.request() as req:
            # Each order's own patience: it leaves the queue when it runs out
            yield req | env.timeout(patience)
            if not req.triggered:
                publish({"path_id": path_id, "status": "reneged"})
                env.stats.task_reneged("pack", task_id)
                env.stats.flow_ended(flow)
                return
            publish({"path_id": path_id, "status": "started"})
            env.stats.task_started("pack", task_id)
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            publish({"event_type": "order_packed"})
        env.stats.task_finished("pack", task_id)
        env.stats.flow_finished(flow)
    ```

    A low-level process writes both by hand: an `if` before the request, and SimPy's `request | env.timeout(patience)`, which triggers when either event is processed. Leaving the `with` block takes a waiting request out of the queue at once, so it is never granted later.

`patience` takes seconds, a duration such as `30 s`, an expression such as `attr.patience`, or in the builder a function of the task id and the entity's attributes. `balk` takes any condition an expression can write, such as `level.stock == 0`.

### Ties Between a Grant and a Patience

A unit can come free at the same instant a waiting order's patience runs out. Plain SimPy does not settle this with a rule: the event scheduled first is processed first. desflow grants units with the same events SimPy uses, so it gives the same answer:

- a patience timeout scheduled after the holder's service end (the order arrived at 3 with a patience of 3, the holder started at 0 with 6 seconds of service) is processed after the release, and the order is served;
- a patience timeout scheduled before it (the order arrived at 0 with a patience of 6, just after the holder) is processed first, and the order reneges.

Each form runs both from one script, `check_reneging.py`, made of the blocks in this section and the next, in order. It starts with the imports of the standard library and a class that keeps every record the model publishes. The imports of desflow follow the class, in the line `from desflow import CapacityConfig, DynamicRealtimeEnvironment, DynamicResource, Sampler, SimParameter, SimulationContext`:

```python title="docs/snippets/guides/reneging_and_balking.py:capture"
import asyncio
import importlib.util
import queue
import shutil
import tempfile
from collections import Counter
from pathlib import Path


class Capture:
    """An egress that keeps every record it receives."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        try:
            while True:
                try:
                    self.records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.005)
        except asyncio.CancelledError:
            pass
```

The function `_outcome` tells what the order did, from the records of its `pack` task:

```python title="docs/snippets/guides/reneging_and_balking.py:outcome"
def _outcome(records: list) -> str:
    """What the order did: reneged if any record of its pack task says so."""
    statuses = [
        r["value"].get("status")
        for r in records
        if r["stream_type"] == "event" and r["task"] == "pack"
    ]
    return "reneged" if "reneged" in statuses else "served"
```

The YAML form replays the two arrivals at their times from CSV files. Save its blueprint as `reneging_tie.yaml` beside the script:

=== "YAML"

    ```yaml title="docs/snippets/yaml/reneging_tie.yaml:tie"
    simulation:
      sim_id: Shop
      factor: 0

    resources:
      packers: {current_cap: 1, max_cap: 1}

    services:
      hold: {dist: constant, value: 6}
      packing: {dist: constant, value: 1}

    arrivals:
      holders: {replay: {path: holders.csv}, spawn: holder}
      orders: {replay: {path: orders.csv}, spawn: pack}

    tasks:
      holder:
        service: hold
        resource: packers
        payload: {event_type: held}
      pack:
        service: packing
        resource: packers
        patience: attr.patience  # from the patience column of orders.csv
        payload: {event_type: order_packed}

    run:
      until: 8
    ```

    `tie_yaml` replays the holder at 0 and the order from CSV files beside the blueprint:

    ```python title="docs/snippets/guides/reneging_and_balking.py:tie-yaml"
    def tie_yaml(
        waiter_at: float,
        patience: float,
        blueprint: str = "reneging_tie.yaml",
    ) -> str:
        """The same, with the blueprint and the two arrivals replayed from CSV files."""
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            shutil.copy(blueprint, folder / "reneging_tie.yaml")
            (folder / "holders.csv").write_text("time\n0\n")
            (folder / "orders.csv").write_text(f"time,patience\n{waiter_at},{patience}\n")
            app = SimulationContext.from_yaml(folder / "reneging_tie.yaml")
            capture = Capture()
            app.add_egress(capture)
            app.run()  # until 8, from the blueprint
        return _outcome(capture.records)
    ```

=== "Builder"

    ```python title="docs/snippets/guides/reneging_and_balking.py:tie"
    def tie(waiter_at: float, patience: float) -> str:
        """The outcome of an order whose patience ends when the packer comes free."""
        app = SimulationContext("Shop", factor=0)
        app.add_resource("packers", current_cap=1, max_cap=1)
        app.add_service("hold", dist="constant", value=6)
        app.add_service("packing", dist="constant", value=1)
        capture = Capture()
        app.add_egress(capture)

        @app.task(service_id="hold", resource_id="packers")
        def holder(task_id):
            return {"event_type": "held"}

        @app.task(service_id="packing", resource_id="packers", patience=patience)
        def pack(task_id):
            return {"event_type": "order_packed"}

        def script(context):
            context.spawn(holder(0))  # takes the packer at 0 and keeps it until 6
            if waiter_at:
                yield context.env.timeout(waiter_at)
            context.spawn(pack(0))
            yield context.env.timeout(0)

        app.add_process(script)
        app.run(until=8)
        return _outcome(capture.records)
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/reneging_and_balking.py:tie-low-level"
    def tie_low_level(waiter_at: float, patience: float) -> str:
        """The same, with SimPy's request | timeout."""
        env = DynamicRealtimeEnvironment(factor=0)
        env.registry.register_sim_parameter(
            SimParameter(
                sim_id="Shop",
                resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
            )
        )
        capture = Capture()
        env.setup_egress([capture])
        packers = DynamicResource(env, "Shop", "packers")

        def holder():
            with packers.request() as unit:
                yield unit
                yield env.timeout(6)  # keeps the packer until 6

        def pack():
            if waiter_at:  # at 0, its patience starts before the holder's service
                yield env.timeout(waiter_at)
            with packers.request() as unit:
                yield unit | env.timeout(patience)
                if not unit.triggered:
                    env.publish_event("orders-1", {"status": "reneged"}, "pack", 0)
                    return
                yield env.timeout(1)
                env.publish_event("orders-1", {"event_type": "order_packed"}, "pack", 0)

        env.process(holder())
        env.process(pack())
        env.run(until=8)
        env.teardown()
        return _outcome(capture.records)
    ```

In each form, an order arriving at 3 with a patience of 3 is served, and one arriving at 0 with a patience of 6 reneges. A model that must not depend on this order can avoid exact ties, for example by drawing patience from a distribution.

---

## Check the Result

Every order that arrives ends in one of four places: packed, reneged, balked, or still in the shop when the run ends, waiting (`queued`) or being packed (`started`). The last event of each entity key says which. With statistics at `basic` or above, the `finished`, `reneged` and `balked` counts must match those events. This function checks them, and is the same for every form:

```python title="docs/snippets/guides/reneging_and_balking.py:balance"
def balance(records: list, stats: dict) -> Counter:
    """Every order that arrived, by what became of it."""
    last = {}
    for record in records:
        if record["stream_type"] == "event":
            value = record["value"]
            last[record["key"]] = value.get("status", value.get("event_type"))
    outcome = Counter(last.values())
    # queued or started: still in the shop when the run ended
    name = "Shop.stats.tasks.pack.{}".format
    assert outcome["order_packed"] == stats[name("finished")]
    assert outcome["reneged"] == stats.get(name("reneged"), 0)
    assert outcome["balked"] == stats.get(name("balked"), 0)
    return outcome
```

Each form runs the example below for 120 seconds with a capturing egress, and passes what it captured to `balance`. Run `desflow examples get patience` in the folder of the script first. The forms read the example from `patience/yaml/`, `patience/builder/` and `patience/low-level/`, and a function loads an example file without running it:

```python title="docs/snippets/guides/reneging_and_balking.py:loader"
def _example(path: str):
    """Loads an example script as a module, without running it."""
    spec = importlib.util.spec_from_file_location("patience_example", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
```


=== "YAML"

    ```python title="docs/snippets/guides/reneging_and_balking.py:yaml"
    def yaml_form(path: str = "patience/yaml/patience.yaml") -> Counter:
        app = SimulationContext.from_yaml(path)
        capture = Capture()
        app.add_egress(capture)
        app.run()  # until 120, from the blueprint
        return balance(capture.records, app.env.stats.readings())
    ```

=== "Builder"

    ```python title="docs/snippets/guides/reneging_and_balking.py:builder"
    def builder_form(path: str = "patience/builder/patience_example.py") -> Counter:
        app = _example(path).app
        capture = Capture()
        app.add_egress(capture)
        app.run(until=120)
        return balance(capture.records, app.env.stats.readings())
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/reneging_and_balking.py:low-level"
    def low_level_form(path: str = "patience/low-level/patience_example.py") -> Counter:
        example = _example(path)
        env = DynamicRealtimeEnvironment(factor=0)
        env.registry.register_sim_parameter(example.PARAMS)
        capture = Capture()
        env.setup_egress([capture])
        env.setup_stats("Shop", "basic", interval=30.0)
        packers = example.build(env)
        for process in example.processes(env, Sampler(seed=42), packers):
            env.process(process)
        env.run(until=120)
        env.teardown()  # publishes the last statistics and flushes the capture
        return balance(capture.records, env.stats.readings())
    ```

Last, the script prints the results of the ties and of the three forms. The example also prints its events, so keep the lines that start with `check:`:

```python title="docs/snippets/guides/reneging_and_balking.py:main"
if __name__ == "__main__":
    print("check: arrives at 3, patience 3:", tie(3, 3))
    print("check: arrives at 0, patience 6:", tie(0, 6))
    print("check: the same in YAML:", tie_yaml(3, 3), tie_yaml(0, 6))
    print("check: the same, low-level:", tie_low_level(3, 3), tie_low_level(0, 6))
    for form in (yaml_form, builder_form, low_level_form):
        print("check:", dict(form()))
```

```bash
# With uv
uv run --no-project --with desflow check_reneging.py | grep "^check:"
# With pip
pip install desflow
python check_reneging.py | grep "^check:"
```

```text
check: arrives at 3, patience 3: served
check: arrives at 0, patience 6: reneged
check: the same in YAML: served reneged
check: the same, low-level: served reneged
check: {'order_packed': 49, 'reneged': 7, 'started': 1, 'balked': 17}
check: {'order_packed': 49, 'reneged': 7, 'started': 1, 'balked': 17}
check: {'order_packed': 49, 'reneged': 7, 'started': 1, 'balked': 17}
```

Each form returns 49 packed, 7 reneged, 17 balked and 1 being packed, 74 orders in all. A mean wait counts only the orders that were served, so report the reneged and balked counts beside it.

---

## Example

The `patience` example is the model above: one packer, online orders with a patience of 20 seconds and walk-in orders with 10, and no new orders from 100 seconds. It needs no Docker service and ends on its own after 120 simulation seconds.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get patience --api yaml
        cd patience/yaml

        # 3. Run it
        desflow run patience.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get patience --api yaml
        cd patience/yaml

        # 3. Run it
        desflow run patience.yaml
        ```

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get patience --api builder
        cd patience/builder

        # 3. Run it
        uv run --no-project --with desflow patience_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get patience --api builder
        cd patience/builder

        # 3. Run it
        python patience_example.py
        ```

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get patience --api low-level
        cd patience/low-level

        # 3. Run it
        uv run --no-project --with desflow patience_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get patience --api low-level
        cd patience/low-level

        # 3. Run it
        python patience_example.py
        ```
