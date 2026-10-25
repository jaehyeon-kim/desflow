# Add Reneging and Balking

Give a task a time limit on its wait, a condition that stops it joining at all, or both, and account for every entity afterwards. It builds on [Patience](https://jaehyeon.me/desflow/latest/tutorials/patience/index.md), which explains both ideas, and on [Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/#how-it-works), which explains how a request is granted.

______________________________________________________________________

## Leave After a Wait and Refuse to Join

A task's `patience` is how long it may wait before it leaves the queue and publishes `reneged`. The waiting covers its `wait`, then its resource and then its `get`. Patience counts waiting time only and pauses while the task is served. A task that is preempted and queues again therefore goes on with the time it has left. A get that gives up is cancelled, so it takes nothing later. A task's `balk` is a condition checked before it joins the queue. When the condition holds, the task publishes `balked` and never requests the resource. Here each order brings its own patience as an attribute, which its arrival sets with `attributes:`, and no order joins after the courier's last collection at 100 seconds:

```yaml
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

```python
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

```python
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

______________________________________________________________________

## Ties Between a Grant and a Patience

A unit can come free at the same instant a waiting order's patience runs out. Plain SimPy does not settle this with a rule: the event scheduled first is processed first. desflow grants units with the same events SimPy uses, so it gives the same answer:

- a patience timeout scheduled after the holder's service end (the order arrived at 3 with a patience of 3, the holder started at 0 with 6 seconds of service) is processed after the release, and the order is served;
- a patience timeout scheduled before it (the order arrived at 0 with a patience of 6, just after the holder) is processed first, and the order reneges.

This function runs both:

```python
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
    statuses = [
        r["value"].get("status")
        for r in capture.records
        if r["stream_type"] == "event" and r["task"] == "pack"
    ]
    return "reneged" if "reneged" in statuses else "served"
```

`tie(3, 3)` returns `served` and `tie(0, 6)` returns `reneged`. A model that must not depend on this order can avoid exact ties, for example by drawing patience from a distribution.

______________________________________________________________________

## Check the Balance

Every order that arrives ends in one of four places: packed, reneged, balked, or still in the shop when the run ends, waiting (`queued`) or being packed (`started`). The last event of each entity key says which. With statistics at `basic` or above, the `finished`, `reneged` and `balked` counts must match those events:

```python
def balance(path: str) -> Counter:
    """Every order that arrived, by what became of it."""
    app = SimulationContext.from_yaml(path)
    capture = Capture()
    app.add_egress(capture)
    app.run()
    last = {}
    for record in capture.records:
        if record["stream_type"] == "event":
            value = record["value"]
            last[record["key"]] = value.get("status", value.get("event_type"))
    outcome = Counter(last.values())
    # queued or started: still in the shop when the run ended
    stats = app.env.stats.readings()
    name = "Shop.stats.tasks.pack.{}".format
    assert outcome["order_packed"] == stats[name("finished")]
    assert outcome["reneged"] == stats.get(name("reneged"), 0)
    assert outcome["balked"] == stats.get(name("balked"), 0)
    return outcome
```

For one 120-second run of the example below it returns 49 packed, 7 reneged, 17 balked and 1 being packed, 74 orders in all. A mean wait counts only the orders that were served, so report the reneged and balked counts beside it.

______________________________________________________________________

## Example

The `patience` example is the model above: one packer, online orders with a patience of 20 seconds and walk-in orders with 10, and no new orders from 100 seconds. It needs no Docker service and ends on its own after 120 simulation seconds.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get patience --api yaml
cd patience/yaml

# 3. Run it
desflow run patience.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get patience --api yaml
cd patience/yaml

# 3. Run it
desflow run patience.yaml
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get patience --api builder
cd patience/builder

# 3. Run it
uv run --no-project --with desflow patience_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get patience --api builder
cd patience/builder

# 3. Run it
python patience_example.py
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get patience --api low-level
cd patience/low-level

# 3. Run it
uv run --no-project --with desflow patience_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get patience --api low-level
cd patience/low-level

# 3. Run it
python patience_example.py
```
