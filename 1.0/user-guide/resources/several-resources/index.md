# Several Resources

A task can hold more than one [resource](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/index.md) at once. This page shows how.

______________________________________________________________________

## What It Is

Some work needs more than one resource at a time: an order needs a picker and a bench, and a patient needs a doctor and a room. A task can hold several resources at once, or keep a unit and hand it to the next task of its flow.

______________________________________________________________________

## How to Use It

A task can hold several resources at once: `resource: [picker, bench]` asks for a picker, then, while it holds the picker, for the bench. The task is served once it holds all of them, and it releases all of them when it finishes. While it waits for the bench, it holds the picker. The units are granted one at a time, never together, so a task can hold some of its resources while it waits for the rest. `patience` covers the whole acquisition. A plain interrupt keeps the units already held and asks again only for the one it waits for, and `on_interrupt: abandon` gives every unit back.

A task can also hand a unit to the next task of its flow with `keep`: the next task holds it from the start and gives it back once it publishes `started`. An order can therefore keep the picker until it is on the packing bench, as [Hand Off to a Resource](https://jaehyeon.me/desflow/latest/how-to/modelling/multi-resource-handoffs/index.md) shows step by step. A task that reneges, whose wait fails with no `on_fail`, or that has no next task gives it back when it ends. If the unit is preempted while the next task holds it, that task is interrupted and follows its `on_interrupt`. The unit a task keeps stays with the entity until that next task starts. A child flow started by `wait: {task: ...}` gives back only the units its own tasks asked for. The `competing-resources` example does both:

```yaml
tasks:
  pick:
    service: picking
    resource: picker
    # The picker stays with the order until pack has started.
    keep: [picker]
    payload: {event_type: order_picked}
    next: pack
  pack:
    service: packing
    resource: bench
    payload: {event_type: order_packed}
  wrap:
    service: wrapping
    # Holds the picker, then the bench too: acquired in this order.
    resource: [picker, bench]
    payload: {event_type: gift_wrapped}
```

```python
# The picker stays with the order until pack has started
@app.task(service_id="picking", resource_id="picker", keep=["picker"], next="pack")
def pick(task_id, context):
    return {"event_type": "order_picked"}


@app.task(service_id="packing", resource_id="bench")
def pack(task_id, context):
    return {"event_type": "order_packed"}


# Holds the picker, then the bench too: acquired in this order
@app.task(service_id="wrapping", resource_id=["picker", "bench"])
def wrap(task_id, context):
    return {"event_type": "gift_wrapped"}
```

```python
def pick(shop, key):
    """Picks an order and keeps the picker for pack, which gives it back."""
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "pick", "picking")
    stats.flow_started("orders")  # the order enters the shop
    status("queued")
    stats.task_queued("pick", task_id)
    picker = shop.picker.request()
    yield picker
    status("started")
    stats.task_started("pick", task_id)
    yield env.timeout(duration())
    publish({"event_type": "order_picked"})
    stats.task_finished("pick", task_id)
    yield from pack(shop, key, picker)


def pack(shop, key, picker):
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "pack", "packing")
    status("queued")
    stats.task_queued("pack", task_id)
    with shop.bench.request() as bench:
        yield bench
        status("started")
        stats.task_started("pack", task_id)
        shop.picker.release(picker)  # the order is on the bench
        yield env.timeout(duration())
        publish({"event_type": "order_packed"})
        stats.task_finished("pack", task_id)
    stats.flow_finished("orders")


def wrap(shop, key):
    """Holds the picker, then the bench too: acquired in this order."""
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "wrap", "wrapping")
    stats.flow_started("gifts")
    status("queued")
    stats.task_queued("wrap", task_id)
    with shop.picker.request() as picker:
        yield picker
        with shop.bench.request() as bench:
            yield bench
            status("started")
            stats.task_started("wrap", task_id)
            yield env.timeout(duration())
            publish({"event_type": "gift_wrapped"})
            stats.task_finished("wrap", task_id)
    stats.flow_finished("gifts")
```

In the low-level API several resources are nested requests, and a handoff is a request released by the next process.

### Hold a Unit Between Steps

`seize` asks for a unit at the start of a task and keeps it until a `release`, in this task or a later one of the flow, so the unit is held across steps that name no `resource`. `seize` is a resource name, or `{resource, priority, preempt, patience}`. `release` frees the oldest unit of a resource that the flow holds from a `seize` or an `open`, and does nothing when it holds none. A unit still held when the flow ends stays held, as in SimPy, so a flow writes `release` before it ends. A unit lost to a preemption is asked for again under `resume` and `restart`, and is gone under `abandon`. `release` also takes `{handle: name}` for one opened request and `{resource, task, where}` for the units that other flows of a task hold. A `priority` on a `seize` needs the resource to set `queue: priority`, as `picker` does here. The `clear` task below frees the picker from every rush order that holds it.

```yaml
tasks:
  pick:
    service: picking
    # Asks for the picker now, with priority 1 and without taking the unit from
    # another order, and keeps it across the next task
    seize: {resource: picker, priority: 1, preempt: false}
    payload: {event_type: order_picked}
    next: pack
  pack:
    service: packing
    resource: bench
    # Gives the picker back once the order is on the bench
    release: picker
    payload: {event_type: order_packed}
  # Frees the picker from every rush order that still holds it
  clear:
    release: {resource: picker, task: pick, where: attr.rush}
    payload: {event_type: picker_cleared}
```

`@app.task(seize="picker")` and `@app.task(release="picker")` take the same values.

The process holds the request itself: `picker = shop.picker.request()` and `yield picker`, then `shop.picker.release(picker)` when the order is on the bench.

______________________________________________________________________

## How It Works

**One order of acquisition.** Two tasks that acquire the same two resources in opposite orders can each hold one and wait for ever for the other. desflow loads such a model and logs one WARNING, from the logger `desflow.builder.context`, that names both tasks. A unit held with `seize` counts as held. Every task should acquire shared resources in one order. The rule also covers `keep` handoffs and the `request` branches of a wait, and a `keep` for a request branch of a wait is still refused when the model is built ([Wait for a Unit, an Item or Room](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-a-unit-an-item-or-room)). [Competing Resources](https://jaehyeon.me/desflow/latest/learn/shop-logic/competing-resources/index.md) shows what a partial holding costs.

______________________________________________________________________

## Example: Competing Resources

In the `competing-resources` example, orders keep the picker until they are on the bench, and gifts hold the picker and the bench together. It needs no Docker service and ends on its own after 120 simulation seconds.

The tutorial page [Competing Resources](https://jaehyeon.me/desflow/latest/learn/shop-logic/competing-resources/index.md) teaches these features step by step.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get competing-resources --api yaml
cd competing-resources/yaml

# 3. Run it
desflow run competing_resources.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get competing-resources --api yaml
cd competing-resources/yaml

# 3. Run it
desflow run competing_resources.yaml
```

The whole file is [`examples/yaml/competing_resources.yaml`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0/examples/yaml/competing_resources.yaml).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get competing-resources --api builder
cd competing-resources/builder

# 3. Run it
uv run --no-project --with desflow competing_resources_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get competing-resources --api builder
cd competing-resources/builder

# 3. Run it
python competing_resources_example.py
```

The whole file is [`examples/builder/competing_resources_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0/examples/builder/competing_resources_example.py).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get competing-resources --api low-level
cd competing-resources/low-level

# 3. Run it
uv run --no-project --with desflow competing_resources_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get competing-resources --api low-level
cd competing-resources/low-level

# 3. Run it
python competing_resources_example.py
```

The whole file is [`examples/low-level/competing_resources_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0/examples/low-level/competing_resources_example.py).
