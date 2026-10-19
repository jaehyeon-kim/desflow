# Resources: `DynamicResource`

A resource is what an entity must hold while it works, and may have to wait for. This page shows how to declare one, how its queue is ordered, how it hands out units, and what happens when its capacity changes during a run.

______________________________________________________________________

## What It Is

A resource is a number of identical units: packers, packing benches, checkout lanes, loading bays. A task asks for one unit, waits in the resource's queue while every unit is busy, holds the unit while it works, and gives it back. How many units there are is the resource's capacity, and it can change while the run goes on, as when packers go on a break.

______________________________________________________________________

## How to Use It

### Declare a Resource

A resource has a `current_cap`, the number of units now, and a `max_cap`, the most a live change can set. `run()` builds a `DynamicResource` for each one the context declares; in the low-level API you build it after registering the parameters:

```yaml
resources:
  # fifo (the default), lifo or priority; priority serves the lowest number first,
  # and equal priorities first in, first out
  packers: {current_cap: 1, max_cap: 4, queue: priority}
```

```python
app = SimulationContext(sim_id="Shop", factor=0.0)
# fifo (the default), lifo or priority
app.add_resource("packers", current_cap=1, max_cap=4, queue="priority")
```

```python
packers = DynamicResource(env, "Shop", "packers")  # capacity from the registry: 2


def order(env, order_id):
    with packers.request() as request:
        yield request  # waits until a unit is free
        yield env.timeout(10.0)  # holds the unit for 10 s
    # leaving the with block releases the unit
```

`packers.request()` is an event that happens when a unit is granted, and leaving the `with` block gives the unit back. A request whose `with` block is left before it is granted, for example after a timeout, is cancelled and never takes a unit.

### Queue Order

`queue` sets the order in which a resource serves the requests waiting for it:

- `fifo`, the default: the oldest first;
- `lifo`: the newest first;
- `priority`: the lowest priority number first, and the oldest first among equal priorities.

Four requests that join while the only unit is busy are served in these orders:

```python
def served_order(queue):
    """Four requests join while the only bench is busy; returns who is served when."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            resources={"benches": CapacityConfig(current_cap=1, max_cap=1)},
        )
    )
    bench = DynamicResource(env, "Shop", "benches", queue=queue)
    served = []

    def use(name, priority, after=0.0):
        yield env.timeout(after)
        # The priority counts only under the priority order
        with bench.request(priority=priority) as request:
            yield request
            served.append(name)
            yield env.timeout(1.0)

    env.process(use("first", 1))  # takes the bench at once
    for name, priority in [
        ("orders-1", 1),
        ("express-1", 0),
        ("orders-2", 1),
        ("express-2", 0),
    ]:
        env.process(use(name, priority, after=0.1))  # all join while it is busy
    env.run(until=10)
    return served[1:]


for queue in ("fifo", "lifo", "priority"):
    print(f"{queue:<8} {', '.join(served_order(queue))}")
```

```text
fifo     orders-1, express-1, orders-2, express-2
lifo     express-2, orders-2, express-1, orders-1
priority express-1, express-2, orders-1, orders-2
capacity 2, in use 4
```

The last line is the capacity drop that Under Live Changes describes, below. The same four requests, drawn:

### Task Priority

Under `queue: priority`, each task gives its requests a priority: a number, the same for every run of the task; an expression, read when the task joins the queue, such as `attr.tier` for the entity's attribute `tier`; or, in the builder, a function of the task id and the entity's attributes. The attributes exist before the request queues, and the task's own payload does not, so a priority cannot read it; an expression can read the previous task's payload as `payload.<field>`. The lowest priority is served first, and equal priorities first in, first out, which is the order of SimPy's `PriorityResource`: it ranks a request by `(priority, time, not preempt)`, so among equal priorities and times a request that may preempt goes before one with `preempt: false`. The tests check it, ties included, in all three APIs ([SimPy Compatibility](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/index.md)).

```yaml
tasks:
  pack_order:
    service: packing
    resource: packers
    # A number, or an expression read when the order joins the queue
    priority: attr.tier
    payload: {event_type: order_packed}
```

```python
app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=4, queue="priority")
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5, attributes={"tier": 2})
)


# Each order's own priority, from its attributes, when it joins the queue;
# a number or an expression such as "attr.tier" works too
@app.task(
    service_id="packing",
    resource_id="packers",
    priority=lambda task_id, attributes: attributes["tier"],
)
def pack_order(task_id: int):
    return {"event_type": "order_packed"}
```

A low-level process passes the priority to the request: `packers.request(priority=entity.attributes["tier"])`, as the queue-order example above does. A resource whose queue is `fifo` or `lifo` ignores it.

A `priority` on a task whose resource does not have `queue: priority` is refused: when the blueprint loads, or when `run()` starts. [Priorities](https://jaehyeon.me/desflow/latest/tutorials/priorities/index.md) shows what priority does to waits.

### Preemption

On a resource with `preempt: true`, which needs `queue: priority`, a request that finds every unit busy takes a unit from a task of a higher priority value, as SimPy's `PreemptiveResource` does. The task that loses the unit is chosen by SimPy's rule: the user whose `(priority, request time, may not preempt)` sorts last, and only when that sorts after the new request, so equal priorities do not preempt; the one exception is a user whose request was made at the same moment as the new one with `preempt: false`, which sorts after a request that may preempt. The task publishes `preempted` with the resource's name and follows its `on_interrupt`, as an interrupt would ([Interruptions](https://jaehyeon.me/desflow/latest/tutorials/interruptions/index.md)); a task with `preempt: false` never takes a unit this way. The `interruptions` example lets express orders preempt regular ones:

```yaml
# A lower priority value takes the packer from a higher one.
packers: {current_cap: 1, max_cap: 4, queue: priority, preempt: true}
```

```yaml
tasks:
  pack:
    service: packing
    resource: packers
    priority: attr.priority
    # A preempted or interrupted order queues again and serves what it had left.
    on_interrupt: resume
    payload: {event_type: order_packed}
    attributes: [priority]
```

```python
# A lower priority value takes the packer from a higher one
.add_resource("packers", current_cap=1, max_cap=4, queue="priority", preempt=True)
```

```python
@app.task(
    service_id="packing",
    resource_id="packers",
    priority="attr.priority",
    # A preempted or interrupted order queues again and serves what it had left
    on_interrupt="resume",
)
def pack(task_id, context, entity):
    return {"event_type": "order_packed", "priority": entity.attributes["priority"]}
```

```python
def pack(env, sampler, shop, key, priority, task_id):
    """Packs one order; a preemption or an interrupt sends it back to the queue
    with the packing time it had left."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    env.stats.flow_started(key.rsplit("-", 1)[0])  # the order enters the shop
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    remaining, started = None, False
    shop["packing"].append(env.active_process)
    while True:
        request = shop["packers"].request(priority=priority)
        since = None
        try:
            yield request
            if not started:
                started = True
                publish({"path_id": path_id, "status": "started"})
                env.stats.task_started("pack", task_id)
            else:
                publish({"path_id": path_id, "status": "resumed"})
                env.stats.task_resumed("pack", task_id)
            if remaining is None:
                remaining = sampler.sample(env.registry.get_config(path_id))
            since = env.now
            yield env.timeout(remaining)
            payload = {"event_type": "order_packed", "priority": priority}
            publish(payload)
            env.stats.task_finished("pack", task_id)
            shop["packers"].release(request)
            break
        except simpy.Interrupt as interrupt:
            request.cancel()
            shop["packers"].release(request)  # nothing to give back if preempted
            if since is not None:
                remaining -= env.now - since
            if isinstance(interrupt.cause, Preempted):
                value = {
                    "path_id": path_id,
                    "status": "preempted",
                    "resource": "packers",
                }
                publish(value)
                env.stats.task_interrupted("pack", task_id, preempted=True)
            else:
                value = {"path_id": path_id, "status": "interrupted"}
                publish({**value, "cause": interrupt.cause})
                env.stats.task_interrupted("pack", task_id)
    shop["packing"].remove(env.active_process)
    env.stats.flow_finished(key.rsplit("-", 1)[0])
```

`DynamicResource(env, "Shop", "packers", queue="priority", preempt=True)` builds the resource, and `request(priority, preempt=True)` asks for a unit; the process that loses its unit receives `simpy.Interrupt` with a SimPy `Preempted` cause, whose `resource`, `by` and `usage_since` say what happened.

### Cancel and Release

A task that leaves a queue cancels its request, so the request is never granted later: patience that runs out, an interrupt, and an `on_interrupt: abandon` all do. Once a unit is granted, the task gives it back when it finishes, when it is interrupted, and when it is preempted (the preempting task has it then). In the low-level API these are SimPy's calls:

```python
held = packers.request()  # granted at once: the packer was free
waiting = packers.request()  # waits behind it

waiting.cancel()  # leaves the queue at once; it is never granted
held.cancel()  # granted already, so this releases nothing, as in SimPy
assert packers.in_use == 1 and packers.queue.items == []

packers.release(held)  # gives the packer back
packers.release(held)  # a second release does nothing
```

`request.cancel()` takes a waiting request out of the queue and does nothing to a granted one, and `packers.release(request)` gives a granted unit back once; releasing it again, or releasing a request that was never granted, gives nothing back. A `with` block does both on its way out, as in SimPy.

### Several Resources

A task can hold several resources at once: `resource: [picker, bench]` asks for a picker, then, while it holds the picker, for the bench. The task is served once it holds all of them, and it releases all of them when it finishes. While it waits for the bench it holds the picker: the holding is partial, and nothing is granted to several resources at once. `patience` covers the whole acquisition, and an interrupt gives back every unit held.

A task can also hand a unit to the next task of its flow with `keep`: the next task holds it from the start and gives it back once it publishes `started`, so an order keeps the picker until it is on the packing bench. The `competing-resources` example does both:

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
    env.process(pack(shop, key, picker))


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

**One order of acquisition.** Two tasks that acquire the same two resources in opposite orders can each hold one and wait for ever for the other. desflow refuses such a model when it is built, naming both tasks: every task, `keep` handoffs included, must acquire shared resources in one order. [Competing Resources](https://jaehyeon.me/desflow/latest/tutorials/competing-resources/index.md) shows what a partial holding costs.

### Breakdowns

A `breakdown` makes a resource fail at random and be repaired. `time_to_failure` is the time from the start, or from a repair, to the next failure, on calendar time, busy or idle; `repair` is how long a repair takes. While the resource is down its effective capacity is 0 and no request is granted:

```yaml
resources:
  station:
    current_cap: 2
    max_cap: 3
    breakdown:
      # Calendar time from a repair, or the start, to the next failure.
      time_to_failure: {dist: exponential, rate: 0.0167}
      repair: {dist: uniform, low: 5 s, high: 15 s}
      # The orders being packed are interrupted, and follow their on_interrupt.
      work: interrupt
```

```python
.add_resource("station", current_cap=2, max_cap=3)
.add_breakdown(
    "station",
    # Calendar time from a repair, or the start, to the next failure
    time_to_failure={"dist": "exponential", "rate": 0.0167},
    repair={"dist": "uniform", "low": 5, "high": 15},
    # The orders being packed are interrupted, and follow their on_interrupt
    work="interrupt",
)
```

```python
breakdowns={
    "station": BreakdownConfig(
        # Calendar time from a repair, or the start, to the next failure
        time_to_failure=DistributionConfig(dist="exponential", rate=0.0167),
        repair=DistributionConfig(dist="uniform", low=5, high=15),
        # The orders being packed are interrupted
        work="interrupt",
    )
},
```

Start `run_breakdowns(env, station, sampler, work="interrupt")` as a SimPy process beside the arrivals. `station.fail()` and `station.repair()` put a resource down and back by hand.

`work` says what happens to the tasks being served: `interrupt`, the default, interrupts each with the cause `breakdown`, and it follows its `on_interrupt`; `finish` lets them finish. Each failure and repair publishes an event keyed by the resource's name, with `status` `down` or `repaired` and `reason: failure`. [Breakdowns](https://jaehyeon.me/desflow/latest/guides/breakdowns/index.md) checks the downtime of the example, and [Interruptions](https://jaehyeon.me/desflow/latest/tutorials/interruptions/index.md) explains `on_interrupt`.

### Registry Paths and Readings

Each resource has two registry paths, `<sim_id>.resources.<name>.current_cap` and `.max_cap`, which a scenario, an ingress message or a process can change; [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-parameters/index.md) explains how. A resource that breaks down adds `<sim_id>.breakdown.<name>.time_to_failure` and `.repair`, its two distributions. With statistics at the `minimal` level or above, each resource publishes four readings, such as `Shop.stats.resources.packers.utilization`: `capacity`, `in_use`, `queue_length` and `utilization`, the busy units as a percentage of the capacity; one that breaks down adds `down`, 1 while it is down. [Statistics](https://jaehyeon.me/desflow/latest/user-guide/statistics/index.md) lists what the other levels add, among them `downtime` and `failures`.

### Example: Interruptions

The `interruptions` example: express orders preempt regular ones, and a fire drill at 60 seconds interrupts the order being packed; each resumes with the packing time it had left. It needs no Docker service and ends on its own after 120 simulation seconds.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get interruptions --api yaml
cd interruptions/yaml

# 3. Run it
desflow run interruptions.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get interruptions --api yaml
cd interruptions/yaml

# 3. Run it
desflow run interruptions.yaml
```

The whole file is [`examples/yaml/interruptions.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/interruptions.yaml).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get interruptions --api builder
cd interruptions/builder

# 3. Run it
uv run --no-project --with desflow interruptions_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get interruptions --api builder
cd interruptions/builder

# 3. Run it
python interruptions_example.py
```

The whole file is [`examples/builder/interruptions_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/interruptions_example.py).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get interruptions --api low-level
cd interruptions/low-level

# 3. Run it
uv run --no-project --with desflow interruptions_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get interruptions --api low-level
cd interruptions/low-level

# 3. Run it
python interruptions_example.py
```

The whole file is [`examples/low-level/interruptions_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/interruptions_example.py).

### Example: Competing Resources

The `competing-resources` example: orders keep the picker until they are on the bench, and gifts hold the picker and the bench together. It needs no Docker service and ends on its own after 120 simulation seconds.

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

The whole file is [`examples/yaml/competing_resources.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/competing_resources.yaml).

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

The whole file is [`examples/builder/competing_resources_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/competing_resources_example.py).

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

The whole file is [`examples/low-level/competing_resources_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/competing_resources_example.py).

______________________________________________________________________

## How It Works

A `DynamicResource` grants units the way SimPy's `Resource` does, with two lists: `queue`, the waiting requests ranked by the queue order and a sequence number, so requests that rank the same are served in the order they arrived, and `users`, the granted requests. A request is granted when it is made if a unit is free and nothing waits ahead of it. Otherwise it waits, and each release grants the head of the queue when its release event is processed, one event later, as SimPy's `Release` does:

Because each step is the one SimPy takes, two events at the same simulation time are granted in SimPy's order: a waiting order whose patience runs out at the instant a unit is released reneges or is served exactly as it would in plain SimPy. `in_use` counts `users`: it goes up when a request is granted and down when a unit is released.

A waiting request leaves the queue at once when it is cancelled, which a `with` block does when it is left before the grant; `request.cancel()` does the same by hand. `packers.release(request)` gives a granted unit back. Cancelling a granted request releases nothing, and releasing a request twice, or one never granted, gives nothing back, as in SimPy. Every release still tries the head of the queue when its release event is processed, as SimPy's does: when a request that reneged was the head, the request behind it may then be granted or preempt a user.

**SimPy and desflow.** SimPy's `Resource` serves requests in the order they arrive, and `PriorityResource` by priority. Both take their capacity when they are created and keep it: `capacity` has no setter, so assigning a new one raises `AttributeError` (SimPy 4.1). desflow builds its own resource so the capacity can follow the registry and the queue order can be chosen; the `request()` call and the `with` block are SimPy's, so code written for SimPy works unchanged. [SimPy and desflow](https://jaehyeon.me/desflow/latest/des/simpy-and-desflow/index.md) compares the two.

______________________________________________________________________

## Under Live Changes

A change to `current_cap` takes effect at once, without stopping any work:

- **Growing** grants waiting requests at once, up to the new capacity.
- **Shrinking** stops nothing. The units still busy above the new capacity are owed: each one given back brings the busy count down, and no waiting task starts until the busy count is within the new capacity.
- **Growing again** before the owed units are given back cancels them first, so only the rest of the rise grants waiting requests.
- A `current_cap` outside 0 to `max_cap` is clamped to that range, with a warning.
- **Lowering `max_cap`** lowers the capacity with it. A `current_cap` above the new `max_cap` is stored as `max_cap`, with a warning.
- **During a repair** the capacity is 0 and `current_cap` keeps the planned value. A change to it sets the capacity the resource returns to and does not end the repair.

The example below takes three of five packers off the floor while four orders are being packed: the capacity is 2 at once, and `in_use` stays 4 until two orders finish. The animation steps through it:

```python
def capacity_drop(env):
    """Takes 3 of 5 packers off the floor at t=5 s. Orders being packed are not stopped."""
    env.registry.update("Shop.resources.packers.current_cap", 5)
    yield env.timeout(5.0)
    env.registry.update("Shop.resources.packers.current_cap", 2)
    yield env.timeout(0.1)  # the resource applies the change in its own process
    print(f"capacity {packers.capacity}, in use {packers.in_use}")
```

Until the owed units finish, `in_use` is above `capacity`, and the `utilization` reading is above 100.

______________________________________________________________________

## Stability and Limits

- **Utilisation decides waits.** A resource whose load is at or above its capacity has a queue that grows for as long as the run lasts; [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md) shows how to work it out.
- **Priority needs `queue: priority`.** A task priority on a resource of another order is a load error, while a low-level request's priority is ignored under `fifo` and `lifo`.
- **Priority can starve.** Under `priority`, a request with a high number is served only when no lower number is waiting; if the low numbers alone keep the resource busy, it is never served.
- **One order of acquisition.** A model whose tasks acquire two resources in opposite orders is refused; [Several Resources](#several-resources) explains why.
- **A shrink never stops work.** Lowering `current_cap` lets busy units finish. Work in progress stops only for an interrupt, a preemption or a breakdown: [Interruptions](https://jaehyeon.me/desflow/latest/tutorials/interruptions/index.md), and [Breakdowns](https://jaehyeon.me/desflow/latest/guides/breakdowns/index.md) for a machine that fails.
- **A preempted task queues again.** With `resume` or `restart` it joins the back of its priority among the waiting requests, as a new SimPy request does; the time it waited before counts in its wait.

Amounts and items that tasks take and add, rather than hold, are [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/index.md).
