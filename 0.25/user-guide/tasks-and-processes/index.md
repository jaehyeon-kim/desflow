# Tasks and Processes: `SimulationContext`

A **task** is what one entity does: an order being packed, a return being logged. A **process** is anything else that runs over time: packers going on a break every hour, a speed that drifts, a shift plan. This page shows how to write a task, what `run()` does with tasks, and what a task does step by step. [Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/index.md) shows how to write a process.

______________________________________________________________________

## What It Is

Both are SimPy generators: functions that `yield` to wait. The builder and YAML write the common ones for you, and the low-level API writes them by hand. A task waits for what it needs, does its work for a sampled time, publishes an event at each step, and may lead to the next task. A process can do anything a generator can: wait, change values in the registry, publish records or start tasks.

______________________________________________________________________

## Pages in This Section

- [Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/index.md): run a generator alongside the tasks, with an example of processes that interact.
- [Signals and Waiting](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/index.md): wait for signals, conditions and child flows, start flows, and run actions when an event happens.
- [Failures](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/failures/index.md): when a task fails, what a failure does, and what happens to one nobody handles.

______________________________________________________________________

## How to Use It

### A Task

A task with a service and a resource asks for a unit of the resource, holds it for a time drawn from the service, and releases it. A task with neither takes no time and holds nothing: it publishes its payload at once. Such a task suits a step that only records an event, such as logging a return.

```yaml
tasks:
  pack_order:
    service: packing  # how long the work takes
    resource: packers  # what it holds while it works
    payload: {event_type: order_packed}  # the finished event's value
    next: print_label  # the task that follows; Task Flow has the other forms
  return_logged:
    # No service and no resource: publishes its payload at once
    payload: {event_type: return_logged}
```

`payload` is a fixed mapping. A payload that changes per entity is a Python function, called as `function(task_id, context)`:

```yaml
# also under tasks: a payload computed by a Python function
pack_with_delivery:
  service: packing
  resource: packers
  # Called as choose_delivery(task_id, context) when the order gets a packer
  payload: !python shop_logic.choose_delivery
```

```python
@app.task(service_id="packing", resource_id="packers", next="print_label")
def pack_order(task_id: int):
    # Runs when the order gets a packer, before the packing time.
    # The returned dict is the finished event's value.
    return {"event_type": "order_packed"}


@app.task()  # no service and no resource: publishes at once
def return_logged(task_id: int):
    return {"event_type": "return_logged"}
```

The function is called with the task id and whatever the caller passes. It also receives `entity`, `upstream` and `item` by keyword when it declares a parameter of that name: the entity, what the previous task handed on, and what a store `get` took. A function that declares none of them is called without them.

```python
def pack_order_process(env, sampler, packers, key, task_id, label_ids):
    """What @app.task does, by hand."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        payload = {"event_type": "order_packed"}
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish(payload)
    # next: print_label, a new instance for the same order
    env.process(print_label_process(env, sampler, key, next(label_ids)))
```

This is ordinary SimPy: `packers.request()` waits for a unit, `env.timeout` waits the service time, and leaving the `with` block releases the unit. The `env.publish_event` call at each step produces the records.

A task can also take from or add to a store or container (`get`, `put`), rank its requests (`priority`), and name the task that follows it (`next`). [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/index.md), [Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/index.md) and [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/task-flow/index.md) describe each.

### A Timed Step

A task with a `service` and no `resource` is a **timed step**: work that takes time but needs nothing that can be busy, such as printing a label or waiting for a supplier. It publishes `started` at once, with no `queued` event, waits the sampled time, and publishes its finished event. Its wait is 0. It is SimPy's `env.timeout`: the tests check that it ends at the same simulation time as a plain SimPy timeout in all three APIs ([SimPy Compatibility](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/index.md)).

```yaml
print_label:
  # A service and no resource: a timed step that starts at once
  service: labelling
  payload: {event_type: label_printed}
```

```python
@app.task(service_id="labelling")  # a service and no resource: starts at once
def print_label(task_id: int):
    return {"event_type": "label_printed"}
```

```python
def print_label_process(env, sampler, key, task_id):
    """A timed step: started at once, with no queued event."""
    path_id = "Shop.service.labelling"

    def publish(value):
        env.publish_event(key, value, task="print_label", instance=task_id)

    publish({"path_id": path_id, "status": "started"})
    yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    publish({"event_type": "label_printed"})
```

### Arrivals Start Tasks

An arrival starts one task per arrival. The loop draws a gap from the arrival's distribution, waits for that duration, starts a task for the new entity, and repeats. The task runs on its own while the loop waits for the next arrival.

```yaml
arrivals:
  # Each arrival spawns one pack_order, and one return_logged
  # An order every 5 s on average (rate 0.2)
  orders: {dist: exponential, rate: 0.2, spawn: pack_order}
  # A return every 10 s on average (rate 0.1)
  returns: {dist: exponential, rate: 0.1, spawn: return_logged}
```

```python
# Each arrival starts one pack_order
# An order every 5 s on average (rate 0.2)
app.add_arrival("orders", dist="exponential", rate=0.2, spawn="pack_order")
```

`spawn=` starts the named task for each arrival's entity, whose key is the arrival name and a counter: `orders-1`, `orders-2`. The record's `instance` comes from the task's one counter, which `next` and other arrivals share, so no two runs of the task share an id. A `spawn` that names a task never added is refused when `run()` checks the model.

`@app.arrival_loop` runs a generator of your own. The loop below starts one `return_logged` for each return, which is what `spawn="return_logged"` does in one argument, as the YAML tab writes it. A loop is needed only when it does more than start one task, such as choosing the task from the entity's attributes. `context.wait_for_arrival` returns the new `Entity`. Pass it to the task with `entity=`. A task called without one starts an entity of its own, keyed by the task's name and a counter, such as `return_logged-1`. The `task_id` becomes the record's `instance`. Take it from `context.next_instance`, the task's one counter.

```python
# A return every 10 s on average (rate 0.1)
app.add_arrival("returns", dist="exponential", rate=0.1)


@app.arrival_loop("returns")
def returns(context: SimulationContext):
    while True:
        # Draws the next gap, waits for it, and returns the new entity, keyed returns-1, ...
        parcel = yield context.wait_for_arrival("returns")
        task_id = context.next_instance("return_logged")  # the task's one counter
        context.spawn(return_logged(task_id, entity=parcel))  # the loop goes on at once
```

```python
def orders_process(env, sampler, packers):
    """What an arrival with spawn does, by hand."""
    gap = env.registry.get_config("Shop.arrival.orders")
    label_ids = itertools.count()
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        key = f"orders-{task_id + 1}"
        env.process(pack_order_process(env, sampler, packers, key, task_id, label_ids))
        task_id += 1
```

In the low-level API every loop is started once, before the run:

```python
env.process(orders_process(env, sampler, packers))  # start each loop once
env.process(breaks_process(env, "packers", every=50, duration=5))
```

### Records of a Task

Every event of a task carries three fields beside its value: `key`, the entity's key; `task`, the task's name; and `instance`, the number of this run of the task, counted from 0 for each task. The first order of the blueprint in [A Task](#a-task), packed and then labelled:

```text
[EVT] {'sim_ts': 3.672, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'status': 'queued'}}
[EVT] {'sim_ts': 3.672, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'status': 'started'}}
[EVT] {'sim_ts': 5.369, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'event_type': 'order_packed'}}
[EVT] {'sim_ts': 5.369, 'timestamp': '...', 'key': 'orders-1', 'task': 'print_label', 'instance': 0, 'value': {'path_id': 'Shop.service.labelling', 'status': 'started'}}
[EVT] {'sim_ts': 7.369, 'timestamp': '...', 'key': 'orders-1', 'task': 'print_label', 'instance': 0, 'value': {'event_type': 'label_printed'}}
```

All five events share the key `orders-1`, so an order can be followed through every task it passes. The finished event's value is exactly the payload. [Records and Telemetry](https://jaehyeon.me/desflow/latest/reference/records-and-telemetry/index.md) describes every field.

______________________________________________________________________

## How It Works

### What `run()` Does

`run()` builds everything the context describes and runs it, in this order:

Until `run()` is called nothing exists but the description, and the runtime helpers that [Builder API](https://jaehyeon.me/desflow/latest/user-guide/builder-api/index.md) lists raise `RuntimeError`. Teardown runs in a `finally` block, so the last statistics are published and every sink is flushed even when the run stops with an error or Ctrl+C.

### What `@app.task` Does

`@app.task` turns your function into a SimPy process that does these steps, publishing a record at three of them:

1. If its `balk` condition holds, it publishes `balked` and stops.
1. If it has a `wait`, a resource or a `get`, it publishes `queued` and starts its `patience` timer, which runs only while it waits. Then it waits until its `wait` ends.
1. It asks for the resource, in the resource's queue order, then takes its `get` from a store or container, waiting while there is not enough.
1. It publishes `started`, calls your function, and keeps what it returns as the payload. The payload is computed before the service time, so it can use the state of the simulation at that moment.
1. It waits the service time, drawn from the live distribution.
1. It adds its `put` to a store or container, waiting while there is no room, and checks its `fail` condition.
1. It publishes the payload as the finished event, or `failed`, and releases the resource.
1. It fires its `signal` (or fails it) and sends its `interrupt` action to the tasks it names. It applies `set` to the entity's attributes. Then it starts the next task that its `next` chooses, or its `on_fail` task, as a new instance for the same entity.

A YAML task and its `spawn` make the same task and arrival loop that the builder code above makes. The code that runs these steps is `TaskRun` in `desflow/core/task.py`. In SimPy terms each task run is one process started with `env.process`, and each arrival loop is another. [SimPy and desflow](https://jaehyeon.me/desflow/latest/des/simpy-and-desflow/index.md) describes processes.

### Processes and the Low-level API

`add_process(func, **kwargs)` starts `func(context, **kwargs)` when the run starts, and a YAML `processes` entry does the same with a `!python` function. Inside it, `context.env` is the `DynamicRealtimeEnvironment`, so a process can do anything a low-level script does: `context.env.timeout`, `context.env.registry.update`, `context.env.publish_event`. A process is how low-level code joins a builder or YAML model.

`context.env.active_process` is the running SimPy process. Its `name` is the task's name, because a task's process is named after the task. Its `target` is the event it waits on now, or None once it has ended. A condition reads the same three values as `process.name`, `process.instance` (the task's instance id, the `instance` field of its records) and `process.target`, a string such as `timeout`, `signal.<name>`, `task.<name>`, `condition`, `resource.<name>`, `get.<name>`, `put.<name>` or `service`. In a task's own expressions they name that task. In an `interrupt` or `running` `where`, they name each instance tested.

______________________________________________________________________

## Stability and Limits

- **Resources are acquired in one order.** A task can hold several resources and hand one to the next task, but tasks that acquire two resources in opposite orders are refused: [Several Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/#several-resources).
- **An interrupt gives back everything.** An interrupted task cancels its waits and releases every unit it holds before it follows its `on_interrupt`, so a resumed task queues again behind the requests already waiting: [Interruptions](https://jaehyeon.me/desflow/latest/tutorials/interruptions/index.md).
- **A finished event is exactly the payload.** It has no `status` or `path_id` unless the payload includes them.
- **A condition branch checks after every change.** In a model with many waits on conditions and many changes, the work grows in proportion to both.
- **Runtime helpers need a running simulation.** They raise `RuntimeError` before `run()`, as [Builder API](https://jaehyeon.me/desflow/latest/user-guide/builder-api/index.md) lists. Call them inside a loop, task or process.
