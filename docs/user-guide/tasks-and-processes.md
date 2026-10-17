# Tasks and Processes: `SimulationContext`

A **task** is what one entity does: an order being packed, a return being logged. A **process** is anything else that runs over time: packers going on a break every hour, a speed that drifts, a shift plan. This page shows how to write each, what `run()` does with them, and what a task does step by step.

---

## What It Is

Both are SimPy generators: functions that `yield` to wait. The builder and YAML write the common ones for you, and the low-level API writes them by hand. A task waits for what it needs, does its work for a sampled time, publishes an event at each step, and may lead to the next task. A process can do anything a generator can: wait, change values in the registry, publish records or start tasks.

---

## How to Use It

### A Task

A task with a service and a resource asks for a unit of the resource, holds it for a time drawn from the service, and releases it. A task with neither takes no time and holds nothing: it publishes its payload at once, which suits an entity that only records that it happened, such as a return logged.

=== "YAML"

    ```yaml title="docs/snippets/yaml/tasks.yaml:task"
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

    ```yaml title="docs/snippets/yaml/tasks.yaml:payload-function"
    # also under tasks: a payload computed by a Python function
    pack_with_delivery:
      service: packing
      resource: packers
      # Called as choose_delivery(task_id, context) when the order gets a packer
      payload: !python shop_logic.choose_delivery
    ```

=== "Builder"

    ```python title="docs/snippets/pages/tasks.py:builder-task"
    @app.task(service_id="packing", resource_id="packers", next="print_label")
    def pack_order(task_id: int):
        # Runs when the order gets a packer, before the packing time.
        # The returned dict is the finished event's value.
        return {"event_type": "order_packed"}


    @app.task()  # no service and no resource: publishes at once
    def return_logged(task_id: int):
        return {"event_type": "return_logged"}
    ```

    The function is called with the task id and whatever the caller passes. It also receives `entity`, `upstream` and `item` by keyword when it declares a parameter of that name: the entity, what the previous task handed on, and what a store `get` took. A function that declares none of them works as before.

=== "Low-level"

    ```python title="docs/snippets/pages/tasks.py:low-level-task"
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

    This is ordinary SimPy: `packers.request()` waits for a unit, `env.timeout` waits the service time, and leaving the `with` block releases the unit. `env.publish_event` at each step is what makes it produce records.

A task can also take from or add to a store or container (`get`, `put`), rank its requests (`priority`), and name the task that follows it (`next`); [Stores and Containers](stores-and-containers.md), [Resources](resources.md) and [Task Flow](task-flow.md) describe each.

### A Timed Step

A task with a `service` and no `resource` is a **timed step**: work that takes time but needs nothing that can be busy, such as printing a label or waiting for a supplier. It publishes `started` at once, with no `queued` event, waits the sampled time, and publishes its finished event. Its wait is 0.

=== "YAML"

    ```yaml title="docs/snippets/yaml/tasks.yaml:timed-step"
    print_label:
      # A service and no resource: a timed step that starts at once
      service: labelling
      payload: {event_type: label_printed}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/tasks.py:builder-timed-step"
    @app.task(service_id="labelling")  # a service and no resource: starts at once
    def print_label(task_id: int):
        return {"event_type": "label_printed"}
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/tasks.py:low-level-timed-step"
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

An arrival starts one task per arrival. The loop draws a gap from the arrival's distribution, waits for that duration, starts a task for the new entity, and repeats; the task runs on its own while the loop waits for the next arrival.

=== "YAML"

    ```yaml title="docs/snippets/yaml/tasks.yaml:arrival"
    arrivals:
      # Each arrival spawns one pack_order, and one return_logged
      orders: {dist: exponential, rate: 0.2, spawn: pack_order}
      returns: {dist: exponential, rate: 0.1, spawn: return_logged}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/tasks.py:builder-arrival"
    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        task_id = 0
        while True:
            # Draws the next gap, waits for it, and returns the new entity, keyed orders-1, ...
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))  # the loop goes on at once
            task_id += 1
    ```

    `context.wait_for_arrival` returns the new `Entity`, whose key is the arrival name and a counter: `orders-1`, `orders-2`. Pass it to the task with `entity=`. A task called without one starts an entity of its own, keyed by the task's name and a counter, such as `pack_order-1`. The `task_id` becomes the record's `instance`. When `next` or a second arrival also starts the task, take the id from `context.next_instance("pack_order")` instead: it is the task's one counter, which `next` uses too, so no two runs of the task share an id.

=== "Low-level"

    ```python title="docs/snippets/pages/tasks.py:low-level-arrival"
    def orders_process(env, sampler, packers):
        """What @app.arrival_loop does, by hand."""
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

    ```python title="docs/snippets/pages/tasks.py:low-level-start"
    env.process(orders_process(env, sampler, packers))  # start each loop once
    env.process(breaks_process(env, "packers", every=50, duration=5))
    ```

### A Process

A process runs alongside the tasks. This one sends the packers on a 5-second break every 50 seconds:

=== "YAML"

    ```yaml title="docs/snippets/yaml/tasks.yaml:process"
    processes:
      # Called as breaks(context, resource="packers", every=50, duration=5)
      - function: !python shop_logic.breaks
        kwargs: {resource: packers, every: 50, duration: 5}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/tasks.py:builder-process"
    def breaks(context: SimulationContext, resource: str, every: float, duration: float):
        """Takes every packer off for a break of `duration` seconds every `every` seconds."""
        path = f"{context.sim_id}.resources.{resource}.current_cap"
        registry = context.env.registry
        while True:
            yield context.env.timeout(every)
            normal = registry.get(path).value
            registry.update(path, 0)
            yield context.env.timeout(duration)
            registry.update(path, normal)


    app.add_process(breaks, resource="packers", every=50, duration=5)
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/tasks.py:low-level-process"
    def breaks_process(env, resource: str, every: float, duration: float):
        """What add_process starts, by hand: a plain SimPy generator."""
        path = f"Shop.resources.{resource}.current_cap"
        while True:
            yield env.timeout(every)
            normal = env.registry.get(path).value
            env.registry.update(path, 0)
            yield env.timeout(duration)
            env.registry.update(path, normal)
    ```

Setting `current_cap` to 0 stops new work but not the work in progress: [Resources](resources.md) explains what happens to busy units when a capacity drops.

### Records of a Task

Every event of a task carries three fields beside its value: `key`, the entity's key; `task`, the task's name; and `instance`, the number of this run of the task, counted from 0 for each task. The first order of the blueprint above, packed and then labelled:

<!-- records: task-events -->
```text
[EVT] {'sim_ts': 3.672, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'status': 'queued'}}
[EVT] {'sim_ts': 3.672, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'status': 'started'}}
[EVT] {'sim_ts': 5.369, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'event_type': 'order_packed'}}
[EVT] {'sim_ts': 5.369, 'timestamp': '...', 'key': 'orders-1', 'task': 'print_label', 'instance': 0, 'value': {'path_id': 'Shop.service.labelling', 'status': 'started'}}
[EVT] {'sim_ts': 7.369, 'timestamp': '...', 'key': 'orders-1', 'task': 'print_label', 'instance': 0, 'value': {'event_type': 'label_printed'}}
```

All five events share the key `orders-1`, so an order can be followed through every task it passes. The finished event's value is exactly the payload. [Records and Telemetry](../reference/records-and-telemetry.md) describes every field.

---

## How It Works

### What `run()` Does

`run()` builds everything the context describes and runs it, in this order:

![The steps of run(), top to bottom: check the tasks; call run.before; build the environment; make the Sampler from random_seed or a seed drawn and logged; switch on statistics; register the parameters; start ingress, then egress with its warm-up; build resources, stores and containers; start loops and processes; env.run(until); and in finally, the teardown](../assets/diagrams/run-sequence.png)

Until `run()` is called nothing exists but the description: `context.env`, `context.spawn` and `context.get_resource` raise `RuntimeError` before it. Teardown runs in a `finally` block, so the last statistics are published and every sink is flushed even when the run stops with an error or Ctrl+C.

### What `@app.task` Does

`@app.task` turns your function into a SimPy process that does these steps, publishing a record at three of them:

![pack_order in three steps. queued: orders-1 joins the packers queue, and the record has status queued. started: it gets a packer and the function returns the payload, and the record has status started. finished: after the packing time the payload is published and the packer released](../assets/diagrams/task-life.gif)

1. If it has a resource or a `get`, it publishes `queued` and asks for the resource, in the resource's queue order.
2. Once it has the resource, it takes its `get` from a store or container, waiting while there is not enough.
3. It publishes `started`, calls your function, and keeps what it returns as the payload. The payload is computed before the service time, so it can use the state of the simulation at that moment.
4. It waits the service time, drawn from the live distribution.
5. It adds its `put` to a store or container, waiting while there is no room, then publishes the payload as the finished event and releases the resource.
6. It applies `set` to the entity's attributes, and starts the next task its `next` chooses, if any, as a new instance for the same entity.

A YAML task and its `spawn` make the same task and arrival loop that the builder code above makes. In SimPy terms each task run is one process started with `env.process`, and each arrival loop is another; [SimPy and desflow](../des/simpy-and-desflow.md) describes processes.

### Processes and the Low-level API

`add_process(func, **kwargs)` starts `func(context, **kwargs)` when the run starts, and a YAML `processes` entry does the same with a `!python` function. Inside it, `context.env` is the `DynamicRealtimeEnvironment`, so a process can do anything a low-level script does: `context.env.timeout`, `context.env.registry.update`, `context.env.publish_event`. This is how low-level code joins a builder or YAML model.

---

## Stability and Limits

- **A task holds one resource.** An order that needs a picker and a packing bench at once, or a handover between them, is a process: [Multi-Resource Handoffs](../guides/multi-resource-handoffs.md) shows how.
- **A task cannot be interrupted.** Breakdowns that stop work in progress, such as a label printer that jams, need a process that catches SimPy's `Interrupt`: [Preemptive Breakdowns](../guides/preemptive-breakdowns.md).
- **A finished event is exactly the payload.** It has no `status` or `path_id` unless the payload includes them.
- **Runtime helpers need a running simulation.** `context.spawn`, `context.get_resource`, `context.get_store`, `context.get_container` and `context.env` raise `RuntimeError` before `run()`; call them inside a loop, task or process.
