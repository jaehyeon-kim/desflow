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

A task with a `service` and no `resource` is a **timed step**: work that takes time but needs nothing that can be busy, such as printing a label or waiting for a supplier. It publishes `started` at once, with no `queued` event, waits the sampled time, and publishes its finished event. Its wait is 0. It is SimPy's `env.timeout`: the tests check that it ends at the same simulation time as a plain SimPy timeout in all three APIs ([SimPy Compatibility](../reference/simpy-compatibility.md)).

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

### Wait, Signals and Child Flows

A task can wait for something before it queues for its resource: its `wait`. A wait has one branch, or `any` or `all` of several named branches, as SimPy's `AnyOf` and `AllOf` wait for the first or for every event:

| Branch | Waits for | Its value |
|---|---|---|
| `{signal: name}` | the next firing of a signal declared under `signals` (builder `add_signal`) | the firing's value, the payload of the task that fired it |
| `{task: name}` | a **child flow**: that task, started for the same entity with its own instance id, and the tasks its `next` leads to, until the flow ends | the last task's finished payload |
| `{timeout: 40 s}` | a time | none |
| `{condition: level.stock >= 5}` | a condition to hold, checked when the wait starts and again after every store or container change, registry update and signal firing | `True` |

A single branch is named after its signal or task, or `timeout` or `condition`. After the wait, `wait.<branch>.done` says whether the branch happened before the wait ended, `.ok` whether it succeeded, and `.value` (or `.value.<field>`) is its value; `next`, `fail` and amounts read them, and a builder function that declares `wait` receives them as a mapping. A task fires a signal with `signal: name` when it finishes, with its payload as the value, and `signal.<name>.count`, `.ok` and `.value` read the signal in any condition. The `process-interaction` example waits for a child flow, then for the first of a courier's signal and a time limit:

=== "YAML"

    ```yaml title="examples/yaml/process_interaction.yaml:check"
    check:
      # Starts the child flow inspect and waits for it to end.
      wait: {task: inspect}
      payload: {event_type: order_checked}
      next: dispatch
    inspect:
      service: inspecting
      payload: {event_type: inspected}
      # A gift fails inspection; on_fail sends it to wrap, and the flow goes on.
      fail: attr.gift == 1
      fail_reason: gift needs wrapping
      on_fail: wrap
    wrap:
      service: wrapping
      payload: {event_type: wrapped}
    ```

    ```yaml title="examples/yaml/process_interaction.yaml:dispatch"
    dispatch:
      # The courier's departure, or 40 seconds, whichever comes first.
      wait:
        any:
          courier: {signal: courier_left}
          limit: {timeout: 40 s}
      payload: {event_type: dispatched}
      next:
        when: [{condition: wait.courier.done, task: by_courier}]
        default: by_post
    ```

    ```yaml title="examples/yaml/process_interaction.yaml:courier"
    courier:
      payload: {event_type: courier_left}
      # Fires courier_left with this payload; every order waiting for it resumes.
      signal: courier_left
    ```

=== "Builder"

    ```python title="examples/builder/process_interaction_example.py:check"
    # Starts the child flow inspect and waits for it to end
    @app.task(wait={"task": "inspect"}, next="dispatch")
    def check(task_id, context):
        return {"event_type": "order_checked"}


    # A gift fails inspection; on_fail sends it to wrap, and the flow goes on
    @app.task(
        service_id="inspecting",
        fail="attr.gift == 1",
        fail_reason="gift needs wrapping",
        on_fail="wrap",
    )
    def inspect(task_id, context):
        return {"event_type": "inspected"}


    @app.task(service_id="wrapping")
    def wrap(task_id, context):
        return {"event_type": "wrapped"}
    ```

    ```python title="examples/builder/process_interaction_example.py:dispatch"
    # The courier's departure, or 40 seconds, whichever comes first
    @app.task(
        wait={"any": {"courier": {"signal": "courier_left"}, "limit": {"timeout": 40}}},
        next={
            "when": [{"condition": "wait.courier.done", "task": "by_courier"}],
            "default": "by_post",
        },
    )
    def dispatch(task_id, context):
        return {"event_type": "dispatched"}
    ```

    ```python title="examples/builder/process_interaction_example.py:courier"
    # Fires courier_left with this payload; every order waiting for it resumes
    @app.task(signal="courier_left")
    def courier(task_id, context):
        return {"event_type": "courier_left"}
    ```

    `context.signal(name)` returns the `Signal` a process can fire, fail or wait for, and `context.call(task, entity=...)` starts a child flow from a process and returns its SimPy process, which a process can yield. `context.spawn` returns the process it starts too, so `is_alive`, `triggered`, `ok` and `value` inspect it, as in SimPy.

=== "Low-level"

    ```python title="examples/low-level/process_interaction_example.py:check"
    def check(shop, key, arrival):
        """Starts the child process inspect and waits for it to end."""
        env, stats, task_id = shop.env, shop.env.stats, shop.next_id("check")
        shop.publish(key, "check", task_id, {"status": "queued"})
        stats.task_queued("check", task_id)
        yield env.any_of([env.process(inspect(shop, key, arrival))])
        shop.publish(key, "check", task_id, {"status": "started"})
        stats.task_started("check", task_id)
        shop.publish(key, "check", task_id, {"event_type": "order_checked"})
        stats.task_finished("check", task_id)
        env.process(dispatch(shop, key, arrival))


    def inspect(shop, key, arrival):
        """A gift fails inspection and is wrapped instead; returns the last payload."""
        env, stats, task_id = shop.env, shop.env.stats, shop.next_id("inspect")
        path_id = "Shop.service.inspecting"
        shop.publish(key, "inspect", task_id, {"path_id": path_id, "status": "started"})
        stats.task_started("inspect", task_id)
        yield env.timeout(shop.service("inspecting")[1])
        if GIFT[arrival] == 1:
            failed = {
                "path_id": path_id,
                "status": "failed",
                "reason": "gift needs wrapping",
            }
            shop.publish(key, "inspect", task_id, failed)
            stats.task_failed("inspect", task_id)
            return (yield env.process(wrap(shop, key)))
        payload = {"event_type": "inspected"}
        shop.publish(key, "inspect", task_id, payload)
        stats.task_finished("inspect", task_id)
        return payload


    def wrap(shop, key):
        env, stats, task_id = shop.env, shop.env.stats, shop.next_id("wrap")
        path_id = "Shop.service.wrapping"
        shop.publish(key, "wrap", task_id, {"path_id": path_id, "status": "started"})
        stats.task_started("wrap", task_id)
        yield env.timeout(shop.service("wrapping")[1])
        payload = {"event_type": "wrapped"}
        shop.publish(key, "wrap", task_id, payload)
        stats.task_finished("wrap", task_id)
        return payload
    ```

    ```python title="examples/low-level/process_interaction_example.py:dispatch"
    def dispatch(shop, key, arrival):
        """The courier's departure, or 40 seconds, whichever comes first."""
        env, stats, task_id = shop.env, shop.env.stats, shop.next_id("dispatch")
        shop.publish(key, "dispatch", task_id, {"status": "queued"})
        stats.task_queued("dispatch", task_id)
        courier = shop.courier_left.wait()
        done = yield env.any_of([courier, env.timeout(40)])
        shop.publish(key, "dispatch", task_id, {"status": "started"})
        stats.task_started("dispatch", task_id)
        shop.publish(key, "dispatch", task_id, {"event_type": "dispatched"})
        stats.task_finished("dispatch", task_id)
        task = "by_courier" if courier in done else "by_post"
        env.process(send(shop, key, arrival, task))
    ```

    ```python title="examples/low-level/process_interaction_example.py:courier"
    def courier(shop, key):
        """Fires courier_left; every order waiting for it resumes."""
        task_id = shop.next_id("courier")
        shop.env.stats.flow_started("couriers")
        payload = {"event_type": "courier_left"}
        shop.publish(key, "courier", task_id, payload)
        shop.env.stats.task_finished("courier", task_id)
        shop.courier_left.fire(payload)
        shop.env.stats.flow_finished("couriers")
        yield shop.env.timeout(0)
    ```

    The low-level API is SimPy: a child is `env.process(...)`, its value is what it returns, and `env.any_of` and `env.all_of` wait for the first or for every event. `Signal(env, name)` is the named event the builder and YAML use; `env.watch_changes(check)` calls `check` after every change a condition can read.

The task publishes `queued` when its wait starts, so the wait counts in its wait statistics, and its `patience` covers the wait as well as its resource and its `get`. When an `any` wait ends, its losing signal, timeout and condition branches are dropped, and its losing child flows keep running, as SimPy leaves processes running; [Wait for One or All](../guides/wait-for-one-or-all.md) shows ties and failures. A wait holds no resource, so waiting for all of several things grants nothing at once.

### Failures

A task fails when its `fail` condition holds after its service, when the wait it needs fails, or, in the builder, when its function raises. `fail` reads the task's own payload as `payload.<field>`, as `next` does. A failed task publishes `failed` with a `reason`, the condition's text or its `fail_reason`, instead of its finished payload; it fails its `signal`, so every waiter of the signal fails too; and it goes to its `on_fail` task. Without `on_fail` the entity's flow ends, and a child flow fails the task waiting for it, with the same reason. Each failure is counted as `tasks.<task>.failed`, and a flow that ends in one as `flows.<arrival>.failed`, at `basic` and above. The `inspect` task of the example above fails a gift and sends it to be wrapped.

In the low-level API a failure is SimPy's: an exception raised in a process reaches the process that yields it, and one nobody handles stops the run. The builder and YAML end only the entity's flow, because a data generator should not stop on one failed order.

### Example: Process Interaction

The `process-interaction` example puts these together: a child flow, a failure that `on_fail` handles, and orders that wait for a courier's signal or a time limit. It needs no Docker service and ends on its own after 300 simulation seconds.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get process-interaction --api yaml
        cd process-interaction/yaml

        # 3. Run it
        desflow run process_interaction.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get process-interaction --api yaml
        cd process-interaction/yaml

        # 3. Run it
        desflow run process_interaction.yaml
        ```

    The whole file is [`examples/yaml/process_interaction.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/process_interaction.yaml).

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get process-interaction --api builder
        cd process-interaction/builder

        # 3. Run it
        uv run --no-project --with desflow process_interaction_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get process-interaction --api builder
        cd process-interaction/builder

        # 3. Run it
        python process_interaction_example.py
        ```

    The whole file is [`examples/builder/process_interaction_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/process_interaction_example.py).

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get process-interaction --api low-level
        cd process-interaction/low-level

        # 3. Run it
        uv run --no-project --with desflow process_interaction_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get process-interaction --api low-level
        cd process-interaction/low-level

        # 3. Run it
        python process_interaction_example.py
        ```

    The whole file is [`examples/low-level/process_interaction_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/process_interaction_example.py).

### Records of a Task

Every event of a task carries three fields beside its value: `key`, the entity's key; `task`, the task's name; and `instance`, the number of this run of the task, counted from 0 for each task. The first order of the blueprint in [A Task](#a-task), packed and then labelled:

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

1. If its `balk` condition holds, it publishes `balked` and stops.
2. If it has a `wait`, a resource or a `get`, it publishes `queued`, starts its `patience` timer, and waits for its `wait`.
3. It asks for the resource, in the resource's queue order, then takes its `get` from a store or container, waiting while there is not enough.
4. It publishes `started`, calls your function, and keeps what it returns as the payload. The payload is computed before the service time, so it can use the state of the simulation at that moment.
5. It waits the service time, drawn from the live distribution.
6. It adds its `put` to a store or container, waiting while there is no room, and checks its `fail` condition.
7. It publishes the payload as the finished event, or `failed`, and releases the resource.
8. It fires its `signal` (or fails it), applies `set` to the entity's attributes, and starts the next task its `next` chooses, or its `on_fail` task, as a new instance for the same entity.

A YAML task and its `spawn` make the same task and arrival loop that the builder code above makes. The code that runs these steps is `TaskRun` in `desflow/core/task.py`. In SimPy terms each task run is one process started with `env.process`, and each arrival loop is another; [SimPy and desflow](../des/simpy-and-desflow.md) describes processes.

### Processes and the Low-level API

`add_process(func, **kwargs)` starts `func(context, **kwargs)` when the run starts, and a YAML `processes` entry does the same with a `!python` function. Inside it, `context.env` is the `DynamicRealtimeEnvironment`, so a process can do anything a low-level script does: `context.env.timeout`, `context.env.registry.update`, `context.env.publish_event`. This is how low-level code joins a builder or YAML model.

---

## Stability and Limits

- **Resources are acquired in one order.** A task can hold several resources and hand one to the next task, but tasks that acquire two resources in opposite orders are refused: [Resources](resources.md#several-resources).
- **An interrupt gives back everything.** An interrupted task cancels its waits and releases every unit it holds before it follows its `on_interrupt`, so a resumed task queues again behind the requests already waiting: [Interruptions](../tutorials/interruptions.md).
- **A finished event is exactly the payload.** It has no `status` or `path_id` unless the payload includes them.
- **A condition branch checks after every change.** A model with many waits on conditions and many changes does work in proportion to both.
- **Runtime helpers need a running simulation.** `context.spawn`, `context.call`, `context.signal`, `context.get_resource`, `context.get_store`, `context.get_container` and `context.env` raise `RuntimeError` before `run()`; call them inside a loop, task or process.
