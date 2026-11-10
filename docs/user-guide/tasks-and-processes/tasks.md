# Tasks

A **task** is what one entity does: an order being packed, a return being logged. This page shows how to write a task, which keys a task has, how an order leaves the line, what a task does when it is interrupted, what `run()` does with tasks, and what a task does step by step. [Processes](processes.md) shows work that does not belong to one entity.

---

## What It Is

A task is a SimPy generator: a function that `yield`s to wait. The builder and YAML write the common ones for you, and the low-level API writes them by hand. A task waits for what it needs, does its work for a sampled time, publishes an event at each step, and may lead to the next task.

---

## How to Use It

### A Task

A task with a service and a resource asks for a unit of the resource, holds it for a time drawn from the service, and releases it. A task with neither takes no time and holds nothing: it publishes its payload at once. Such a task suits a step that only records an event, such as logging a return.

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

    A `payload` mapping may hold generated fields. This task adds a `delivery` to the finished event, standard for nine orders in ten and express for the tenth:

    ```yaml title="docs/snippets/yaml/tasks.yaml:payload-mapping"
    # A payload with generated fields: nine orders in ten are standard, one is express
    pack_with_delivery:
      service: packing
      resource: packers
      payload:
        event_type: order_packed
        delivery:
          gen: weighted
          options: {standard: 0.9, express: 0.1}
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

    The function is called with the task id and whatever the caller passes. It also receives `entity`, `upstream` and `item` by keyword when it declares a parameter of that name: the entity, what the previous task handed on, and what a store `get` took. A function that declares none of them is called without them.

    A function that returns a `Template` gives the same generated fields as the YAML tab:

    ```python title="docs/snippets/pages/tasks.py:builder-payload-mapping"
    # A payload with generated fields: nine orders in ten are standard, one is express
    delivery = Template(
        {
            "event_type": "order_packed",
            "delivery": {"gen": "weighted", "options": {"standard": 0.9, "express": 0.1}},
        },
        name="tasks.pack_with_delivery",
    )


    @app.task(service_id="packing", resource_id="packers")
    def pack_with_delivery(task_id: int):
        return delivery
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/tasks.py:low-level-task"
    def pack_order_process(env, sampler, packers, key, task_id, label_ids):
        """What @app.task does, by hand."""
        path_id = "Shop.service.packing"

        def publish(value):
            env.publish_event(key, value, task="pack_order", instance=task_id)

        publish(
            {"path_id": path_id, "sim_id": "Shop", "service": "packing", "status": "queued"}
        )
        with packers.request() as request:
            yield request
            publish(
                {
                    "path_id": path_id,
                    "sim_id": "Shop",
                    "service": "packing",
                    "status": "started",
                }
            )
            payload = {"event_type": "order_packed"}
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            publish(payload)
        # next: print_label, a new instance for the same order
        yield from print_label_process(env, sampler, key, next(label_ids))
    ```

    This is ordinary SimPy: `packers.request()` waits for a unit, `env.timeout` waits the service time, and leaving the `with` block releases the unit. The `env.publish_event` call at each step produces the records.

A task can also take from or add to a store or container (`get`, `put`), rank its requests (`priority`), and name the task that follows it (`next`). [Stores and Containers](../model-parts/stores-and-containers.md), [Units and Queues](../resources/units-and-queues.md) and [Task Flow](../model-parts/task-flow.md) describe each.

### A Timed Step

A task with a `service` and no `resource` is a **timed step**: work that takes time but needs nothing that can be busy, such as printing a label or waiting for a supplier. It publishes `started` at once, with no `queued` event, waits the sampled time, and publishes its finished event. Its wait is 0. It is SimPy's `env.timeout`: the tests check that it ends at the same simulation time as a plain SimPy timeout in all three APIs ([SimPy Compatibility](../../simpy/compatibility.md)).

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

        publish(
            {
                "path_id": path_id,
                "sim_id": "Shop",
                "service": "labelling",
                "status": "started",
            }
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "label_printed"})
    ```

### Arrivals Start Tasks

An arrival starts one task per arrival. The loop draws a gap from the arrival's distribution, waits for that duration, starts a task for the new entity, and repeats. The task runs on its own while the loop waits for the next arrival.

=== "YAML"

    ```yaml title="docs/snippets/yaml/tasks.yaml:arrival"
    arrivals:
      # Each arrival spawns one pack_order, and one return_logged
      # An order every 5 s on average (rate 0.2)
      orders: {dist: exponential, rate: 0.2, spawn: pack_order}
      # A return every 10 s on average (rate 0.1)
      returns: {dist: exponential, rate: 0.1, spawn: return_logged}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/tasks.py:builder-arrival"
    # Each arrival starts one pack_order
    # An order every 5 s on average (rate 0.2)
    app.add_arrival("orders", dist="exponential", rate=0.2, spawn="pack_order")
    ```

    `spawn=` starts the named task for each arrival's entity, whose key is the arrival name and a counter: `orders-1`, `orders-2`. The record's `instance` comes from the task's one counter, which `next` and other arrivals share, so no two runs of the task share an id. A `spawn` that names a task never added is refused when `run()` checks the model.

    `@app.arrival_loop` runs a generator of your own. The loop below starts one `return_logged` for each return, which is what `spawn="return_logged"` does in one argument, as the YAML tab writes it. A loop is needed only when it does more than start one task, such as choosing the task from the entity's attributes. `context.wait_for_arrival` returns the new `Entity`. Pass it to the task with `entity=`. A task called without one starts an entity of its own, keyed by the task's name and a counter, such as `return_logged-1`. The `task_id` becomes the record's `instance`. Take it from `context.next_instance`, the task's one counter.

    ```python title="docs/snippets/pages/tasks.py:builder-arrival-loop"
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

=== "Low-level"

    ```python title="docs/snippets/pages/tasks.py:low-level-arrival"
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

    ```python title="docs/snippets/pages/tasks.py:low-level-start"
    env.process(orders_process(env, sampler, packers))  # start each loop once
    env.process(breaks_process(env, "packers", every=50, duration=5))
    ```

### Task Keys

A task has these keys. Each row names where the keys are described, and [YAML Keys](../../reference/yaml-keys.md) lists every key with its type.

| Keys | What they set | Described in |
|---|---|---|
| `service`, `service_time`, `resource` | how long the work takes, and the resource it holds | [A Task](#a-task) and [Units and Queues](../resources/units-and-queues.md) |
| `priority`, `preempt` | the task's place in a resource's line, and whether it may take a unit | [Priority and Preemption](../resources/priority-and-preemption.md) |
| `keep`, `seize`, `release` | holding a unit across tasks, or from one step to another | [Several Resources](../resources/several-resources.md) |
| `patience`, `balk`, `on_renege`, `on_balk` | leaving the line | [Leave the Line](#leave-the-line) |
| `on_interrupt`, `on_interrupt_next`, `on_interrupt_run` | what the task does when it is interrupted | [Interrupts](#interrupts) |
| `interrupt`, `subscribe` | interrupting other tasks, and being told when a flow ends | [Signals and Waiting](signals-and-waiting.md) |
| `payload`, `id_field`, `item_field`, `attributes` | the value of the finished event | [A Task](#a-task) and [Task Flow](../model-parts/task-flow.md#record-key-and-fields) |
| `key`, `key_field` | the key of the task's records | [Task Flow](../model-parts/task-flow.md#record-key-and-fields) |
| `next`, `set`, `set_args`, `activate`, `deactivate` | where the entity goes next and what it carries | [Task Flow](../model-parts/task-flow.md) |
| `vars`, `vars_once`, `on_start`, `finish_vars`, `history` | values drawn for the task, and the rows it keeps | [Task Flow](../model-parts/task-flow.md#values-a-task-draws) |
| `get`, `put` | taking from and adding to a store or container | [Stores and Containers](../model-parts/stores-and-containers.md) |
| `wait`, `open`, `listen`, `cancel`, `resume` | waiting for, naming and watching events | [Signals and Waiting](signals-and-waiting.md) |
| `signal`, `spawn`, `on` | what happens when the task ends | [Signals and Waiting](signals-and-waiting.md) |
| `fail`, `fail_reason`, `on_fail` | when a task fails and where it goes | [Failures](failures.md) |
| `batch` | serving several entities with one run of the service | [Serve Entities Together](../../how-to/modelling/serve-entities-together.md) |

### Leave the Line

An order that waits too long may leave, and an order that finds the line long may not join it. Four keys set this:

- `patience` is the longest the task waits for its `wait`, its resource and its `get`. It is a number of seconds, a duration such as `8 s`, an expression or a generator. A task whose patience runs out **reneges**: it publishes `reneged`, gives back what it holds and leaves.
- `balk` is a condition read before the task joins the line. When it holds, the task publishes `balked` and leaves. A resource whose `queue_limit` is full makes the task balk too ([Units and Queues](../resources/units-and-queues.md#queue-limit)).
- `on_renege` and `on_balk` name the task that gets the entity next. Without them the flow ends. That task reads how the entity left as `upstream.status`, `reneged` or `balked`.

This model has orders that give up after 8 seconds or do not join the line from 150 seconds on, and send both kinds to a follow-up task:

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_options.yaml:leave"
    pack_order:
      service: packing
      resource: packers
      priority: 1
      # The order leaves the line when it has waited 8 seconds
      patience: 8 s
      # The order does not join the line from 150 seconds on
      balk: t >= 150
      # Where an order goes after it leaves the line; it reads upstream.status
      on_renege: follow_up
      on_balk: follow_up
      payload: {event_type: order_packed}
    follow_up:
      payload:
        event_type: order_left
        how: {gen: expr, expr: upstream.status}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/task_options.py:leave"
    @app.task(
        service_id="packing",
        resource_id="packers",
        priority=1,
        patience="8 s",  # the order leaves the line when it has waited 8 seconds
        balk="t >= 150",  # the order does not join the line from 150 seconds on
        # Where an order goes after it leaves the line; it reads upstream.status
        on_renege="follow_up",
        on_balk="follow_up",
    )
    def pack_order(task_id):
        return {"event_type": "order_packed"}


    order_left = Template(
        {"event_type": "order_left", "how": {"gen": "expr", "expr": "upstream.status"}},
        name="tasks.follow_up",
    )


    @app.task()
    def follow_up(task_id):
        return order_left
    ```

=== "Low-level"

    SimPy's reneging pattern races the request against a timeout: `result = yield request | env.timeout(patience)`, and `request.cancel()` when `request not in result`. A balk is an `if` before `packers.request()`. [Add Reneging and Balking](../../how-to/modelling/reneging-and-balking.md) shows both in the three forms, and [Patience](../../learn/shop-logic/patience.md) teaches them step by step.

### Interrupts

A task is interrupted by a preemption, a breakdown, an `interrupt` action or a scenario step. `on_interrupt` says what it does next:

| Value | What the task does |
|---|---|
| `resume` | The default. It goes on at once with a unit it still holds, or queues again, and then serves the time it had left. |
| `restart` | The same, but it serves a new time, drawn from the same stream. |
| `abandon` | It gives back every unit and ends, unless `on_interrupt_next` names a task for the entity to go to. |

`on_interrupt_next` is for `abandon`. The task it names reads why the entity was interrupted as `cause.<name>`: `by`, `usage_since` and `resource` after a preemption, `value` after an `interrupt`, and `left`, the service time that was left.

`on_interrupt_run` is for `resume` and `restart`. It names a task that runs as a child flow of the same entity after each interrupt, and then the task goes on with the time it had left, or a new time under `restart`. It is the SimPy loop `try: yield env.timeout(left)` with `except Interrupt: yield from repair()`. The child reads `cause.<name>` as a task reached by `on_interrupt_next` does. It is for work that holds no unit of its own, so it is refused with a `resource` and with a `request` in the `wait`.

A gift order that loses its packer is packed by hand instead (`abandon`). A label that is interrupted by a jam is retooled and then printed with the time it had left (`on_interrupt_run`):

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_options.yaml:abandon"
    pack_gift:
      service: packing
      resource: packers
      priority: 1
      # A gift that loses its packer gives it up and is packed by hand instead
      on_interrupt: abandon
      on_interrupt_next: pack_by_hand
      payload: {event_type: gift_packed}
    pack_by_hand:
      service: packing
      payload: {event_type: packed_by_hand}
    ```

    ```yaml title="docs/snippets/yaml/task_options.yaml:run"
    print_label:
      service: labelling
      # After an interrupt, retool runs as a child flow of the same entity, and then
      # the label goes on with the time it had left
      on_interrupt: resume
      on_interrupt_run: retool
      payload: {event_type: label_printed}
    retool:
      service: repairing
      payload:
        event_type: retooled
        left: {gen: expr, expr: cause.left}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/task_options.py:abandon"
    @app.task(
        service_id="packing",
        resource_id="packers",
        priority=1,
        # A gift that loses its packer gives it up and is packed by hand instead
        on_interrupt="abandon",
        on_interrupt_next="pack_by_hand",
    )
    def pack_gift(task_id):
        return {"event_type": "gift_packed"}


    @app.task(service_id="packing")
    def pack_by_hand(task_id):
        return {"event_type": "packed_by_hand"}
    ```

    ```python title="docs/snippets/pages/task_options.py:run"
    @app.task(
        service_id="labelling",
        # After an interrupt, retool runs as a child flow of the same entity, and then
        # the label goes on with the time it had left
        on_interrupt="resume",
        on_interrupt_run="retool",
    )
    def print_label(task_id):
        return {"event_type": "label_printed"}


    retooled = Template(
        {"event_type": "retooled", "left": {"gen": "expr", "expr": "cause.left"}},
        name="tasks.retool",
    )


    @app.task(service_id="repairing")
    def retool(task_id):
        return retooled
    ```

=== "Low-level"

    The process catches `simpy.Interrupt`. For a preemption, its cause is SimPy's `Preempted`. A process that is preempted, or whose request still waits, cancels its request and asks again, and a process that holds its unit keeps the request. [Interruptions](../../learn/shop-logic/interruptions.md) shows the loop, and [Priority and Preemption](../resources/priority-and-preemption.md) says what takes a unit.

### Records of a Task

Every event of a task carries three fields beside its value: `key`, the entity's key; `task`, the task's name; and `instance`, the number of this run of the task, counted from 0 for each task. The first order of the blueprint in [A Task](#a-task), packed and then labelled:

<!-- records: task-events -->
```text
[EVT] {'sim_ts': 3.672097157355851, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'sim_id': 'Shop', 'service': 'packing', 'status': 'queued'}}
[EVT] {'sim_ts': 3.672097157355851, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'path_id': 'Shop.service.packing', 'sim_id': 'Shop', 'service': 'packing', 'status': 'started'}}
[EVT] {'sim_ts': 5.369082605964606, 'timestamp': '...', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'event_type': 'order_packed'}}
[EVT] {'sim_ts': 5.369082605964606, 'timestamp': '...', 'key': 'orders-1', 'task': 'print_label', 'instance': 0, 'value': {'path_id': 'Shop.service.labelling', 'sim_id': 'Shop', 'service': 'labelling', 'status': 'started'}}
[EVT] {'sim_ts': 7.369082605964606, 'timestamp': '...', 'key': 'orders-1', 'task': 'print_label', 'instance': 0, 'value': {'event_type': 'label_printed'}}
```

All five events share the key `orders-1`, so an order can be followed through every task it passes. The finished event's value is exactly the payload. [Records and Telemetry](../../reference/records-and-telemetry.md) describes every field.

---

## How It Works

### What run() Does

`run()` builds everything the context describes and runs it, in this order:

![The steps of run(), top to bottom. Check the tasks, call run.before and build the environment. Make the Sampler from random_seed, or from a seed drawn and logged. Switch on statistics and register the parameters. Start ingress, then egress with its warm-up. Build resources, stores and containers, start loops and processes, and call env.run(until). In finally, tear down](../../assets/diagrams/run-sequence.png)

Until `run()` is called nothing exists but the description, and the runtime helpers that [Builder API](../writing-a-model/builder-api.md) lists raise `RuntimeError`. Teardown runs in a `finally` block, so the last statistics are published and every sink is flushed even when the run stops with an error or Ctrl+C.

### What @app.task Does

`@app.task` turns your function into a SimPy process that does these steps, publishing a record at three of them:

![pack_order in three steps. queued: orders-1 joins the packers queue, and the record has status queued. started: it gets a packer and the function returns the payload, and the record has status started. finished: after the packing time the payload is published and the packer released](../../assets/diagrams/task-life.gif)

1. If its `balk` condition holds, it publishes `balked` and stops.
2. It publishes `queued` if it has a `wait`, a resource or a `get`, and starts its `patience` timer, which runs only while it waits. It starts its `open` branches, makes its `listen` callbacks and runs its `cancel`. Then it asks for the units named by `seize`.
3. It waits until its `wait` ends.
4. It asks for the resource, in the resource's queue order, then takes its `get` from a store or container, waiting while there is not enough.
5. It publishes `started`, calls your function, and keeps what it returns as the payload. The payload is computed before the service time, so it can use the state of the simulation at that moment.
6. It waits the service time, drawn from the live distribution.
7. It adds its `put` to a store or container, waiting while there is no room, and checks its `fail` condition.
8. It publishes the payload as the finished event, or `failed`, and releases the resource.
9. It fires its `signal` (or fails it) and sends its `interrupt` action to the tasks it names. It applies `set` to the entity's attributes and `set_args` to the process's local names, and runs the `on:` actions. It frees the units named by `release`. A unit still held when the flow ends stays held, as in SimPy, so a flow that used `seize` writes `release` before it ends.
10. It starts the next task that its `next` chooses, or its `on_fail`, `on_renege` or `on_balk` task, as a new instance for the same entity.

A YAML task and its `spawn` make the same task and arrival loop that the builder code above makes. The code that runs these steps is `TaskRun` in `desflow/core/task.py`. In SimPy terms each flow of tasks is one process started with `env.process`, and each arrival loop is another. [SimPy and desflow](../../simpy/index.md) describes processes.

A process outside a task runs as [Processes](processes.md#how-it-works) describes.

---

## Stability and Limits

- **Resources are acquired in one order.** A task can hold several resources and hand one to the next task. Tasks that acquire two resources in opposite orders still load, and the model logs one WARNING, because two of them could then wait for each other for ever: [Several Resources](../resources/several-resources.md#several-resources).
- **`abandon` gives back everything.** An interrupted task cancels its waits. Under `resume` and `restart` it keeps a unit it holds through a plain interrupt and asks again only for a unit it lost, at the back of the line. A preemption or a breakdown takes the unit. Under `abandon` it releases every unit before it follows `on_interrupt_next`: [Interruptions](../../learn/shop-logic/interruptions.md).
- **A finished event is exactly the payload.** It has no `status` or `path_id` unless the payload includes them.
- **A condition branch checks after every change.** In a model with many waits on conditions and many changes, the work grows in proportion to both.
- **Runtime helpers need a running simulation.** They raise `RuntimeError` before `run()`, as [Builder API](../writing-a-model/builder-api.md) lists. Call them inside a loop, task or process.
