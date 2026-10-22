# Signals and Waiting

A [task](index.md) can wait for signals, conditions and child flows, start other flows and run actions when an event happens; this page shows each.

---

## How to Use It

### Wait, Signals and Child Flows

A task can wait for something before it queues for its resource: its `wait`. A wait has one branch, or `any`, `all` or `at_least` of several named branches, which may themselves be groups so waits nest; an empty `any` or `all` succeeds at once. SimPy's `AnyOf` and `AllOf` wait for the first or for every event, and a `Condition` with a count rule for `at_least`. [Wait for One or All](../../guides/wait-for-one-or-all.md) shows the groups. A leaf branch is one of:

| Branch | Waits for | Its value |
|---|---|---|
| `{signal: name}` | the next firing of a signal declared under `signals` (builder `add_signal`) | the firing's value, the payload of the task that fired it |
| `{task: name}` | a **child flow**: that task, started for the same entity with its own instance id, and the tasks its `next` leads to, until the flow ends | the last task's finished payload |
| `{timeout: 40 s}`, or `{timeout: {after: 40 s, value: ...}}` | a time; `value` is carried to the waiter, as SimPy's `env.timeout(delay, value)` | `None`, or the `value` given |
| `{condition: level.stock >= 5}` | a condition to hold, checked when the wait starts and again after every store or container change, change in a queue that the condition reads, registry update, signal firing and finished task | `True` |
| `{request: name}` | a unit of a resource; a won unit is held through the service and given back when the task ends | the request |
| `{get: {store: name}}` or `{get: {container: name, amount: 2}}` | an item or an amount, as a task `get` does | the item taken |
| `{put: {store: name}}` or `{put: {container: name, amount: 2}}` | room to add the task's payload or an amount | none |
| `{running: {task: name, where: ...}}` | the running instances of a task on the waiter's entity and the entities it spawned to all end, as SimPy's `yield process`; `where` narrows them, read in each instance's scope; none matching is done at once | the one instance's finished payload, or a mapping from instance id to payload for several, in start order |

A single branch is named after its signal, task or resource, or `timeout`, `condition`, `get` or `put`. After the wait, `wait.<branch>.done` says whether the branch happened before the wait ended, `.ok` whether it succeeded, and `.value` (or `.value.<field>`) is its value; `next`, `fail` and amounts read them, and a builder function that declares `wait` receives them as a mapping. A task fires a signal with `signal: name` when it finishes, with its payload as the value, and `signal.<name>.count`, `.ok` and `.value` read the signal in any condition. The `process-interaction` example waits for a child flow, then for the first of a courier's signal and a time limit:

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

    The low-level API is SimPy: a child is `env.process(...)`, its value is what it returns, and `env.any_of` and `env.all_of` wait for the first or for every event. `Signal(env, name)` is the named event the builder and YAML use; `env.watch_changes(check)` calls `check` after every change a condition can read, and `env.watch_changes(check, queues=True)` also after a change in the number of puts or gets waiting at a store or container.

The task publishes `queued` when its wait starts, so the wait counts in its wait statistics, and its `patience` covers the wait as well as its resource and its `get`. When an `any` wait ends, its losing signal, timeout and condition branches are dropped, and its losing child flows keep running, as SimPy leaves processes running; [Wait for One or All](../../guides/wait-for-one-or-all.md) shows ties and failures. A wait holds no resource, so waiting for all of several things grants nothing at once.

### One-Shot Signals

A signal written as a mapping with `once: true` (builder `add_signal(name, once=True)`) fires once, as SimPy's `Event` does: it does not re-arm, so a task that waits for it after it has fired resumes at once, and a second firing is refused. A plain signal keeps re-arming after each firing, so a waiter that starts later waits for the next. A closing-time signal is a one-shot: `signals: {closing: {once: true}}`.

### Start a Flow Without Waiting

A task's `spawn` starts another task as a **new entity**, keyed `<task>-n`, and goes on at once, as SimPy's `env.process` starts a process without yielding it. A courier that leaves once an order is packed is a spawn: `spawn: {task: courier}` on the pack task, or `@app.task(spawn={"task": "courier"})`; a process spawns a generator with `context.spawn(generator)`. The spawned entity enters and leaves the model as any entity does, so a drain waits for it, and it counts as a flow of its own under the spawned task's name. Its records carry its own key, and the spawning entity's key is read as `attr.parent`. A spawned task cannot share its name with an arrival, because their flow statistics and entity numbers would merge.

`spawn: {task: courier, after: 3 s}` starts the new entity that long after the task finishes, as SimPy's `start_delayed`; `context.spawn(generator, delay=3)` does the same for a process. A drain that ends during the delay ends the run before the spawned entity starts, as it does for an arrival not yet due. An `after` of 0 or less is refused.

### Be Told When a Flow Ends

A task's `subscribe` asks to be interrupted when something happens, rather than pausing for it: `subscribe: {task: charger}` starts the `charger` child flow and interrupts the task when it ends, with the cause `{"event": "charger", "value": <charger's payload>}`. The task follows its `on_interrupt` then, as it does for any interrupt. `context.subscribe(event)` subscribes a process to any SimPy event, a process included, as SimPy's `subscribe_at`; it refuses an event that has already happened.

### Actions When an Event Happens

An `on:` list runs actions when a signal fires or a task ends, before any waiter, as a SimPy callback runs when its event fires. It hangs on a task (every end: finish, balk, renege, abandon, failure) or on a signal, and each entry is one of:

- `fire: {signal: name}` fires a signal. `value` fires it with a literal value; `from: name` instead copies the latest outcome of a signal, or the finished payload (or failure reason) of the task with that name that last ended on the entity, as SimPy's `trigger` copies another event's outcome. With nothing to copy yet it fires nothing and logs a warning. `value` and `from` together are refused. On a one-shot signal, `urgent: true` makes the waiters resume before the other events at their time, as SimPy's `URGENT` priority orders them, and `after: 5 s` fires it that long from now, as `env.schedule(event, priority, delay)` does. The signal counts as fired now, so a second `fire` is refused, and a drain does not count the entity that fired it as waiting for the later firing. On a signal that re-arms, both are refused at build with `once: true` named as the fix.
- `spawn: {task, after}` starts a new entity, as the `spawn` key does.
- `interrupt: {task, where, cause}` interrupts running instances, as a task's `interrupt` does.
- `path` with `value` sets a registry value, as a scenario step does.

A drain waits for a task's `on:` actions and for the `on:` actions of signals its tasks fire, so a final task that fires a signal with `on:` actions leaves only after they have run.

### Example: Process Events

The `process-events` example puts the event features together: a guard who starts an alarm round and is told when it ends (`subscribe`), a packed order that fires the `ready` signal from its own payload (an `on:` action with `fire ... from`) for a waiting shipper, and a one-shot `closing` signal whose `on:` spawns a sweep one second later (`spawn` with `after`). The arrival gaps are set so each happens once. It needs no Docker service and ends on its own after 12 simulation seconds.

```yaml title="examples/yaml/process_events.yaml:events"
signals:
  # A plain signal re-arms; the one-shot closing fires once and spawns a sweep
  ready: {}
  closing:
    once: true
    on:
      - spawn: {task: sweep, after: 1 s}

arrivals:
  orders: {dist: constant, value: 6, spawn: pack}
  shippers: {dist: constant, value: 7, spawn: ship}
  closers: {dist: constant, value: 9, spawn: close}
  guards: {dist: constant, value: 8, spawn: guard}

tasks:
  # A packed order fires ready from its own finished payload; a shipper reads it
  pack:
    service: packing
    payload: {event_type: order_packed}
    on:
      - fire: {signal: ready, from: pack}
  ship:
    wait: {signal: ready}
    payload: {event_type: order_shipped}
  # Fires the one-shot closing when the shop closes
  close:
    signal: closing
    payload: {event_type: shop_closed}
  sweep:
    service: sweeping
    payload: {event_type: floor_swept}
  # The guard starts an alarm round and is interrupted when it ends
  guard:
    service: guarding
    subscribe: {task: alarm}
    on_interrupt: abandon
    payload: {event_type: shift_done}
  alarm:
    service: alarming
    payload: {event_type: alarm_rang}
```

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get process-events --api yaml
        cd process-events/yaml

        # 3. Run it
        desflow run process_events.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get process-events --api yaml
        cd process-events/yaml

        # 3. Run it
        desflow run process_events.yaml
        ```

    The whole file is [`examples/yaml/process_events.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/process_events.yaml).

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get process-events --api builder
        cd process-events/builder

        # 3. Run it
        uv run --no-project --with desflow process_events_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get process-events --api builder
        cd process-events/builder

        # 3. Run it
        python process_events_example.py
        ```

    The whole file is [`examples/builder/process_events_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/process_events_example.py).

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get process-events --api low-level
        cd process-events/low-level

        # 3. Run it
        uv run --no-project --with desflow process_events_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get process-events --api low-level
        cd process-events/low-level

        # 3. Run it
        python process_events_example.py
        ```

    The whole file is [`examples/low-level/process_events_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/process_events_example.py).
