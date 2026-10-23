# Priority and Preemption

Task priority, preemption and breakdowns are the forms of a [resource](index.md) that rank waiting requests and take units from tasks.

---

## How to Use It

### Task Priority

Under `queue: priority`, each task gives its requests a priority: a number, the same for every run of the task; an expression, read when the task joins the queue, such as `attr.tier` for the entity's attribute `tier`; or, in the builder, a function of the task id and the entity's attributes. The attributes exist before the request queues, and the task's own payload does not, so a priority cannot read it; an expression can read the previous task's payload as `payload.<field>`. The lowest priority is served first, and a task with no `priority` ranks 0, as a SimPy `PriorityRequest` does. Equal priorities are served first in, first out, which is the order of SimPy's `PriorityResource`: it ranks a request by `(priority, time, not preempt)`, so among equal priorities and times a request that may preempt goes before one with `preempt: false`. The tests check it, ties included, in all three APIs ([SimPy Compatibility](../../reference/simpy-compatibility.md)).

=== "YAML"

    ```yaml title="docs/snippets/yaml/resources.yaml:priority"
    tasks:
      pack_order:
        service: packing
        resource: packers
        # A number, or an expression read when the order joins the queue
        priority: attr.tier
        payload: {event_type: order_packed}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/resources.py:priority" hl_lines="14"
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

=== "Low-level"

    A low-level process passes the priority to the request: `packers.request(priority=entity.attributes["tier"])`, as the queue-order example above does; a request with no priority has priority 0. A resource whose queue is `fifo` or `lifo` ignores it.

A `priority` on a task whose resource does not have `queue: priority` is refused: when the blueprint loads, or when `run()` starts. [Priorities](../../tutorials/priorities.md) shows what priority does to waits.

### Preemption

On a resource with `preempt: true`, which needs `queue: priority`, a request that finds every unit busy takes a unit from a task of a higher priority value, as SimPy's `PreemptiveResource` does. The task that loses the unit is chosen by SimPy's rule: the user whose `(priority, request time, may not preempt)` sorts last, and only when that sorts after the new request, so equal priorities do not preempt; the one exception is a user whose request was made at the same moment as the new one with `preempt: false`, which sorts after a request that may preempt. The task publishes `preempted` with the resource's name, the key of the entity that took the unit (`preempted_by`) and the time the task was granted the unit (`usage_since`), and follows its `on_interrupt`, as an interrupt would ([Interruptions](../../tutorials/interruptions.md)); a task with `preempt: false` never takes a unit this way. The `interruptions` example lets express orders preempt regular ones:

=== "YAML"

    ```yaml title="examples/yaml/interruptions.yaml:packers"
    # A lower priority value takes the packer from a higher one.
    packers: {current_cap: 1, max_cap: 4, queue: priority, preempt: true}
    ```

    ```yaml title="examples/yaml/interruptions.yaml:pack"
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

=== "Builder"

    ```python title="examples/builder/interruptions_example.py:packers"
    # A lower priority value takes the packer from a higher one
    .add_resource("packers", current_cap=1, max_cap=4, queue="priority", preempt=True)
    ```

    ```python title="examples/builder/interruptions_example.py:pack"
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

=== "Low-level"

    ```python title="examples/low-level/interruptions_example.py:pack"
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
        shop["keys"][env.active_process] = key
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
                        "preempted_by": shop["keys"].get(interrupt.cause.by),
                        "usage_since": interrupt.cause.usage_since,
                    }
                    publish(value)
                    env.stats.task_interrupted("pack", task_id, preempted=True)
                else:
                    value = {"path_id": path_id, "status": "interrupted"}
                    publish({**value, "cause": interrupt.cause})
                    env.stats.task_interrupted("pack", task_id)
        shop["packing"].remove(env.active_process)
        del shop["keys"][env.active_process]
        env.stats.flow_finished(key.rsplit("-", 1)[0])
    ```

    `DynamicResource(env, "Shop", "packers", queue="priority", preempt=True)` builds the resource, and `request(priority, preempt=True)` asks for a unit; the process that loses its unit receives `simpy.Interrupt` with a SimPy `Preempted` cause, whose `resource`, `by` (the process that took the unit) and `usage_since` say what happened.

### Breakdowns

A `breakdown` makes a resource fail at random and be repaired. `time_to_failure` is the time from the start, or from a repair, to the next failure, on calendar time, busy or idle; `repair` is how long a repair takes. While the resource is down its effective capacity is 0 and no request is granted:

=== "YAML"

    ```yaml title="examples/yaml/breakdowns.yaml:station"
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

=== "Builder"

    ```python title="examples/builder/breakdowns_example.py:station"
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

=== "Low-level"

    ```python title="examples/low-level/breakdowns_example.py:station"
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

`work` says what happens to the tasks being served: `interrupt`, the default, interrupts each with the cause `breakdown`, and it follows its `on_interrupt`; `finish` lets them finish. Each failure and repair publishes an event keyed by the resource's name, with `status` `down` or `repaired` and `reason: failure`. [Breakdowns](../../guides/breakdowns.md) checks the downtime of the example, and [Interruptions](../../tutorials/interruptions.md) explains `on_interrupt`.

---

## Stability and Limits

The limits of this page are listed with those of the whole section, in [Stability and Limits](index.md#stability-and-limits) on the Resources page.

---

## Example: Interruptions

The `interruptions` example: express orders preempt regular ones, and a fire drill at 60 seconds interrupts the order being packed; each resumes with the packing time it had left. It needs no Docker service and ends on its own after 120 simulation seconds.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get interruptions --api yaml
        cd interruptions/yaml

        # 3. Run it
        desflow run interruptions.yaml
        ```

    === "pip"

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

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get interruptions --api builder
        cd interruptions/builder

        # 3. Run it
        uv run --no-project --with desflow interruptions_example.py
        ```

    === "pip"

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

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get interruptions --api low-level
        cd interruptions/low-level

        # 3. Run it
        uv run --no-project --with desflow interruptions_example.py
        ```

    === "pip"

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
