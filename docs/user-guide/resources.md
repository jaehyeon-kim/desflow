# Resources: `DynamicResource`

A resource is what an entity must hold while it works, and may have to wait for. This page shows how to declare one, how its queue is ordered, how it hands out units, and what happens when its capacity changes during a run.

---

## What It Is

A resource is a number of identical units: packers, packing benches, checkout lanes, loading bays. A task asks for one unit, waits in the resource's queue while every unit is busy, holds the unit while it works, and gives it back. How many units there are is the resource's capacity, and it can change while the run goes on, as when packers go on a break.

---

## How to Use It

### Declare a Resource

A resource has a `current_cap`, the number of units now, and a `max_cap`, the most a live change can set. `run()` builds a `DynamicResource` for each one the context declares; in the low-level API you build it after registering the parameters:

=== "YAML"

    ```yaml title="docs/snippets/yaml/resources.yaml:queue-order"
    resources:
      # fifo (the default), lifo or priority; priority serves the lowest number first,
      # and equal priorities first in, first out
      packers: {current_cap: 1, max_cap: 4, queue: priority}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/resources.py:declare"
    app = SimulationContext(sim_id="Shop", factor=0.0)
    # fifo (the default), lifo or priority
    app.add_resource("packers", current_cap=1, max_cap=4, queue="priority")
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/resources.py:resource"
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

```python title="docs/snippets/pages/resources.py:queue-order"
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

<!-- records: resources-printed -->
```text
fifo     orders-1, express-1, orders-2, express-2
lifo     express-2, orders-2, express-1, orders-1
priority express-1, express-2, orders-1, orders-2
capacity 2, in use 4
```

The last line is the capacity drop that Under Live Changes describes, below. The same four requests, drawn:

![Four requests join in the order orders-1, express-1, orders-2, express-2. fifo serves them in that order; lifo serves express-2, orders-2, express-1, orders-1; priority serves express-1 and express-2 first, then orders-1 and orders-2](../assets/diagrams/queue-order.png)

### Task Priority

Under `queue: priority`, each task gives its requests a priority: a number, the same for every run of the task; an expression, read when the task joins the queue, such as `attr.tier` for the entity's attribute `tier`; or, in the builder, a function of the task id and the entity's attributes. The attributes exist before the request queues, and the task's own payload does not, so a priority cannot read it; an expression can read the previous task's payload as `payload.<field>`.

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

    A low-level process passes the priority to the request: `packers.request(priority=entity.attributes["tier"])`, as the queue-order example above does. A resource whose queue is `fifo` or `lifo` ignores it.

A `priority` on a task whose resource does not have `queue: priority` is refused: when the blueprint loads, or when `run()` starts. [Priorities](../tutorials/priorities.md) shows what priority does to waits.

### Registry Paths and Readings

Each resource has two registry paths, `<sim_id>.resources.<name>.current_cap` and `.max_cap`, which a scenario, an ingress message or a process can change; [Live Parameters](live-parameters.md) explains how. With statistics at the `minimal` level or above, each resource publishes four readings, such as `Shop.stats.resources.packers.utilization`: `capacity`, `in_use`, `queue_length` and `utilization`, the busy units as a percentage of the capacity. [Statistics](statistics.md) lists what the other levels add.

---

## How It Works

A `DynamicResource` is built from SimPy parts. Each request waits in a SimPy `PriorityStore`, ranked by the queue order and a sequence number, so requests that rank the same are served in the order they arrived. Free units are tokens in a SimPy `Container`, the pool, whose size follows the registry. A dispatcher process waits for a request and a token, and gives the token to the first request:

![A task's request waits in the request queue, a PriorityStore ordered by queue order and a sequence number; the dispatcher, a SimPy process, takes the next request and a token from the pool, a Container with one token per free unit whose size follows current_cap and max_cap in the registry, and grants it](../assets/diagrams/resource-dispatch.png)

If the request it takes was cancelled, the dispatcher puts the token back and takes the next. `in_use` counts the units held by tasks: it goes up when a request is granted and down when a unit is released.

**SimPy and desflow.** SimPy's `Resource` serves requests in the order they arrive, and `PriorityResource` by priority. Both take their capacity when they are created and keep it: `capacity` has no setter, so assigning a new one raises `AttributeError` (SimPy 4.1). desflow builds its own resource so the capacity can follow the registry and the queue order can be chosen; the `request()` call and the `with` block are SimPy's, so code written for SimPy works unchanged. [SimPy and desflow](../des/simpy-and-desflow.md) compares the two.

---

## Under Live Changes

A change to `current_cap` takes effect at once, without stopping any work:

- **Growing** adds tokens to the pool, and waiting requests start.
- **Shrinking** removes idle tokens at once. The units still busy above the new capacity are owed: while tokens are owed, each released token is discarded instead of returning to the pool, so no waiting task starts until the busy units are within the new capacity.
- **Growing again** before the owed tokens arrive first cancels them, then adds any difference to the pool.
- A `current_cap` outside 0 to `max_cap` is clamped to that range, with a warning.
- **Lowering `max_cap`** lowers the pool's limit. A `current_cap` above the new `max_cap` is stored as `max_cap`, with a warning.

The example above takes three of five packers off the floor while four orders are being packed: the capacity is 2 at once, and `in_use` stays 4 until two orders finish. The animation steps through it:

![Capacity drops from 5 to 2 with 4 units busy: the idle token leaves, two tokens are owed and discarded as tasks finish, and only then does a waiting task start](../assets/diagrams/resource-shrink.gif)

```python title="docs/snippets/pages/resources.py:shrink"
def breakdown(env):
    """Takes 3 of 5 packers off the floor at t=5 s. Orders being packed are not stopped."""
    env.registry.update("Shop.resources.packers.current_cap", 5)
    yield env.timeout(5.0)
    env.registry.update("Shop.resources.packers.current_cap", 2)
    yield env.timeout(0.1)  # the resource applies the change in its own process
    print(f"capacity {packers.capacity}, in use {packers.in_use}")
```

Until the owed units finish, `in_use` is above `capacity`, and the `utilization` reading is above 100.

---

## Stability and Limits

- **Utilisation decides waits.** A resource whose load is at or above its capacity has a queue that grows for as long as the run lasts; [Queues and Capacity](../tutorials/queues-and-capacity.md) shows how to work it out.
- **Priority needs `queue: priority`.** A task priority on a resource of another order is a load error, while a low-level request's priority is ignored under `fifo` and `lifo`.
- **Priority can starve.** Under `priority`, a request with a high number is served only when no lower number is waiting; if the low numbers alone keep the resource busy, it is never served.
- **One resource per task.** A task that needs two resources at once is a process: [Multi-Resource Handoffs](../guides/multi-resource-handoffs.md).
- **A shrink never stops work.** A resource that must stop work in progress, such as a breakdown, needs a process: [Preemptive Breakdowns](../guides/preemptive-breakdowns.md).

Amounts and items that tasks take and add, rather than hold, are [Stores and Containers](stores-and-containers.md).
