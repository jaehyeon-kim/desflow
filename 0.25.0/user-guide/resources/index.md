# Resources: `DynamicResource`

A resource is what an entity must hold while it works, and may have to wait for. This page shows how to declare one, how its queue is ordered, how it hands out units, and what happens when its capacity changes during a run.

______________________________________________________________________

## What It Is

A resource is a number of identical units: packers, packing benches, checkout lanes, loading bays. A task asks for one unit, waits in the resource's queue while every unit is busy, holds the unit while it works, and gives it back. How many units there are is the resource's capacity, and it can change while the run goes on, as when packers go on a break.

______________________________________________________________________

## Pages in This Section

- [Priority and Preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/index.md): rank waiting requests by task priority, let a more urgent request take a unit from a less urgent task, and fail a resource at random. A lower `priority` value is more urgent.
- [Several Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/index.md): hold several resources in one task, and the order in which tasks acquire them.
- [Custom Resource Types](https://jaehyeon.me/desflow/latest/user-guide/resources/custom-resource-types/index.md): change how a resource grants units with your own class.

______________________________________________________________________

## How to Use It

### Declare a Resource

A resource has a `current_cap`, the number of units now, and a `max_cap`, the most a live change can set. Both are whole numbers, and both may be left out: `current_cap` is then 1 and `max_cap` is the `current_cap`, so `packers: {}` is a resource of one unit, as `simpy.Resource(env)` is. `current_cap: .inf` (`float("inf")` in Python) is a resource with no limit: every request is granted at once, `capacity` is published as `null` and `utilization` as 0. `queue_limit` sets the most requests that may wait, as [Queue Limit](#queue-limit) describes. `run()` builds a `DynamicResource` for each one the context declares. In the low-level API you build it yourself, after registering the parameters. A resource of one unit needs no registration at all:

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

The last line is a capacity drop, which [Under Live Changes](#under-live-changes) describes below. The same four requests, drawn:

### Queue Limit

A resource with a `queue_limit` of 5 (`queue_limit: 5` in a blueprint, `add_resource(..., queue_limit=5)` in the builder, and the same argument of `DynamicResource`) refuses a request made while five requests wait already, as SimPy's `SortedQueue(maxlen=5)` does. The refusal comes before a grant or a preemption, so a limit of 0 refuses every request. A task that is refused balks: it gives back the units it holds, publishes `balked` with the name of the resource, counts as balked and ends its flow. When the limited resource is the first one the task asks for, and the task has no wait before it, the task balks before it publishes `queued`. A `request` branch of a `wait` on a full resource, in an `any` or in an `all`, fails the wait at once, with SimPy's message "Cannot append event. Queue is full.". When a `request` branch has won its wait and the unit is then taken by preemption, the task asks for a unit again. It balks if the line is full then, and it reneges if its patience runs out while it waits, as for a `resource:` unit. In the low-level API `request()` raises `QueueFull` (in `desflow.resources.resource`), a `RuntimeError` with that message, which your process catches as it would catch a balk condition. A reneged or cancelled request leaves the line, which makes room for the next. [Refuse to Join](https://jaehyeon.me/desflow/latest/tutorials/patience/#refuse-to-join) shows the other way an order balks, by a condition of its own.

In a blueprint, `queue_limit` sits beside the capacity. `packers` has the default of one unit, and refuses a fourth waiting request:

```yaml
resources:
  # No capacity: one unit. At most 3 requests wait; a fourth is refused.
  packers: {queue_limit: 3}
  # No limit: every request is granted at once
  benches: {current_cap: .inf}
```

### Cancel and Release

A task leaves a queue when its patience runs out, when it is interrupted, or under `on_interrupt: abandon`. It then cancels its request, so the request is never granted later. Once a unit is granted, the task gives it back when it finishes, when it is interrupted, and when it is preempted (the preempting task has it then). In the low-level API these are SimPy's calls:

```python
held = packers.request()  # granted at once: the packer was free
waiting = packers.request()  # waits behind it

waiting.cancel()  # leaves the queue at once; it is never granted
held.cancel()  # granted already, so this releases nothing, as in SimPy
assert packers.in_use == 1 and packers.queue == []

released = packers.release(held)  # gives the packer back
assert released.request is held  # the release is an event that holds its request
packers.release(held)  # a second release does nothing
```

`request.cancel()` takes a waiting request out of the queue, and does nothing to a granted one. `packers.release(request)` gives a granted unit back once. Releasing it again, or releasing a request that was never granted, gives nothing back. A release is an event, a `DynamicRelease`, as SimPy's `Release` is. Its `request` is the request it releases (with no request, the oldest unit held). It is triggered as soon as it is made, and the next waiting request is granted one event later, when the release is processed. A `with` block does both on its way out, as in SimPy.

### Read the Queue and Users

A resource counts the units held and the requests waiting. In a condition, `users.<resource>` is how many units are held, and `queue.<resource>` is how many requests wait. A task can therefore turn an order away when the line is long, or wait until nobody holds a unit. The names are checked when the blueprint loads, as `level.<name>` is:

```yaml
tasks:
  pack_order:
    service: packing
    resource: packers
    # Turn an order away when two are waiting already
    balk: queue.packers >= 2
    payload: {event_type: order_packed}
  close_up:
    # Wait until nobody holds a packer
    wait: {condition: users.packers < 1}
    payload: {event_type: packers_idle}
```

`context.get_resource("packers")` returns the running `DynamicResource`. Its `count` is the number of units held. `users` lists the requests that hold them, in the order of their grants. `queue` lists the waiting requests in the order they will be served, so `queue[0]` is served next. A request has `usage_since`, the time it was granted its unit, and `proc`, the process that made it. A task's process has the task's name.

```python
def look(context):
    """A process of the model: reads the packers every 30 seconds."""
    packers = context.get_resource("packers")
    while True:
        yield context.env.timeout(30)
        held = [
            request.proc.name for request in packers.users
        ]  # the tasks holding a unit
        seen.append((packers.count, len(packers.queue), held))


app.add_process(look)
```

```python
requests = [packers.request() for _ in range(4)]  # two are granted, two wait

assert packers.count == len(packers.users) == 2  # the units held now
assert packers.users == requests[:2]  # the granted requests, in the order of the grants
assert len(packers.queue) == 2  # the requests waiting
assert packers.queue[0] is requests[2]  # the next to be served
assert packers.users[0].usage_since == 0.0  # when this request was granted its unit
```

`count` is the same as `in_use`, SimPy's name for it. `queue` is a list of the waiting requests, in service order.

### Registry Paths and Readings

Each resource has two registry paths, `<sim_id>.resources.<name>.current_cap` and `.max_cap`, which a scenario, an ingress message or a process can change. [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-parameters/index.md) explains how. A resource that breaks down adds `<sim_id>.breakdown.<name>.time_to_failure` and `.repair`, its two distributions. With statistics at the `minimal` level or above, each resource publishes four readings, such as `Shop.stats.resources.packers.utilization`: `capacity`, `in_use`, `queue_length` and `utilization`. `utilization` is the busy units as a percentage of the capacity. A resource with no limit publishes a `capacity` of `null` and a `utilization` of 0. A resource that breaks down adds `down`, which is 1 while it is down. [Statistics](https://jaehyeon.me/desflow/latest/user-guide/statistics/index.md) lists what the other levels add, among them `downtime` and `failures`.

______________________________________________________________________

## How It Works

A `DynamicResource` grants units the way SimPy's `Resource` does, with two lists. `queue` holds the waiting requests, ranked by the queue order and then by a sequence number, so requests that rank the same are served in the order they arrived. `users` holds the granted requests. A request is granted when it is made if a unit is free and nothing waits ahead of it. Otherwise it waits, and each release grants the head of the queue when its release event is processed, one event later, as SimPy's `Release` does:

Because each step is the one SimPy takes, two events at the same simulation time are granted in SimPy's order: a waiting order whose patience runs out at the instant a unit is released reneges or is served exactly as it would in plain SimPy. `in_use` counts `users`: it goes up when a request is granted and down when a unit is released.

A waiting request leaves the queue at once when it is cancelled. A `with` block cancels it when the block is left before the grant, and `request.cancel()` does the same by hand. `packers.release(request)` gives a granted unit back. Cancelling a granted request releases nothing, and releasing a request twice, or one never granted, gives nothing back, as in SimPy. Every release still tries the head of the queue when its release event is processed, as SimPy's does: when a request that reneged was the head, the request behind it may then be granted or preempt a user.

**SimPy and desflow.** SimPy's `Resource` serves requests in the order they arrive, and `PriorityResource` by priority. Both take their capacity when they are created and keep it: `capacity` has no setter, so assigning a new one raises `AttributeError` (SimPy 4.1). desflow builds its own resource, so that the capacity can follow the registry and the queue order can be chosen. The `request()` call and the `with` block are SimPy's, so code written for SimPy works unchanged. [SimPy and desflow](https://jaehyeon.me/desflow/latest/des/simpy-and-desflow/index.md) compares the two.

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

Until the owed units finish, `in_use` is above `capacity`, and the `utilization` reading is above 100%.

______________________________________________________________________

## Stability and Limits

- **Utilisation decides waits.** A resource whose load is at or above its capacity has a queue that grows for as long as the run lasts. [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md) shows how to work it out, and [Queueing Basics](https://jaehyeon.me/desflow/latest/des/queueing-basics/#utilisation) explains utilisation.
- **Priority needs `queue: priority`.** A task priority on a resource of another order is a load error, while a low-level request's priority is ignored under `fifo` and `lifo`.
- **Priority can starve.** Under `priority`, a request with a high number is served only when no lower number is waiting. If the low numbers alone keep the resource busy, it is never served.
- **One order of acquisition.** A model whose tasks acquire two resources in opposite orders is refused. [Several Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/#several-resources) explains why.
- **A shrink never stops work.** Lowering `current_cap` lets busy units finish. Work in progress stops only for an interrupt, a preemption or a breakdown: [Interruptions](https://jaehyeon.me/desflow/latest/tutorials/interruptions/index.md), and [Model Breakdowns](https://jaehyeon.me/desflow/latest/guides/breakdowns/index.md) for a machine that fails.
- **A preempted task queues again.** With `resume` or `restart` it joins the back of its priority among the waiting requests, as a new SimPy request does. The time it waited before counts in its wait.

Amounts and items that tasks take and add, rather than hold, are [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/index.md).
