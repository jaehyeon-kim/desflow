# SimPy and desflow

desflow is built on [SimPy](https://simpy.readthedocs.io/), a Python library for discrete-event simulation. SimPy provides the event list, the clock, processes and resources, and desflow keeps all of them. This page teaches the SimPy constructs one at a time, each on the packing shop in plain SimPy with no desflow in it, and then lists the four things desflow changes. A low-level desflow model is ordinary SimPy code with these additions.

The code is plain SimPy 4.1.2, the version desflow is tested against. We confirmed each statement about a construct against SimPy's source in that version (`core.py`, `events.py`, `rt.py` and the `resources` package). Every function below runs as a script in the docs tests, and its output is the one shown.

______________________________________________________________________

## Environment and the Event List

An `Environment` holds the clock, `env.now`, and the event list. The list is ordered by time, then by priority, then by the order in which events were scheduled, so two events at one time run in scheduling order unless one has the higher priority (`URGENT` is 0 and `NORMAL` is 1). [What Is DES?](https://jaehyeon.me/desflow/latest/des/how-it-works/index.md) explains why the clock jumps from event to event, and [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/index.md) shows how the order matters at the end of a run.

`env.timeout(delay)` is an event that SimPy schedules `delay` seconds from now, and a negative delay raises `ValueError`. `env.step()` takes the earliest event, moves the clock to its time and runs the event's callbacks. `env.peek()` gives the time of the next event, or infinity when none is left. `env.run(until=...)` calls `step` until the time or the event you give it. The function below schedules two timeouts in the wrong order and steps through them.

```python
def event_list():
    """Two events scheduled out of order are processed in time order."""
    env = simpy.Environment()
    env.timeout(2.6)  # order 2 arrives
    env.timeout(1.5)  # order 1 is packed
    seen = []
    while env.peek() != math.inf:  # peek is the time of the next event
        seen.append((env.peek(), env.now))
        env.step()  # take the earliest event, move the clock to its time
    return seen, env.now
```

Each pair is the time of the next event and the clock before the step. The event at 2.6 was scheduled first, but the list is ordered by time, so the event at 1.5 comes first and the clock ends at 2.6.

```text
event list: [(1.5, 0), (2.6, 1.5)], clock at 2.6
```

In desflow, `env` is a `DynamicRealtimeEnvironment`, which is a SimPy environment with pacing added. The builder and YAML give the same operations as `app.run(until=...)`, `app.step()`, `app.peek()` and `run.until`.

______________________________________________________________________

## Events and Callbacks

An event is something that happens once. It starts pending, becomes **triggered** when `succeed(value)` or `fail(exception)` is called, which schedules it, and becomes **processed** when the environment takes it from the list and runs its **callbacks**. A callback is a function that takes the event, and callbacks run in the order they were added. A process that yields an event waits on it by adding its own resume function to the callbacks, so one event can wake any number of processes. Calling `succeed` twice raises `RuntimeError`, and a failed event that nothing handles stops the run with its exception.

In the shop, packing an order is an event several things care about. Here a callback and two couriers wait on one event, and the packer triggers it at 1.5 seconds.

```python
def events():
    """An event is triggered once, and its callbacks and waiting processes run when it is processed."""
    env = simpy.Environment()
    packed = env.event()
    log = []
    packed.callbacks.append(
        lambda event: log.append(f"callback at {env.now}: {event.value}")
    )

    def courier(name):
        value = yield packed  # every process waiting on the event resumes with its value
        log.append(f"{name} at {env.now}: {value}")

    env.process(courier("courier 1"))
    env.process(courier("courier 2"))

    def packer():
        yield env.timeout(1.5)
        packed.succeed("order 1")  # triggers the event, which schedules it for now

    env.process(packer())
    env.run()
    return log, (packed.triggered, packed.processed, packed.ok)
```

The callback was added first, so it runs first, and both couriers resume at the same instant with the event's value. The last line shows the three states after the run: `triggered` and `processed` are both true, and `ok` is true because the event succeeded.

```text
callback at 1.5: order 1
courier 1 at 1.5: order 1
courier 2 at 1.5: order 1
triggered, processed, ok: (True, True, True)
```

In desflow, a **signal** is an event that tasks wait for and fire, such as a restock. `Signal` is its low-level form, `add_signal` its builder form and `signals:` its YAML form. A failed event that nothing handles stops a SimPy run, and the low-level API keeps that. The builder and YAML publish a `failed` record and go on only by default (`on_unhandled_failure: continue`); the setting `stop` keeps SimPy's behaviour. [Failures](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/failures/index.md) explains both.

______________________________________________________________________

## Processes and Waiting for a Process

A **process** is a Python generator that SimPy runs: each `yield` hands SimPy an event, and the process resumes when that event is processed. `env.process(generator)` starts one. The process is itself an event, which triggers when the generator returns, with the return value. So a process can wait for another by yielding it, and it receives the other's return value. If the child raises an exception, the exception is raised in the parent at its `yield`.

The shop below takes an order every 2 seconds. Each order starts a `pack` process and waits for it, and `pack` takes 1.5 seconds.

```python
def processes():
    """A process is a generator; yielding another process waits for it and returns its value."""
    env = simpy.Environment()
    log = []

    def pack(order):
        yield env.timeout(1.5)
        return f"order {order} packed"

    def shop():
        for order in (1, 2):
            yield env.timeout(2)  # time to the next arrival
            result = yield env.process(pack(order))  # wait for the child
            log.append((env.now, result))

    env.process(shop())
    env.run()
    return log
```

The first order arrives at 2 and is packed at 3.5. The second arrives 2 seconds after the first was packed, at 5.5, and is packed at 7.0.

```text
[(3.5, 'order 1 packed'), (7.0, 'order 2 packed')]
```

In desflow, a task is a process, and `@app.task` and `@app.arrival_loop` write the common ones. A task waits for another task with `wait: {task: ...}` in YAML, `wait={"task": ...}` or `yield context.call(...)` in the builder, and reads the child's last payload as its value. A process you write yourself runs through `add_process`, which is an extension point.

______________________________________________________________________

## Interrupts

`process.interrupt(cause)` raises `simpy.Interrupt` inside the process, at the `yield` where it waits. The cause is any object, and the process reads it as `interrupt.cause`. The interrupt is scheduled as `URGENT`, so it takes effect before any other event at that time. A process that catches the exception may wait again, for the same event or a new one. A process that has ended, or the process itself, cannot be interrupted, and `interrupt` raises `RuntimeError`.

Here a packer is interrupted for a break at 1.0 seconds, in the middle of a 1.5 second packing. It works out how much is left, takes the break, and finishes.

```python
def interrupts():
    """A packer is interrupted at 1.0 s and finds how much of the 1.5 s packing is left."""
    env = simpy.Environment()
    log = []

    def packer():
        left = 1.5
        while left > 0:
            started = env.now
            try:
                yield env.timeout(left)
                left = 0
            except (
                simpy.Interrupt
            ) as interrupt:  # raised inside the process, at its yield
                left -= env.now - started
                log.append((env.now, interrupt.cause, left))
                yield env.timeout(0.5)  # the break
        log.append((env.now, "packed", 0))

    process = env.process(packer())

    def supervisor():
        yield env.timeout(1.0)
        process.interrupt("break")  # the cause is any object

    env.process(supervisor())
    env.run()
    return log
```

The packer is interrupted at 1.0 with 0.5 seconds left, rests for 0.5, and finishes the rest at 2.0.

```text
[(1.0, 'break', 0.5), (2.0, 'packed', 0)]
```

In desflow, `on_interrupt: resume`, `restart` or `abandon` on a task chooses what happens to the packing in progress, as this code does for `resume`, and `interrupt:` sends one. [Interruptions](https://jaehyeon.me/desflow/latest/tutorials/interruptions/index.md) uses all three.

______________________________________________________________________

## Waiting for One or All

`a | b` is an event that triggers when the first of `a` and `b` is processed, and `a & b` is one that triggers when both are. They are the `AnyOf` and `AllOf` classes, and `env.any_of(events)` and `env.all_of(events)` build them from a list. The value is a `ConditionValue`. It holds the events that had been processed when the condition is processed, so `event in result` tells you which ones, and `result.todict()` gives their values.

A courier below waits for an order to be packed or for 1 second to pass, whichever is first. Then it waits for the packing and for another 3 seconds.

```python
def conditions():
    """`|` waits for the first of several events, `&` for all of them."""
    env = simpy.Environment()
    log = []

    def courier():
        packed = env.timeout(2.0, value="packed")
        limit = env.timeout(1.0, value="limit")
        first = yield packed | limit  # AnyOf: holds every event processed so far
        log.append((env.now, packed in first, limit in first))
        both = yield packed & env.timeout(3.0)  # AllOf: waits for both
        log.append((env.now, len(both.todict())))

    env.process(courier())
    env.run()
    return log
```

At 1.0 the limit was processed and the packing was not, so the courier gave up. The second wait ends at 4.0, when the timeout created at 1.0 triggers, and both events are in its value.

```text
[(1.0, False, True), (4.0, 2)]
```

In desflow, `wait: {any: ...}` and `wait: {all: ...}` in YAML and `wait={"any": ...}` in the builder wait on named branches of signals, tasks and timeouts. [Wait for One or All](https://jaehyeon.me/desflow/latest/guides/wait-for-one-or-all/index.md) shows ties and failures.

______________________________________________________________________

## Resources

A `Resource(env, capacity)` has `capacity` units, one by default. `request()` returns an event that triggers when a unit is free, and requests are served in the order they arrive. Used in a `with` block, a request is released when the block ends, and a request still waiting at that point is cancelled. `count` is the units in use, `users` the granted requests and `queue` the waiting ones.

Three orders arrive at 0 and the shop has one packer. Each waits for the one before it.

```python
def resource():
    """Three orders at 0 s and one packer: each waits in the queue for the one before."""
    env = simpy.Environment()
    packers = simpy.Resource(env, capacity=1)
    log = []

    def order(name):
        with packers.request() as request:  # leaving the block releases the packer
            yield request
            log.append((name, env.now))
            yield env.timeout(1.5)

    for name in "ABC":
        env.process(order(name))
    env.run()
    return log
```

```text
[('A', 0), ('B', 1.5), ('C', 3.0)]
```

`PriorityResource` takes a `priority` with each request: the lowest number is served first, and requests with the same priority are served in arrival order. In the shop below, the packer is busy with an order at 0. A standard order arrives at 0.5 and an express order at 1.0, and the express order is served first.

```python
def priority():
    """The packer is busy at 0 s; a lower priority number is served first, ties in arrival order."""
    env = simpy.Environment()
    packers = simpy.PriorityResource(env, capacity=1)
    log = []

    def order(name, priority, arrives):
        yield env.timeout(arrives)
        with packers.request(priority=priority) as request:
            yield request
            log.append((name, env.now))
            yield env.timeout(1.5)

    env.process(order("first", priority=1, arrives=0))
    env.process(order("standard", priority=1, arrives=0.5))
    env.process(order("express", priority=0, arrives=1.0))
    env.run()
    return log
```

```text
[('first', 0), ('express', 1.5), ('standard', 3.0)]
```

`PreemptiveResource` adds preemption: a request with a lower number than a current user, and `preempt=True` (the default), takes that user's unit. The user is interrupted, and `interrupt.cause` is a `Preempted` object with `by`, `usage_since` and `resource`. The preempted process does not get the unit back, so it must request again if it wants to continue. Below, an express order arrives at 1.0 and interrupts a standard order that has used the packer since 0.

```python
def preemptive():
    """An express order takes the packer from a standard one, which is interrupted."""
    env = simpy.Environment()
    packers = simpy.PreemptiveResource(env, capacity=1)
    log = []

    def standard():
        with packers.request(priority=1) as request:
            yield request
            try:
                yield env.timeout(1.5)
                log.append(("standard packed", env.now))
            except simpy.Interrupt as interrupt:
                cause = interrupt.cause  # a Preempted object
                log.append(("standard preempted", env.now, cause.usage_since))

    def express():
        yield env.timeout(1.0)
        with packers.request(priority=0) as request:  # preempt=True is the default
            yield request
            yield env.timeout(1.0)
            log.append(("express packed", env.now))

    env.process(standard())
    env.process(express())
    env.run()
    return log
```

```text
[('standard preempted', 1.0, 0), ('express packed', 2.0)]
```

In desflow, `DynamicResource` is all three: `queue: fifo` is `Resource`, `queue: priority` is `PriorityResource`, and `preempt: true` with it is `PreemptiveResource`. It also follows a capacity in the registry. [Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/index.md) and [Priority and Preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/index.md) explain them.

______________________________________________________________________

## Containers and Stores

A `Container(env, capacity, init)` holds a quantity, such as boxes on a shelf. `put(amount)` waits while there is no room and `get(amount)` waits until enough is held, and `level` is the quantity now. Waiting requests are served in order, so a large `get` at the head holds back smaller ones behind it. Here the packer needs 3 boxes while the shelf holds 2, so it waits for a delivery of 5 at 4 seconds.

```python
def container():
    """A shelf holds a quantity of boxes: a get for 3 waits until a put makes them available."""
    env = simpy.Environment()
    shelf = simpy.Container(env, capacity=10, init=2)
    log = []

    def packer():
        yield shelf.get(3)
        log.append(("took 3 boxes", env.now, shelf.level))

    def delivery():
        yield env.timeout(4)
        yield shelf.put(5)  # the waiting get is served as the boxes arrive

    env.process(packer())
    env.process(delivery())
    env.run()
    return log
```

The `get` is served as the delivery arrives, and the shelf then holds 4 of the 7 boxes.

```text
[('took 3 boxes', 4, 4)]
```

A `Store` holds distinct items rather than a quantity. `put(item)` waits while it is full, and `get()` waits while it is empty and returns the oldest item. A `FilterStore` lets `get` take a function that picks the first item it accepts. A `PriorityStore` returns the smallest item first, and `PriorityItem(priority, item)` wraps items that cannot be compared. The three below hold the shop's parcels, which differ in size and in urgency.

```python
def stores():
    """A store holds distinct items; the filtered and priority kinds change which one a get returns."""
    env = simpy.Environment()
    parcels = simpy.Store(env)
    by_size = simpy.FilterStore(env)
    by_urgency = simpy.PriorityStore(env)
    got = {}

    def run():
        for name, size in (("p1", "small"), ("p2", "large"), ("p3", "small")):
            yield parcels.put(name)
            yield by_size.put((name, size))
        yield by_urgency.put(simpy.PriorityItem(2, "standard"))
        yield by_urgency.put(simpy.PriorityItem(1, "express"))
        got["store"] = yield parcels.get()  # first in, first out
        got["filter"] = yield by_size.get(
            lambda item: item[1] == "large"
        )  # first match
        got["priority"] = (yield by_urgency.get()).item  # smallest priority first

    env.process(run())
    env.run()
    return got
```

```text
{'store': 'p1', 'filter': ('p2', 'large'), 'priority': 'express'}
```

In desflow, a task's `put` and `get` use these: `put: {container: stock, amount: 2}` for a container, and `get: {store: parcels, filter: ...}` for a filtered store. A store declared with `priority: true` is a `PriorityStore`. [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/index.md) and [Stock](https://jaehyeon.me/desflow/latest/tutorials/stock/index.md) explain them.

______________________________________________________________________

## Real-time Environment

`RealtimeEnvironment(factor)` is an environment whose clock follows the wall clock: a step of one time unit takes `factor` seconds of real time. With `strict=True`, the default, `step` raises `RuntimeError` when the model falls behind by more than one `factor`. Set `strict=False` to let it catch up instead. `sync()` restarts the real-time reference from the present. One simulated second takes 0.2 seconds here, so the 2 second run takes at least 0.4.

```python
def realtime():
    """One simulated second takes 0.2 wall-clock seconds; the run lasts at least 2 x 0.2 s."""
    env = simpy.rt.RealtimeEnvironment(factor=0.2)  # strict=True is the default

    def packer():
        yield env.timeout(2)

    env.process(packer())
    started = time.monotonic()
    env.run()
    return env.now, time.monotonic() - started
```

```text
clock 2, wall clock at least 0.4 s: True
```

SimPy's `factor` is fixed when the environment is made. desflow's environment changes that, as the table at the end of this page shows. [Time and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/index.md) explains it.

______________________________________________________________________

## What desflow Changes

desflow keeps SimPy's event list, clock, processes and resources, and changes four things: the clock can switch pacing, every parameter can change while the run goes on, random times are named distributions, and everything that happens is published as records.

| Part             | In SimPy                                                                                                                                                                                                                                            | In desflow                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Clock and pacing | `Environment` runs without waiting; `RealtimeEnvironment(factor, strict=True)` follows the wall clock and raises `RuntimeError` when the model falls behind                                                                                         | one class, `DynamicRealtimeEnvironment`, does both: `factor=0` runs without waiting, `go_live_at` switches to real time partway through a run (SimPy's `factor` cannot change once set), `logical_start_time` gives every record a date and time, and falling behind is reported as telemetry; `strict` raises `RuntimeError` as SimPy's does, at a `factor` above 0 and never at `factor: 0`, where SimPy's check would raise as soon as the wall clock moves; it is on by default in the low-level API, as in SimPy, and off by default in the builder and in YAML (`simulation.strict`); `sync()` is SimPy's, and `resume_pacing()` also moves the simulation anchor after a pause; `advance(until)` runs a started run in segments, as repeated `env.run(until=...)` does |
| Processes        | generators started with `env.process`                                                                                                                                                                                                               | the same, and the builder adds `@app.task`, `@app.arrival_loop`, `@app.telemetry_loop` and `add_process`, which write the common ones for you                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Random times     | none built in: a model calls `random` or NumPy itself                                                                                                                                                                                               | ten named distributions with checked parameters, drawn by a `Sampler` from the current values each time; each distribution draws from a random sequence of its own, made from the run's seed and its registry path                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Resources        | `Resource`, `Container` and `Store` keep the capacity they were created with: it has no setter, so assigning a new one raises `AttributeError` (SimPy 4.1). `Resource` serves requests in the order they arrive, and `PriorityResource` by priority | `DynamicResource`, `DynamicContainer` and `DynamicStore` follow a capacity in the registry, and a resource that shrinks lets busy units finish. A resource serves its queue `fifo`, `lifo` or by `priority`, equal priorities first in, first out. `run()` builds the stores and containers a model declares, and tasks put to and get from them                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Parameters       | Python variables, changed only by the model's own code                                                                                                                                                                                              | a registry of named paths, changed by connectors, scenarios or processes, with each change checked                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Output           | whatever the model prints or logs                                                                                                                                                                                                                   | events and telemetry with a fixed shape, written to Kafka, Redis, PostgreSQL, files or Iceberg on background threads                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

______________________________________________________________________

## Each Construct in desflow

Each row names a construct, its form in the three ways of writing a model, and the group of conformance checks on [SimPy Compatibility](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/index.md) that runs it in plain SimPy and in all three forms and compares the results. A construct with no native form in one of the three is marked as a difference there, with the reason.

| SimPy construct                             | YAML                                                     | Builder                                                   | Low-level                                         | Checks                                                                                                                      |
| ------------------------------------------- | -------------------------------------------------------- | --------------------------------------------------------- | ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `Environment`: `run`, `step`, `peek`, `now` | `run.until`                                              | `app.run(until=)`, `app.step()`, `app.peek()`             | `env.run(until=)`                                 | [Run Control](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c6-run-control)                             |
| `timeout`                                   | a task with `service` and no `resource`                  | `@app.task(service_id=...)`                               | `yield env.timeout(delay)`                        | [Events and Processes](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c1-events-and-processes)           |
| Events and callbacks                        | `signals:`, `on:`                                        | `add_signal`, `on=`                                       | `Signal`, `event.callbacks.append(fn)`            | [Events and Processes](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c1-events-and-processes)           |
| Processes and waiting for one               | `wait: {task: ...}`                                      | `wait={"task": ...}`, `context.call(...)`                 | `yield env.process(child())`                      | [Events and Processes](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c1-events-and-processes)           |
| Interrupts                                  | `interrupt:`, `on_interrupt`                             | `context.interrupt(...)`, `on_interrupt=`                 | `process.interrupt(cause)`                        | [Interrupts and Preemption](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c3-interrupts-and-preemption) |
| `AnyOf`, `AllOf`                            | `wait: {any: ...}`, `wait: {all: ...}`                   | `wait={"any": ...}`, `wait={"all": ...}`                  | `yield a \| b`, `yield a & b`                     | [Wait for One or All](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c2-wait-for-one-or-all)             |
| `Resource`, `PriorityResource`              | `resource: packers`, `queue: priority`                   | `@app.task(resource_id=...)`, `add_resource(queue=...)`   | `with packers.request() as req: yield req`        | [Resources](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c4-resources)                                 |
| `PreemptiveResource`                        | `preempt: true, queue: priority`                         | `add_resource(queue="priority", preempt=True)`            | `DynamicResource(queue="priority", preempt=True)` | [Interrupts and Preemption](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c3-interrupts-and-preemption) |
| `Container`                                 | `put` and `get` with `container` and `amount`            | `put={"container": ...}`, `get={"container": ...}`        | `yield stock.put(2)`, `yield stock.get(1)`        | [Stores and Containers](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c5-stores-and-containers)         |
| `Store`, `FilterStore`, `PriorityStore`     | `put` and `get` with `store`, `filter`, `priority: true` | `put={"store": ...}`, `get={"store": ..., "filter": ...}` | `parcels.get(filter=...)`                         | [Stores and Containers](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c5-stores-and-containers)         |
| `RealtimeEnvironment`                       | `simulation.factor`, `simulation.strict`                 | `SimulationContext(factor=, strict=)`                     | `DynamicRealtimeEnvironment(factor=)`             | [Run Control](https://jaehyeon.me/desflow/latest/reference/simpy-compatibility/#c6-run-control)                             |

`!python` and `add_process` run SimPy code you write yourself. They are extension points, so a construct that has no native form in YAML or the builder is not covered by using them. User Guide pages that build on SimPy name the parts they use in their How It Works section, starting with [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/arrivals-and-services/index.md). [Coming from SimPy](https://jaehyeon.me/desflow/latest/des/coming-from-simpy/index.md) maps a whole SimPy model onto desflow.
