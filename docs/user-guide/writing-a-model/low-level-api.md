# Low-level API

This page writes a simulation with SimPy and the environment directly: a packing team with a stock of boxes and a store of packed orders, every step written by hand. It builds on [SimPy and desflow](../../simpy/index.md), which describes the SimPy constructs, and on [Builder API](builder-api.md), which writes the same kind of model for you.

---

## What It Is

Use the low-level API when you need what the builder does not do: events and interactions of your own shape, such as a resource created while the run goes on in [Add Resources Mid-run](../../how-to/modelling/dynamic-topology.md). A builder or blueprint model can also start a low-level process with `add_process`, as [Tasks and Processes](../tasks-and-processes/processes.md#how-it-works) shows.

A low-level script takes these steps, in this order:

![The six calls of a low-level run, in order: create the environment, register the parameters, attach the connectors, create the resources, start the processes, run and tear down](../../assets/diagrams/low-level-steps.png)

---

## How to Use It

### Create the Environment

```python title="docs/snippets/pages/low_level.py:imports"
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    DynamicStore,
    Sampler,
    SimParameter,
    time_to_seconds,
)
```

```python title="docs/snippets/pages/low_level.py:environment"
env = DynamicRealtimeEnvironment(factor=0.0)  # unpaced; 1.0 follows the wall clock
```

`factor` sets the pacing: `1.0` follows the wall clock, and `0.0` runs as fast as possible. `logical_start_time` and `go_live_at` work as [Time and Pacing](../time-and-pacing/index.md) describes.

### Register the Parameters

```python title="docs/snippets/pages/low_level.py:parameters"
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop",
        # An order every 2 s on average (rate 0.5)
        arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
        service={"packing": DistributionConfig(dist="normal", mean=1.5, std=0.3)},
        resources={"packers": CapacityConfig(current_cap=1, max_cap=3)},
        containers={"boxes": ContainerConfig(current_cap=100, max_cap=100, level=100)},
        stores={"finished": CapacityConfig(current_cap=50, max_cap=50)},
        variables={"boxes_per_order": 1.0},
    )
)
```

One `SimParameter` holds every parameter of one simulation under its `sim_id`. Registering it gives each value a path, such as `Shop.arrival.orders.rate`, and only the registered paths exist. [Live Parameters](../live-runs-and-connectors/live-parameters.md) lists the paths.

### Attach the Connectors

```python title="docs/snippets/pages/low_level.py:connectors"
env.setup_egress([ConsoleEgress()])  # starts the egress thread
# env.setup_ingress([KafkaIngress(...)]) would start the ingress thread
env.setup_stats("Shop", "basic", interval=60)  # readings and counts every minute
```

`setup_egress` starts the egress thread, which writes every record the processes publish. `setup_ingress` starts the ingress thread for parameter changes from outside, and `setup_stats` switches on the built-in statistics. [Egress](../live-runs-and-connectors/egress.md) lists the arguments of `setup_egress`, including a [warm-up](../../learn/concepts/output-analysis.md#one-run-starts-empty) and labels sinks.

### Create Resources, Containers and Stores

```python title="docs/snippets/pages/low_level.py:resources"
packers = DynamicResource(env, "Shop", "packers")  # reads Shop.resources.packers.*
boxes = DynamicContainer(env, "Shop", "boxes")  # starts at its level, 100
finished = DynamicStore(env, "Shop", "finished")
sampler = Sampler(seed=42)  # a random stream per distribution, from one seed
```

Each one reads its capacity from the registry when it is created, so the parameters must be registered first. Each one also adds itself to the statistics. `Sampler(seed=42)` gives each registered distribution a random stream of its own, so a run repeats exactly, as [Arrivals and Services](../model-parts/arrivals-and-services.md#seeding) describes.

### Write the Processes

A process is a SimPy generator: it `yield`s to wait, and publishes records as it goes. This one does what `@app.task` does in the builder, with a box from stock and a store for the result. It publishes each event with the entity key, the task's name and its instance id, and reports each step to the statistics:

```python title="docs/snippets/pages/low_level.py:order"
def order(env, key, task_id):
    """One order: queue for a packer, take a box, pack, then store the result."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish(
        {"path_id": path_id, "sim_id": "Shop", "service": "packing", "status": "queued"}
    )
    env.stats.task_queued("pack_order", task_id)  # what @app.task reports for you
    with packers.request() as request:
        yield request
        count = env.registry.get("Shop.variables.boxes_per_order").value
        yield boxes.get(count)  # waits if the stock is too low
        publish(
            {
                "path_id": path_id,
                "sim_id": "Shop",
                "service": "packing",
                "status": "started",
            }
        )
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        yield finished.put({"order": key})
        publish({"event_type": "order_packed"})
    env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")
```

The service time is drawn once the order has a packer and a box, from the configuration the registry holds at that moment, so a change made while it waited still applies to it.

The arrival loop keeps the configuration object rather than a copy of the rate. The registry changes that object in place when a new rate arrives, so the next draw uses the current rate. It gives each order its key, `orders-1`, `orders-2`:

```python title="docs/snippets/pages/low_level.py:arrivals"
def arrivals(env):
    gap = env.registry.get_config("Shop.arrival.orders")  # the live config object
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))  # each draw reads the current rate
        env.process(order(env, f"orders-{task_id + 1}", task_id))  # the entity key
        task_id += 1
```

A reading of your own is a process that publishes at an interval. `publish_telemetry` uses the name exactly as given:

```python title="docs/snippets/pages/low_level.py:telemetry"
def telemetry(env):
    """A reading of your own, beside the built-in statistics."""
    while True:
        env.publish_telemetry("Shop.boxes_per_packer", boxes.level / packers.capacity)
        yield env.timeout(10.0)
```

A process can also change the simulation itself, through the registry, as a connector or a scenario would:

```python title="docs/snippets/pages/low_level.py:control"
def refill_and_rush(env):
    """Refills boxes every minute, and doubles arrivals after five minutes."""
    while True:
        yield env.timeout(60.0)
        missing = boxes.capacity - boxes.level
        if missing > 0:  # SimPy refuses a put of 0
            yield boxes.put(missing)
        if env.now >= 300 and env.registry.get("Shop.arrival.orders.rate").value < 1.0:
            env.registry.update("Shop.arrival.orders.rate", 1.0)
            env.registry.update("Shop.resources.packers.current_cap", 2)
```

### Start the Processes and Run

```python title="docs/snippets/pages/low_level.py:start"
env.process(arrivals(env))
env.process(telemetry(env))
env.process(refill_and_rush(env))
```

```python title="docs/snippets/pages/low_level.py:run"
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
try:
    env.run(until=time_to_seconds("10 min"))  # until is in simulation seconds
finally:
    env.teardown()  # flushes the buffers and stops the threads
```

`ConsoleEgress` writes each record as a log line at INFO level, so logging is switched on first. `env.run` is SimPy's, so `until` is a number of simulation seconds. `time_to_seconds` converts a duration such as `"10 min"`. `teardown` in `finally` publishes the last statistics, flushes the last records and stops both threads, even when the run stops with Ctrl+C or an error.

A script can run the model in segments by calling `env.run(until=...)` again, as in SimPy. `env.schedule(event, priority, delay)` is SimPy's own: `simpy.events.URGENT` puts an event before the `NORMAL` ones at its time. [Run Length](../time-and-pacing/run-length.md#run-in-segments) covers segments, `strict`, `sync()` and `env.resume_pacing()`.

An environment subclass is plain Python: `class Traced(DynamicRealtimeEnvironment)`, whose `__init__` takes `**kwargs` and passes them on, and whose `step` or `schedule` calls `super()`. Inside `run()`, `step` runs once per event, desflow's own events included.

### Low-level Forms of the Keys

A script writes by hand what the keys of a blueprint do. These are the calls and attributes that match the arguments of the [Builder API](builder-api.md#arguments-for-the-keys-of-a-blueprint):

- **Generated values.** `ValueRuntime.value(spec, entity=None)` evaluates a typed generator from `desflow.values.generator`, or any value that holds one, now. Call it on the same object each time: a `sequence` counts and a `unique` choice refuses on it. A `row` generator is read from the working directory at the call, and a missing or unusable file raises `ValueError` naming the generator's place and the file. A `row` gives a mapping, so a place that needs a number or text takes it only inside another generator.
- **Resources and signals.** `DynamicResourceRequest.preempted` is true once a request of a higher rank took the unit. `Signal.fire(value, urgent, after, priority)` and `Signal.fail(reason, urgent, after, priority, defused)` take a `priority`, and `fail` takes `defused`. `Signal.processed` is true once the latest firing's event has been processed.
- **Transfers.** `Transfer(item=...)` is the item a store `put` adds.
- **Trace.** `env.trace_events()` switches the event trace on, and `env.trace_events(False)` off. A trace record goes only to the sinks added as trace sinks.

### Read a Value, an Expression and a Trace

`env.registry.get(path)` gives the registry's `DynamicValue` for a path. Its `value` is the value now, and `name` is the path:

```python title="docs/snippets/pages/low_level_reads.py:value"
env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop",
        resources={"packers": CapacityConfig(current_cap=2, max_cap=5)},
    )
)
packers: DynamicValue = env.registry.get("Shop.resources.packers.current_cap")
print(packers.name, packers.value)
```

This prints `Shop.resources.packers.current_cap 2`.

An `Expression` is parsed and checked when it is made, and `evaluate` reads its names from a `Scope`. A scope you make yourself holds only the parts you give it, here a payload and the time `t`:

```python title="docs/snippets/pages/low_level_reads.py:expression"
rule = Expression("payload.total > 100 and t >= 60")
scope = Scope(payload={"total": 120}, now=75.0)
print(rule.evaluate(scope))
```

This prints `True`.

`read_trace` reads the arrivals of a trace, from a CSV or Parquet path or from a list of mappings, into a list of `Row` objects. Each has its `time`, its row `number` and the `fields` that become the entity's attributes:

```python title="docs/snippets/pages/low_level_reads.py:trace"
rows: list[Row] = read_trace(
    [{"time": 0.5, "id": "a"}, {"time": 2.0, "id": "b"}], fields=["id"]
)
for row in rows:
    print(row.number, row.time, row.fields)
```

This prints `1 0.5 {'id': 'a'}` and `2 2.0 {'id': 'b'}`.

---

## How It Works

The environment does little for you. It paces the clock, holds the registry, writes records on the egress thread and keeps the statistics that `setup_stats` switches on. Everything else is your code. [Builder API](builder-api.md#how-it-works) lists what `SimulationContext.run()` writes for you on top of the same steps.

The threads, the queues and the processes the environment starts on the SimPy thread are in [Threads and Queues](../how-desflow-works.md#threads-and-queues). `env.step()` and `env.peek()` do not count those processes.

---

## Stability and Limits

Nothing is checked before the run as a whole. Each call checks only what it reads, when it runs:

- **An unregistered path raises `KeyError`** when a resource, a process or a `registry.get` asks for it, so register the parameters before creating anything that reads them.
- **A record without egress is dropped.** Until `setup_egress` runs, `publish_event` and `publish_telemetry` do nothing, without an error.
- **A `Sampler()` with no seed draws nothing at random,** as [Arrivals and Services](../model-parts/arrivals-and-services.md#how-it-works) explains, so give it a seed.
- **`strict` is left unset by default.** It is true when `factor` is above 0 or `go_live_at` is set, and false otherwise. [Strict Pacing](../time-and-pacing/clocks-and-pacing.md#strict-pacing) gives what it does in each API.
- **An unhandled failure stops the run under `stop`.** `DynamicRealtimeEnvironment(on_unhandled_failure=...)` is `stop` by default, as a plain SimPy environment is. Under `stop`, a one-shot `Signal` failure that nobody waits for raises `SignalFailed`, and the run stops. Pass `"continue"` to defuse such a failure and let the run go on. A re-arming signal's failure is always defused, and `Signal.fail(reason, defused=True)` defuses one failure of a one-shot signal. A failure with an empty or 0 reason keeps that reason. An exception raised in a process stops the run under both settings. The keyword must be `stop` or `continue`.

Beyond those, a process can do anything SimPy can model.

---

The complete file, which the documentation tests run, is `docs/snippets/pages/low_level.py`. [Builder API](builder-api.md) writes the same kind of model with less code.
