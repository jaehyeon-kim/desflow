# Low-level API

The low-level API is `DynamicRealtimeEnvironment` used directly, with SimPy processes you write yourself. A script registers the parameters, attaches the connectors, creates the resources and starts its processes with `env.process`. The [declarative API](context.md) does each of these steps inside `run()`, so this page is also a description of what the builder does for you. [First Simulation](../tutorials/first-simulation.md) builds a first factory this way, in its Low-level tabs.

Use the low-level API when you need what the builder does not do:

- **Several resources at once**, or a handover from one to another, as in [Multi-Resource Handoffs](../guides/multi-resource-handoffs.md).
- **Resources created while the simulation runs**, as in [Dynamic Topology](../guides/dynamic-topology.md).
- **Containers and stores** used by the processes: the builder registers a container's capacity but does not create it, and has no stores at all.
- **Events of your own shape**, rather than the `queued`, `started` and finished events of `@app.task`.

---

## Steps of a Run

A low-level script takes these steps, in this order. The sections below take them one at a time, with the code of one complete script: a lathe line with a coolant tank and a store for finished parts. The whole script is at the [end of the page](#full-script).

![The six calls of a low-level run, in order: create the environment, register the parameters, attach the connectors, create the resources, start the processes, run and tear down](../assets/diagrams/low-level-steps.png)

---

## Create the Environment

```python title="docs/snippets/pages/low_level.py:imports"
import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
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

`factor` sets the pacing: `1.0` follows the wall clock, `0.0` runs as fast as possible. `logical_start_time` sets the date and time that simulation time 0 maps to, and `go_live_at` switches an unpaced run to real time partway through. [Time and Pacing](time.md) explains all three.

---

## Register the Parameters

```python title="docs/snippets/pages/low_level.py:parameters"
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Line_A",
        arrival={"parts": DistributionConfig(dist="exponential", rate=0.5)},
        service={"milling": DistributionConfig(dist="normal", mean=1.5, std=0.3)},
        resources={"lathe": CapacityConfig(current_cap=1, max_cap=3)},
        containers={"coolant": CapacityConfig(current_cap=100.0, max_cap=100.0)},
        stores={"finished": CapacityConfig(current_cap=50, max_cap=50)},
        variables={"coolant_per_part": 0.5},
    )
)
```

One `SimParameter` holds every parameter of one simulation under its `sim_id`. Registering it gives each value a path, such as `Line_A.arrival.parts.rate` or `Line_A.resources.lathe.current_cap`, and only the registered paths exist: a resource or a process that asks for any other raises `KeyError`. Stores can only be registered here; the builder has no way to declare them. [Live Parameters](registry.md) lists every path.

---

## Attach the Connectors

```python title="docs/snippets/pages/low_level.py:connectors"
env.setup_egress([ConsoleEgress()])  # starts the egress thread
# env.setup_ingress([KafkaIngress(...)]) would start the ingress thread
```

`setup_egress` starts the egress thread, which writes every record the processes publish; without it, `publish_event` and `publish_telemetry` do nothing. `setup_ingress` starts the ingress thread, which applies parameter changes from Kafka, Redis or PostgreSQL. [Connectors](connectors.md) shows each one, and [Batching and Delivery](batching.md) the arguments of `setup_egress`.

---

## Create Resources, Containers and Stores

```python title="docs/snippets/pages/low_level.py:resources"
lathe = DynamicResource(env, "Line_A", "lathe")  # reads Line_A.resources.lathe.*
coolant = DynamicContainer(env, "Line_A", "coolant", init=100.0)
finished = DynamicStore(env, "Line_A", "finished")
sampler = Sampler(rng=np.random.default_rng(42))  # one seeded generator for every draw
```

Each reads its capacity from the registry when it is created, so the parameters must be registered first. A resource then follows `current_cap` while the simulation runs, as [Resources](../building-blocks/resources.md) explains. The `Sampler` holds the one random generator every draw comes from; seeding it makes a run repeat exactly.

---

## Write the Processes

A process is a SimPy generator: it `yield`s to wait, and publishes records as it goes. This one is what `@app.task` does in the builder, plus a coolant tank and a store:

```python title="docs/snippets/pages/low_level.py:part"
def part(env, part_id):
    """One part: queue for the lathe, use coolant, machine, then store the result."""
    key, path_id = f"task-{part_id}", "Line_A.service.milling"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})
    with lathe.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        litres = env.registry.get("Line_A.variables.coolant_per_part").value
        yield coolant.get(litres)  # waits if the tank is too low
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    yield finished.put({"part_id": part_id})
    env.publish_event(key, {"status": "finished", "part_id": part_id})
```

The time is drawn after the lathe is acquired, from the configuration the registry holds at that moment, so a change made while the part waited in the queue still applies to it.

The arrival loop keeps the configuration object rather than a copy of the rate. The registry updates that object in place, so the next draw uses the new rate:

```python title="docs/snippets/pages/low_level.py:arrivals"
def arrivals(env):
    gap = env.registry.get_config("Line_A.arrival.parts")  # the live config object
    part_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))  # each draw reads the current rate
        env.process(part(env, part_id))
        part_id += 1
```

Telemetry is a process that publishes at an interval. `publish_telemetry` uses the path exactly as given; the builder's `context.publish` adds the `sim_id` in front:

```python title="docs/snippets/pages/low_level.py:telemetry"
def telemetry(env):
    while True:
        env.publish_telemetry("Line_A.lathe.in_use", lathe.in_use)
        env.publish_telemetry("Line_A.coolant.level", coolant.level)
        yield env.timeout(10.0)
```

A process can also change the simulation itself, through the registry, as a connector or a scenario would:

```python title="docs/snippets/pages/low_level.py:control"
def refill_and_rush(env):
    """Refills coolant every minute, and doubles arrivals after five minutes."""
    while True:
        yield env.timeout(60.0)
        missing = coolant.capacity - coolant.level
        if missing > 0:  # SimPy refuses a put of 0
            yield coolant.put(missing)
        if env.now >= 300 and env.registry.get("Line_A.arrival.parts.rate").value < 1.0:
            env.registry.update("Line_A.arrival.parts.rate", 1.0)
            env.registry.update("Line_A.resources.lathe.current_cap", 2)
```

---

## Start the Processes and Run

```python title="docs/snippets/pages/low_level.py:start"
env.process(arrivals(env))
env.process(telemetry(env))
env.process(refill_and_rush(env))
```

```python title="docs/snippets/pages/low_level.py:run"
try:
    env.run(until=time_to_seconds("10 min"))  # until is in simulation seconds
finally:
    env.teardown()  # flushes the buffers and stops the threads
```

`env.run` is SimPy's, so `until` is a number of simulation seconds; `time_to_seconds` converts a duration such as `"10 min"`. Without `until`, SimPy runs until no events remain, which for a model with an arrival loop is never. `teardown` in `finally` flushes the last records and stops both threads, even when the run stops with Ctrl+C or an error.

---

## What the Builder Adds

`SimulationContext.run()` takes the same steps, and adds these:

* **A seeded sampler.** The builder creates `Sampler(rng=np.random.default_rng(random_seed))`. A low-level script creates its own. A `Sampler` with no `rng` draws nothing at random: it returns the mean of each distribution, or `1 / rate` for an exponential one, every time.
* **One resource per `add_resource`.** The builder creates a `DynamicResource` for each, and `context.get_resource(name)` returns it.
* **Task lifecycle events.** `@app.task` publishes `queued` and `started` events and samples the service time. A low-level process publishes its own events with `env.publish_event(key, value)`.
* **Metric names with the `sim_id`.** `context.publish(name, value)` prefixes the name with the `sim_id`. `env.publish_telemetry(path_id, value)` publishes the path exactly as given.

`publish_event` and `publish_telemetry` do nothing when no egress is attached, so a script without `setup_egress` runs without error and publishes nothing.

---

## Full Script

The complete file, which the documentation tests run:

```python title="docs/snippets/pages/low_level.py"
"""A complete low-level simulation, as the Low-level API page explains it step by step.

One lathe line with a coolant tank and a finished-goods store. tests/unit runs this
file unpaced, so every call on the page is checked.
"""

import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    DynamicStore,
    Sampler,
    SimParameter,
    time_to_seconds,
)

env = DynamicRealtimeEnvironment(factor=0.0)  # unpaced; 1.0 follows the wall clock

env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Line_A",
        arrival={"parts": DistributionConfig(dist="exponential", rate=0.5)},
        service={"milling": DistributionConfig(dist="normal", mean=1.5, std=0.3)},
        resources={"lathe": CapacityConfig(current_cap=1, max_cap=3)},
        containers={"coolant": CapacityConfig(current_cap=100.0, max_cap=100.0)},
        stores={"finished": CapacityConfig(current_cap=50, max_cap=50)},
        variables={"coolant_per_part": 0.5},
    )
)

env.setup_egress([ConsoleEgress()])  # starts the egress thread
# env.setup_ingress([KafkaIngress(...)]) would start the ingress thread

lathe = DynamicResource(env, "Line_A", "lathe")  # reads Line_A.resources.lathe.*
coolant = DynamicContainer(env, "Line_A", "coolant", init=100.0)
finished = DynamicStore(env, "Line_A", "finished")
sampler = Sampler(rng=np.random.default_rng(42))  # one seeded generator for every draw


def part(env, part_id):
    """One part: queue for the lathe, use coolant, machine, then store the result."""
    key, path_id = f"task-{part_id}", "Line_A.service.milling"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})
    with lathe.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        litres = env.registry.get("Line_A.variables.coolant_per_part").value
        yield coolant.get(litres)  # waits if the tank is too low
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    yield finished.put({"part_id": part_id})
    env.publish_event(key, {"status": "finished", "part_id": part_id})


def arrivals(env):
    gap = env.registry.get_config("Line_A.arrival.parts")  # the live config object
    part_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))  # each draw reads the current rate
        env.process(part(env, part_id))
        part_id += 1


def telemetry(env):
    while True:
        env.publish_telemetry("Line_A.lathe.in_use", lathe.in_use)
        env.publish_telemetry("Line_A.coolant.level", coolant.level)
        yield env.timeout(10.0)


def refill_and_rush(env):
    """Refills coolant every minute, and doubles arrivals after five minutes."""
    while True:
        yield env.timeout(60.0)
        missing = coolant.capacity - coolant.level
        if missing > 0:  # SimPy refuses a put of 0
            yield coolant.put(missing)
        if env.now >= 300 and env.registry.get("Line_A.arrival.parts.rate").value < 1.0:
            env.registry.update("Line_A.arrival.parts.rate", 1.0)
            env.registry.update("Line_A.resources.lathe.current_cap", 2)


env.process(arrivals(env))
env.process(telemetry(env))
env.process(refill_and_rush(env))

if __name__ == "__main__":
    try:
        env.run(until=time_to_seconds("10 min"))  # until is in simulation seconds
    finally:
        env.teardown()  # flushes the buffers and stops the threads
    assert (
        lathe.capacity == 2
        and env.registry.get("Line_A.arrival.parts.rate").value == 1.0
    )
    assert len(finished.items) > 0
```
