# How desflow Works

This page shows how desflow is built: the parts that run a model, why each one exists, and how a run moves through them. Each part links to the How It Works section that describes it in full.

---

## What It Is

desflow is built on [SimPy](https://simpy.readthedocs.io/). SimPy gives the event list, the clock, processes and resources, and desflow keeps all of them. SimPy leaves seven things to the modeller, and each part of desflow adds one of them:

| Left to the modeller in SimPy | What desflow adds | Read |
|---|---|---|
| the code around a model: a class of parameters, an entity class, a model class and a loop over runs | three ways to write a model, each built on the one below | [Ways to Write](ways-to-write.md) |
| one pacing `factor`, fixed when the environment is made | one environment that runs history at full speed, then switches to the wall clock | [Time and Pacing](time-and-pacing/index.md#how-it-works) |
| random times, drawn by the model's own code | ten checked distributions, each with a random stream of its own | [Arrivals and Services](arrivals-and-services.md#how-it-works) |
| parameters that only the model's code changes | a registry of named paths that connectors, scenarios and processes change during a run | [Live Parameters](live-parameters.md#how-it-works) |
| resources, stores and containers that keep the capacity they were made with | resources, stores and containers whose capacity follows the registry | [Resources](resources/index.md#how-it-works) |
| output, which the model prints or logs | records of a fixed shape, written to sinks on a thread of their own | [Egress](egress.md#how-it-works) |
| statistics, replications and confidence intervals | statistics collected during the run, replications, experiments and `explain` | [Built-in Statistics](statistics.md#how-it-works) |

[Coming from SimPy](../des/coming-from-simpy.md#what-desflow-changes) compares each change with plain SimPy.

Look at how one run moves through these parts:

![The three ways to write a simulation build one DynamicRealtimeEnvironment. Its registry holds every live parameter; ingress connectors on background threads update it from Kafka, Redis, PostgreSQL or a local schedule, and the resources follow it. Telemetry and events go to egress connectors on their own thread, which write them to Kafka, Redis, PostgreSQL, Parquet or JSONL files, Iceberg or the console](../assets/diagrams/architecture.png)

Whichever way a model is written, it builds one environment, and one run goes the same way from there. The environment holds the registry of live parameters, and the resources, stores and containers follow it. Ingress connectors change registry values from outside while the run goes on. Tasks and other processes publish events and telemetry, and egress connectors write them to their sinks on a thread of their own. The registry paths, the [records](../reference/records-and-telemetry.md), the batching and the connectors are therefore the same in all three ways.

---

## How It Works

### Writing a Model

A YAML blueprint, the builder API and the low-level API are layers, not alternatives. `from_yaml` reads a blueprint, checks it and makes the calls a Python script would make on the builder, `SimulationContext`. The builder's `run()` then takes the steps a low-level script writes by hand: it builds the environment, registers the parameters, builds the resources, stores and containers, and starts the processes. The layers mix: a blueprint calls Python with `!python`, and a builder process can use the environment directly. [YAML Blueprints](blueprints.md#how-it-works) describes how a file is loaded, and [What `run()` Does](tasks-and-processes/index.md#what-run-does) lists the steps of `run()` in order.

### Environment and Pacing

`DynamicRealtimeEnvironment` is SimPy's `RealtimeEnvironment` with four additions: the registry, the ingress connectors, the egress connectors and the statistics. SimPy's `factor` cannot change once the environment exists. desflow runs without waiting at `factor: 0` and switches to real time at `go_live_at`, so one run can write a week of history and then stream live. Every record carries a logical date and time from `logical_start_time`. [Time and Pacing](time-and-pacing/index.md#how-it-works) describes the switch and the end of a run.

### Random Times

Every distribution family is declared once, in one table that YAML, the builder and the registry all read. A `Sampler` reads a distribution's parameters at each draw, so a change during a run applies from the next draw. Each distribution draws from a random stream of its own, made from the run's seed and the distribution's registry path. Adding a distribution therefore leaves the draws of every other one unchanged. [Arrivals and Services](arrivals-and-services.md#how-it-works) describes the streams.

### Registry

The registry holds every live value under a dot path, such as `Shop.service.packing.mean`. Each path holds a `DynamicValue`: the value, and a signal that fires when the value changes. An update is checked before it is stored. An unknown path is ignored, a value is converted to the path's type, a distribution parameter must pass its range rules, and a capacity is kept within its limits. [Live Parameters](live-parameters.md#how-it-works) describes each check.

### Resources, Stores and Containers

desflow's resources, stores and containers wait on their registry paths, so their capacity changes during a run. A resource that shrinks lets its busy units finish. A resource grants its units in the same steps as SimPy's `Resource`, so two events at the same time resolve as they would in plain SimPy. [Resources](resources/index.md#how-it-works) and [Stores and Containers](stores-and-containers.md#how-it-works) describe them.

### Threads, Ingress and Egress

A run uses three threads. The SimPy thread runs the model. The ingress thread runs the sources, such as Kafka or PostgreSQL, and each source puts `(path, value)` pairs on a queue. The egress thread runs the sinks, which write batches of records. Each connector has one method, `async run(queue)`, which runs on its thread's asyncio loop, so a network wait never stalls a simulation step. [Time and Pacing](time-and-pacing/index.md#how-it-works) shows the threads, [Ingress](ingress.md#how-it-works) how a change reaches the registry, and [Egress](egress.md#how-it-works) how a record reaches a sink. [Write a Connector](../guides/write-a-connector.md) writes one of your own.

### Two Kinds of Queue

The word queue means two different things in desflow. Look at the two side by side:

![Left: waiting lines in the model. Orders wait in resource.queue, in fifo, lifo or priority order, until they are granted a packer in resource.users; a request past queue_limit raises QueueFull. They are part of the model, and their length and waits are in the records. Right: hand-off queues between threads. The ingress queue has no limit and is applied to the registry every 0.1 simulation seconds; each sink has a buffer on the SimPy thread, then a queue of at most max_queued_batches that the egress thread writes from. They are not part of the model](../assets/diagrams/two-queues.png)

A waiting line in the model holds entities, such as orders waiting for a packer. A resource keeps its line in `queue`, ordered `fifo`, `lifo` or by `priority`, and a request past its `queue_limit` raises `QueueFull`. Stores and containers have lines of puts and gets that wait. These lines are what a study measures: their lengths and waits are in the records and the statistics.

A hand-off queue holds data on its way between threads. The ingress queue has no limit, and the ingress monitor applies what it holds every 0.1 simulation seconds. Each sink has its own queue of at most `max_queued_batches` batches. When a sink's queue is full, the simulation waits, so a slow sink slows the run instead of building a backlog. When it stays full for `drain_stall_seconds`, the run stops rather than lose records. These queues change how fast the records arrive, never what they hold.

### Measurement and Explain

The environment keeps one statistics object, which collects readings, time-weighted averages, counts, waits and service times while the run goes on. Replications build the model again for each seed, run it without pacing, ingress or egress, and give a Student t interval over the runs. Experiments run replications for each design. `desflow explain` reads the model's graph before anything runs and applies queueing formulas to it. [Built-in Statistics](statistics.md#how-it-works), [Replications](replications.md#how-it-works), [Experiments](experiments/index.md#how-it-works) and [Command Line](command-line.md#how-it-works) describe each.
