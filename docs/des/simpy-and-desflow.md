# SimPy and desflow

desflow is built on [SimPy](https://simpy.readthedocs.io/), a Python library for discrete-event simulation. SimPy provides the event list, the clock, processes and resources. desflow keeps all of them and changes four things: the clock can switch pacing, every parameter can change while the run goes on, random times are named distributions, and everything that happens is published as records. A low-level desflow model is ordinary SimPy code with these additions.

| Part | In SimPy | In desflow |
|---|---|---|
| Clock and pacing | `Environment` runs without waiting; `RealtimeEnvironment(factor, strict=True)` follows the wall clock and raises `RuntimeError` when the model falls behind | one class, `DynamicRealtimeEnvironment`, does both: `factor=0` runs without waiting, `go_live_at` switches to real time partway through a run (SimPy's `factor` cannot change once set), `logical_start_time` gives every record a date and time, and falling behind is reported as telemetry rather than raised |
| Processes | generators started with `env.process` | the same, and the builder adds `@app.task`, `@app.arrival_loop`, `@app.telemetry_loop` and `add_process`, which write the common ones for you |
| Random times | none built in: a model calls `random` or NumPy itself | ten named distributions with checked parameters, drawn by a `Sampler` from the current values each time; each distribution draws from a random sequence of its own, made from the run's seed and its registry path |
| Resources | `Resource`, `Container` and `Store` keep the capacity they were created with: it has no setter, so assigning a new one raises `AttributeError` (SimPy 4.1). `Resource` serves requests in the order they arrive, and `PriorityResource` by priority | `DynamicResource`, `DynamicContainer` and `DynamicStore` follow a capacity in the registry, and a resource that shrinks lets busy units finish. A resource serves its queue `fifo`, `lifo` or by `priority`, equal priorities first in, first out. `run()` builds the stores and containers a model declares, and tasks put to and get from them |
| Parameters | Python variables, changed only by the model's own code | a registry of named paths, changed by connectors, scenarios or processes, with each change checked |
| Output | whatever the model prints or logs | events and telemetry with a fixed shape, written to Kafka, Redis, PostgreSQL, files or Iceberg on background threads |

User Guide pages that build on SimPy name the parts they use in their How It Works section, starting with [Arrivals and Services](../user-guide/arrivals-and-services.md).
