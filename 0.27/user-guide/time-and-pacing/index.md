# Time and Pacing

A run has two clocks of its own, simulation time and logical time. A run can follow the wall clock, ignore it, or ignore it at first and follow it later. This page shows how to read the current time and which parts of the environment run on each thread. [Clocks and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/index.md) shows the clock settings, and [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/index.md) shows the [warm-up](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#one-run-starts-empty) and how long a run lasts.

______________________________________________________________________

## What It Is

**Simulation time** is the number of seconds SimPy has advanced, starting at `initial_time`, 0 by default. Every record carries it as `sim_ts`. **Logical time** is the date and time a record carries as `timestamp`: `logical_start_time` plus the simulation time. `factor` sets how simulation time relates to the wall clock, and `go_live_at` switches that relation once during a run. One hour of simulation time is one hour of logical time, and takes as long on the wall clock as `factor` says:

The settings are the same on `DynamicRealtimeEnvironment`, on `SimulationContext` and in the `simulation` section of a blueprint.

______________________________________________________________________

## Pages in This Section

- [Clocks and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/index.md): the logical start time, the time zone, the factor, going live, the lag and strict pacing.
- [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/index.md): the warm-up, the initial time, how long a run lasts, stopping early, draining, stepping and running in segments.

______________________________________________________________________

## How to Use It

### Read the Current Time

`t` is the simulation clock in a condition or an expression, the value of `context.env.now` in the builder and `env.now` in the low-level API. A route or a balk can depend on it, such as `next: {when: [{condition: "t < 110", task: early}], default: late}`. A `wait: {condition: ...}` that reads `t` is not a timer. A condition is checked again only after a change of state, not as time passes. To wait for a time, use a `timeout` branch.

______________________________________________________________________

## How It Works

**What the environment holds.** A `DynamicRealtimeEnvironment` is SimPy's `RealtimeEnvironment` with four additions: the registry at `env.registry`, the ingress connectors, the egress connectors with their buffers, and the statistics at `env.stats`.

**Threads.** A run uses three threads, with queues between them. Records flow from the processes to the sinks, and parameter changes flow from the sources to the registry:

- **SimPy thread:** runs the model. When a process publishes a record, the record goes into a buffer for each sink, and a full buffer is handed to that sink's queue.
- **Egress thread:** runs the sinks' asyncio code, which drains the queues and writes to Kafka, files or databases, so network waits never stall a simulation step.
- **Ingress thread:** runs the sources, which put `(path, value)` pairs on a queue.

**Two kinds of queue.** The word queue means two different things in desflow:

- **A waiting line in the model** holds entities, such as orders waiting for a packer. A resource keeps its line in `queue`, ordered `fifo`, `lifo` or by `priority`, and a request past its `queue_limit` raises `QueueFull`. Stores and containers have lines of puts and gets that wait. These lines are what a study measures: their lengths and waits are in the records and the statistics.
- **A hand-off queue** holds data on its way between the threads above. The ingress queue has no limit, and the ingress monitor applies it. Each sink has its own queue of at most `max_queued_batches` batches. When a sink's queue is full, the simulation waits, so a slow sink slows the run instead of building a backlog. When it stays full for `drain_stall_seconds`, the run stops rather than lose records. These queues change how fast the records arrive, never what they hold.

**Processes it starts.** Besides your own processes, the environment starts these, as events of its own that `step()` and `peek()` do not count:

- an ingress monitor, which applies queued changes every 0.1 simulation seconds;
- a lag monitor, which publishes `system.simulation.lag_seconds` every `lag_interval`, at a `factor` above 0 or after going live;
- a timed flush per sink, at a `factor` above 0;
- the statistics timer;
- the warm-up reset.

They share SimPy's one event queue with the model's events, so `run()` processes them in order with the model's events. A script that steps the model never sees them. `advance()` and `finish()` run them in the same way. An environment subclass that overrides `step()` sees every event inside `run()`, desflow's own included, and only the model's events when a script steps outside `run()`.

[Clocks and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#how-it-works) describes how the pacing switches during a run, and [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#how-it-works) how the model and the run end.

**SimPy and desflow.** SimPy's `RealtimeEnvironment` takes one `factor` for the whole run. With `strict=True`, it raises an error when the model falls behind the wall clock. desflow runs without waiting at `factor: 0`, switches to real time at `go_live_at`, and stamps every record with a logical time. By default it reports falling behind as telemetry, and with `strict=True` it raises, as SimPy's does. [SimPy and desflow](https://jaehyeon.me/desflow/latest/simpy/index.md) compares the two.

______________________________________________________________________

## Stability and Limits

- **A run with a sink or ingress needs `until`**, or Ctrl+C, as [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#run-length_1) explains.
- **At `factor: 0`** no timed flush runs, so records leave a buffer when it reaches `batch_size` and at teardown, as [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/index.md) explains. `LocalIngress` follows the wall clock, so its changes can land after an unpaced run has ended. A scenario follows the simulation clock. No `system.simulation.lag_seconds` is published unless `lag_interval` is set, and it reports 0 while the run is ahead of the wall clock.
- **`warmup` drops, `go_live_at` routes.** Records before a warm-up are gone. Records before `go_live_at` go to the sinks that take history.
- **A model slower than the wall clock** falls behind at `factor: 1.0` and does not catch up by skipping events. `lag_seconds` shows by how much.
