# Time and Pacing

A run has two clocks, and a run can follow the wall clock, ignore it, or switch from one to the other. This page shows how to read the current time and which parts of the environment run on each thread; [Clocks and Pacing](clocks-and-pacing.md) shows the clock settings, and [Run Length](run-length.md) the warm-up and how long a run lasts.

---

## What It Is

**Simulation time** is the number of seconds SimPy has advanced, starting at `initial_time`, 0 by default; every record carries it as `sim_ts`. **Logical time** is the date and time a record carries as `timestamp`: `logical_start_time` plus the simulation time. `factor` sets how simulation time relates to the wall clock, and `go_live_at` switches that relation once during a run. One hour of simulation time is one hour of logical time, and takes as long on the wall clock as `factor` says:

![Simulation time 0 to 3,600 seconds maps to logical time 09:00 to 10:00; on the wall clock it takes an hour at factor 1.0, 30 minutes at 0.5, and no waiting at 0.0](../../assets/diagrams/two-clocks.png)

The settings are the same on `DynamicRealtimeEnvironment`, on `SimulationContext` and in the `simulation` section of a blueprint.

---

## Pages in This Section

- [Clocks and Pacing](clocks-and-pacing.md): the logical start time, the time zone, the factor, going live, the lag and strict pacing.
- [Run Length](run-length.md): the warm-up, the initial time, how long a run lasts, stopping early, draining, stepping and running in segments.

---

## How to Use It

### Read the Current Time

`t` is the simulation clock in a condition or an expression, the value of `context.env.now` in the builder and `env.now` in the low-level API. A route or a balk can depend on it, such as `next: {when: [{condition: "t < 110", task: early}], default: late}`. A `wait: {condition: ...}` that reads `t` is not a timer: a condition is checked again only after a change of state, not as time passes, so use a `timeout` branch to wait for a time.

---

## How It Works

**What the environment holds.** A `DynamicRealtimeEnvironment` is SimPy's `RealtimeEnvironment` with four additions: the registry at `env.registry`, the ingress connectors, the egress connectors with their buffers, and the statistics at `env.stats`.

**Threads.** A run uses three threads, with queues between them. Records flow from the processes to the sinks, and parameter changes flow from the sources to the registry:

![The SimPy thread buffers records and hands full batches to a queue per egress, which the egress thread writes to the sinks; the ingress thread puts parameter changes on a queue that the ingress monitor applies to the registry](../../assets/diagrams/runtime-threads.png)

- **SimPy thread:** runs the model. When a process publishes a record, the record goes into a buffer for each sink, and a full buffer is handed to that sink's queue.
- **Egress thread:** runs the sinks' asyncio code, which drains the queues and writes to Kafka, files or databases, so network waits never stall a simulation step.
- **Ingress thread:** runs the sources, which put `(path, value)` pairs on a queue.

**Processes it starts.** Besides your own, the environment starts, as events of its own that `step()` and `peek()` do not count, an ingress monitor, which applies queued changes every 0.1 simulation seconds; a lag monitor, which publishes `system.simulation.lag_seconds` every `lag_interval`, at a `factor` above 0 or after going live; a timed flush per sink, at a `factor` above 0; the statistics timer; and the warm-up reset. They share SimPy's one queue with the model's events, so `run()` processes them in order with the model's, and a script that steps the model never sees them. `advance()` and `finish()` run them in the same way. An environment subclass that overrides `step()` sees every event inside `run()`, desflow's own included, and only the model's events when a script steps outside `run()`.

**Pacing and the switch.** Before each event, the environment checks the logical clock. At the first event at or after `go_live_at`, it sets the factor to 1.0 and moves its two anchors, `env_start` and `real_start`, so the go-live instant counts as now; without that, the first paced event would sleep off the whole backfill. The switch happens once and always to 1.0. A `go_live_at` at or before `logical_start_time` paces the whole run, with a warning, and a run that ends before it stays unpaced. A `go_live_at` or `logical_start_time` without an offset is read in the run's time zone, so the two can be compared whichever way each is written.

**The end of the model and the end of the run.** The model ends when the clock stops: at `until`, at a `stop`, or when a drain empties the model. The run ends after that, at `close()`, which `run()` calls in a `finally` block: it publishes the last statistics, still at the time the model ended, flushes and stops the egress connectors, then stops the ingress connectors. A sink may take a while to drain its queue, but no record it receives is later than the model's end.

**SimPy and desflow.** SimPy's `RealtimeEnvironment` takes one `factor` for the whole run and, with `strict=True`, raises an error when the model falls behind the wall clock. desflow runs without waiting at `factor: 0`, switches to real time at `go_live_at`, stamps every record with a logical time, and by default reports falling behind as telemetry; with `strict=True` it raises, as SimPy's does. [SimPy and desflow](../../des/simpy-and-desflow.md) compares the two.

---

## Stability and Limits

- **A run with a sink or ingress needs `until`**, or Ctrl+C, as Run Length explains.
- **At `factor: 0`** no timed flush runs, so records leave a buffer when it reaches `batch_size` and at teardown, as [Egress](../egress.md) explains. `LocalIngress` follows the wall clock, so its changes can land after an unpaced run has ended; a scenario follows the simulation clock. No `system.simulation.lag_seconds` is published unless `lag_interval` is set, and it reports 0 while the run is ahead of the wall clock.
- **`warmup` drops, `go_live_at` routes.** Records before a warm-up are gone; records before `go_live_at` go to the sinks that take history.
- **A model slower than the wall clock** falls behind at `factor: 1.0` and does not catch up by skipping events; `lag_seconds` shows by how much.
