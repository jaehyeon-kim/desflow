# Time and Pacing

A run has two clocks, and a run can follow the wall clock, ignore it, or switch from one to the other. This page shows the clock settings, the warm-up, how long a run lasts, and which parts of the environment run on each thread.

---

## What It Is

**Simulation time** is the number of seconds SimPy has advanced, starting at 0; every record carries it as `sim_ts`. **Logical time** is the date and time a record carries as `timestamp`: `logical_start_time` plus the simulation time. `factor` sets how simulation time relates to the wall clock, and `go_live_at` switches that relation once during a run. One hour of simulation time is one hour of logical time, and takes as long on the wall clock as `factor` says:

![Simulation time 0 to 3,600 seconds maps to logical time 09:00 to 10:00; on the wall clock it takes an hour at factor 1.0, 30 minutes at 0.5, and no waiting at 0.0](../assets/diagrams/two-clocks.png)

The settings are the same on `DynamicRealtimeEnvironment`, on `SimulationContext` and in the `simulation` section of a blueprint.

---

## How to Use It

### Logical Start Time

`logical_start_time` is the date and time that simulation time 0 maps to; without it, the run uses the moment the environment is created. A run with a backdated start writes history:

```python title="docs/snippets/pages/pacing.py:logical-start"
app = SimulationContext(
    sim_id="Shop",
    factor=0.0,
    logical_start_time=datetime.now() - timedelta(days=7),  # a week of history
)
```

A naive datetime gives timestamps with no time zone, and a timezone-aware one gives timestamps with its offset. In a blueprint it takes `now`, a signed duration such as `-7d`, or an ISO datetime, as [YAML Blueprints](blueprints.md#times-and-durations) describes.

### Factor

The wall-clock duration of a stretch of the run is its simulation duration times `factor`:

```python title="docs/snippets/pages/pacing.py:factor"
live = DynamicRealtimeEnvironment(factor=1.0)  # 1 simulated second takes 1 real second
twice_as_fast = DynamicRealtimeEnvironment(factor=0.5)  # takes half a real second
unpaced = DynamicRealtimeEnvironment(factor=0.0)  # takes no real time at all
```

- `factor: 1.0` follows the wall clock: a process that waits 5 simulation seconds waits 5 real seconds. It suits a live twin feeding a dashboard.
- `factor: 0` does not wait at all: the run goes as fast as the machine allows. It suits history, tests and experiments.

### Go Live

`factor` applies until the logical clock reaches `go_live_at`, and from that instant the run is paced at one simulation second per real second. With a backdated `logical_start_time` and `factor: 0`, one run writes the history as fast as it can, then keeps going in real time:

```python title="docs/snippets/pages/pacing.py:go-live"
go_live_at = datetime.now()

env = DynamicRealtimeEnvironment(
    factor=0.0,  # until go_live_at
    logical_start_time=go_live_at - timedelta(days=7),
    go_live_at=go_live_at,  # from here, 1 simulated second per real second
)
```

A sink with `when: history` receives the records stamped before `go_live_at`, and one with `when: live` the rest, so the two halves can go to different places:

![From logical_start_time to go_live_at the run writes history at factor 0 to Parquet; from go_live_at to the end it runs at one simulation second per real second and writes to Kafka](../assets/diagrams/backfill-timeline.png)

[Parquet History, Kafka Live](../examples/backfill-live.md) builds such a run.

### Warm-up

`warmup` leaves the start of a run out of what it reports. It is one time per run, in simulation seconds or a duration. No sink receives a record or telemetry stamped before it, and the built-in statistics are reset once at that moment, so they describe the rest of the run. A run without it sends everything, which is what a data generator usually wants.

=== "YAML"

    ```yaml title="docs/snippets/yaml/trusting_results.yaml:warmup"
    # Records and statistics start after the first 2 minutes; 0 keeps everything
    warmup: ${WARMUP:-2 min}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/pacing.py:warmup"
    # No sink receives a record stamped before 10 minutes, and the statistics restart then
    measured = SimulationContext(sim_id="Shop", factor=0.0, warmup="10 min")
    # In the low-level API, the warm-up is set with the sinks:
    # env.setup_egress(sinks, warmup=600)
    ```

=== "Low-level"

    The warm-up is an argument of the sinks, because it is the sinks that leave records out: `env.setup_egress(sinks, warmup=600)`. A run with no sink has nothing to leave out, and its statistics are reset at the warm-up as well when it is set through the builder.

The warm-up drops early records; `go_live_at` drops nothing and only decides which sink gets each one. [Trusting Results](../tutorials/trusting-results.md) shows how to choose a warm-up length.

### Run Length

`until` is in simulation seconds. `run()` and a blueprint's `run.until` also take a duration such as `"10 min"`, `"8 hours"` or `"1 week"`; a month counts as 30 days and a year as 365.

```python title="docs/snippets/pages/pacing.py:run-length"
app.add_resource("packers", current_cap=1, max_cap=1)
app.run(until="1 week")  # 604,800 simulation seconds; a number is seconds too
```

Without `until`, SimPy runs until no events remain, and a run with an arrival never runs out of events. Neither does any run with a sink or an ingress connector, even when its own work is done, because the environment's own processes repeat for as long as the run lasts. Give such a run an `until`, or stop it with Ctrl+C.

---

## How It Works

**What the environment holds.** A `DynamicRealtimeEnvironment` is SimPy's `RealtimeEnvironment` with four additions: the registry at `env.registry`, the ingress connectors, the egress connectors with their buffers, and the statistics at `env.stats`.

**Threads.** A run uses three threads, with queues between them. Records flow from the processes to the sinks, and parameter changes flow from the sources to the registry:

![The SimPy thread buffers records and hands full batches to a queue per egress, which the egress thread writes to the sinks; the ingress thread puts parameter changes on a queue that the ingress monitor applies to the registry](../assets/diagrams/runtime-threads.png)

- The **SimPy thread** runs the model. When a process publishes a record, the record goes into a buffer for each sink, and a full buffer is handed to that sink's queue.
- The **egress thread** runs the sinks' asyncio code, which drains the queues and writes to Kafka, files or databases, so network waits never stall a simulation step.
- The **ingress thread** runs the sources, which put `(path, value)` pairs on a queue.

**Processes it starts.** Besides your own, the environment starts an ingress monitor, which applies queued changes every 0.1 simulation seconds; a lag monitor, which publishes `system.simulation.lag_seconds` every simulation second; a timed flush per sink, at a `factor` above 0; the statistics timer; and the warm-up reset.

**Pacing and the switch.** Before each event, `step` checks the logical clock. At the first event at or after `go_live_at`, it sets the factor to 1.0 and moves its two anchors, `env_start` and `real_start`, so the go-live instant counts as now; without that, the first paced event would sleep off the whole backfill. The switch happens once and always to 1.0. A `go_live_at` at or before `logical_start_time` paces the whole run, with a warning, and a run that ends before it stays unpaced. `go_live_at` and `logical_start_time` must both be naive or both timezone-aware, or the environment refuses them.

**Teardown.** `teardown()`, which `run()` calls in a `finally` block, publishes the last statistics, flushes and stops the egress connectors, then stops the ingress connectors.

**SimPy and desflow.** SimPy's `RealtimeEnvironment` takes one `factor` for the whole run and, with `strict=True`, raises an error when the model falls behind the wall clock. desflow runs without waiting at `factor: 0`, switches to real time at `go_live_at`, stamps every record with a logical time, and reports falling behind as telemetry instead of raising. [SimPy and desflow](../des/simpy-and-desflow.md) compares the two.

---

## Stability and Limits

- **A run with a sink or ingress needs `until`**, or Ctrl+C, as Run Length explains.
- **At `factor: 0`** no timed flush runs, so records leave a buffer when it reaches `batch_size` and at teardown, as [Egress](egress.md) explains. `LocalIngress` follows the wall clock, so its changes can land after an unpaced run has ended; a scenario follows the simulation clock. `system.simulation.lag_seconds` reports 0 while the run is ahead of the wall clock.
- **`warmup` drops, `go_live_at` routes.** Records before a warm-up are gone; records before `go_live_at` go to the sinks that take history.
- **A model slower than the wall clock** falls behind at `factor: 1.0` and does not catch up by skipping events; `lag_seconds` shows by how much.
