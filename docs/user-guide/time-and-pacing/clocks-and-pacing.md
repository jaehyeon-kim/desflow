# Clocks and Pacing

This page shows the clock settings of a run, described in [Time and Pacing](index.md): the logical start time, the time zone, the factor, going live, the lag and strict pacing.

---

## How to Use It

### Logical Start Time

`logical_start_time` is the date and time that simulation time 0 maps to; without it, the run uses the moment the environment is created. A run with a backdated start writes history:

```python title="docs/snippets/pages/pacing.py:logical-start"
app = SimulationContext(
    sim_id="Shop",
    factor=0.0,
    logical_start_time=datetime.now(timezone.utc) - timedelta(days=7),  # a week
)
```

A datetime without an offset is read in the run's time zone, described next. In a blueprint it takes `now`, a signed duration such as `-7d`, or an ISO datetime, as [YAML Blueprints](../blueprints.md#times-and-durations) describes.

### Time Zone

`timezone` names the zone that record timestamps are written in, as an IANA name such as `Australia/Melbourne`. Every `timestamp` carries the offset of its moment, such as `2026-10-03T12:00:00.000+10:00`, or `+00:00` in UTC. By default the zone is that of `logical_start_time` when it has one, and UTC otherwise. A `logical_start_time` or `go_live_at` without an offset, `now`, and the default start are read in it:

=== "YAML"

    ```yaml title="docs/snippets/yaml/environment.yaml:timezone"
    # Under simulation: timestamps written in Melbourne time, with its offset; the
    # start and now are read in it. UTC by default
    timezone: ${SIM_TIMEZONE:-Australia/Melbourne}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/pacing.py:timezone"
    # Timestamps in Melbourne time, with their offset: 2026-10-03T12:00:00.000+10:00
    melbourne = SimulationContext(
        sim_id="Shop",
        factor=0.0,
        timezone="Australia/Melbourne",
        logical_start_time=datetime(2026, 10, 3, 12, 0),  # read in Melbourne time
    )
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/pacing.py:low-level-timezone"
    zoned = DynamicRealtimeEnvironment(
        factor=0.0,
        timezone="Australia/Melbourne",
        logical_start_time=datetime(2026, 10, 3, 12, 0),
    )
    print(zoned.start_datetime)  # 2026-10-03 12:00:00+10:00
    ```

The clock runs in UTC, so a daylight-saving change in the zone never adds or drops simulation time: in Melbourne, where the clocks went forward an hour on 4 October 2026, 24 hours of simulation from 12:00 on 3 October end at 13:00 on 4 October, `2026-10-04T13:00:00.000+11:00`. A replayed timestamp without an offset is read in the zone too.

### Factor

The wall-clock duration of a stretch of the run is its simulation duration times `factor`:

```python title="docs/snippets/pages/pacing.py:factor"
live = DynamicRealtimeEnvironment(factor=1.0)  # 1 simulated second takes 1 real second
twice_as_fast = DynamicRealtimeEnvironment(factor=0.5)  # takes half a real second
unpaced = DynamicRealtimeEnvironment(factor=0.0)  # takes no real time at all
```

- `factor: 1.0` follows the wall clock: a process that waits 5 simulation seconds waits 5 real seconds. It suits a live twin feeding a dashboard.
- `factor: 0` does not wait at all: the run goes as fast as the machine allows. It suits history, tests and experiments. At `factor: 0` each event is processed by SimPy's own `Environment.step`, without the real-time arithmetic, and `strict` is not checked, as there is no wall clock to fall behind. A pending `go_live_at` is still checked before each event, and pacing starts at it.

### Go Live

`factor` applies until the logical clock reaches `go_live_at`, and from that instant the run is paced at one simulation second per real second. With a backdated `logical_start_time` and `factor: 0`, one run writes the history as fast as it can, then keeps going in real time:

```python title="docs/snippets/pages/pacing.py:go-live"
go_live_at = datetime.now(timezone.utc)

env = DynamicRealtimeEnvironment(
    factor=0.0,  # until go_live_at
    logical_start_time=go_live_at - timedelta(days=7),
    go_live_at=go_live_at,  # from here, 1 simulated second per real second
)
```

A sink with `when: history` receives the records stamped before `go_live_at`, and one with `when: live` the rest, so the two halves can go to different places:

![From logical_start_time to go_live_at the run writes history at factor 0 to Parquet; from go_live_at to the end it runs at one simulation second per real second and writes to Kafka](../../assets/diagrams/backfill-timeline.png)

[Parquet History, Kafka Live](../../examples/backfill-live.md) builds such a run.

### Lag

At a `factor` above 0 the run follows the wall clock, and `system.simulation.lag_seconds` says how many seconds it is behind, as telemetry. `lag_interval` sets how often, in simulation seconds or a duration. By default it is every simulation second at a `factor` above 0, and off at `factor: 0`, where the run is meant to be ahead of the wall clock; a run at `factor: 0` that goes live starts it at the go-live instant. 0 turns it off:

=== "YAML"

    ```yaml title="docs/snippets/yaml/environment.yaml:lag"
    # Under simulation: how far the run is behind the wall clock, every 10 s; by
    # default off at factor 0 and every second otherwise, and 0 turns it off
    lag_interval: 10 s
    ```

=== "Builder"

    ```python title="docs/snippets/pages/pacing.py:lag"
    # system.simulation.lag_seconds every 10 simulation seconds; 0 turns it off
    paced = SimulationContext(sim_id="Shop", factor=1.0, lag_interval="10 s")
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/pacing.py:low-level-lag"
    # The low-level API sets it with the sinks; the default "auto" is off at factor 0
    # env.setup_egress(sinks, lag_monitor_interval=10)
    ```

### Strict Pacing

A paced step that starts more than `factor` real seconds late has fallen behind the wall clock. By default the run goes on and `system.simulation.lag_seconds` reports the delay. With `strict` on, the run raises `RuntimeError` (`Simulation too slow for real time (1.500s).`) instead, as SimPy's `RealtimeEnvironment` does. The setting has a different default in each API:

- **Low-level API:** `strict=True`, SimPy's default. A live script with a sink that can block passes `strict=False`.
- **Builder:** `SimulationContext(strict=False)`, the default, as a live run with a sink goes on when the sink is slow.
- **YAML:** `simulation.strict`, default `false`.

=== "YAML"

    ```yaml title="docs/snippets/yaml/environment.yaml:strict"
    # Under simulation: raise RuntimeError when a paced step is more than factor
    # seconds behind the wall clock; false by default, and never checked at factor 0
    strict: true
    ```

=== "Builder"

    ```python title="docs/snippets/pages/pacing.py:strict"
    # A paced step more than factor seconds behind the wall clock raises RuntimeError;
    # the builder's default is False
    checked = SimulationContext(sim_id="Shop", factor=1.0, strict=True)
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/pacing.py:low-level-strict"
    # The low-level API is strict unless you pass strict=False, as SimPy's is
    raising = DynamicRealtimeEnvironment(factor=1.0)
    tolerant = DynamicRealtimeEnvironment(factor=1.0, strict=False)
    ```

`strict` is never checked at `factor: 0`, including before `go_live_at`: an unpaced run has no wall clock to fall behind, and SimPy's check would raise on the first step of every unpaced script. After a pause, `env.sync()` (SimPy's) moves only the wall-clock anchor to now, so the next event would wait `factor` seconds for each simulation second since `env_start`, which is the start of the run, or the go-live instant once the run has gone live, or the last `resume_pacing()`. `env.resume_pacing()` moves both anchors, so the next event waits `factor` seconds for each simulation second after `now`:

```python title="docs/snippets/pages/pacing.py:resume-pacing"
paused = DynamicRealtimeEnvironment(factor=0.01, strict=False)  # 1.0 follows the clock
paused.run(until=10)
# ... a pause, such as a stop in a debugger: the wall clock is now ahead of the run
paused.resume_pacing()  # the next event waits factor seconds per second from here
paused.run(until=20)
```

---

## Stability and Limits

The limits of this page are listed with those of the whole section, in [Stability and Limits](index.md#stability-and-limits) on the Time and Pacing page.
