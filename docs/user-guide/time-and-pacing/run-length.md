# Run Length

This page shows how a run starts and ends, described in [Time and Pacing](index.md): the [warm-up](../../des/output-analysis.md#one-run-starts-empty), the initial time, the run length, stopping early, draining, stepping and running in segments.

---

## How to Use It

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

The warm-up drops early records; `go_live_at` drops nothing and only decides which sink gets each one. [Trusting Results](../../tutorials/trusting-results.md) shows how to choose a warm-up length.

### Initial Time

`initial_time` is the reading of the simulation clock when the run starts, 0 when left out, in seconds or as a duration. A model of a shift that starts at 8 in the morning can start its clock at 8 hours, so every event time, every record's `sim_ts` and `until` read that clock; a record's `timestamp` is still `logical_start_time` plus `sim_ts`, to within the 1 millisecond by which a rounded `sim_ts` and a cut `timestamp` can differ:

=== "YAML"

    ```yaml title="docs/snippets/yaml/initial_time.yaml:initial-time"
    simulation:
      sim_id: Shop
      factor: 0
      # The clock starts at 8 h, so the first order at 8 h 10 min is at sim_ts 29400
      initial_time: 8 h
    ```

=== "Builder"

    ```python title="docs/snippets/pages/pacing.py:initial-time"
    # The clock starts at 8 hours: event times, sim_ts and until all read it
    shift = SimulationContext(sim_id="Shop", factor=0.0, initial_time="8 h")
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/pacing.py:low-level-initial-time"
    # The clock starts at 8 hours, in seconds, as in SimPy's Environment(initial_time=)
    morning = DynamicRealtimeEnvironment(factor=0.0, initial_time=8 * 3600)
    ```

`until` is a clock reading too, so a run with `initial_time: 8 h` and `until: 16 h` lasts 8 hours. A scenario step's `at` and the `warmup` are lengths of time from the start of the run instead, so with `initial_time: 8 h` a step at `30 min` applies at 8.5 hours, and a warm-up of `10 min` ends at 8 hours 10 minutes. The state the run starts with is declared beside it: a resource's `current_cap`, a container's `level`, a store's `items` and an arrival's `attributes` hold from the initial time, before the first arrival, and the tests check each against plain SimPy ([SimPy Compatibility](../../reference/simpy-compatibility.md)).

### Run Length

`until` is in simulation seconds. `run()` and a blueprint's `run.until` also take a duration such as `"10 min"`, `"8 hours"` or `"1 week"`; a month counts as 30 days and a year as 365.

```python title="docs/snippets/pages/pacing.py:run-length"
app.add_resource("packers", current_cap=1, max_cap=1)
app.run(until="1 week")  # 604,800 simulation seconds; a number is seconds too
```

The run stops at `until` before any event due at exactly that time, as SimPy's `run(until=...)` does: an order due at the horizon never arrives, and a task due to finish there stays unfinished. Statistics published at teardown read the model at `until`.

Events due at the same time run by priority, and then in the order they were scheduled (SimPy 4.1.2, `core.py`: `schedule` and `run`; `URGENT` and `NORMAL` in `events.py`). `run(until=...)` schedules its stop at urgent priority, so the stop comes before the ordinary events due at that time. How a scenario step or a change from an ingress connector ranks among them is in [Live Parameters](../live-parameters.md#script-an-experiment). Here two orders are packed at 5 seconds, and one is due at the horizon of 10 seconds:

```python title="docs/snippets/pages/pacing.py:same-time"
ties = DynamicRealtimeEnvironment(factor=0.0)
packed = []


def pack(name, seconds):
    yield ties.timeout(seconds)
    packed.append(name)


ties.process(pack("first", 5))
ties.process(pack("second", 5))  # due at the same time: runs after "first"
ties.process(pack("at the horizon", 10))
ties.run(until=10)
print(packed)  # ['first', 'second']: nothing due at 10 runs
```

Without `until`, SimPy runs until no events remain, and a run with an arrival never runs out of events. Neither does any run with a sink or an ingress connector, even when its own work is done, because the environment's own processes repeat for as long as the run lasts. Give such a run an `until`, a `stop` or a drain, or stop it with Ctrl+C.

### Stop Early

`stop` ends the run before `until` when something happens: `{signal: name}`, the next firing of a signal, or `{condition: ...}`, the first moment a condition holds, checked after every change of a level, a capacity, a registry value or a signal, and after every finished task. The builder's `run(stop=...)` also takes a signal's name, or a function of the context, called once the run is built, that returns any SimPy event, such as `lambda context: context.env.timeout(30)`. The run ends at whichever comes first, `stop` or `until`, and no other event at that moment runs after it; when both fall at the same moment, `until` ends the run, as SimPy's own `until` would. In the low-level API it is SimPy's `env.run(until=event)`.

`run()` and `finish()` return what SimPy's `env.run(until=event)` returns: the value of the event that ended the run. A `stop` on a signal gives the signal's value, such as the finished payload of the task that fired it; a `stop` on a condition gives `True`; a function's event gives its own value. A run that `until` or a drain ends returns `None`, and so does one where `until` and the stop fall at the same moment. If the signal fails, the run closes, the sinks receive their records, and then `run()` raises the `SignalFailed` the signal failed with, as SimPy raises the exception of a failed event; `desflow run` reports it and exits with an error.

### Drain

A fixed horizon cuts the run at `until` with orders still in the shop. A **drain** stops the arrivals at `until` and keeps running until every entity in the model has left: `drain: true`, or `drain: {limit: 1 h}` to stop at the latest that long after `until`; `desflow run --drain` sets it from the command line. The environment's own processes, such as the statistics timer, never hold a drain open. The `end-conditions` example closes the shop at 120 seconds, drains for at most a minute, and stops at once if the boxes run out:

=== "YAML"

    ```yaml title="examples/yaml/end_conditions.yaml:run"
    run:
      # No new orders after closing time.
      until: 120
      # Then pack the orders in the shop, for at most 60 seconds.
      drain: {limit: 60}
      # Stop at once when the last box is taken.
      stop: {condition: level.boxes == 0}
    ```

=== "Builder"

    ```python title="examples/builder/end_conditions_example.py:run"
    app.run(
        # No new orders after closing time
        until=120,
        # Then pack the orders in the shop, for at most 60 seconds
        drain={"limit": 60},
        # Stop at once when the last box is taken
        stop={"condition": "level.boxes == 0"},
    )
    ```

    `context.stop_arrivals()` stops the arrival loops by hand, as a drain does at `until`.

=== "Low-level"

    ```python title="examples/low-level/end_conditions_example.py:run"
    def run_to_end(env, shop, source):
        """Runs to closing time, then drains for at most 60 s, or until no boxes are left."""
        out_of_boxes = env.event()

        def check():
            if shop["boxes"].level == 0 and not out_of_boxes.triggered:
                out_of_boxes.succeed()

        env.watch_changes(check)
        # Closing time, unless the boxes run out first, which ends the run at once
        env.run(until=env.timeout(120) | out_of_boxes)
        if out_of_boxes.triggered:
            return
        source.interrupt()  # no new orders after closing time
        packed = env.all_of([p for p in shop["orders"] if p.is_alive])
        env.run(until=packed | env.timeout(60) | out_of_boxes)
    ```

    The low-level API writes a drain as SimPy does: interrupt the source process, then run until all the admitted processes end.

With statistics at `basic` or above, `flows.<arrival>.started`, `finished` and `in_system` say how many entities entered, left and are still in the model, and from `minimal`, `tasks.<task>.in_progress` how many instances of each task are queued or being served; [End Conditions](../../tutorials/end-conditions.md) compares a fixed horizon with a drain.

### Step and Peek

`start()` builds everything `run()` builds and starts every process, with the clock still at its start. `step()` then processes the next event of the model, and `peek()` gives the time of the next one, infinity when none is left, as SimPy's `env.step()` and `env.peek()` do. Both count only the model's events, so a script steps the model as it would in SimPy. The events desflow adds, such as the statistics timer, the flush timers, the lag and ingress monitors and a resource's capacity watcher, do not count: `peek()` skips them, and `step()` runs those due before the model's event in the same call. When only desflow's events are left, `step()` raises `EmptySchedule` and `peek()` gives infinity; `finish()` and `run()` still process them. `finish()` runs a started run on to the end `run()` would reach, `until`, the `stop` or the end of the drain, then closes it, so `run()` is `start()` then `finish()`; when `advance()` or `step()` has brought the clock to `until` or past it and there is no `stop` or `drain`, it closes the run at once, as a run that ended at `until` does, and when `step()` brought the clock to exactly `until`, the records hold one run's records (which end with the final statistics) plus those of the events at `until` that `step()` processed, as in SimPy; a statistics reading due at `until` is not published again, and the close adds only the values that changed after it; after steps past `until` the run closes where the steps stopped, with the final statistics at that time; `close()` ends the run where it is. A context from `from_yaml` steps the same way:

```python title="docs/snippets/pages/pacing.py:step"
def every_ten(context):
    while True:
        yield context.env.timeout(10)


stepped = SimulationContext(sim_id="Shop", factor=0.0).add_process(every_ten)
stepped.start()  # builds everything run() builds; the clock is still at 0
while stepped.peek() < 30:  # the time of the next event
    stepped.step()  # processes the next model event, after desflow's own events due before it
print(f"stopped before {stepped.peek()} at {stepped.env.now}")
stepped.close()  # the teardown run() does at its end
```

It prints `stopped before 30 at 20`. Stepping does not stop at `until`: compare `peek()` with the time you want to stop at.

### Run in Segments

`advance(until)` runs a started run up to a time and returns with the run still open, so a script can read the model, change it with `context.env`, or wait, then run the next segment. `finish()` runs on to the end, as it does after `start()`:

```python title="docs/snippets/pages/pacing.py:segments"
def pulse(context):
    while True:
        yield context.env.timeout(10)


segmented = SimulationContext(sim_id="Shop", factor=0.0).add_process(pulse)
segmented.start(until=60)
segmented.advance(25)  # runs the model to 25; the events at 25 wait for the next call
segmented.advance("40 s")  # a duration is seconds from the start, not from now
print(f"paused at {segmented.env.now}")
segmented.finish()  # on to start()'s until, 60, and closes
```

It prints `paused at 40.0`. Segments give the records one `run()` gives, the telemetry and statistics included, because nothing is flushed at a segment boundary. The same holds for a script that calls `env.run(until=...)` again on a low-level environment, as in SimPy. A few rules:

- `until` is a time after `now`, in seconds or as a duration such as `"10 min"`, as a time in the run; a time that is not after `now` raises `ValueError`, as SimPy does. `advance()` before `start()` or after `close()` raises `RuntimeError`.
- The run stops at the earlier of `until` and its own end: `start()`'s `until`, the `stop` or the end of a drain. A later `advance()` raises `RuntimeError`; call `finish()`, which returns the end's value (`None` for `until` or a drain, the stop's value for a stop, and a failed stop's exception raised again), or `close()`.
- `until` may be an event, such as `context.signal("gate").wait()`: the run stops where the event fires and returns its value, and the next `advance()` goes on, as SimPy's `env.run(until=event)` does.
- The event that ends a segment stays in the queue: `peek()` gives the segment's time and `step()` processes it.
- `advance()` does not move the wall-clock anchor. A paced run that waited between segments is behind when it goes on, so call `env.resume_pacing()` first.

---

## Stability and Limits

The limits of this page are listed with those of the whole section, in [Stability and Limits](index.md#stability-and-limits) on the Time and Pacing page.

---

## Example: End Conditions

The `end-conditions` example: a drain after closing time, a stop when the boxes run out, and the counts of orders still in the shop. It needs no Docker service and ends on its own.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get end-conditions --api yaml
        cd end-conditions/yaml

        # 3. Run it
        desflow run end_conditions.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get end-conditions --api yaml
        cd end-conditions/yaml

        # 3. Run it
        desflow run end_conditions.yaml
        ```

    The whole file is [`examples/yaml/end_conditions.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/end_conditions.yaml).

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get end-conditions --api builder
        cd end-conditions/builder

        # 3. Run it
        uv run --no-project --with desflow end_conditions_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get end-conditions --api builder
        cd end-conditions/builder

        # 3. Run it
        python end_conditions_example.py
        ```

    The whole file is [`examples/builder/end_conditions_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/end_conditions_example.py).

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get end-conditions --api low-level
        cd end-conditions/low-level

        # 3. Run it
        uv run --no-project --with desflow end_conditions_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get end-conditions --api low-level
        cd end-conditions/low-level

        # 3. Run it
        python end_conditions_example.py
        ```

    The whole file is [`examples/low-level/end_conditions_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/end_conditions_example.py).
