# Model Breakdowns

Make a resource fail at random and be repaired, decide what happens to the work it was doing, and check how long it was down.

______________________________________________________________________

## Before You Start

- [Interruptions](https://jaehyeon.me/desflow/latest/learn/shop-logic/interruptions/index.md) explains `on_interrupt`.
- [Priority and Preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#breakdowns) describes the down state.

______________________________________________________________________

## Steps

### Give the Resource a Breakdown

Give the resource a `breakdown` with two distributions: `time_to_failure`, the time from the start or from a repair to the next failure, and `repair`, how long a repair takes.

Here a packing station of two units fails about once a minute on average (`rate: 0.0167`) and takes 5 to 15 seconds to repair:

```yaml
resources:
  station:
    current_cap: 2
    max_cap: 3
    breakdown:
      # Calendar time from a repair, or the start, to the next failure.
      # A failure every 60 s on average (rate 0.0167)
      time_to_failure: {dist: exponential, rate: 0.0167}
      repair: {dist: uniform, low: 5 s, high: 15 s}
      # The orders being packed are interrupted, and follow their on_interrupt.
      work: interrupt
```

```python
.add_resource("station", current_cap=2, max_cap=3)
.add_breakdown(
    "station",
    # Calendar time from a repair, or the start, to the next failure
    # A failure every 60 s on average (rate 0.0167)
    time_to_failure={"dist": "exponential", "rate": 0.0167},
    repair={"dist": "uniform", "low": 5, "high": 15},
    # The orders being packed are interrupted, and follow their on_interrupt
    work="interrupt",
)
```

```python
breakdowns={
    "station": BreakdownConfig(
        # Calendar time from a repair, or the start, to the next failure
        # A failure every 60 s on average (rate 0.0167)
        time_to_failure=DistributionConfig(dist="exponential", rate=0.0167),
        repair=DistributionConfig(dist="uniform", low=5, high=15),
        # The orders being packed are interrupted
        work="interrupt",
    )
},
```

The breakdowns run as a SimPy process, started beside the arrivals:

```python
def processes(env, sampler, station):
    """The breakdowns and the arrivals, in the builder's order."""
    return [
        # A SimPy process: down at each failure, repaired after each repair time
        run_breakdowns(env, station, sampler, work="interrupt"),
        order_generator(env, sampler, station),
    ]
```

An interrupted order catches `simpy.Interrupt`, whose cause is `"breakdown"`, gives back its request and asks again, as the full file shows.

With the default `clock: calendar`, the next failure is drawn at the start and after each repair, and the time to it passes whether the station is busy or idle. A failure works as follows:

- All the station's units fail together. Its capacity is 0 until the repair ends, so no order starts.
- Its `current_cap` keeps the planned value. A scenario step or an ingress message that changes it during the repair sets the capacity the station returns to. It does not end the repair.
- The station publishes an event keyed by its name at each failure, `{"status": "down", "reason": "failure"}`, and at each repair, `{"status": "repaired", "reason": "failure"}`.

The two distributions are registry paths, `Shop.breakdown.station.time_to_failure` and `.repair`, so a live change can make failures more or less frequent. The two cannot both always give 0, because the station would then fail and be repaired for ever at one instant. The load refuses a pair that always gives 0. A live change that makes both 0 stops the run with an error that names the station. So does a pair that gives only 0 although its distribution has a spread, such as a normal time whose mean is many times its std below 0: the run stops after 10,000 failures at one instant.

A breakdown that lets work finish and then holds the resource for a fixed time each day is a capacity schedule. [Vary Arrivals by Time](https://jaehyeon.me/desflow/latest/how-to/modelling/time-varying-arrivals/index.md) writes it as scenario steps.

### Choose What Happens to the Work in Service

Choose `work`: `interrupt`, the default, stops the tasks being served when the resource fails. `finish` lets them finish and stops only new work.

### Choose What an Interrupted Order Does

With `interrupt`, give the task an `on_interrupt`: `resume` packs what was left after the repair, `restart` packs again from the start, and `abandon` ends the order.

### Count Only the Time the Station Works

Choose the failure `clock`. `calendar`, the default, counts the time to failure whether the station is busy or idle. `clock: busy` counts it only while at least one unit of the resource is in use, so an idle station does not wear out. The time to failure is still drawn once for each cycle. A task's `resource`, a `seize`, a wait `request` and an `open` unit all count as in use. A time to failure of 0 fails at the grant of the first unit.

The clock waits for two events of the resource. `when_busy()` succeeds when the resource next holds a unit after holding none, and `when_idle()` succeeds when it next holds none after holding one. A station that is down keeps the units its users hold, so it is not idle until they are given back.

Here a station fails after 5 seconds of work. One order arrives at 10 seconds and takes 8, and `work: finish` lets it finish:

```yaml
resources:
  station:
    current_cap: 1
    breakdown:
      time_to_failure: {dist: constant, value: 5}
      repair: {dist: constant, value: 2}
      work: finish
      # Count the time to failure only while a unit is in use. The default, calendar,
      # counts it from the start or the last repair, busy or idle.
      clock: busy
```

```python
app.add_breakdown(
    "station",
    time_to_failure={"dist": "constant", "value": 5},
    repair={"dist": "constant", "value": 2},
    work="finish",
    # Count the time to failure only while a unit is in use. The default,
    # "calendar", counts it from the start or the last repair, busy or idle.
    clock=clock,
)
```

```python
breakdowns={
    "station": BreakdownConfig(
        time_to_failure=DistributionConfig(dist="constant", value=5),
        repair=DistributionConfig(dist="constant", value=2),
        work="finish",
        # Count the time to failure only while a unit is in use. The default,
        # "calendar", counts it from the start or the last repair.
        clock=clock,
    )
},
```

The process that fails and repairs the station takes the same `clock`:

```python
# A SimPy process that puts the station down at each failure and repairs it;
# clock is passed to it as well as set in the config
env.process(
    run_breakdowns(env, station, Sampler(seed=42), work="finish", clock=clock)
)
```

In the Builder and Low-level blocks, `clock` is the argument of the function that builds the model: `"busy"` or `"calendar"`.

With the busy clock, every form puts the station down at 15 seconds, 5 seconds into the order, and repairs it at 17 seconds: `[(15.0, 'down'), (17.0, 'repaired')]`. With `clock: calendar`, the same station fails at 5, 12, 19 and 26 seconds, mostly while it is idle.

______________________________________________________________________

## Check the Result

Download the example's three forms into the folder you work in with `desflow examples get breakdowns`. The check is a script, `check_breakdowns.py`, made of the blocks below in order. It reads the example from `breakdowns/yaml/`, `breakdowns/builder/` and `breakdowns/low-level/`, so run it from the folder where you ran the command.

With statistics at `minimal` or above, the station publishes `down`: 1 while it is down and 0 otherwise. From `basic` it also publishes `downtime`, the seconds it has been down, and `failures`, how many times it has failed. The script starts with a class that keeps every record the example publishes, and a function that loads an example file without running it. The imports of desflow follow the class, in the line `from desflow import DynamicRealtimeEnvironment, Sampler, SimulationContext`:

```python
import asyncio
import importlib.util
import queue


class Capture:
    """An egress that keeps every record it receives."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        try:
            while True:
                try:
                    self.records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.005)
        except asyncio.CancelledError:
            pass
```

```python
def _example(path: str):
    """Loads an example script as a module, without running it."""
    spec = importlib.util.spec_from_file_location("breakdowns_example", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
```

The next function takes the records of a run, its end time and its statistics, finds the down periods from the events, and checks both counts against them. It is the same for every form:

```python
def availability(records: list, end: float, stats: dict) -> dict:
    """The down periods of the station, checked against its statistics."""
    periods, outcomes = [], {"interrupted": 0, "resumed": 0, "order_packed": 0}
    for record in records:
        if record["stream_type"] != "event":
            continue
        value = record["value"]
        status = value.get("status", value.get("event_type"))
        if record["key"] == "station" and status == "down":
            periods.append([record["sim_ts"], None])
        elif record["key"] == "station" and status == "repaired":
            periods[-1][1] = record["sim_ts"]
        elif status in outcomes:
            outcomes[status] += 1
    # A repair still going on at the end counts up to the end
    downtime = sum((repaired or end) - down for down, repaired in periods)
    name = "Shop.stats.resources.station.{}".format
    assert stats[name("failures")] == len(periods)
    # Records round times to the millisecond
    assert abs(stats[name("downtime")] - downtime) < 0.01
    return {
        "periods": [(round(d, 1), r if r is None else round(r, 1)) for d, r in periods],
        "downtime": round(downtime, 1),
        "share_down": round(downtime / end, 3),
        **outcomes,
    }
```

The YAML and builder forms add a capturing egress beside the example's own. The low-level form builds the example's environment with the capture as its only egress. Each passes what it captured to `availability`:

```python
def yaml_form(path: str = "breakdowns/yaml/breakdowns.yaml") -> dict:
    app = SimulationContext.from_yaml(path)
    capture = Capture()
    app.add_egress(capture)
    app.run()  # until 300, from the blueprint
    return availability(capture.records, app.env.now, app.env.stats.readings())
```

```python
def builder_form(path: str = "breakdowns/builder/breakdowns_example.py") -> dict:
    app = _example(path).app
    capture = Capture()
    app.add_egress(capture)
    app.run(until=300)
    return availability(capture.records, app.env.now, app.env.stats.readings())
```

```python
def low_level_form(path: str = "breakdowns/low-level/breakdowns_example.py") -> dict:
    example = _example(path)
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(example.PARAMS)
    capture = Capture()
    env.setup_egress([capture])
    env.setup_stats("Shop", "basic", interval=60.0)
    station = example.build(env)
    for process in example.processes(env, Sampler(seed=42), station):
        env.process(process)
    env.run(until=300)
    env.teardown()  # publishes the last statistics and flushes the capture
    return availability(capture.records, env.now, env.stats.readings())
```

Last, the script runs the three forms. Run it, and keep the lines that start with a brace, because the example also prints each of its events:

```python
if __name__ == "__main__":
    for form in (yaml_form, builder_form, low_level_form):
        print(form())
```

```bash
# With uv
uv run --no-project --with desflow check_breakdowns.py | grep "^{"
# With pip
pip install desflow
python check_breakdowns.py | grep "^{"
```

```text
{'periods': [(84.6, 98.5), (150.1, 157.3), (238.9, 253.5), (272.7, 282.6), (285.0, 291.8)], 'downtime': 52.5, 'share_down': 0.175, 'interrupted': 8, 'resumed': 8, 'order_packed': 184}
{'periods': [(84.6, 98.5), (150.1, 157.3), (238.9, 253.5), (272.7, 282.6), (285.0, 291.8)], 'downtime': 52.5, 'share_down': 0.175, 'interrupted': 8, 'resumed': 8, 'order_packed': 184}
{'periods': [(84.6, 98.5), (150.1, 157.3), (238.9, 253.5), (272.7, 282.6), (285.0, 291.8)], 'downtime': 52.5, 'share_down': 0.175, 'interrupted': 8, 'resumed': 8, 'order_packed': 184}
```

For one 300-second run of the example, every form returns five down periods. The first is from 84.6 to 98.5 seconds. Together they give **52.5** seconds of downtime in 300, or 17.5 percent. **8** orders are interrupted, and all 8 resume after a repair. Report the downtime beside the waits: a station that is down 17.5 percent of the time has that much less capacity, and its waits grow as on [Queues and Capacity](https://jaehyeon.me/desflow/latest/learn/first-model/queues-and-capacity/index.md).

______________________________________________________________________

## Example

The `breakdowns` example is the model above: two units at a packing station, an order every 1.67 seconds on average (`rate: 0.6`), and every order packed once the station is back. It needs no Docker service and ends on its own after 300 simulation seconds.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get breakdowns --api yaml
cd breakdowns/yaml

# 3. Run it
desflow run breakdowns.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get breakdowns --api yaml
cd breakdowns/yaml

# 3. Run it
desflow run breakdowns.yaml
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get breakdowns --api builder
cd breakdowns/builder

# 3. Run it
uv run --no-project --with desflow breakdowns_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get breakdowns --api builder
cd breakdowns/builder

# 3. Run it
python breakdowns_example.py
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get breakdowns --api low-level
cd breakdowns/low-level

# 3. Run it
uv run --no-project --with desflow breakdowns_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get breakdowns --api low-level
cd breakdowns/low-level

# 3. Run it
python breakdowns_example.py
```
