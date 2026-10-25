# Model Breakdowns

Make a resource fail at random and be repaired, decide what happens to the work it was doing, and check how long it was down. It builds on [Interruptions](https://jaehyeon.me/desflow/latest/tutorials/interruptions/index.md), which explains `on_interrupt`, and on [Priority and Preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#breakdowns), which describes the down state.

______________________________________________________________________

## Steps

1. Give the resource a `breakdown` with two distributions: `time_to_failure`, the time from the start or from a repair to the next failure, and `repair`, how long a repair takes.
1. Choose `work`: `interrupt`, the default, stops the tasks being served when the resource fails. `finish` lets them finish and stops only new work.
1. With `interrupt`, give the task an `on_interrupt`: `resume` packs what was left after the repair, `restart` packs again from the start, and `abandon` ends the order.

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

The failure clock runs on calendar time: the next failure is drawn at the start and after each repair, whether the station is busy or idle. A failure works as follows:

- All the station's units fail together. Its capacity is 0 until the repair ends, so no order starts.
- Its `current_cap` keeps the planned value. A scenario step or an ingress message that changes it during the repair sets the capacity the station returns to. It does not end the repair.
- The station publishes an event keyed by its name at each failure, `{"status": "down", "reason": "failure"}`, and at each repair, `{"status": "repaired", "reason": "failure"}`.

The two distributions are registry paths, `Shop.breakdown.station.time_to_failure` and `.repair`, so a live change can make failures more or less frequent.

A breakdown that lets work finish and then holds the resource for a fixed time each day is a capacity schedule. [Vary Arrivals by Time](https://jaehyeon.me/desflow/latest/guides/time-varying-arrivals/index.md) writes it as scenario steps.

______________________________________________________________________

## Check the Downtime

With statistics at `minimal` or above, the station publishes `down`: 1 while it is down and 0 otherwise. From `basic` it also publishes `downtime`, the seconds it has been down, and `failures`, how many times it has failed. This function runs the example, finds the down periods from the events, and checks both counts against them:

```python
def availability(path: str) -> dict:
    """The down periods of the station, checked against its statistics."""
    app = SimulationContext.from_yaml(path)
    capture = Capture()
    app.add_egress(capture)
    app.run()
    periods, outcomes = [], {"interrupted": 0, "resumed": 0, "order_packed": 0}
    for record in capture.records:
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
    end = app.env.now
    # A repair still going on at the end counts up to the end
    downtime = sum((repaired or end) - down for down, repaired in periods)
    stats = app.env.stats.readings()
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

For one 300-second run of the example it returns five down periods, the first from 84.6 to 98.5 seconds, **52.5** seconds of downtime in 300 (17.5 percent), and **8** orders interrupted, all 8 resumed after a repair. Report the downtime beside the waits: a station that is down 17.5 percent of the time has that much less capacity, and its waits grow as on [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md).

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
