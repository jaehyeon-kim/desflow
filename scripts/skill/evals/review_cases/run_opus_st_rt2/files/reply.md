# ST-RT2: a paced run at factor 0.1, with the run called twice

I wrote the model in all three desflow forms. All three print the same 16 records, byte for byte, and the tests pass (11 passed).

## The model

- **Pacing:** factor 0.1, so one simulation second takes 0.1 s of wall time.
- **Arrival `a`:** starts 2 entities at time 0, `a-1` and `a-2`. Each runs task `pem`.
- **Task `pem`:** each pass takes 1 s, publishes `{event_type: tick}`, then starts `pem` again, for ever.
- **The run is called twice:** first to 2 s, then on to 4 s. After each call the run writes a `system.simulation.segment` record: `{index: 0, now: 2, peek: 2}`, then `{index: 1, now: 4, peek: 4}`.
- **What comes out:** ticks at 1, 2 and 3 for each entity, 6 in all. The run stops at 4, before the tick at 4 is published. Each pass also writes a `started` record, so the 16 records are 8 `started`, 6 `tick` and 2 segment records.
- **Seed:** 42, written as a literal number. The model draws no random numbers, so the seed changes nothing.

## Settings I added

You didn't ask for these two; I added them so the three forms' outputs can be compared exactly:

- **A fixed start time** (`logical_start_time` = 2026-01-01T00:00:00Z). Without it, each record's timestamp depends on when the run starts.
- **No lag readings** (`lag_interval: 0`). By default a paced run writes a `system.simulation.lag_seconds` record every simulation second. Those measure the wall clock, not the model, and change from run to run.

## Files

- **`model.yaml`**: the YAML blueprint. Records go to the console as JSON lines (`Console` with `format: json`), and `run.segments` with `{until: 2}` and `{until: 4}` makes the two run calls. Run it with `desflow run model.yaml`.
- **`builder.py`**: the same model with `SimulationContext`. It uses `add_initial_arrival("a", 2, spawn="pem")`, a `@app.task(service_time=1, next="pem")` that returns the tick, and `app.run(until=4, segments=[{"until": 2}, {"until": 4}])`. Run it with `python builder.py`.
- **`low_level.py`**: the same model on `DynamicRealtimeEnvironment(factor=0.1)`. It calls `env.run(until=2)` and then `env.run(until=4)`. Run it with `python low_level.py`.
- **`test_model.py`**: runs each form as its own process and checks:
  - the three forms give the same records;
  - each entity ticks at exactly 1, 2 and 3, with no event at 4;
  - both segment records are there, and no lag readings;
  - each run takes at least 0.4 s of wall time.

  It also times the builder run inside the test process, so Python's start-up time isn't counted: it must take from 0.4 s up to 5 s. Run it with `pytest test_model.py`.

## Differences between the forms

- **Writing the records by hand.** In the YAML and builder forms, desflow writes the `started` and `tick` records and the segment readings. In `low_level.py`, the `pem` loop writes the `started` and `tick` events itself, with one instance counter shared by both entities, in the order the instances start (0 to 7). After each `env.run` call, `low_level.py` writes the segment reading the same way `run.segments` does.
- **The seed.** In YAML and the builder it is `random_seed: 42`. In `low_level.py` it is a `Sampler(rng=np.random.default_rng(42))` that is never used, because nothing in the model is random.
- **Wall time.** The four simulation seconds take about 0.4 s. A whole command takes longer, about 1.3 s for `desflow run`, because of start-up and the sinks being shut down at the end. That is why the timing tests check "at least 0.4 s" and not an exact time.
