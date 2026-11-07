# Paced tick model (factor 0.1)

Two entities from arrival `a` start at time 0 and run task `pem`. Each entity waits 1 s, publishes `{event_type: tick}`, then starts `pem` again, so it ticks for ever. The environment is paced at factor 0.1. The run is called twice, to 2 s and then on to 4 s. Each entity ticks at 1, 2 and 3. Nothing is published at 4, because the run stops before events due at 4.

## Files
- `model.yaml`: the YAML blueprint (seed 42 in `simulation.random_seed`). The Console sink writes JSON lines. The two calls are `run.segments: [{until: 2}, {until: 4}]`. Run it with `desflow run model.yaml`.
- `builder.py`: the same model with `SimulationContext`. `python builder.py` prints one JSON line per record.
- `low_level.py`: the same model with `DynamicRealtimeEnvironment`. It calls `env.run(until=2)` and then `env.run(until=4)`. `python low_level.py` prints one JSON line per record.
- `test_model.py`: 11 tests, all passing, run with `pytest test_model.py`. They run all three forms and check:
  - the six ticks;
  - that no tick falls at 4;
  - the two segment records at 2 and 4;
  - that the wall time is at least 0.4 s;
  - that the three forms give the same records, apart from the wall-clock `timestamp`.

## Differences between the forms
- The records match exactly across the three forms, apart from `timestamp`. Each run has 26 records: 6 ticks, the `queued` and `started` records of each loop instance, and telemetry.
- The self-loop starts a new `pem` instance every second, so `queued` and `started` records appear next to the ticks.
- `lag_seconds` telemetry is published by the environment. The `system.simulation.segment` records come from the `segments` option in the YAML and builder forms. In `low_level.py` they are published by hand after each `env.run`.
- `low_level.py` draws no random numbers. Its seed 42 is only the literal in a `Sampler`'s `default_rng(42)`.
- In `builder.py` the task function takes `(task_id, *args, **kwargs)`, because the initial entities pass it no `context`.
- `model.yaml` sets the Console sink with `config: {format: json}`, so it prints JSON lines like the other two.
