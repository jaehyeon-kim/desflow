# Model P: `set_capacity` from a flow

The model is built three ways, and the three give the same records. I ran each file and `pytest test_model.py`; all 3 tests pass.

## What happens in the run
- `m` starts at capacity 1.
- At 0 s, `a-1` takes `m`.
- At 1 s, `a-2` queues.
- At 2 s, `boss-1` runs `grow`. It publishes `{event_type: grow}` and raises `m` to 3, so `a-2` starts at once.
- `a-1` is done at 5 s and `a-2` at 7 s. The run ends at 20 s.

## Files
- `model.yaml`: the blueprint. `grow` raises the capacity with an `on:` entry that sets `P.resources.m.current_cap` to 3. The seed is the literal 42.
- `builder.py`: the same model with `SimulationContext`, with `job` and `grow` as decorated tasks. Run it with `python builder.py`.
- `low_level.py`: the same model with `DynamicRealtimeEnvironment`, written as plain SimPy processes. The seed is the literal 42. Run it with `python low_level.py`.
- `test_model.py`: one test checks the exact event sequence of the YAML run. Two tests check that `builder.py` and `low_level.py` give the same records as the YAML.

## Points to know
- **Resource limit:** `m` has `max_cap: 3` (the request only said capacity 1), because the capacity cannot be raised above that limit.
- **Arrival timing:** `a` is a constant 1 s gap with `first_at: 0` and `max_events: 2`. `boss` has `first_at: 2` and `max_events: 1`.
- **Timestamp field:** each record's `timestamp` starts from the wall-clock time of the run, so it differs between runs and between the three forms. The tests leave it out. Every other field is identical, including `sim_ts`.
- **Console output:** the records print as JSON lines on stdout, and desflow's log lines go to stderr. The Console egress is set to JSON format, and `python builder.py` and `python low_level.py` need the `py/` interpreter.
- **Instance counters:** in `low_level.py` I set the `task`, `instance` and key fields by hand to match what the YAML produces (`a-1`, `a-2`, `boss-1`).
- **Example download:** `desflow examples get` was blocked by the network sandbox, so I wrote the models from the skill's reference files instead of a downloaded example.
