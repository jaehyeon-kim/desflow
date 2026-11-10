# set_capacity from a flow: model P

The model is built three ways, and all three give the same records. Each run ends at 20 s and prints one JSON line per record.

**Behaviour**
- `m` starts with capacity 1.
- `a-1` takes `m` at 0 s and finishes at 5 s.
- `a-2` arrives at 1 s and queues.
- `boss-1` runs `grow` at 2 s. It publishes `{event_type: grow}` and sets `P.resources.m.current_cap` to 3. `a-2` starts at once and finishes at 7 s. Capacity 1 would have made it 10 s.

**Files**
- `model.yaml`: the blueprint. Seed 42 is a literal. The `Console` egress is set to JSON lines.
- `builder.py`: the same model with `SimulationContext`. Run it with `python builder.py`.
- `low_level.py`: the same model with `DynamicRealtimeEnvironment`, `DynamicResource` and `Sampler(default_rng(42))`. Run it with `python low_level.py`.
- `test_model.py`: runs all three forms. It checks they agree, that the event timeline is as above, and that `a-2` finishes at 7 s.

**Things to know**
- I set `max_cap: 3` on `m`. It is my addition, because the capacity can't be raised above `max_cap`.
- `factor: 0` is set in every form, so the runs are unpaced and finish at once.
- `timestamp` is wall-clock and differs between runs. The tests ignore it and the `system.simulation.lag_seconds` telemetry. Everything else is identical.
- The `grow` entry in `on:` runs when the task finishes, after its record is published. The record comes first and the capacity change follows at the same instant.
- In `low_level.py` the arrival and the `grow` step are written out by hand. In `builder.py` a task function takes `task_id` only, because the arrival's `spawn` starts it.
- The tests passed (3 passed). I ran each of the three forms myself: each prints 7 records.
