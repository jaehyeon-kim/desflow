# Group arrivals: Poisson(2) group size per arrival

The model is built three ways, and all three give the same 63 records at seed 42. Arrival `a` fires at 5, 10, …, 200 (40 firings, every 5 s from t=5). At each firing it starts a group of `member` entities, with the size drawn from Poisson(mean 2). A size of 0 starts nobody, and the next firing still happens. Each `member` publishes `{event_type: member}`. The run lasts 205 s.

## Files
- `model.yaml` is the YAML blueprint. It uses `gap: 5`, `first_at: 5` and `max_events: 40`. The group size is `group: {gen: sample, dist: poisson, mean: 2}`. The seed is the literal `random_seed: 42`. Run it with `desflow run model.yaml`.
- `builder.py` is the same model with `SimulationContext`. Run it with `python builder.py`.
- `low_level.py` is the same model with `DynamicRealtimeEnvironment`, with the seed written as a literal 42. Run it with `python low_level.py`.
- `test_model.py` is the pytest file. Run it with `pytest test_model.py`. It runs all three forms and checks that:
  - the records are identical across the forms;
  - firings fall only on multiples of 5 between 5 and 200;
  - group sizes vary, and some firings are empty;
  - keys are numbered in order;
  - the total is plausible (63 at seed 42).
- `reply.md` is this note.

Builder and low-level print one JSON line per record. The YAML form prints the same lines through `Console` with `config: {format: json}`.

## What differs between the forms
- I fixed `logical_start_time` at 2026-01-01T00:00:00Z in all three, and `factor` is 0 so they run unpaced. Without a fixed start, the `timestamp` field would be the wall-clock time and the forms would differ.
- The blueprint draws each group size from its own random stream, derived from the seed and the place of the `group` entry in the file. `low_level.py` reproduces that stream by hand: `SeedSequence(42, spawn_key=(crc32("arrivals.a.group"),))`. This is why the group sizes match. If you rename the arrival or move the entry in the YAML, that string no longer matches and the low-level sizes would differ.
- The builder passes the group as `desflow.values.models.Sample(...)`, because `add_arrival` rejects a plain dict there.
- I set no resources, services or stats, so the only records are the 63 `member` finished events. There is no telemetry.
- With this seed the drawn sizes average 1.6 per firing, and some firings start nobody. This is ordinary Poisson variation.
