# Groups model: reply

## What I built

Arrival `a` fires at 5, 10, ..., 200 seconds (40 firings). Each firing starts a group whose size is a Poisson(2) draw, so a firing can start 0 entities. Each entity runs task `member`, which publishes `{event_type: member}` as soon as it starts. The run stops at 205 seconds, and the seed is 42.

With seed 42 the run gives 63 records. The group sizes at the 40 firings are 2, 2, 1, 0, 2, 2, 2, 1, 1, 3, 1, 3, 3, 0, 2, 0, 2, 0, 1, 1, 0, 2, 1, 3, 0, 1, 0, 1, 3, 3, 3, 1, 3, 2, 3, 1, 2, 2, 0, 3.

## The files

- `model.yaml`: the blueprint. Run it with `desflow run model.yaml --stdout`. It prints one JSON line per record.
- `builder.py`: the same model with the Builder API. Run it with `python builder.py`. It runs to 205 seconds and prints one JSON line per record.
- `low_level.py`: the same model on `DynamicRealtimeEnvironment`. Run it with `python low_level.py`. It prints one JSON line per record.
- `test_model.py`: pytest tests. Run them with `pytest test_model.py`. They run all three forms and check that the records match. They also check the firing times, the group sizes (including the 0s), the keys and instances, the time order, and that a repeat run gives the same records. All 6 pass.

## Differences between the forms

- **Timestamps.** Every record carries a wall-clock `timestamp` from when it was published, so it differs from run to run. The three forms agree on every other field: `stream_type`, `sim_ts`, `key`, `task`, `instance` and `value`. The tests compare records without `timestamp`.
- **Random draws.** The blueprint and the Builder form draw each group size from desflow's own generator, `sample` with `dist: poisson`. The Low-level form does not use the generator, because the Low-level API has no blueprint to run. It rebuilds the same random stream: the seed 42 with the spawn key from the generator's place in the file, `arrivals.a.group`. This matches the other forms, and the tests check it. It depends on desflow's stream naming, so if that naming changes, the Low-level form will stop matching the other two.
- **Builder generator.** The Builder form passes `Sample(gen="sample", dist="poisson", mean=2)` from `desflow.values.models`. That is an internal module, not part of the public API, so the import may break in a later desflow version.
- **Output format.** The blueprint and both Python files print the same record shape. The blueprint uses `--stdout`, and the Python files use `ConsoleEgress(format="json")`.
- **Pacing.** All three use `factor: 0`, so the run is not paced in real time and finishes in under a second.
- **Firings and `max_events`.** A firing with a group of 0 still counts toward the 40 firings. I checked this in desflow's source: the firing count goes up before the group is made. So the last firing is at 200 seconds, and the run has no firing at 205.

## Notes

- I ran `desflow run model.yaml`, `python builder.py`, `python low_level.py` and `pytest test_model.py` from this folder before writing this reply. Each one exited 0, and the tests passed.
- I accidentally tried to delete a file in the skill's `references` folder while I was checking paths. The delete was blocked, and I found no such file left there. Nothing in this folder depends on it.
