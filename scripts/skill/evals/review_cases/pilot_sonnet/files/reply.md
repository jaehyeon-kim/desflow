I built the model "set_capacity from a flow" in three forms and a test file. All three forms print the same 7 records (they differ only in the wall-clock `timestamp`). Run each file and the tests: all 13 tests pass.

Files:

- model.yaml: the YAML blueprint, with `random_seed: 42`. Run it with `desflow run model.yaml`.
- builder.py: the same model with `SimulationContext`. Run it with `python builder.py`.
- low_level.py: the same model with `DynamicRealtimeEnvironment`, seed 42 as a literal. Run it with `python low_level.py`.
- test_model.py: runs the three forms and checks the records.
- commands.txt: the commands I ran.

What the model does: `m` starts with capacity 1. Job `a-1` holds it from 0 to 5. Job `a-2` queues at 1. At time 2 the `grow` task publishes `{event_type: grow}` and sets the capacity of `m` to 3, so `a-2` starts at 2 and is done at 7. Without the change it would have started at 5.

Differences between the forms:

- `run_task.md` was not in the folder, so I followed the task text only.
- `m` has `max_cap: 3` in all forms, because the capacity cannot be raised above `max_cap`.
- The YAML and Builder forms use `on: [{path: P.resources.m.current_cap, value: 3}]` on `grow`. The Low-level form calls `env.registry.update` for the same path.
- All forms use `factor: 0` (unpaced) and the Console egress with the `json` format, so each record is one JSON line.
- The tests compare the records without `timestamp`, which is a wall-clock value.
