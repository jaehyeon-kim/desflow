# set_capacity from a flow

I built the model in all three forms. They print the same 7 records, byte for byte, and the 6 tests pass.

The task `grow` (arrival `boss`, at t=2) publishes `{event_type: grow}` and then raises the capacity of resource `m` from 1 to 3. It does this by setting the registry path `P.resources.m.current_cap` to 3. desflow has no `set_capacity` call, so writing to that path is how a flow changes a capacity. Job `a-2` arrives at t=1 and waits behind `a-1`. Because `grow` raises the capacity, `a-2` starts at t=2 instead of t=5:

| sim_ts | key | record |
|---|---|---|
| 0 | a-1 | job queued |
| 0 | a-1 | job started |
| 1 | a-2 | job queued |
| 2 | boss-1 | grow |
| 2 | a-2 | job started |
| 5 | a-1 | done |
| 7 | a-2 | done |

## Numbers I added (please confirm)

- **`max_cap: 3` on `m`.** You only gave capacity 1. A resource's `max_cap` defaults to its `current_cap`, so I set it to 3 to give `m` room to grow.
- **`factor: 0` (unpaced).** The run finishes at once instead of taking 20 real seconds. The records are the same either way.
- **`logical_start_time` fixed at 2026-01-01T00:00:00Z.** The `timestamp` field otherwise depends on the wall clock. Fixing it makes all three outputs identical, timestamps included.

## Files

- **model.yaml**: the blueprint, with seed 42 written as a number in `simulation.random_seed`. Console egress in JSON format prints one line per record. The capacity change is an `on:` entry of `grow`: `{path: P.resources.m.current_cap, value: 3}`. Run it with `desflow run model.yaml`.
- **builder.py**: the same model built with `SimulationContext`. `grow` is a task with the same `on=[...]` entry. `python builder.py` runs it to t=20 and prints JSON lines.
- **low_level.py**: the same model written directly on `DynamicRealtimeEnvironment` with a `DynamicResource`. `grow` is a process that publishes its event and then calls `env.registry.update("P.resources.m.current_cap", 3)`. Seed 42 is a literal in the `Sampler`. `python low_level.py` prints JSON lines.
- **test_model.py**: runs all three forms as subprocesses. It checks that:
  - all three give the same records
  - the full list of records is as in the table above
  - `a-2` queues at 1 and starts at 2
  - `grow` is published before `a-2` starts
  - each job holds `m` for 5 s
  - there are 2 `done` records and 1 `grow` record

## Differences between the forms

The records don't differ. The code does, in three ways:

- **Capacity change:** the YAML and builder forms use the task's `on:` entry. The low-level form makes the same registry update in Python, after publishing `grow`.
- **Arrivals:** the low-level form writes the arrivals as plain processes: one job at 0 and one at 1, then `boss` at 2. The other two forms use constant arrivals with `first_at` and `max_events`.
- **Seed:** nothing in this model is random, since the service and the arrival gaps are constants. The seed is set but changes no output.
