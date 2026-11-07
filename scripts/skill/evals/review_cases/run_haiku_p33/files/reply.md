# set_capacity from a flow: the model

The model is in three forms that give the same seven records. A task called `grow` raises the capacity of resource `m` from 1 to 3 when it finishes at 2 seconds, and the job waiting for `m` starts at that moment. The run goes to 20 seconds, with simulation id `P` and seed 42.

## Files

- **model.yaml** is the blueprint. It has `simulation.random_seed: 42`, and `desflow run model.yaml` prints the records as JSON lines.
- **builder.py** is the same model with the Builder API (`SimulationContext`). `python builder.py` runs it to 20 seconds and prints the same records.
- **low_level.py** is the same model on `DynamicRealtimeEnvironment`, with the seed written as a literal. `python low_level.py` prints the same records.
- **test_model.py** runs the three forms as subprocesses and checks the following:
  - each form gives the expected seven records;
  - the three agree with one another;
  - the second job starts at 2 s, not 5 s;
  - each job holds `m` for 5 s;
  - there are two jobs and one grow.

Run the tests with `pytest test_model.py`. All 7 tests pass, and each form prints 7 records.

## What the records show

| sim_ts | key | task | record |
|---|---|---|---|
| 0 | a-1 | job | queued, then started |
| 1 | a-2 | job | queued |
| 2 | boss-1 | grow | grow |
| 2 | a-2 | job | started (capacity is now 3) |
| 5 | a-1 | job | done |
| 7 | a-2 | job | done |

## Differences between the forms

- **Resource capacity.** The resource needs `max_cap: 3`. Without it, `max_cap` defaults to 1, and the capacity change to 3 is stored as 1 with a warning.
- **Arrivals in the builder.** `max_events` needs a `spawn` in the Builder API, and the spawn passes no context to the task, so `builder.py` starts its tasks in `arrival_loop` functions. Its arrival `a` stops after two starts, but its source keeps making entities that nothing takes. The YAML arrivals stop on their own.
- **Random seed.** The model draws no random numbers, so the seed does not change the output. The YAML and the Builder API pass it to the simulation. In `low_level.py` it is only a constant (`SEED = 42`), because `DynamicRealtimeEnvironment` takes no seed.
- **Output.** The records are the same apart from `timestamp`, which is wall-clock time and differs on each run. The tests drop it before comparing.
- **Pacing.** All three use `factor` 0, so the run is not paced in real time.

## Notes

- The change is made from a task's `on:` list in YAML, or the `on=` argument of `@app.task` in the Builder API. In the low-level form, it is `env.registry.update("P.resources.m.current_cap", 3)`.
- The Builder API and the low-level form both publish the same records as the YAML. Nothing was installed beyond the existing `py/` environment.
