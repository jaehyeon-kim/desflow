# Model M: resource `res` with per-call monitoring

The model has one resource `res` of capacity 1 and one arrival `a` at time 0. It starts one entity that runs task `use`: it takes `res`, holds it for 1 second and publishes `{event_type: used}`. The run stops at 5 seconds, with seed 42.

Monitoring uses desflow's built-in statistics with `on_change`, which publishes the readings of a resource right after each `request` and each `release` call, instead of on a timer. The statistic names are `M.stats.resources.res.in_use` and `M.stats.resources.res.queue_length`. The timer interval is 1000 seconds, longer than the run, so no timer publication happens.

## Files

- **model.yaml**: the blueprint. `simulation.random_seed: 42` is a literal. Run it with `desflow run model.yaml`.
- **builder.py**: the same model with the Builder API (`SimulationContext`). Run it with `python builder.py`. It prints one JSON line per telemetry record of `res`.
- **low_level.py**: the same model with the Low-level API (`DynamicRealtimeEnvironment`, `DynamicResource`, and a SimPy process for the entity). Run it with `python low_level.py`. It prints the same lines.
- **test_model.py**: pytest tests. Run them with `pytest test_model.py`. They run all three forms, check the blueprint's values, check the expected `res` records, and check that the three forms give the same records. All 7 pass.

## Records

Each form prints these `res` records. Only `in_use` and `queue_length` are the monitored values; `capacity` and `utilization` are also published by desflow for the resource.

| sim time | reason | in_use | queue_length |
|---|---|---|---|
| 0 | request by the entity | 1 | 0 |
| 1 | release by the entity | 0 | 0 |
| 5 | end of run (desflow's final publication) | 0 | 0 |

The queue length stays 0 because only one entity arrives.

## Differences between the forms

- **Final record at 5 s.** desflow publishes the readings once more at the end of the run. This is not a request or release, so it is an extra record. The Low-level form publishes it too, because `env.teardown()` does the same thing.
- **Task statistic in YAML and Builder.** These two forms also print `M.stats.tasks.use.in_progress` at 5 s, because `use` is a desflow task. The Low-level form has no desflow task, so it does not print it. The tests compare only the `res` records, so the records match.
- **Telemetry filter.** The YAML form uses `when: telemetry`. The Builder form uses a lambda, because `add_egress` does not accept the string `"telemetry"`. The Low-level form uses a `predicates` list in `setup_egress`. All three print only telemetry records; the finished event `{event_type: used}` is produced (checked in the YAML form at 1 s) but not printed.
- **Task body.** In the Builder form, `use` returns the payload `{event_type: used}`. The signature `def use(instance, **_)` is needed by the Builder task wrapper. The Low-level form publishes the event itself with `publish_event`.
- **Seed.** The Low-level API has no seed argument for the environment. `low_level.py` uses `Sampler(seed=42)`, a literal, but the model draws no random numbers, so the seed does not change any record.
- **Speed.** All three use `factor: 0`, so they run without waiting for the wall clock.

## Notes

- I could not download the desflow examples (the sandbox refused the network), so the files were written from the installed package's schema and docstrings (desflow 0.29.0). The `monitoring.rst` lines were not available to me either; I took "patch request and release to record" to mean desflow's `stats.on_change`.
