# Group size drawn for each arrival (Poisson, mean 2)

I wrote the model in all three desflow forms. All three print the same 96 records, byte for byte, and the 7 tests pass.

## The model

- Arrival `a` fires every 5 s from t = 5, 40 times in all (t = 5, 10, …, 200).
- At each firing it draws a group size from Poisson(mean 2) and starts that many entities. A size of 0 starts nobody.
- Each entity runs task `member`. The task takes no time and publishes `{event_type: member}`.
- The run lasts until 205 s. It is unpaced (`factor: 0`) and uses seed 42.
- With seed 42 the 40 sizes are `4, 2, 3, 4, 1, 1, 1, 3, 0, 4, 2, 2, 3, 1, 3, 2, 3, 2, 0, 3, 4, 3, 3, 4, 2, 2, 1, 4, 0, 2, 6, 2, 3, 2, 2, 0, 1, 5, 2, 4`. That makes 96 entities. Four firings drew 0 and started nobody.

Each record is one JSON line, such as:

```json
{"stream_type":"event","sim_ts":5.0,"timestamp":"2026-01-01T00:00:05.000+00:00","key":"a-1","task":"member","instance":0,"value":{"event_type":"member"}}
```

## Files

- **model.yaml**: the YAML blueprint. The arrival uses `gap: 5`, `first_at: 5` and `max_events: 40`, and `group: {gen: sample, dist: poisson, mean: 2}` gives the size. The seed is the literal `random_seed: 42`. Run it with `desflow run model.yaml`.
- **builder.py**: the same model with `SimulationContext`. It passes the same arrival settings to `add_arrival`, with the group made by `generator({...})`, and declares the task with `@app.task()`. Run it with `python builder.py`.
- **low_level.py**: the same model with `DynamicRealtimeEnvironment`. A SimPy process fires the arrival, draws each size with `rng.poisson(2)` and starts one `member` process per entity.
- **test_model.py**: runs all three forms and checks that:
  - the three outputs are the same;
  - every record is a `member` event at one of the 40 firing times;
  - the number of records at each time equals the Poisson draw for that firing;
  - some groups are empty and some have more than one entity;
  - the total is plausible for a mean of 2;
  - keys and instance numbers are consecutive.

  Run it with `pytest test_model.py`.

## Differences between the forms

- **Random numbers in the low-level form.** desflow gives the blueprint's group generator its own random stream, named after where it sits in the file: `SeedSequence(42, spawn_key=<the eight 32-bit words of sha256("arrivals.a.group")>)`. The builder gets the same stream automatically. The low-level form has no blueprint, so `low_level.py` creates that stream itself, with the seed written as a literal 42. If the arrival or the generator is renamed or moved in the blueprint, the low-level file must be changed to match.
- **Key numbers.** desflow numbers keys only for entities that actually start, so an empty group uses no number and the keys run `a-1` … `a-96` with no gaps. `low_level.py` numbers keys the same way.
- **Fixed timestamps.** All three forms set the start of the run to 2026-01-01T00:00:00Z. Without that, the `timestamp` field would come from the wall clock and the outputs could not match exactly.
- **JSON output.** All three print through the Console egress set to JSON. In YAML that is `{type: Console, config: {format: json}}`; in Python it is `ConsoleEgress(format="json")`. Log messages go to stderr, so stdout holds only the records.
- **Import path.** `generator` is not exported from the top-level `desflow` package, so `builder.py` imports it from `desflow.values.template`.

## Nothing assumed

The request fixed every number. I added two settings it did not mention: no pacing (`factor: 0`) so the run finishes at once, and the fixed start time above. Neither changes which records come out. `desflow explain` cannot estimate a load for this model because its gaps are computed, and there is no resource to load anyway.
