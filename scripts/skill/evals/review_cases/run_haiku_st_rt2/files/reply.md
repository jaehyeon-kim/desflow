# ST-RT1 variant: realtime model in three forms

The model: two entities start at time 0 and run `pem`. Each pass takes one simulated second and publishes `{event_type: tick}`, then loops back to `pem`. It is paced at factor 0.1, so one simulated second takes 0.1 s of wall time. The run is called twice: to 2 s, then to 4 s. Each entity ticks at 1, 2 and 3; there is no tick at 4.

## Files

- `model.yaml`: the blueprint. Run it with `desflow run model.yaml`. The seed is `random_seed: 42`. The run is `run: {segments: [{until: 2}, {until: 4}]}`.
- `builder.py`: the same model with the Builder API. `python builder.py` prints the records as JSON lines. `build()` returns the model and `run()` runs it in the two calls.
- `low_level.py`: the same model on `DynamicRealtimeEnvironment`. `python low_level.py` prints the records as JSON lines.
- `test_model.py`: pytest tests. They run all three forms and check that the records match, that each entity ticks at 1, 2 and 3 and not at 4, that the two entities start at 0, that the run is called to 2 then 4, and that 4 simulated seconds take at least 0.4 s of wall time for the builder and low-level forms. Run them with `pytest test_model.py`.

## Results

All three forms print the same 16 records, apart from the wall-clock `timestamp`. `pytest test_model.py` passes all 10 tests. The text of the records is also the same, apart from timestamps.

## Differences between the forms

- **Printing:** all three use the Console egress in JSON mode. In YAML this is `config: {format: json}`.
- **Lag records:** desflow publishes `system.simulation.lag_seconds` records in a paced run by default. I turned them off in all three forms (`lag_interval: 0` in YAML and the builder, `lag_monitor_interval=None` in the low-level form), so the records are only the model's own.
- **Segments:** YAML and the builder use the segment mechanism, which publishes a `system.simulation.segment` record after each call. The low-level form calls `env.run` twice and publishes the same record itself with `env.publish_telemetry`, because it has no segment mechanism of its own.
- **`pem` in the low-level form:** I wrote the process by hand. It publishes the `started` record at each pass and the `tick` at its end, and it numbers the passes itself, as the task engine does in the other two forms. This is the part most likely to drift if the task engine changes.
- **Seed:** the model draws no random numbers, so the seed changes nothing. The low-level form keeps `default_rng(42)` only so the seed appears as a literal there, as in the other two files.
- **Records per pass:** every pass emits a `started` record with a new instance number, as well as the `tick`. So the output has 16 records, not 6. Instance numbers go 0 and 1 at time 0, then 2 and 3 at time 1, and so on.
- **Startup:** the wall time of a whole `desflow run` is about 1.3 s, mostly interpreter startup. The 0.4 s bound applies to the simulated time, and the test checks it in-process.
