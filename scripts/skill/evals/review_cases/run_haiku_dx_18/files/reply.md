# Shop model: orders packed by one packer

The model runs to 20 seconds, unpaced (`factor: 0`), with seed 42. Orders arrive every 2 seconds from time 2. Each order takes the one packer for 1.5 seconds and publishes `{event_type: order_packed}`. A Console sink prints one JSON line per record, but only for records whose `value.event_type` is `order_packed`. Queued and started records and telemetry are not printed.

The run prints nine lines, one for each order packed by 19.5 seconds (`orders-1` to `orders-9`). All three forms print the same nine lines, byte for byte.

## Files

- **model.yaml**: the model as a YAML blueprint. Run it with `py/bin/desflow run model.yaml`. The seed is the literal `random_seed: 42`. The sink uses `when: {expr: "record.value.event_type == 'order_packed'"}`.
- **builder.py**: the same model with `SimulationContext`. Run it with `python builder.py`. The sink's `when` is a Python function over the record, and the task payload is a decorated function.
- **low_level.py**: the same model on `DynamicRealtimeEnvironment`. Run it with `python low_level.py`. The queued, started and finished events are published by hand, and the sink filter is a `predicates` function. The service time is drawn by a `Sampler` seeded with `default_rng(42)`.
- **test_model.py**: pytest tests. Run them with `pytest test_model.py`. They check the seed literals, compare the nine expected records for each form, check that the three forms print the same lines, and check that only `order_packed` events reach standard output. All 7 tests pass.

## Differences between the forms

- **Timestamps**: I set `logical_start_time` to `2026-01-01T00:00:00+00:00` in all three forms. Without it, desflow stamps records with the wall clock at load, so the timestamps would differ between runs. This is not in your spec.
- **Sink filter**: the YAML form uses the `when: {expr: ...}` form. The builder and low-level forms use Python functions with the same meaning. A telemetry record or an event without `event_type` is dropped in every form.
- **Engine records**: in the YAML and builder forms, desflow publishes the queued and started records itself. In the low-level form they are published by hand. Both are filtered out by the sink, so they do not affect the output.
- **Design note**: the `events`, `telemetry` and `expr` forms of `when` already exist in the installed desflow 0.29.0. I did not use the `events` or `telemetry` names, because the model needs only the `expr` filter.
- **Python `when` in builder.py**: I passed a function to `add_egress(when=...)`, not the expression text. I did not test whether the expression text is also accepted there.

I did not install anything. The working folder also has a `py/` folder that was already there, which I left alone.
