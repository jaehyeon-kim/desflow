# Shop: a sink that keeps only `order_packed` records

I wrote the model in all three forms. All three print the same nine records, and all 7 tests pass.

The design you wrote down as "not prototyped" is already in the installed desflow. An egress `when` accepts `events`, `telemetry` and `{expr: ...}` over `record.stream_type`, `record.key`, `record.task` and `record.value.<field>`. So `model.yaml` uses `when: {expr: "record.value.event_type == 'order_packed'"}` exactly as written.

## The model

- **Simulation:** `Shop`, unpaced (`factor: 0`), seed 42.
- **Resource:** `packers`, with capacity 1.
- **Arrival:** `orders` starts one entity every 2 s from t = 2, and each one runs `pack_order`.
- **Task:** `pack_order` takes one packer for a constant 1.5 s, then publishes `{event_type: order_packed}`. It also produces its `queued` and `started` records.
- **Sink:** one Console sink in JSON format, filtered by `packed_only`. Nothing else writes to standard output.
- **Run length:** 20 s.

Orders arrive at 2, 4, …, 18 and none of them waits. An arrival at exactly 20 is not processed, because the run stops there. So the output is 9 records, keys `orders-1` to `orders-9`, with instances 0–8 and `sim_ts` 3.5, 5.5, …, 19.5. The queued and started records, and any telemetry, are not printed.

## Files

- **`model.yaml`:** the YAML blueprint. Run it with `desflow run model.yaml`.
- **`builder.py`:** the same model with `SimulationContext`. `python builder.py` runs it to 20 s and prints the records as JSON lines.
- **`low_level.py`:** the same model on `DynamicRealtimeEnvironment`. It has its own arrival process, a `DynamicResource` and a `Sampler` seeded with the literal 42. It publishes the queued, started and finished records by hand. Run it with `python low_level.py`.
- **`test_model.py`:** runs all three forms and checks four things:
  - Each form prints exactly the 9 expected records.
  - The three forms agree.
  - The blueprint has the settings above.
  - The filter is doing the work: with the `when` removed, the same sink also prints the `queued` and `started` records.

  It also unit-tests the Python predicate. Run it with `pytest test_model.py`.

## Differences between the forms

- **How the filter is written.** The builder's `add_egress(when=...)` accepts a Python function, or the strings `"labels"` and `"trace"`. It does not accept the `{expr: ...}` mapping or the names `events` and `telemetry`. The low-level `setup_egress(predicates=[...])` likewise accepts only functions. So `builder.py` and `low_level.py` use a function, `packed_only(record)`, which returns true when `record["value"]` is a mapping with `event_type == "order_packed"`. That has the same effect as the YAML expression, where a field the record lacks reads as None.
- **The builder task function.** When an arrival `spawn`s a builder task, desflow calls it with just the task id. So `pack_order` declares `context=None`.
- **Timestamps.** The `timestamp` field is wall-clock time, so it differs from run to run. The tests compare every other field, and those are identical across the three forms.
- **The seed.** It has no effect on these records, because every time in the model is fixed (a 2 s gap and a 1.5 s constant service).
