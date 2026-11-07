# Shop model: orders, one packer, `packed_only` console sink

I built the Shop model in three forms. All three print the same 9 records to standard output, and `pytest test_model.py` passes (4 tests).

## What the model does
- **Simulation:** id `Shop`, unpaced (`factor: 0`), seed 42 as a literal, run to 20 s.
- **Resource:** `packers` has capacity 1.
- **Arrivals:** `orders` starts one entity every 2 s from t=2. I used a fixed gap, not a random one.
- **Task:** `pack_order` takes a packer for 1.5 s (a constant service time). It publishes `{event_type: order_packed}`. The engine also publishes its queued and started records.
- **Sink:** one Console sink in `json` format, so each record is one JSON line. Its `when` keeps only records whose `value.event_type` is `order_packed`. Queued, started and telemetry records are not printed.
- **Output:** nine records, at sim_ts 3.5, 5.5, …, 19.5, with keys `orders-1` to `orders-9`. Because the packer is never busy when an order arrives, there is no queueing.

## Files
- `model.yaml` is the YAML blueprint. The sink filter is `when: {expr: "record.value.event_type == 'order_packed'"}`.
- `builder.py` is the Builder API version (`SimulationContext`). Run it with `python builder.py`.
- `low_level.py` is the Low-level API version (`DynamicRealtimeEnvironment`). Run it with `python low_level.py`.
- `test_model.py` runs all three forms. It checks the YAML records (times, keys, payload, task, stream type), that builder and low-level match the YAML, and that only `order_packed` records are printed.

## Differences between the forms
- **Sink `when`:** the installed desflow already supports it, so nothing needed prototyping. In Python the Builder and Low-level forms use a predicate function with the same logic as the YAML expression. YAML uses `{expr: ...}`, and the Python forms cannot take that mapping. The Python predicates also check that `value` is a mapping, so a record without one is skipped.
- **`timestamp` field:** it is wall-clock time, so it differs on every run and between forms. The tests ignore it. Everything else is identical.
- **Logging:** desflow's log lines go to standard error, not standard output.
- **Seed:** the model draws no random numbers, so seed 42 does not change anything. I kept it as asked.
