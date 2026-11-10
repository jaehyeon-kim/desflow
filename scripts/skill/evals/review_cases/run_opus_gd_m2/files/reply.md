# Resource monitoring: usage and queue length after every request and release

This is the "patch request and release" case from SimPy's monitoring guide (monitoring.rst:125-183). You don't need to patch anything in desflow. Set `stats: {level: minimal, on_change: true}` and the resource publishes its readings right after each `request()` call and each `release()` call. They come out as telemetry named `M.stats.resources.res.<reading>`. The readings are `capacity`, `in_use` (units in use), `queue_length` and `utilization`. Each record also carries `sim_ts`, which is the "now" in SimPy's tuple.

## What the run gives

One entity, `a-1`, arrives at time 0. It takes `res` (capacity 1), holds it for 1 s, publishes `{event_type: used}` and lets go. The run stops at 5 s. It prints 16 JSON lines:

| sim_ts | records |
|---|---|
| 0 | event `queued`; after the **request**: capacity 1, in_use 1, queue_length 0, utilization 100.0; event `started` |
| 1 | event `{event_type: used}`; after the **release**: capacity 1, in_use 0, queue_length 0, utilization 0.0 |
| 5 | the readings once more (in_use 0, queue_length 0), plus `M.stats.tasks.use.in_progress` 0 |

Nothing is published between those times, because no timer fires.

**Things to know:**

- **The records at 5 s are not from a request or release.** desflow always publishes the statistics once more when a run ends, and I found no setting to turn that off. If a check should count only the readings at the calls, keep `sim_ts < 5`. Those are exactly the two readings, at 0 and 1.
- **There is still a timer setting.** `stats` always has an interval. I set it to 60 s so it never fires in a 5 s run, which leaves `on_change` as the only source of readings before the end.
- **The queue is never longer than 0.** That follows from the spec: there is only one entity and the capacity is 1.
- **Settings I picked that the request didn't give:**
  - `factor: 0` runs it unpaced, so it finishes at once instead of taking 5 real seconds.
  - A fixed `logical_start_time` of 2026-01-01T00:00:00Z makes the `timestamp` fields the same on every run and in every form.
  - The seed is 42, as asked. The model draws no random numbers, so it changes nothing here.

## Files

- **model.yaml**: the blueprint. `desflow run model.yaml` prints the records as JSON lines on stdout through `Console` with `config: {format: json}`. Its log lines go to stderr.
- **builder.py**: the same model with `SimulationContext`. `python builder.py` runs it to 5 s and prints the same JSON lines. `.with_stats("minimal", interval=60, on_change=True)` turns the readings on.
- **low_level.py**: the same model on `DynamicRealtimeEnvironment`, with a `DynamicResource`. The seed is a literal 42 in `np.random.default_rng(42)`. `python low_level.py` prints the same JSON lines.
- **test_model.py**: run with `pytest test_model.py`. It runs all three forms and checks that their records are identical. It also checks:
  - the `queued`, `started` and `used` events and the 1 s hold;
  - the `in_use` series (0→1, 1→0, 5→0) and the `queue_length` series (all 0);
  - capacity and utilization;
  - that readings appear only at 0, 1 and 5;
  - that each set of readings comes right after its call: between `queued` and `started` for the request, and right after `used` for the release;
  - the seed, id, capacity and run length in the YAML.

I ran all three files, and they gave byte-identical output. All 10 tests pass. `desflow explain` gives no queueing estimate, because a single arrival has no rate.

## Differences between the forms

The records are the same. Only the way the model is written differs:

- **Arrival.** The builder API requires a `dist` or a `gap` on every arrival. So both YAML and the builder write `a` as `gap: 1, first_at: 0, max_events: 1`: the one entity arrives at 0, and the gap is never drawn. The low-level file just starts one process at time 0 with the key `a-1`.
- **Turning the readings on.** `setup_stats` in the low-level API has no `on_change` argument, so `low_level.py` sets `env.stats.on_change = True` after calling it.
- **Task events and counts.** The low-level file publishes `queued`, `started` and `used` itself and updates the task counts itself (`env.stats.task_*`). YAML and the builder do this for you.
- **The end-of-run readings.** In the low-level file they come from `env.teardown()`.
- **Payload function.** In the builder, the task's payload function takes `(task_id, entity)`.
