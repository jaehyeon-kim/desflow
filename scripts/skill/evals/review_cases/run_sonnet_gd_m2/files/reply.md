# Model M: one entity, one resource, stats on every change

I built the model in all three forms. They give identical records (the wall-clock `timestamp` field aside). I ran each file, and the 7 tests pass.

## What the model does
- Simulation `M`, seed 42, run to 5 s.
- Resource `res` has capacity 1.
- Arrival `a` starts one entity at time 0 (`first_at: 0`, `max_events: 1`, with a long gap that never comes).
- The entity runs task `use`. It takes `res`, holds it for a constant 1 s and publishes `{event_type: used}`.
- The stats are the built-in `minimal` level with `on_change: true`. After each request and release, the run publishes `M.stats.resources.res.in_use`, `queue_length`, `capacity` and `utilization`, each stamped with `sim_ts`.
- The stats interval is set to 100 s so the timer never fires inside the run.

## Files
- `model.yaml`: the YAML blueprint, with `random_seed: 42` as a literal. Run it with `desflow run model.yaml`. It writes JSON lines through the Console egress. Add `--stdout` to print the records as JSON lines.
- `builder.py`: the same model with `SimulationContext`. `python builder.py` prints one JSON line per record.
- `low_level.py`: the same model with `DynamicRealtimeEnvironment`, seed 42 literal. `python low_level.py` prints one JSON line per record. It sets `env.stats.on_change = True` directly, because `setup_stats` has no such argument.
- `test_model.py`: runs all three forms and checks three things.
  - The records are equal across the forms.
  - `in_use` goes 1 at t=0 and 0 at t=1, with queue length 0 throughout.
  - There is one `used` event at t=1.
- `reply.md`: this note.

## Records
- At t=0 (request): `in_use` 1, `queue_length` 0, `utilization` 100.
- At t=1 (release): `in_use` 0, `queue_length` 0, `utilization` 0.

## Differences and things to know
- **Closing samples at t=5:** desflow always publishes the statistics once more at the end of the run. So you also get a set of `res` readings at t=5 and a `M.stats.tasks.use.in_progress` reading. They are not changes of `res`, so they are not "after each call". I found no switch to turn this off. The three forms all emit it.
- **Request-time publishing:** the request at t=0 is published after the grant, so it shows `in_use` 1.
- **Event records:** the stream also has the task's `queued` and `started` events and the `used` event, besides the telemetry.
- **Pacing:** `factor` is 0, so the run is not paced in real time.
- **Seed and timestamps:** the seed has no visible effect because every time is constant. The `timestamp` field is the only thing that differs between runs, so the tests drop it.
- **Arrival `a` in the low-level form:** it is a one-shot process, not a desflow arrival source.
- **Entity key:** the entity key is `a-1` in all three forms.
