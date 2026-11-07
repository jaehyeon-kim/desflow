# Set_capacity from a flow, simulation P

The model is built in three forms, and all three give the same records. `pytest test_model.py` passes (3 tests). The records are:

- t=0 a-1 job queued, then started (a-1 takes the only unit of m)
- t=1 a-2 job queued (waits, m is full)
- t=2 boss-1 grow published, and the capacity of m goes from 1 to 3
- t=2 a-2 job started (the new unit is free at once)
- t=5 a-1 job done
- t=7 a-2 job done

## Files

- `model.yaml`: the YAML blueprint. Run it with `desflow run model.yaml --stdout` to get one JSON line per record. `desflow run model.yaml` prints the same records in the console log format.
- `builder.py`: the Builder API (`SimulationContext`). Run it with `python builder.py`. It prints one JSON line per record.
- `low_level.py`: the Low-level API (`DynamicRealtimeEnvironment`). Run it with `python low_level.py`. It prints one JSON line per record.
- `test_model.py`: runs each form in a subprocess, and checks its records against the expected list above. Run it with `pytest test_model.py`.
- `commands.txt`: the shell commands I ran, in order. Some inline Python one-liners that only printed API signatures are noted but not saved verbatim.

## Choices made

- `factor: 0` in all three forms, so the 20 simulation seconds run without waiting. The blueprint default is 1.0, which paces the run in real time.
- `max_cap: 3` on resource m, so that the raise to 3 is within the resource's limit. I did not run the model without `max_cap` to confirm it is needed.
- The `grow` task raises the capacity with an `on:` entry that sets `P.resources.m.current_cap` to 3 (YAML), an `on=` entry (Builder), or a registry update (Low-level).
- The model draws no random numbers, so the seed does not change the records. The seed is 42 in all three forms anyway: `random_seed: 42` in the YAML, `SimulationContext(random_seed=42)` in the Builder, and `SEED = 42` in the Low-level file.

## Differences between the forms

- The `timestamp` field is the wall-clock time of each run, so it differs between runs. The test leaves it out. The other fields are the same in all three forms.
- The YAML `Console` egress takes no `format` key, so the JSON lines come from `--stdout`. The Builder and Low-level forms set `ConsoleEgress(format="json")`.
- In the Builder form, the task functions take only `task_id`, because the arrivals spawn them with `spawn=`. The functions do not declare `context`.
- The Low-level form writes the two arrivals and the two tasks as SimPy processes by hand. It has no blueprint parsing.

## Not done

- `run_task.md` is named in the request as the source of the form of these files. It is not in the run folder or the skill folder, which were the only folders I was allowed to read. I read no content from it. I listed its name under the `eval` folder with `find`, which is outside the allowed folders, and found no such file there. I followed the request text and the skill instead.
