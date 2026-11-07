---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> PriorityStore with string priorities (resources.rst:500-528).

Details:

- The simulation id is `R`. A store `repairs` returns the item with the lowest priority first, and its priorities are text: `P0` before `P1` before `P2` before `P3`.
- Arrival `inspector` starts one entity at time 0 running task `tick`: for each issue in the list (`P2`, `#0000`), (`P0`, `#0001`), (`P3`, `#0002`), (`P1`, `#0003`), it waits 1 second, publishes `{event_type: log, priority: <priority>, item: <item>}`, puts the issue in `repairs` with its priority, and publishes `{event_type: filed}` in task `file`.
- Arrival `maintainer` starts one entity at time 0 running task `wait3`, then `take`, for ever: it waits 3 seconds and publishes `{event_type: waited}` (task `wait3`), then gets the item with the lowest priority from `repairs` and publishes `{event_type: repair, priority: <priority>, item: <item>}` (task `take`).
- Run until nothing is left to do. The last wait ends at 15 with the store empty.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, and anything that differs between the forms. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
