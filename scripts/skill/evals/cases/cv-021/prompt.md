---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> `Condition.all_events`, `any_events` (Events and conditions): status existing; form `all`, `any`, `at_least`

Details:

- Three timeouts exist in each of the three tasks below: `a` after 3 seconds with the value 30, `b` after 1 second with the value 10, `c` after 2 seconds with the value 20.
- Arrival `a` starts one entity at time 0 running task `two_of_three`: it waits until at least 2 of the three timeouts are done.
- Arrival `b` starts one entity at time 0 running task `all_three`: it waits until all three are done.
- Arrival `c` starts one entity at time 0 running task `any_of_three`: it waits until any one is done.
- Each of the three tasks then publishes `{event_type: done, a: <value of timeout a, or null if it is not done>, b: <same for b>, c: <same for c>}`.
- Run until nothing is left to do.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, and anything that differs between the forms. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
