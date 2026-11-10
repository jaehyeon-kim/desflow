---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> branch by attribute or state; fixed weights

Details:

- Arrival `a` starts one entity every 1 second, 40 in all, from time 1. Each entity has an attribute `vip` that is true with probability 0.3, drawn for each entity.
- Task `route` publishes `{event_type: route}`. A vip entity goes on to task `fast`. Any other goes on to task `weighted`.
- Task `weighted` publishes `{event_type: weighted}`, then sends the entity to task `a` with weight 3 or to task `b` with weight 1, drawn at random for each entity.
- Tasks `fast`, `a` and `b` each publish `{event_type: <the task name>}`.
- Run to 50 seconds. The weights and the share of vip entities are part of the request: keep the random draws.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, and anything that differs between the forms. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
