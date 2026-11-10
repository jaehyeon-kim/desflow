---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> subscribe_at(other): the subscriber is interrupted with other's value when other ends at 1.

Details:

- Arrival `s` starts one entity at time 0, running task `subscriber`. The entity starts a child flow, task `other`, and subscribes to it. The subscriber then waits for an event that never comes.
- Task `other` works for 3 seconds, publishes `{event_type: other_done, value: ohai}` and ends. When it ends, the subscriber is interrupted, and the interrupt carries what `other` returned, the string `ohai`.
- The interrupted subscriber publishes its interrupted status, then goes on to task `told`, which publishes `{event_type: interrupted, value: <the string ohai from the interrupt>}`.
- Run until nothing is left to do.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, and anything that differs between the forms. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
