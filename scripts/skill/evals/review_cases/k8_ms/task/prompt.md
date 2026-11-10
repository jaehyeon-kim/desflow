---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> a variant of ST-RT1: RealtimeEnvironment(factor=0.1): ten steps take about one second of wall time. ST-RT2 calls run twice.

Details:

- The model runs paced against the wall clock at a factor of 0.1: one simulation second takes 0.1 second of wall time.
- Arrival `a` starts 2 entities at time 0, running task `pem`: every 1 second it publishes `{event_type: tick}`, for ever.
- The run is called twice: first to 2 seconds, then on to 4 seconds. Four simulation seconds take at least 0.4 second of wall time. The ticks are at 1, 2 and 3 for each entity; the run ends at 4, before a tick at 4 is published.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, and anything that differs between the forms. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
