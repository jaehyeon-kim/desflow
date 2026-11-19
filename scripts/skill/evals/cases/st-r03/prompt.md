---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> nine users on three units; grants at 0, 0, 0, 1, 1, 1, 2, 2, 2.

Details:

- The simulation id is `T`. A resource `res` has capacity 3.
- Arrival `users` starts 9 entities at time 0, each running task `pem`: it takes one unit of `res`, holds it for 1 second and then publishes `{event_type: granted}`.
- Run until nothing is left to do.
- The person also wants the statistics of one run kept in a file for a report: run model.yaml once so that desflow itself writes that run's statistics to summary.json, then read from summary.json the mean time a user waited for a unit of `res`.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- summary.json: the statistics of one run of model.yaml, written by desflow.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, anything that differs between the forms, and the mean wait for a unit of `res` as summary.json gives it. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
