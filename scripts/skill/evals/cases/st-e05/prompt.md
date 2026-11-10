---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> a periodic event fires three times; five consumers each count it; run until the event three times and then to the end. Keys settled for T8.

Details:

- The simulation id is `T`. A variable `counter` starts at 0.
- Arrival `period` starts one entity at time 0 running task `tick`: three times, it waits 1 second, publishes `{event_type: tick}` and sends the signal `periodic`.
- Arrival `consumers` starts 5 entities at time 0 running task `consume`: each waits for `periodic`, publishes `{event_type: seen}`, adds 1 to `counter`, and waits for the signal again. Each consumer sees each of the three firings.
- Run in four segments: to the first firing of `periodic`, then to the second, then to the third, then to the end. After the third firing nothing is left, so the run ends at 3.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, and anything that differs between the forms. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
