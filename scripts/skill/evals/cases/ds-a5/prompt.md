---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> turn one source off and on from a flow (drop mode; use off_mode restart for the second program).

Details:

- Source `src` starts one entity every 1 second from time 1, each running task `t` (`{event_type: arrival}`). While the source is off, an arrival that falls due is dropped, and the source goes on ticking.
- Arrival `switch_off` fires once at 2.5 and runs task `src_off` (`{event_type: off}`), which turns `src` off. Arrival `switch_on` fires once at 5.5 and runs task `src_on` (`{event_type: on}`), which turns `src` on.
- Source `src2` starts one entity every 2 seconds from time 2, running task `t`. Turning it off drops the arrival that is pending, and turning it on starts a whole new gap of 2 seconds.
- Arrival `switch2_off` fires once at 3 and runs task `src2_off` (`{event_type: off}`), which turns `src2` off. Arrival `switch2_on` fires once at 7 and runs task `src2_on` (`{event_type: on}`), which turns `src2` on.
- Run to 12 seconds.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, and anything that differs between the forms. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
