---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> A process waited for by one that did not start it, or by two waiters (Program shapes): status existing and new design; form `wait: {running: {task}}` for the entity's own and its spawned instances; for another entity, a `once` signal the task fires, which any number wait for, also after the firing, and they read `wait.x.value`; Y1's family for one signal per process

Details:

- Arrival `w` starts one entity at time 0, running task `worker`: it works for 2 seconds, publishes `{event_type: worker_done}` and then fires a signal once, carrying the value 7. The signal fires only once in the run.
- Arrival `a` starts one entity at time 0, running task `waiter_a`: it waits for the signal and then publishes `{event_type: a_saw, v: <the value the signal carried>}`.
- Arrival `b` starts one entity at time 5, once, running task `waiter_b`: it waits for the signal, which has fired already, and publishes `{event_type: b_saw_after, v: <the value the signal carried>}`.
- Run until nothing is left to do.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, and anything that differs between the forms. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
