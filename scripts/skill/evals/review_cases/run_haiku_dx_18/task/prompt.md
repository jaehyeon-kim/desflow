---
tags: [desflow]
max_turns: 80
timeout_seconds: 1500
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash]
---

You work in the current folder. desflow is installed in py/; use py/bin/python3.12, py/bin/desflow and py/bin/pytest, and do not install anything else. Use the desflow skill.

Write a desflow model for the request below. It is one row of a list of test cases for a simulation tool, quoted as it stands, and the details after it fix the names and numbers that the checks read.

> streaming_out_logic.packed_only and shop_logic.events_only: a sink `when` function. Connector behaviour as a function. Design: `when` gains the names `events` and `telemetry`, and a mapping `{expr: ...}` over `record.stream_type`, `record.key`, `record.task` and `record.value.<field>`; `when: {expr: "record.value.event_type == 'order_packed'"}` is `packed_only`. Not prototyped.

Details:

- The simulation id is `Shop`, unpaced. A resource `packers` has capacity 1.
- Arrival `orders` starts one entity every 2 seconds from time 2. Each runs task `pack_order`: it takes one packer for 1.5 seconds and publishes `{event_type: order_packed}`. The task also produces its engine records (queued and started).
- The model has one sink, a Console that prints each record to standard output as a JSON line, and it keeps only the records whose `value.event_type` is `order_packed`. The queued and started records and any telemetry are not printed. In every form the records reach standard output only through this sink.
- Run to 20 seconds.

Write these files in the current folder, and no others:

- model.yaml: the model as a YAML blueprint. Keep the seed as a literal number in simulation.random_seed, and use seed 42.
- builder.py: the same model with the Builder API (SimulationContext). A reader runs it with `python builder.py`, so it must run the model to its end and print each record to standard output, one JSON line per record.
- low_level.py: the same model with the Low-level API (DynamicRealtimeEnvironment). A reader runs it with `python low_level.py`, and it also prints each record as one JSON line. Keep the seed as a literal number.
- test_model.py: a pytest file that tests the model. A reader runs it with `pytest test_model.py` from the current folder.
- reply.md: your final reply to the person who asked for the model: what you built, what each file does, and anything that differs between the forms. The reply is the last thing you write.

The three forms must give the same records. Run each file, and the tests, before you finish.
