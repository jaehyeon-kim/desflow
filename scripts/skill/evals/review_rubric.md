# Rubric of the blind reviewer

An Opus reviewer reads the folder of one run against the rubric of the kinds that turn on what code, a command or a sentence means: K3, K5, K8, K9, K10, K14, K15 and K17. `grade.py` scores the kinds it can measure and writes the facts below; the reviewer judges the rest from those facts, the run's files and its full transcript. The reviewer is not told which version of the skill, which model or which run number made the run. It reads, runs nothing, and lists each mistake with its file and line.

## What the reviewer gets

One folder, which `grade.py --records-out` writes from the run that `claude plugin eval --keep-temp` kept (`run_task.md`, Steps, step 5). In every file of it, the path of the skill copy reads `<skill>`, a reference page that only one version of the skill has reads `page-1.md`, `page-2.md` and so on, a folder of the suite the run came from reads `<suite>`, and the run's kept folder reads `<run>`, so that no path or page name tells the version.

- `files/`: the five files the agent wrote, `model.yaml`, `builder.py.txt`, `low_level.py.txt`, `test_model.py.txt` and `reply.md`. A Python file has `.txt` added and is otherwise as the agent left it.
- `trace.jsonl`: the run's full transcript, one JSON object per line as `claude -p --output-format stream-json` writes it. A line of `"type": "assistant"` holds the agent's text and its tool calls (`"type": "tool_use"`, with `name`, `input` and `id`); a line of `"type": "user"` holds the result of each call (`"type": "tool_result"`, with the call's id as `tool_use_id`), that is, what the command printed or the file the agent read. The reviewer reads it in full.
- `records/`: `yaml.jsonl`, `builder.jsonl` and `low_level.jsonl`, the records of each form as `grade.py` runs them at the file's own seed. The YAML form runs as the task's `records.yaml` says (`via`: with `--sample 100000`, with `--stdout`, or through the sinks the model names, and with `--until` when the model has no run end of its own), and each Python form as `python FILE`. A run with `--stdout` or `--sample` prints no `system.simulation.lag_seconds` readings, so a paced YAML form's file lacks the readings its Python forms' files hold, and that difference is not a mistake. So a header is read against the records of its own form.
- `tests.json`: for each test id, its outcome on the agent's model (`unchanged`), from three plain runs, three under `pytest -n 4` and three in reverse order, or `"outcome": "flaky"` with the outcome of each run in `runs` when the runs of one way differ; its outcome under each of the task's three changes (`changes`), from the same nine runs of the changed model, `flaky` when the runs of one way differ, and the forms the plugin `form_trace.py` saw it run (`forms`); and `changes`, the list of the three changes with the key and value each applied, or why none was applied.
- `clocks.json`: each assert of `test_model.py` that reads a wall clock, with its line and text, and `paced_wall`, the wall seconds the request's pacing gives, null for an unpaced task.
- `task/`: the case's `prompt.md` (the request as the agent got it, after a settings block), the task's `records.yaml` (the records the request names, written before any run), and `original.py.txt`, the program of the known case, for K17.
- `skill/`: the `SKILL.md` and the `references/` pages of the skill copy the run had that both versions have, so that a claim about desflow can be checked against what the agent could read. A page the agent read that is not in `skill/` is in the agent's tool results in `trace.jsonl`. A run without the skill has no `skill/` folder.

## Output

One JSON line per mistake, and nothing else:

```json
{"task": "p33", "kind": "K3", "count": 1, "file": "builder.py", "line": 3, "evidence": "the header says ... but the run ..."}
```

`count` is always 1, and `file` names the agent's file without the `.txt`. A run with no mistake of a kind has no line for it. After the last mistake, the reviewer writes one line that says it read the run, whatever it found:

```json
{"task": "p33", "kind": "REVIEWED", "count": 0}
```

`summarise.py` exits with an error for a run that has no `REVIEWED` line or two of them, so a run the reviewer skipped is not read as a run with no mistake, and a run read twice is not counted twice. The main session sets `task`, `version`, `model` and `run` from its own list before it gives the lines to `summarise.py`, so the reviewer never sees the last three, and a `task` written in another form, such as `DS-A3`, is replaced by the case's name (`ds-a3`). A mistake is counted once per place: the same wrong sentence repeated in a header and in `reply.md` is two places, a repeated occurrence in one comment is one. `summarise.py` counts each kind once per run.

## K3: a header, docstring, comment or field says what the run does not do

Check every sentence in a file header, docstring, comment and in `reply.md` that states what this run's records hold, and every payload field the model publishes, against the records. A sentence is a mistake when the records contradict it, or when it names a record that the records lack. A state that the model changes and that has no record of its own, such as a capacity, a level or a variable, is judged by its effect in the records: the sentence is not a mistake when the records show that effect. A sentence about another run, such as "without the change it would have started at 5", is judged under K15 only. A sentence about intent is not a mistake.

- Wrong: "Adding two agents ends the hang-ups." when every caller in the run still hangs up. Right: "With three agents, about one caller in five hangs up; `scenario` adds two agents at 1 hour." where the records show both.
- Wrong: "all three forms give the same records" when the three files of `records/` differ. Right: the sentence names the forms that were compared, and their records agree.
- Wrong: a field that has the same value in every record, such as a `count` over `last: 10` with no `where`, which is 10 in every record. It shows the reader nothing; the check is the records, not the intent.
- Wrong: "the timestamps show the run took 16 s" when the run took about a second. A record's `timestamp` is the moment the run started plus the simulation time, so its gaps follow the simulation time, not the wall time.
- Not a mistake: "`timestamp` is wall-clock time, so it differs between runs" in a run that sets no `logical_start_time`. The field starts at the wall-clock moment the run starts, so it does differ between runs.
- Not a mistake: "the capacity of m goes from 1 to 3" and "sets the capacity of m to 3, so `a-2` starts at 2" (P33). No record holds the capacity of `m`, and the records show its effect: `a-2` starts at 2, when the capacity grows, and not at 5, when `a-1` would release the one unit.
- Not a mistake: a sentence about intent, such as "this models a bank".

## K5: a test checks one form or a part of the records and claims more

A test whose name, docstring or comments claim that it compares the forms, or that it checks more of the records than it reads, is a mistake when its code reads fewer forms or records than it claims. A claim is judged as a reader would take the sentence, in any words: "the three forms agree", "every API prints the same events", "parity across the three implementations". `forms` in `tests.json` lists every form it ran or whose file it opened while the test, its fixtures and its module ran; a fixture or an import can run a form that the test then does not read, so the reviewer reads the test's code to see which records reach its asserts. A test that reads a private attribute of a desflow object (a name that starts with `_`) in place of the records it claims to check is a mistake of this kind too; `grade.py` does not count such a read under K6.

- Wrong: `test_all_apis_agree`, docstring "Every API prints the same events.", whose asserts read only the YAML records.
- Wrong: `test_all_three_versions_agree`, which compares the YAML records with the Builder's and never runs `low_level.py`.
- Not a mistake: `test_yaml_run_ends_at_20`, docstring "The YAML form ends at 20; builder.py sets no end of its own." It claims no comparison: the second clause states a fact about another file.
- Not a mistake: `test_yaml_and_builder_end_at_20`, which reads the run end from `model.yaml` with `yaml.safe_load` and compares it with the last Builder record. It claims two forms and reads both.

## K8: a wall-clock assert

`clocks.json` lists the asserts that read a wall clock; the reviewer also reads `test_model.py` for one the list misses. In a task whose `paced_wall` is null, every assert on the wall clock is a mistake, because an unpaced run's wall time depends on the machine. In a paced task, an assert is a mistake when it bounds the wall time from above, or bounds it from below with less than 0.016 seconds allowed under `paced_wall` (the clock of Windows moves in steps of 15.6 ms). Work out the bound in seconds first, whatever the assert does to the reading: a reading multiplied by 1000 is in milliseconds, and a reading divided by the paced time is a ratio. A test whose `outcome` in `tests.json` is `flaky` depends on timing or on the machine; it is a mistake under K8 when its code reads a wall clock or a real-time pace, whether or not `clocks.json` lists its assert. The other half of the kind, paced tests run in parallel, is measured by `grade.py` under K12.

- Right: `assert wall >= 0.4 - 0.016` when `paced_wall` is 0.4.
- Wrong: `assert wall >= 0.4` when `paced_wall` is 0.4: it allows nothing under the paced time.
- Right: `elapsed_ms = (time.perf_counter() - start) * 1000` and `assert elapsed_ms >= 380`: 380 ms is 0.38 s, 0.02 s under 0.4.
- Wrong: `assert (time.perf_counter() - start) / 0.4 >= 0.99`: the bound is 0.396 s, 0.004 s under 0.4.
- Wrong: `assert elapsed < 0.6` in a paced task, and any assert on the wall clock in an unpaced task.

## K9: a test that cannot fail on the change it is meant to catch

`grade.py` counts a change that no test fails. For each other change in `tests.json`, the reviewer reads the tests that pass on the agent's model and fail under the change, and decides whether one of them checks the behaviour the change is about, from the change's name, the key and value it applied, and the test's code. The change is a mistake when every such test only compares forms (YAML records against Builder or Low-level records, which a change to `model.yaml` alone breaks whatever it changes) or fails for a reason the change does not stand for. Write one line per such change; the kind counts once per run.

- Wrong: "capacity 1 to 2", "grow at 4 instead of 2" and "job takes 6 instead of 5" each fail only `test_yaml_and_builder_give_the_same_records`. Three places, one per change.
- Not a mistake: "capacity 1 to 2" fails `test_done_times_show_capacity_one_then_grow[yaml]`, which checks the times the jobs are done, and the capacity decides those times.
- Not a mistake: tests parametrized by form whose `forms` lists all three, because a fixture runs every form once, when the `[yaml]` case reads only the YAML records and fails under the change.

## K10: the expected values come from the model under test

`desflow test --fix` writes the statistics the run gave into a blueprint's `expect` values, so the values a test expects then come from the model under test. Each Bash call in `trace.jsonl` that runs it is a mistake unless a tool result, of the same call or of a later one, shows the lines it changed: the file printed by any command or read with the Read tool, or a diff of it. A result that shows other files, or counts in place of the values, does not show them. A call whose result says it changed nothing is not a mistake.

- Not a mistake: `desflow test --fix model.yaml && cat model.yaml`, `nl -ba model.yaml` or `python -c "print(open('model.yaml').read())"` after it, or a Read of `model.yaml`.
- Wrong: `grep -c def builder.py model.yaml` after it, which prints the number of matching lines of each file.

## K14: a constant with no reason

A run end, a `history`, a tolerance, a sample size or a seed offset that the request does not give, written with no comment or sentence that says why.

- Wrong: `until: 1001`. Right: `until: 1001  # one second past the last report at 1000, so it is published`.
- Wrong: `assert abs(share - 0.75) < 0.1` with no reason for 0.1. Right: a comment that gives the standard error it comes from.
- Not a mistake: a number the prompt gives (a rate, a capacity, a run end it names), a seed of 42 that the prompt asks for, and a number that is clear from the line beside it.

## K15: a missing feature or a cause claimed without a run or a look at the API

A statement in a file or in `reply.md` that desflow lacks a form, that gives the cause of a difference or failure, or that says what another run would give ("without X it would ..."), where no tool call and result in `trace.jsonl` and no page of `skill/` shows it.

- Wrong: "the Low-level environment has no trace" when `env.trace_events()` exists. Right: "`env.trace_events()` gives the trace".
- Wrong: "the records differ because of tie order" with no run that shows the order. Right: the cause is stated after a run shows it, with the run named.
- Wrong: "the YAML `Console` egress takes no `format` key" when no call in `trace.jsonl` tried one. Right: a run that tries `config: {format: json}` comes first; it runs, so the claim is not made.
- Wrong: "I found no switch to turn this off" when no call in `trace.jsonl` looked for one and no page of `skill/` says so. A hedged wording still claims the feature is missing.
- Wrong: "the command takes about 1.3 s because of start-up and shutdown" when the run measured only the total time. A measured total does not show its cause.
- Not a mistake: a limit that the run itself shows, such as a refusal that `desflow check` printed and the file quotes.

The reviewer checks a claim against the pages of `skill/` and the calls in `trace.jsonl`; it does not search a repository.

## K17: a SimPy program ported with SimPy's behaviour assumed

For a task whose `original.py.txt` is a SimPy program, a form that keeps a SimPy behaviour that desflow does not have. Check these, and mark a mistake for each that applies and is not handled in the form:

- A signal fires once unless `once: false`, where SimPy makes a new event after each firing.
- A time in text needs a unit, and `run.until: "3.141592"` is refused: the number is written.
- The capacity of an unbounded container or store reads `None`, not `inf`.
- A subscribed task's interrupt value is the record's payload in a mapping, where SimPy's cause is the returned value.
- A child that ends by an uncaught interrupt needs `run.on_unhandled_failure: continue`.
- A `put` waits for the item to go in, where `out_pipe.put(msg)` with no `yield` does not: write `wait: false`.
- `system.simulation.lag_seconds` depends on the wall clock and is left out of a comparison. A test that compares forms or runs with the readings kept is a K17 mistake, and also a K8 mistake when `tests.json` marks it flaky.
- A model with no `run.until` runs until no event is left, as `env.run()` does.
- SimPy's minutes become seconds unless converted, once and said.
- SimPy prints `env.now` as it is, so numbers are compared, not printed text.
- A SimPy event that has a value but is not processed is `triggered`, not `processed`.

Evidence for K17 is the line of the form and the line of `original.py.txt` that shows the behaviour assumed.
