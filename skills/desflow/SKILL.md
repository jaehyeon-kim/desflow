---
name: desflow
description: Write, check, test and run desflow discrete-event simulation models in YAML, the builder API or the low-level API, and port SimPy programs to them. Use when a task mentions desflow, a blueprint, a test of a simulation model, or a SimPy model that streams records.
license: MIT
compatibility: desflow 1.0.0rc1 or later, Python 3.11 or later, the `desflow` command installed
metadata:
  desflow-version: "1.0.0rc1"
---

# desflow

desflow runs SimPy simulations whose parameters can change while they run, and writes the events and telemetry to the console, Kafka, Parquet or JSONL files, Iceberg, PostgreSQL or Redis. A model can be written in three ways, and all three give the same records.

## Pick a way to write the model

| Way | Use it when |
|---|---|
| YAML blueprint | The model is resources, services, arrivals and tasks. Start here. A blueprint is checked as a whole before anything runs, and an error names the file and line. |
| Builder API | The model is mostly a blueprint, but the file needs logic in Python, such as a payload function or a process. `SimulationContext` has one method for each blueprint section. |
| Low-level API | The model needs SimPy itself, with desflow's resources, clocks and sinks. |

A blueprint can also call Python with `!python module.attribute`, so a model rarely has to leave YAML. A file that references Python runs the imports it names, so treat it like a script.

## Write a blueprint

1. Download the nearest example with `desflow examples list` and `desflow examples get <name> --api yaml`. The example files match the installed version.
2. Edit the file. Only `simulation` is required. [references/blueprint-keys.md](references/blueprint-keys.md) lists every key, and [assets/blueprint.schema.json](assets/blueprint.schema.json) is the JSON Schema of a blueprint for editors and checkers.
3. Run it with `desflow run <file>`. Add `--until 10min` to stop earlier and `--seed 7` to repeat a run. An invalid file prints every error with its file and line and exits with code 1.
4. Read each error where it points. An unknown key names the key it is closest to.

`desflow schema` prints the same JSON Schema as `assets/blueprint.schema.json`, for the installed version.

## Draft a model from a description

Use these steps when the user describes a process in words and has no model yet.

1. Start from the nearest example: `desflow examples list`, then `desflow examples get <name> --api yaml`.
2. Build in stages, and run the file after each stage: the arrivals, then the tasks and their order, then the resources, then the service times, then the working hours.
3. Ask the user for the numbers the description does not give, before assuming them: the spread of each time, the share of each branch, the working hours, and who does each task.
4. Mark every number you had to assume with a comment that says `assumed:` and what was assumed. Put the assumed numbers first in your reply, because the user must confirm them.
5. Check the load by hand when `desflow explain` gives no estimate, as it does for a model with scenario steps or a task that routes on a variable. The work for one case is the sum of its task times, counting a branch by its share and a rework loop by how often it repeats. That work, times the arrival rate, must fit inside the capacity of the working hours. If it does not, the queue grows without limit, and the user should hear that before anything runs.
6. To close an arrival outside working hours, do not set its exponential rate to 0, because a scenario step with that value is refused when the file loads. Let the arrival run all day at the open rate, and make the first task route on a variable that the scenario sets: `next: {when: [{condition: 'registry["Shop.variables.open"] == 1', task: first_task}]}`. An arrival that comes while the desk is closed ends there.
7. Stop after three failed attempts at the same error, and show the user the file and the error.

## Check a model before a long run

1. See the first records with `desflow run <file> --sample 10`. It prints the first 10 events as JSON lines, unpaced, with no ingress and no real sinks, so a model that writes to Kafka or a database needs no service. `<file>` is a blueprint or a builder Python file, and `desflow check <file>` takes both too.
2. Check the model against queueing theory with `desflow explain <file>`. It takes a blueprint or a builder Python file with a module-level `SimulationContext`, and shows the model as a graph with each resource's utilisation, status, waits and tuning hints where the queueing formulas apply, and why where they do not. A resource at utilisation 1 or above is `UNSTABLE`: its queue grows for as long as the run lasts.
3. Add `--verify` to also run the model unpaced and compare its statistics with the analysis, within 10 percent for one run, or with each measure's 95% interval with `--replications N`.

To give a service a measured mean and standard deviation, `desflow dist <family> --mean <mean> --std <std>` converts them to the family's parameters, such as `desflow dist gamma --mean 1.5 --std 0.4832`, and prints them in YAML and Python. `desflow dist` alone lists the families.

## Check a model against the request

Copy this checklist into your reply and tick each line as it is done:

```text
- [ ] List every behaviour the request names, one line each.
- [ ] For each, name the key or the code that makes it, and the record that shows it.
- [ ] Run python <skill folder>/scripts/check_model.py <files>; fix every line it prints; run it again.
- [ ] Find each listed record in the output of desflow run <file> --sample 50.
- [ ] Read each header, docstring and comment against that output.
```

`scripts/check_model.py` is in this skill's folder. Run it, do not read it: `python <skill folder>/scripts/check_model.py model.yaml builder.py low_level.py [--until T]`, with one, two or three forms in any order. It runs each Python file as `python FILE`, unpaced, with its sinks replaced by a JSON console, and sets and reads each form's seed through desflow. It sees only the process `python FILE` starts, so each Python file builds and runs its model in that process, not a child process. It reports:

- a file that `desflow check` refuses;
- an import from desflow that the references do not document;
- a draw with no seed, such as Python's `random`, which makes two runs at one seed differ;
- a random generator the model seeds itself, other than one it gives to a `Sampler`;
- the first record that differs between the forms at the model's seed and the two after it, against the YAML form, or against the Builder form when there is no YAML (one form alone runs at two seeds and is compared with nothing);
- a failure of `desflow test` on the YAML, and on a Builder file that has a `tests.yaml` beside it.

A seed is found where it is given to `random.Random`, `random.seed`, `numpy.random.seed`, `RandomState`, `default_rng`, `SeedSequence` or a NumPy bit generator. A state set another way (`setstate`, the `state` setter, unpickling), a `SeedSequence` with a spawn key, a seeding function called through `functools.partial` or another callable with no Python code of the model's, a generator the model writes itself and one a C extension seeds are not found, and the rule to draw only through desflow still applies to them. Each line it prints names the file and line and what to write instead. It does not look for private attributes such as `app._sinks`: check those yourself against the rule below.

- A header says only what a run shows. Wrong: "Adding two agents ends the hang-ups." when every caller in the run still hangs up. Right: "With three agents, about one caller in five hangs up; `scenario` adds two agents at 1 hour.", checked against `--sample`.
- A field that has the same value in every record shows the reader nothing. Wrong: `count` over `last: 10` with no `where`, which is 10 in every report. Right: a `where` that counts the large orders.
- Every constant that is not the request's has a comment with its reason: a run end, a `history`, a tolerance. Wrong: `until: 1001`. Right: `until: 1001  # one second past the last report at 1000, so it is published`.
- Before writing that desflow lacks a form, search [references/api.md](references/api.md) and [references/blueprint-keys.md](references/blueprint-keys.md) for it. Give the cause of a difference only after a run shows it. Wrong: "the Low-level environment has no trace". Right: "`env.trace_events()` gives the trace".

## Test a model

- Test the statistics with `desflow test` and the records with pytest.
- Run every test unpaced, except a test of pacing, which runs alone. Give every test a fixed seed and a fixed `logical_start_time`.
- Read [references/testing.md](references/testing.md) before writing a test: it has a wrong and a right example of each mistake, and a template.

## Sections of a blueprint

| Section | What it holds |
|---|---|
| `simulation` | Required. The simulation id, pacing factor, random seed, time zone, warm-up and clock. |
| `resources` | Named resources with a capacity, a queue order, preemption and breakdowns. |
| `containers` | Named containers that hold a continuous amount. |
| `stores` | Named stores that hold items. |
| `services` | Named distributions that give how long a task step takes. |
| `arrivals` | When entities arrive: a distribution or a replay of recorded rows, and the task each one starts. |
| `variables` | Values that expressions and tasks read and that a run can change. |
| `signals` | Named events that tasks fire and wait for. |
| `tasks` | What an entity does: wait, request a resource, take or put an amount, pass the time, then go to the next task. |
| `telemetry` | Custom readings published at a fixed interval. |
| `processes` | Python functions that run beside the tasks, named with `!python`. |
| `scenario` | Changes to a parameter at set times in the run, and records of what was true then. |
| `ingress` | Where parameter changes come in from: Local, File, Kafka, Postgres or Redis. |
| `egress` | Where records go out to: Console, Kafka, Parquet, Jsonl, Iceberg, Postgres or Redis. |
| `batching` | How records are grouped before they are sent. |
| `stats` | Which built-in statistics are published, and how often. |
| `run` | How long the run lasts, whether to drain at the end, and functions to call before it starts. |
| `tests` | Named checks that `desflow test` runs: a seed, a run length and the measures expected. |

## Rules assistants get wrong

- A time in YAML and in the Builder takes a unit: write `mean: 4 min`, not `mean: 240`. The Low-level `DistributionConfig` takes seconds only, so give the unit in a comment: `DistributionConfig(dist="normal", mean=240, std=60)  # 4 min, 1 min`. `rate` is a number per second. A SimPy program that counts minutes is ported with each time converted, or says once that one SimPy minute is one second here. [references/simpy.md](references/simpy.md) lists the other differences from SimPy.
- Builder and Low-level code use only the names `from desflow import ...` and `from desflow.values import ...` give ([references/api.md](references/api.md)) and the documented serializer classes. Wrong: `resource._capacity = 3` or `from desflow.builder.task import NO_RECORD`. Right: `env.registry.update("Shop.resources.packers.current_cap", 3)` and `from desflow import NO_RECORD`.
- A Low-level form gives the YAML's records when it draws as the YAML does: each distribution with `sampler.sample(env.registry.get_config("<sim_id>.<path>"))`, which draws from the path's own sequence, and each generated value from `sampler.generator("<place>")`, such as `sampler.generator("tasks.audit.payload.merchant")`, with the same seed. Use the generator's methods: `rng.uniform(-1, 1)`, not `-1 + 2 * rng.random()`. A `DistributionConfig` built by hand and never registered draws from the main generator and gives other numbers.
- A Builder or Low-level file runs with `python <file>`: use `ConsoleEgress(format="json")`, or call `logging.basicConfig(level=logging.INFO)` before the run, because the default `log` format writes at INFO. Import a connector that needs an extra only in a file whose header names that extra.
- Both forms keep 1,000,000 rows of a task that a `lookup` or `aggregate` reads; give the same `history` in both or none. A Builder task read only from a `Template` keeps none: give it `history=`.
- `logical_start_time` and `go_live_at` are instants: `now`, a signed duration such as `-1d`, or an ISO datetime.
- `!python module.attribute` imports the module and returns the attribute. The module is found on `sys.path`, which includes the folder of the YAML file, so a Python file beside the blueprint works from any directory.
- `!include path` puts a mapping from another YAML file in place, with the path relative to the file that names it.
- `${VAR}` reads an environment variable and `${VAR:-default}` gives a default when it is unset or empty. An unquoted value is read as YAML reads it after the replacement, so `factor: ${SIM_FACTOR:-0}` is the number 0. `$${` writes a literal `${`. A variable without a default that is unset stops the load.
- An arrival's `spawn` names one task. The task passes the entity on with `next`.
- A task's `spawn` and an `on:` spawn action start a task as a new entity. A task that a task's `spawn` or an `on:` spawn starts must not share an arrival's name.
- A connector `type` is a short name with its capital letter, such as `Console` or `Kafka`. [references/connectors.md](references/connectors.md) lists them and the extra each one needs.
- A resource or store has a whole-number capacity. A container holds a continuous amount, so its capacity and level can be fractional.
- A misspelt key is refused, not ignored. Fix the key instead of removing it.

## Run a study, not a single run

One run is one sample. To report a result, run a study.

1. Set `simulation.random_seed`, or pass `--seed`, so the run can be repeated.
2. Set `simulation.warmup` to the time the model needs to reach its usual state. The statistics start again from zero at that time, so the start of the run is left out of them.
3. Run replications with `desflow run <file> --replications 20`. Each replication has its own seed, spawned from the seed.
4. Report the mean of each statistic with its confidence interval. The default level is 95%, and `--level` changes it.
5. To compare designs, such as two packers against three, write an experiment file and run `desflow experiment <file>`. It runs every design with the same seeds, compares them, checks bounds, and writes its files and `report.md`. It exits 1 when a check fails. `desflow report <folder>` builds the report again from the files.

To give a service measured times, `desflow fit <file.csv> --column <name> --lower 0 --out <fit.yaml>` fits every family and writes the best as YAML for `!include`. It needs the `fit` extra.

[Command Line](https://jaehyeon.me/desflow/latest/cli/) uses every command on one shop. Read [Replications](https://jaehyeon.me/desflow/latest/user-guide/results/replications/), [Experiments](https://jaehyeon.me/desflow/latest/user-guide/results/experiments/), [Randomness and Seeds](https://jaehyeon.me/desflow/latest/learn/first-model/randomness-and-seeds/) and [Trusting Results](https://jaehyeon.me/desflow/latest/learn/studies/trusting-results/) before choosing the warm-up and the number of replications.

## References

- [references/blueprint-keys.md](references/blueprint-keys.md): every key of a blueprint, with its type and default.
- [references/distributions.md](references/distributions.md): every distribution family, with its parameters and rules.
- [references/connectors.md](references/connectors.md): every ingress and egress short name, its class and the extra it needs.
- [references/apis.md](references/apis.md): one shop model in YAML, the builder API and the low-level API.
- [references/cli.md](references/cli.md): the help of every `desflow` command, with every option.
- [references/api.md](references/api.md): every name `desflow` exports, with what it is and the extra it needs.
- [references/testing.md](references/testing.md): how to test a model, each rule with a wrong and a right example, and a test template.
- [references/simpy.md](references/simpy.md): where desflow differs from SimPy, and the form to write in a port.
- [scripts/check_model.py](scripts/check_model.py): checks the files of a model and compares the records of its forms; run it, do not read it.
- [assets/blueprint.schema.json](assets/blueprint.schema.json): the blueprint JSON Schema.

The documentation is at https://jaehyeon.me/desflow/latest/. Its pages are also available as plain text at https://jaehyeon.me/desflow/latest/llms.txt and https://jaehyeon.me/desflow/latest/llms-full.txt.
