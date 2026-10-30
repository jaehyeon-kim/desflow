---
name: desflow
description: Write, check and run desflow discrete-event simulation models in YAML, the builder API or the low-level API. Use when a task mentions desflow, a blueprint, or a SimPy model that streams records.
license: MIT
compatibility: desflow {{version}} or later, Python 3.11 or later, the `desflow` command installed
metadata:
  desflow-version: "{{version}}"
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
2. Edit the file. Only `simulation` is required. [references/blueprint-keys.md](skill:references/blueprint-keys.md) lists every key, and [assets/blueprint.schema.json](skill:assets/blueprint.schema.json) is the JSON Schema of a blueprint for editors and checkers.
3. Run it with `desflow run <file>`. Add `--until 10min` to stop earlier and `--seed 7` to repeat a run. An invalid file prints every error with its file and line and exits with code 1.
4. Read each error where it points. An unknown key names the key it is closest to.

`desflow schema` prints the same JSON Schema as `assets/blueprint.schema.json`, for the installed version.

## Draft a model from a description

Use these steps when the user describes a process in words and has no model yet.

1. Start from the nearest example: `desflow examples list`, then `desflow examples get <name> --api yaml`.
2. Build in stages, and run the file after each stage: the arrivals, then the tasks and their order, then the resources, then the service times, then the working hours.
3. Ask the user for the numbers the description does not give, before assuming them: the spread of each time, the share of each branch, the working hours, and who does each task.
4. Mark every number you had to assume with a comment that says `assumed:` and what was assumed. Put the assumed numbers first in your reply, because the user must confirm them.
5. Check the load by hand when `desflow explain` gives no estimate, as it does for a model with scenario steps. The work for one case is the sum of its task times, counting a branch by its share and a rework loop by how often it repeats. That work, times the arrival rate, must fit inside the capacity of the working hours. If it does not, the queue grows without limit, and the user should hear that before anything runs.
6. To close an arrival outside working hours, do not set its exponential rate to 0, because a scenario step with that value is refused when the file loads. Let the arrival run all day at the open rate, and make the first task route on a variable that the scenario sets: `next: {when: [{condition: 'registry["Shop.variables.open"] == 1', task: first_task}]}`. An arrival that comes while the desk is closed ends there.
7. Stop after three failed attempts at the same error, and show the user the file and the error.

## Check a model before a long run

1. See the first records with `desflow run <file> --sample 10`. It prints the first 10 events as JSON lines, unpaced, with no ingress and no real sinks, so a model that writes to Kafka or a database needs no service.
2. Check the model against queueing theory with `desflow explain <file>`. It takes a blueprint or a builder Python file with a module-level `SimulationContext`, and shows the model as a graph with each resource's utilisation, status, waits and tuning hints where the queueing formulas apply, and why where they do not. A resource at utilisation 1 or above is `UNSTABLE`: its queue grows for as long as the run lasts.
3. Add `--verify` to also run the model unpaced and compare its statistics with the analysis, within 10 percent for one run, or with each measure's 95% interval with `--replications N`.

To give a service a measured mean and standard deviation, `desflow dist <family> --mean <mean> --std <std>` converts them to the family's parameters, such as `desflow dist gamma --mean 1.5 --std 0.4832`, and prints them in YAML and Python. `desflow dist` alone lists the families.

## Sections of a blueprint

<!-- gen:sections -->

## Rules assistants get wrong

- A time is a number of seconds or a duration such as `90 s`, `10 min` or `2 hours`. `rate` is the exception: it is a number per second.
- `logical_start_time` and `go_live_at` are instants: `now`, a signed duration such as `-1d`, or an ISO datetime.
- `!python module.attribute` imports the module and returns the attribute. The module is found on `sys.path`, which includes the folder of the YAML file, so a Python file beside the blueprint works from any directory.
- `!include path` puts a mapping from another YAML file in place, with the path relative to the file that names it.
- `${VAR}` reads an environment variable and `${VAR:-default}` gives a default when it is unset or empty. An unquoted value is read as YAML reads it after the replacement, so `factor: ${SIM_FACTOR:-0}` is the number 0. `$${` writes a literal `${`. A variable without a default that is unset stops the load.
- An arrival's `spawn` names one task. The task passes the entity on with `next`.
- A task's `spawn` and an `on:` spawn action start a task as a new entity. A task that a task's `spawn` or an `on:` spawn starts must not share an arrival's name.
- A connector `type` is a short name with its capital letter, such as `Console` or `Kafka`. [references/connectors.md](skill:references/connectors.md) lists them and the extra each one needs.
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

- [references/blueprint-keys.md](skill:references/blueprint-keys.md): every key of a blueprint, with its type and default.
- [references/distributions.md](skill:references/distributions.md): every distribution family, with its parameters and rules.
- [references/connectors.md](skill:references/connectors.md): every ingress and egress short name, its class and the extra it needs.
- [references/apis.md](skill:references/apis.md): one shop model in YAML, the builder API and the low-level API.
- [references/cli.md](skill:references/cli.md): the help of every `desflow` command, with every option.
- [references/api.md](skill:references/api.md): every name `desflow` exports, with what it is.
- [assets/blueprint.schema.json](skill:assets/blueprint.schema.json): the blueprint JSON Schema.

The documentation is at https://jaehyeon.me/desflow/latest/. Its pages are also available as plain text at https://jaehyeon.me/desflow/latest/llms.txt and https://jaehyeon.me/desflow/latest/llms-full.txt.
