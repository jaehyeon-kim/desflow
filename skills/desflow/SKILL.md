---
name: desflow
description: Write, check and run desflow discrete-event simulation models in YAML, the builder API or the low-level API. Use when a task mentions desflow, a blueprint, or a SimPy model that streams records.
license: MIT
compatibility: desflow 0.23.0 or later, Python 3.11 or later, the `desflow` command installed
metadata:
  desflow-version: "0.23.0"
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

1. Download the nearest example with `desflow examples` (a list) and `desflow examples get <name> --api yaml`. The example files match the installed version.
2. Edit the file. Only `simulation` is required. [references/blueprint-keys.md](references/blueprint-keys.md) lists every key, and [assets/blueprint.schema.json](assets/blueprint.schema.json) is the JSON Schema of a blueprint for editors and checkers.
3. Run it with `desflow run <file>`. Add `--until 10min` to stop earlier and `--seed 7` to repeat a run. An invalid file prints every error with its file and line and exits with code 1.
4. Read each error where it points. An unknown key names the key it is closest to.

`desflow schema` prints the same JSON Schema as `assets/blueprint.schema.json`, for the installed version.

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
| `ingress` | Where parameter changes come in from: Local, Kafka, Postgres or Redis. |
| `egress` | Where records go out to: Console, Kafka, Parquet, Jsonl, Iceberg, Postgres or Redis. |
| `batching` | How records are grouped before they are sent. |
| `stats` | Which built-in statistics are published, and how often. |
| `run` | How long the run lasts, whether to drain at the end, and functions to call before it starts. |

## Rules assistants get wrong

- A time is a number of seconds or a duration such as `90 s`, `10 min` or `2 hours`. `rate` is the exception: it is a number per second.
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

Read [Replications](https://jaehyeon.me/desflow/latest/user-guide/replications/), [Randomness and Seeds](https://jaehyeon.me/desflow/latest/tutorials/randomness-and-seeds/) and [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/) before choosing the warm-up and the number of replications.

## References

- [references/blueprint-keys.md](references/blueprint-keys.md): every key of a blueprint, with its type and default.
- [references/distributions.md](references/distributions.md): every distribution family, with its parameters and rules.
- [references/connectors.md](references/connectors.md): every ingress and egress short name, its class and the extra it needs.
- [references/apis.md](references/apis.md): one shop model in YAML, the builder API and the low-level API.
- [assets/blueprint.schema.json](assets/blueprint.schema.json): the blueprint JSON Schema.

The documentation is at https://jaehyeon.me/desflow/latest/. Its pages are also available as plain text at https://jaehyeon.me/desflow/latest/llms.txt and https://jaehyeon.me/desflow/latest/llms-full.txt.
