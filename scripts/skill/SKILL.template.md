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

1. Download the nearest example with `desflow examples` (a list) and `desflow examples get <name> --api yaml`. The example files match the installed version.
2. Edit the file. Only `simulation` is required. [references/blueprint-keys.md](skill:references/blueprint-keys.md) lists every key, and [assets/blueprint.schema.json](skill:assets/blueprint.schema.json) is the JSON Schema of a blueprint for editors and checkers.
3. Run it with `desflow run <file>`. Add `--until 10min` to stop earlier and `--seed 7` to repeat a run. An invalid file prints every error with its file and line and exits with code 1.
4. Read each error where it points. An unknown key names the key it is closest to.

`desflow schema` prints the same JSON Schema as `assets/blueprint.schema.json`, for the installed version.

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

Read [Replications](https://jaehyeon.me/desflow/latest/user-guide/replications/), [Randomness and Seeds](https://jaehyeon.me/desflow/latest/tutorials/randomness-and-seeds/) and [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/) before choosing the warm-up and the number of replications.

## References

- [references/blueprint-keys.md](skill:references/blueprint-keys.md): every key of a blueprint, with its type and default.
- [references/distributions.md](skill:references/distributions.md): every distribution family, with its parameters and rules.
- [references/connectors.md](skill:references/connectors.md): every ingress and egress short name, its class and the extra it needs.
- [references/apis.md](skill:references/apis.md): one shop model in YAML, the builder API and the low-level API.
- [assets/blueprint.schema.json](skill:assets/blueprint.schema.json): the blueprint JSON Schema.

The documentation is at https://jaehyeon.me/desflow/latest/. Its pages are also available as plain text at https://jaehyeon.me/desflow/latest/llms.txt and https://jaehyeon.me/desflow/latest/llms-full.txt.
