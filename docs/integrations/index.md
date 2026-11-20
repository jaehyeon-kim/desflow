# Integrations

desflow runs from the tools people already use. An orchestrator such as Airflow runs a study as one task of a pipeline. An AI assistant gets the skill and the desflow commands from the plugin, which [AI Assistants](../ai/index.md) describes.

---

## Pages

| Page | What it does |
|---|---|
| [Airflow](airflow.md) | a DAG with a `BashOperator` that runs the shop and a task that reads its result |
| [Dagster](dagster.md) | an asset that runs the shop through `PipesSubprocessClient` and keeps the figures as metadata |
| [Prefect](prefect.md) | a flow with a task that runs the shop and a task that reads its result |
| [Kestra](kestra.md) | a flow whose Python task installs desflow in a container and runs the shop |

---

## One Shop for Every Orchestrator

The four orchestrator pages run one blueprint, `shop.yaml`, with one command. The shop takes an order every 2 seconds on average, and a packer takes 3 seconds on average to pack one. `factor: 0` runs the model as fast as it can, so an hour of simulated time ends in under a second:

```yaml title="docs/snippets/orchestrators/shop.yaml"
# The shop that every page of the Integrations section runs from an orchestrator. An
# order comes every 2 seconds on average, and the packers come from PACKERS, 2 when
# it is not set. factor: 0 runs it as fast as it can, not at wall-clock speed.
# Run it with: desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 \
#   --summary result.json
simulation: {sim_id: Shop, factor: 0, random_seed: 42}

resources:
  packers: {current_cap: "${PACKERS:-2}", max_cap: 5}

services:
  packing: {dist: normal, mean: 3 s, std: 0.5 s}

arrivals:
  orders: {dist: exponential, rate: 0.5, spawn: pack}

tasks:
  pack:
    service: packing
    resource: packers
    payload: {event_type: order_packed}
```

The command runs the shop with three packers for one simulated hour, at seed 7, and writes the run's statistics to `result.json`:

```bash title="docs/snippets/orchestrators/commands.sh:direct"
desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json
```

---

## Pass the Result Through a File

desflow logs to stderr and prints no result to stdout in a single run, so a later task reads the figures from the file that `--summary` writes, not from the output. Airflow, for example, keeps only the last line of a command's output, which is a desflow log line. [Write the Result to a File](../user-guide/results/replications.md#write-the-result-to-a-file) lists the file's keys.

The Airflow, Dagster and Prefect pages read three rows of the file's `statistics` list, and the Kestra page logs the whole file:

- `tasks.pack.finished`: the orders packed.
- `tasks.pack.wait_mean`: the mean wait for a packer, in seconds.
- `resources.packers.utilization_avg`: the packers' mean utilisation over the run, in percent.

The fixed seed gives every orchestrator the same figures as the command run in a terminal.

The command exits with 0 when the run finishes or is stopped by Ctrl+C or SIGTERM, 1 when the blueprint cannot be loaded, and 2 when a file is missing or an option is refused. Each orchestrator fails its task on any code other than 0. A stopped run flushes its sinks and writes no summary, so the task that reads `result.json` fails, not the task that ran desflow.
