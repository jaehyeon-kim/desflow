# Integrations

desflow runs from the tools people already use. An orchestrator such as Airflow runs a study as one task of a pipeline. An AI assistant gets the skill and the desflow commands from the plugin, which [AI Assistants](../ai/index.md) describes.

---

## Pages

| Page | What it does |
|---|---|
| [Airflow](airflow.md) | a DAG whose task runs the shop in Python and returns its figures |
| [Dagster](dagster.md) | an asset that runs the shop in Python and keeps its figures as metadata |
| [Prefect](prefect.md) | a flow whose task runs the shop in Python and returns its figures |
| [Kestra](kestra.md) | a flow whose Python task installs desflow in a container and runs the shop |

---

## One Shop for Every Orchestrator

The four orchestrator pages run one blueprint, `shop.yaml`. The shop takes an order every 2 seconds on average, and a packer takes 3 seconds on average to pack one. `factor: 0` runs the model as fast as it can, so an hour of simulated time ends in under a second:

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

Each page runs the shop with three packers for one simulated hour, at seed 7. In a terminal, this command does the same and writes the run's statistics to `result.json`:

```bash title="docs/snippets/orchestrators/commands.sh:direct"
desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json
```

---

## Figures Each Page Reads

The Airflow, Dagster and Prefect pages call desflow in Python and read three of the run's statistics. The Kestra page runs the command and logs the whole file that `--summary` writes, whose keys [Write the Result to a File](../user-guide/results/replications.md#write-the-result-to-a-file) lists. The three statistics are:

- `tasks.pack.finished`: the orders packed.
- `tasks.pack.wait_mean`: the mean wait for a packer, in seconds.
- `resources.packers.utilization_avg`: the packers' mean utilisation over the run, in percent.

The fixed seed gives every orchestrator the same figures as the command run in a terminal.

On the Airflow, Dagster and Prefect pages, an error in the run raises an exception, which fails the task. On the Kestra page, the command exits with 0 when the run finishes or is stopped by Ctrl+C or SIGTERM, 1 when the blueprint cannot be loaded, and 2 when a file is missing or an option is refused, and Kestra fails the task on any code other than 0.
