# Prefect

Run a desflow study as a task of a Prefect flow, and return its figures from the flow. The task calls desflow in Python and returns the figures.

______________________________________________________________________

## Before You Start

- [Integrations](https://jaehyeon.me/desflow/latest/integrations/#one-shop-for-every-orchestrator) shows the shop and the figures each page reads.

- Install desflow 1.1 or later with a recent version of Prefect 3. Any version of Prefect 3 that installs alongside desflow 1.1 or later works:

  ```bash
  pip install "desflow>=1.1" "prefect>=3,<4"
  ```

- Put `shop.yaml` and `prefect_flow.py` in one folder.

______________________________________________________________________

## Steps

### Write the Flow

The `run_shop` task loads `shop.yaml` from the flow's folder with the packers the flow passes, three by default, runs one simulated hour at seed 7, and returns the three figures. `with_stats("typical")` switches on the statistics the figures come from, because the mean wait needs the `typical` level. An error in the run raises an exception, which fails the task and the flow. A task called inside a flow returns its value, so the flow returns the figures:

```python
"""A Prefect flow whose task runs the shop with desflow and returns its figures.

Keep this file beside shop.yaml. `python prefect_flow.py` runs the flow once; with no
Prefect server set, Prefect starts a temporary one for the run.
"""

from pathlib import Path

from desflow import SimulationContext
from prefect import flow, task

# The folder that holds this file and shop.yaml
FOLDER = Path(__file__).resolve().parent
# The figures the task returns
FIGURES = (
    "tasks.pack.finished",
    "tasks.pack.wait_mean",
    "resources.packers.utilization_avg",
)


@task
def run_shop(packers: int) -> dict:
    app = SimulationContext.from_yaml(
        FOLDER / "shop.yaml", variables={"PACKERS": str(packers)}
    )
    app.random_seed = 7
    app.with_stats("typical")  # keeps the mean wait as well as the counts
    app.run(until=3600)
    readings = app.env.stats.readings()
    return {name: readings[f"Shop.stats.{name}"] for name in FIGURES}


@flow
def shop_study(packers: int = 3) -> dict:
    return run_shop(packers)


if __name__ == "__main__":
    print(shop_study())
```

### Run the Flow Once

Run this from the folder that holds the two files:

```bash
python prefect_flow.py
```

With no Prefect server set in `PREFECT_API_URL`, Prefect's default profile starts a temporary server for the run and stops it at the end. It keeps the run in a database under `~/.prefect`, or under `PREFECT_HOME` when that is set.

______________________________________________________________________

## Check the Result

Prefect logs the flow run and each task run, and the script prints the value the flow returns:

```text
{'resources.packers.utilization_avg': 49.47475468873524, 'tasks.pack.finished': 1775, 'tasks.pack.wait_mean': 0.25614739363776756}
```

These are the figures that `desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json` writes to `result.json` in a terminal.

______________________________________________________________________

## Sources

The default profile, `ephemeral`, is in `prefect/settings/profiles.toml` of Prefect 3.9.3, and the default home folder in `prefect/settings/models/_defaults.py`. Flows and tasks are described in Prefect's [Write and run workflows](https://www.prefect.io/docs/v3/how-to-guides/workflows/write-and-run).
