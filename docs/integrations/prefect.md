# Prefect

Run a desflow study as a task of a Prefect flow, and return its figures from the flow. The flow has two tasks: one runs the shop, and one reads the figures from the file the run writes.

---

## Before You Start

- [Integrations](index.md#one-shop-for-every-orchestrator) shows the shop, the command and the figures the result file holds.
- You need Prefect 3.9 and desflow 1.1 or later in one environment:

    ```bash title="docs/snippets/orchestrators/commands.sh:prefect-install"
    pip install "desflow>=1.1" prefect
    ```

- Put `shop.yaml` and `prefect_flow.py` in one folder.

---

## Steps

### Write the Flow

The `run_shop` task runs the command with `subprocess.run` in the flow's folder, so it finds `shop.yaml` and writes `result.json` beside it. `check=True` raises an error on a non-zero exit, which fails the task and the flow. The task returns the path of the file, and the `read_result` task opens it and returns the three figures. Two flow runs at the same time would share one `result.json`, so a deployment that runs them side by side gives each its own file, such as one named by the flow run's id. A task called inside a flow returns its value, so the flow returns the figures:

```python title="docs/snippets/orchestrators/prefect_flow.py" hl_lines="27-46"
"""A Prefect flow that runs the shop with desflow and returns its figures.

Keep this file beside shop.yaml. `python prefect_flow.py` runs the flow once; with no
Prefect server set, Prefect starts a temporary one for the run.
"""

import json
import subprocess
from pathlib import Path

from prefect import flow, task

# The folder that holds this file and shop.yaml, where the run writes result.json
FOLDER = Path(__file__).resolve().parent
# The figures the flow returns from the summary
FIGURES = (
    "tasks.pack.finished",
    "tasks.pack.wait_mean",
    "resources.packers.utilization_avg",
)
# The command, run from FOLDER. It writes the run's statistics to result.json
COMMAND = (
    "desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json"
)


@task
def run_shop() -> Path:
    # check=True fails the task when desflow exits with an error
    subprocess.run(COMMAND.split(), cwd=FOLDER, check=True)
    return FOLDER / "result.json"


@task
def read_result(path: Path) -> dict:
    summary = json.loads(path.read_text())
    return {
        row["statistic"]: row["value"]
        for row in summary["statistics"]
        if row["statistic"] in FIGURES
    }


@flow
def shop_study() -> dict:
    return read_result(run_shop())


if __name__ == "__main__":
    print(shop_study())
```

The figures pass through the file because desflow logs to stderr and prints no result in a single run.

### Run the Flow Once

Run this from the folder that holds the two files:

```bash title="docs/snippets/orchestrators/commands.sh:prefect-run"
python prefect_flow.py
```

With no Prefect server set in `PREFECT_API_URL`, Prefect's default profile starts a temporary server for the run and stops it at the end. It keeps the run in a database under `~/.prefect`, or under `PREFECT_HOME` when that is set.

---

## Check the Result

Prefect logs the flow run and each task run, and the script prints the value the flow returns:

```text
{'resources.packers.utilization_avg': 49.47475468873524, 'tasks.pack.finished': 1775, 'tasks.pack.wait_mean': 0.25614739363776756}
```

These are the figures of the command run in a terminal. The `prefect-shell` package can also run a command as a task, but `subprocess.run` needs no package beyond Prefect.

---

## Sources

The default profile, `ephemeral`, is in `prefect/settings/profiles.toml` of Prefect 3.9.3, and the default home folder in `prefect/settings/models/_defaults.py`. Flows and tasks are described in Prefect's [Write and run workflows](https://www.prefect.io/docs/v3/how-to-guides/workflows/write-and-run).
