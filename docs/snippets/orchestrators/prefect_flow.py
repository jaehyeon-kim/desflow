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
