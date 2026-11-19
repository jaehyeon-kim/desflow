# Airflow

Run a desflow study as a task of an Airflow DAG, and pass its figures to the next task. The DAG has two tasks: a `BashOperator` that runs the shop, and a Python task that reads the figures from the file the run writes.

---

## Before You Start

- [Integrations](index.md#one-shop-for-every-orchestrator) shows the shop, the command and the figures the result file holds.
- You need Airflow 3.4 and desflow 1.1 or later in one environment. Airflow asks to be installed with its constraints file, which names one tested version of each package for your Python:

    ```bash title="docs/snippets/orchestrators/commands.sh:airflow-install"
    AIRFLOW_VERSION=3.4.0
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    pip install "desflow>=1.1" "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    ```

- Put `shop.yaml` and `airflow_dag.py` in the DAGs folder, which is `~/airflow/dags` unless `AIRFLOW_HOME` names another folder for Airflow's files.

---

## Steps

### Write the DAG

The `run_shop` task runs the command with `cwd` set to the DAG's folder, so it finds `shop.yaml` and writes `result.json` beside it. Without `cwd`, Airflow runs the command in a temporary folder. A non-zero exit fails the task.

The `read_result` task opens `result.json` and returns the three figures, which Airflow keeps as the task's XCom value for later tasks. The result passes through the file because `BashOperator` pushes only the last line of the command's output to XCom, and desflow's last line is a log line:

```python title="docs/snippets/orchestrators/airflow_dag.py" hl_lines="31-35 37-44"
"""An Airflow DAG that runs the shop with desflow and reads the result.

Put this file and shop.yaml in the DAGs folder. `python airflow_dag.py` runs the DAG
once, in this process, with no scheduler.
"""

import json
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

# The folder that holds this file and shop.yaml, where the run writes result.json
FOLDER = Path(__file__).resolve().parent
# The figures the next task reads from the summary
FIGURES = (
    "tasks.pack.finished",
    "tasks.pack.wait_mean",
    "resources.packers.utilization_avg",
)
# The command, run from FOLDER. It writes the run's statistics to result.json
COMMAND = (
    "desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json"
)


@dag(schedule=None, catchup=False)
def shop_study():
    # desflow logs to stderr, and Airflow keeps only the last line of the output, so
    # the result goes to a file that the next task reads.
    run_shop = BashOperator(
        task_id="run_shop",
        bash_command=COMMAND,
        cwd=str(FOLDER),
    )

    @task
    def read_result() -> dict:
        summary = json.loads((FOLDER / "result.json").read_text())
        return {
            row["statistic"]: row["value"]
            for row in summary["statistics"]
            if row["statistic"] in FIGURES
        }

    run_shop >> read_result()


study = shop_study()

if __name__ == "__main__":
    run = study.test()
    print(run.get_task_instance("read_result").xcom_pull(task_ids="read_result"))
```

When Airflow stops a running task, it sends SIGTERM to the command. desflow then flushes its sinks and exits without writing `result.json`, so `read_result` does not read a stale file.

### Run the DAG Once

`dag.test()` runs every task of one DAG run in the current process, with no scheduler or API server. It needs Airflow's metadata database, which `airflow db migrate` creates as a SQLite file in Airflow's home folder. Run these from the DAGs folder:

```bash title="docs/snippets/orchestrators/commands.sh:airflow-run"
# From the DAGs folder, ~/airflow/dags unless AIRFLOW_HOME names another home
airflow db migrate
python airflow_dag.py
```

---

## Check the Result

Airflow logs each task as it runs, with desflow's log lines inside `run_shop`'s output. The last line the script prints is the value of `read_result`:

```text
{'resources.packers.utilization_avg': 49.47475468873524, 'tasks.pack.finished': 1775, 'tasks.pack.wait_mean': 0.25614739363776756}
```

Three packers packed 1775 orders in the hour, and an order waited 0.26 seconds for a packer on average. These are the figures of the command run in a terminal. After the line, Airflow 3.4.0 logs a `MissingGreenlet` error while it closes its SQLite connection at exit. The DAG run has finished by then, and the script exits with 0.

In a deployed Airflow, the scheduler runs the DAG instead of `python airflow_dag.py`, and each worker that runs `run_shop` needs desflow installed and the two files in its DAGs folder. Passing `result.json` between the two tasks needs both on one machine, such as the `LocalExecutor` or a single worker, and a DAGs folder that Airflow can write to. With several workers, `run_shop` writes its summary to storage that both tasks read, or the two tasks become one task. Two runs of the DAG at the same time share one file, so each run writes its own, such as `result-{{ run_id }}.json`.

---

## Sources

The operator's options and its XCom value are those of `BashOperator` in `apache-airflow-providers-standard` 1.21.0. `dag.test()` and the metadata database it needs are described in Airflow's [Debugging Airflow DAGs](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/debug.html), and the constraints file in [Installation from PyPI](https://airflow.apache.org/docs/apache-airflow/3.4.0/installation/installing-from-pypi.html).
