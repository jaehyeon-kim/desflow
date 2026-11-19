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
