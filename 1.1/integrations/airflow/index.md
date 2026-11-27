# Airflow

Run a desflow study as a task of an Airflow DAG, and pass its figures to later tasks. The task calls desflow in Python and returns the figures.

______________________________________________________________________

## Before You Start

- [Integrations](https://jaehyeon.me/desflow/latest/integrations/#one-shop-for-every-orchestrator) shows the shop and the figures each page reads.

- Install desflow 1.1 or later with a recent version of Airflow. Any version of Airflow that installs alongside desflow 1.1 or later works. On Airflow 2, import `dag` and `task` from `airflow.decorators` instead of `airflow.sdk`:

  ```bash
  pip install "desflow>=1.1" apache-airflow
  ```

- Put `shop.yaml` and `airflow_dag.py` in the DAGs folder, which is `~/airflow/dags` unless `AIRFLOW_HOME` names another folder for Airflow's files.

______________________________________________________________________

## Steps

### Write the DAG

The `run_shop` task loads `shop.yaml` from the DAG's folder with three packers, runs one simulated hour at seed 7, and returns the three figures. Airflow keeps the returned dictionary as the task's XCom value, so a later task can take it as an argument. `with_stats("typical")` switches on the statistics the figures come from, because the mean wait needs the `typical` level. desflow is imported inside the task, so the scheduler does not import it each time it parses the DAG file. An error in the run raises an exception, which fails the task:

```python
"""An Airflow DAG whose task runs the shop with desflow and returns its figures.

Put this file and shop.yaml in the DAGs folder. `python airflow_dag.py` runs the DAG
once, in this process, with no scheduler.
"""

from pathlib import Path

from airflow.sdk import dag, task

# The folder that holds this file and shop.yaml
FOLDER = Path(__file__).resolve().parent
# The figures the task returns
FIGURES = (
    "tasks.pack.finished",
    "tasks.pack.wait_mean",
    "resources.packers.utilization_avg",
)


@dag(schedule=None, catchup=False)
def shop_study():
    @task
    def run_shop(packers: int = 3) -> dict:
        # Imported here, so parsing the DAG file does not import desflow
        from desflow import SimulationContext

        app = SimulationContext.from_yaml(
            FOLDER / "shop.yaml", variables={"PACKERS": str(packers)}
        )
        app.random_seed = 7
        app.with_stats("typical")  # keeps the mean wait as well as the counts
        app.run(until=3600)
        readings = app.env.stats.readings()
        return {name: readings[f"Shop.stats.{name}"] for name in FIGURES}

    run_shop()


study = shop_study()

if __name__ == "__main__":
    run = study.test()
    print(run.get_task_instance("run_shop").xcom_pull(task_ids="run_shop"))
```

### Run the DAG Once

`dag.test()` runs every task of one DAG run in the current process, with no scheduler or API server. It needs Airflow's metadata database, which `airflow db migrate` creates as a SQLite file in Airflow's home folder. Run these from the DAGs folder:

```bash
# From the DAGs folder, ~/airflow/dags unless AIRFLOW_HOME names another home
airflow db migrate
python airflow_dag.py
```

______________________________________________________________________

## Check the Result

Airflow logs each task as it runs, with desflow's log lines inside `run_shop`'s output. The last line the script prints is the value of `run_shop`:

```text
{'resources.packers.utilization_avg': 49.47475468873524, 'tasks.pack.finished': 1775, 'tasks.pack.wait_mean': 0.25614739363776756}
```

Three packers packed 1775 orders in the hour, and an order waited 0.26 seconds for a packer on average. These are the figures that `desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json` writes to `result.json` in a terminal.

In a deployed Airflow, the scheduler runs the DAG instead of `python airflow_dag.py`, and each worker that runs `run_shop` needs desflow installed and the two files in its DAGs folder.

______________________________________________________________________

## Sources

A task's return value as its XCom value is described in Airflow's [TaskFlow](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/taskflow.html), and imports kept inside a task in [Best Practices](https://airflow.apache.org/docs/apache-airflow/3.4.0/best-practices.html#top-level-python-code). `dag.test()` and the metadata database it needs are described in [Debugging Airflow DAGs](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/debug.html).
