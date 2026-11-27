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
