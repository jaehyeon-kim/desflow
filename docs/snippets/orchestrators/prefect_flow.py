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
