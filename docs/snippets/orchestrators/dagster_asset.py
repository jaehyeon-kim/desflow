"""A Dagster asset that runs the shop with desflow and keeps its figures as metadata.

Keep this file beside shop.yaml. `python dagster_asset.py` materializes the asset once,
in this process, with no Dagster instance.
"""

from pathlib import Path

from dagster import Definitions, MaterializeResult, asset, materialize
from desflow import SimulationContext

# The folder that holds this file and shop.yaml
FOLDER = Path(__file__).resolve().parent
# The figures the asset keeps as metadata
FIGURES = (
    "tasks.pack.finished",
    "tasks.pack.wait_mean",
    "resources.packers.utilization_avg",
)


@asset
def shop_result() -> MaterializeResult:
    app = SimulationContext.from_yaml(FOLDER / "shop.yaml", variables={"PACKERS": "3"})
    app.random_seed = 7
    app.with_stats("typical")  # keeps the mean wait as well as the counts
    app.run(until=3600)
    readings = app.env.stats.readings()
    return MaterializeResult(
        metadata={name: readings[f"Shop.stats.{name}"] for name in FIGURES}
    )


defs = Definitions(assets=[shop_result])

if __name__ == "__main__":
    result = materialize([shop_result])
    metadata = result.asset_materializations_for_node("shop_result")[0].metadata
    print({name: value.value for name, value in metadata.items()})
