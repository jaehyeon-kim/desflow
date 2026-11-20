"""A Dagster asset that runs the shop with desflow and keeps its figures as metadata.

Keep this file beside shop.yaml. `python dagster_asset.py` materializes the asset once,
in this process, with no Dagster instance.
"""

import json
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
    materialize,
)

# The folder that holds this file and shop.yaml, where the run writes result.json
FOLDER = Path(__file__).resolve().parent
# The figures the asset keeps from the summary
FIGURES = (
    "tasks.pack.finished",
    "tasks.pack.wait_mean",
    "resources.packers.utilization_avg",
)
# The command, run from FOLDER. It writes the run's statistics to result.json
COMMAND = (
    "desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json"
)


@asset
def shop_result(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    # A non-zero exit of desflow fails the asset
    pipes_subprocess_client.run(
        context=context,
        command=COMMAND.split(),
        cwd=str(FOLDER),
    )
    summary = json.loads((FOLDER / "result.json").read_text())
    return MaterializeResult(
        metadata={
            row["statistic"]: row["value"]
            for row in summary["statistics"]
            if row["statistic"] in FIGURES
        }
    )


defs = Definitions(
    assets=[shop_result],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)

if __name__ == "__main__":
    result = materialize(
        [shop_result], resources={"pipes_subprocess_client": PipesSubprocessClient()}
    )
    metadata = result.asset_materializations_for_node("shop_result")[0].metadata
    print({name: value.value for name, value in metadata.items()})
