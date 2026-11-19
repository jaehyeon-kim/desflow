# Dagster

Run a desflow study as a Dagster asset, and keep its figures as the asset's metadata. The asset runs the shop through `PipesSubprocessClient`, then reads the figures from the file the run writes.

---

## Before You Start

- [Integrations](index.md#one-shop-for-every-orchestrator) shows the shop, the command and the figures the result file holds.
- You need Dagster 1.13 and desflow 1.1 or later in one environment:

    ```bash title="docs/snippets/orchestrators/commands.sh:dagster-install"
    pip install "desflow>=1.1" dagster
    ```

- Put `shop.yaml` and `dagster_asset.py` in one folder.

---

## Steps

### Write the Asset

`PipesSubprocessClient` runs the command with `cwd` set to the asset's folder, so it finds `shop.yaml` and writes `result.json` beside it. It passes desflow's output to Dagster's logs, and a non-zero exit fails the asset with `DagsterPipesExecutionError`. When a run is cancelled, it sends SIGINT to desflow and waits up to 20 seconds; desflow then flushes its sinks and writes no `result.json`.

The asset then opens `result.json` and returns the three figures as `MaterializeResult` metadata, one number each, which Dagster shows on the asset's page. The figures pass through the file because desflow does not report to Dagster itself:

```python title="docs/snippets/orchestrators/dagster_asset.py" hl_lines="38-50"
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
```

`defs` holds the asset and its resource for Dagster's own tools: `dagster dev -f dagster_asset.py` starts Dagster's web server and daemon on your machine with the asset in them. The `__main__` block materializes the asset once with `materialize()`, which runs in the current process and needs no `DAGSTER_HOME`, web server or daemon.

### Materialize It Once

Run this from the folder that holds the two files:

```bash title="docs/snippets/orchestrators/commands.sh:dagster-run"
python dagster_asset.py
```

---

## Check the Result

Dagster logs each step of the run, with desflow's log lines among them. The last line the script prints is the asset's metadata:

```text
{'resources.packers.utilization_avg': 49.47475468873524, 'tasks.pack.finished': 1775, 'tasks.pack.wait_mean': 0.25614739363776756}
```

These are the figures of the command run in a terminal. Dagster also logs the warning "[pipes] did not receive any messages from external process", because desflow does not use the `dagster-pipes` package to report back. The asset is still materialized, with the metadata above.

---

## Sources

The client's options, its failure and its cancel handling are those of `PipesSubprocessClient` in Dagster 1.13.30 (`dagster/_core/pipes/subprocess.py`), and `materialize()` is described in its docstring in `dagster/_core/definitions/materialize.py`. [Dagster Pipes](https://dagster.io/docs/integrations/external-pipelines) describes the client and the `dagster-pipes` package.
