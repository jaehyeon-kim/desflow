# Dagster

Run a desflow study as a Dagster asset, and keep its figures as the asset's metadata. The asset calls desflow in Python and returns the figures.

---

## Before You Start

- [Integrations](index.md#one-shop-for-every-orchestrator) shows the shop and the figures each page reads.
- Install desflow 1.1 or later with a recent version of Dagster. Any version of Dagster that installs alongside desflow 1.1 or later works:

    ```bash title="docs/snippets/orchestrators/commands.sh:dagster-install"
    pip install "desflow>=1.1" dagster
    ```

- Put `shop.yaml` and `dagster_asset.py` in one folder.

---

## Steps

### Write the Asset

The asset loads `shop.yaml` from its folder with three packers, runs one simulated hour at seed 7, and returns the three figures as `MaterializeResult` metadata, one number each, which Dagster shows on the asset's page. `with_stats("typical")` switches on the statistics the figures come from, because the mean wait needs the `typical` level. An error in the run raises an exception, which fails the materialization:

```python title="docs/snippets/orchestrators/dagster_asset.py" hl_lines="22-31"
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
```

`defs` holds the asset for Dagster's own tools: `dagster dev -f dagster_asset.py` starts Dagster's web server and daemon on your machine with the asset in them, once `pip install dagster-webserver` has added the web server, which `dagster` does not install. The `__main__` block materializes the asset once with `materialize()`, which runs in the current process and needs no `DAGSTER_HOME`, web server or daemon.

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

These are the figures that `desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json` writes to `result.json` in a terminal.

---

## Sources

`MaterializeResult` and `materialize()` are described in their docstrings in Dagster 1.13.30, in `dagster/_core/definitions/result.py` and `dagster/_core/definitions/materialize.py`.
