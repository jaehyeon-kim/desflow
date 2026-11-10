"""DX-22: A catalog that a variable chooses.

Builder form of `model.yaml`. The sink is taken away for the run, so this form has the
records only; the catalog is read from the file the variable names.
"""

import os
from pathlib import Path

import yaml

from desflow import SimulationContext
from desflow.connectors.egress.iceberg import IcebergStorageEgress

HERE = Path(__file__).parent


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0)
    name = os.environ.get("ICEBERG_CATALOG", "sqlite")
    catalog = yaml.safe_load((HERE / "catalogs" / f"{name}.yaml").read_text("utf-8"))
    app.add_egress(IcebergStorageEgress(catalog=catalog, default_table="shop.events"))
    app.add_resource("packers", current_cap=1, max_cap=3)
    app.add_arrival("orders", "constant", value=5, spawn="pack")

    @app.task(name="pack", service_time=2, resource_id="packers")
    def pack(task_id):
        return {"order_id": 1, "amount": 19.99}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)
