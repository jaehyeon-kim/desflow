"""Parquet and JSON Lines on Google Cloud Storage and Azure Blob Storage, and an
Iceberg table in a SQL catalog on SQLite, as the Egress page shows them.

Building the two cloud sinks opens no connection, so tests/docs build them without a
cloud account and check the PyArrow filesystem each mapping makes. The SQLite catalog
needs no service, so this file runs the shop and reads the table back.
"""

from pathlib import Path

from desflow import (
    IcebergStorageEgress,
    JsonlStorageEgress,
    ParquetStorageEgress,
    SimulationContext,
)

cloud = SimulationContext("Shop")
# --8<-- [start:cloud]
# Parquet files on Google Cloud Storage, with Google's default credentials
cloud.add_egress(
    ParquetStorageEgress(
        default_path="my-bucket/events.parquet",
        filesystem={"type": "gcs"},
    )
)
# JSON Lines files on Azure Blob Storage, with Azure's default credentials
cloud.add_egress(
    JsonlStorageEgress(
        default_path="my-container/events.jsonl",
        filesystem={"type": "azure", "account_name": "myaccount"},
    )
)
# --8<-- [end:cloud]

app = SimulationContext("Shop", factor=0, random_seed=1)
app.add_resource("packers", current_cap=1, max_cap=1)
app.add_service("packing", dist="constant", value=1.5)
app.add_arrival("orders", dist="exponential", rate=0.5)
# --8<-- [start:sqlite]
# The catalog's database and the table files are under the folder iceberg/
folder = Path("iceberg").resolve()
folder.mkdir(exist_ok=True)
catalog = {
    "name": "local",
    "type": "sql",
    "uri": f"sqlite:///{folder}/catalog.db",
    "warehouse": f"file://{folder}/warehouse",
}
app.add_egress(IcebergStorageEgress(catalog=catalog, default_table="sim.events"))
# --8<-- [end:sqlite]


@app.task(service_id="packing", resource_id="packers")
def pack(task_id):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_loop(context):
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), entity=entity))


if __name__ == "__main__":
    from pyiceberg.catalog import load_catalog

    app.run(until=60)
    properties = {key: value for key, value in catalog.items() if key != "name"}
    table = load_catalog("local", **properties).load_table("sim.events")
    print(table.scan().to_arrow().num_rows, "rows in sim.events")
