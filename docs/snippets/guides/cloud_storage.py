"""The stores, sinks and shop of the Write to Cloud Storage page.

The shop writes an hour of orders as Parquet and JSON Lines files, as fast as the machine
allows. STORAGE picks the store: local (the default), s3, gcs or azure. STORAGE_BUCKET
names the bucket or container, or the folder on the local disk. Run it with
`STORAGE=s3 STORAGE_BUCKET=my-bucket python cloud_storage.py`, with the credentials of
that cloud set up as the page describes.
cloud_storage.yaml and cloud_storage_low_level.py are the same shop in YAML and the
low-level API, and use the store and the sinks of this file.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

# --8<-- [start:stores]
import os

# One filesystem mapping for each store; desflow builds it into a PyArrow filesystem
STORES: dict[str, dict] = {
    # The local disk, for a first run
    "local": {"type": "local"},
    # Amazon S3, with the standard AWS settings: environment variables, ~/.aws or a role
    "s3": {"type": "s3", "region": os.getenv("AWS_REGION", "us-east-1")},
    # Google Cloud Storage, with Google's default credentials
    "gcs": {"type": "gcs"},
    # Azure Blob Storage, with Azure's default credentials for the storage account
    "azure": {"type": "azure", "account_name": os.getenv("AZURE_STORAGE_ACCOUNT")},
}

filesystem = STORES[os.getenv("STORAGE", "local")]
bucket = os.getenv("STORAGE_BUCKET", "output")
# --8<-- [end:stores]


# --8<-- [start:sinks]
from desflow import JsonlStorageEgress, ParquetStorageEgress


def storage_sinks(filesystem: dict, bucket: str) -> list:
    """Every event as Parquet and as JSON Lines, in two folders of the bucket."""
    return [
        ParquetStorageEgress(
            default_path=f"{bucket}/parquet/events.parquet", filesystem=filesystem
        ),
        JsonlStorageEgress(
            default_path=f"{bucket}/jsonl/events.jsonl", filesystem=filesystem
        ),
    ]


sinks = storage_sinks(filesystem, bucket)
# --8<-- [end:sinks]


# --8<-- [start:shop]
import logging

from desflow import SimulationContext


def build_shop(sinks: list) -> SimulationContext:
    """The shop, writing to the sinks given, as fast as the machine allows."""
    app = (
        SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
    )
    for sink in sinks:
        app.add_egress(sink)

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, entity):
        # The finished event is the order record
        return {"order_id": task_id, "amount": 19.99}

    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

    return app


app = build_shop(sinks)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 hour")
# --8<-- [end:shop]


# --8<-- [start:read]
def read_events(store, folder: str, format: str = "parquet"):
    """Every file a sink wrote under `folder`, as one table.

    `store` is a PyArrow filesystem, such as pyarrow.fs.S3FileSystem(region="us-east-1").
    """
    import pyarrow.dataset as ds

    # format="json" reads the files of the JSON Lines sink
    return ds.dataset(folder, format=format, filesystem=store).to_table()


# --8<-- [end:read]
