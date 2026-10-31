"""The stores, sinks and reader of the Write to Cloud Storage page.

Every form of the shop imports the store and the sinks from this file: cloud_storage.py
(builder), cloud_storage.yaml and cloud_storage_low_level.py. It builds no simulation.
STORAGE picks the store: local (the default), s3, gcs or azure. STORAGE_BUCKET names the
bucket or container, or the folder on the local disk, with the credentials of that cloud
set up as the page describes.
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


# --8<-- [start:read]
def read_events(store, folder: str, format: str = "parquet"):
    """Every file a sink wrote under `folder`, as one table.

    `store` is a PyArrow filesystem, such as pyarrow.fs.S3FileSystem(region="us-east-1").
    """
    import pyarrow.dataset as ds

    # format="json" reads the files of the JSON Lines sink
    return ds.dataset(folder, format=format, filesystem=store).to_table()


# --8<-- [end:read]


# --8<-- [start:check]
if __name__ == "__main__":
    from pyarrow import fs

    # The PyArrow class of the store takes the keys of its mapping. It is looked up
    # by name, because some pyarrow builds lack the classes of the other clouds.
    classes = {
        "local": "LocalFileSystem",
        "s3": "S3FileSystem",
        "gcs": "GcsFileSystem",
        "azure": "AzureFileSystem",
    }
    options = {
        key: value for key, value in filesystem.items() if key != "type" and value
    }
    store = getattr(fs, classes[filesystem["type"]])(**options)
    for folder, format in (("parquet", "parquet"), ("jsonl", "json")):
        events = read_events(store, f"{bucket}/{folder}", format)
        print(f"{folder}: {events.num_rows} rows")
# --8<-- [end:check]
