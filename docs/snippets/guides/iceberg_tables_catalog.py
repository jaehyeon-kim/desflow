"""The catalogs, sink and reader of the Write Iceberg Tables page.

Every form of the shop imports the catalog and the sink from this file: iceberg_tables.py
(builder), iceberg_tables.yaml and iceberg_tables_low_level.py. It builds no simulation.
ICEBERG_CATALOG picks the catalog: sqlite (the default), rest, postgres or glue.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

# --8<-- [start:catalogs]
import os
from pathlib import Path

# The SQLite database and the local table files go in the folder iceberg/
FOLDER = Path("iceberg").resolve()
FOLDER.mkdir(exist_ok=True)

# One mapping of PyIceberg catalog properties for each catalog
CATALOGS = {
    # A SQL catalog on SQLite, with no service, for a first run
    "sqlite": {
        "type": "sql",
        "uri": f"sqlite:///{FOLDER}/catalog.db",
        "warehouse": f"file://{FOLDER}/warehouse",
    },
    # The REST catalog of `odctl up catalog`, with the table files on its SeaweedFS
    "rest": {
        "type": "rest",
        "uri": "http://127.0.0.1:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://127.0.0.1:8333",
        "s3.access-key-id": "user",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    },
    # A SQL catalog on the PostgreSQL of `odctl up postgres`, with local table files
    "postgres": {
        "type": "sql",
        "uri": "postgresql+psycopg2://user:password@127.0.0.1:5432/odctl",
        "warehouse": f"file://{FOLDER}/warehouse",
    },
    # The AWS Glue Data Catalog, with the table files in S3 and the standard AWS settings
    "glue": {
        "type": "glue",
        "glue.region": "us-east-1",
        "s3.region": "us-east-1",
        "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://my-bucket/warehouse"),
    },
}

name = os.getenv("ICEBERG_CATALOG", "sqlite")
catalog = {"name": name, **CATALOGS[name]}
# --8<-- [end:catalogs]


# --8<-- [start:sink]
from desflow import IcebergStorageEgress


def iceberg_sink(catalog: dict, table: str = "shop.events") -> IcebergStorageEgress:
    """Every event as a row of `table`, which the sink creates from the first batch."""
    return IcebergStorageEgress(catalog=catalog, default_table=table)


sink = iceberg_sink(catalog)
# One commit per flush, so the sink gets a large batch of its own
BATCH_SIZE = 200_000
# --8<-- [end:sink]


# --8<-- [start:read]
def read_events(catalog: dict, table: str = "shop.events"):
    """Every row of `table`, as one PyArrow table."""
    from pyiceberg.catalog import load_catalog

    properties = {key: value for key, value in catalog.items() if key != "name"}
    return (
        load_catalog(catalog["name"], **properties).load_table(table).scan().to_arrow()
    )


# --8<-- [end:read]


# --8<-- [start:check]
if __name__ == "__main__":
    events = read_events(catalog)
    print(f"{events.num_rows} rows in shop.events")
# --8<-- [end:check]
