"""Imports and constructs what the `iceberg` extra exists to enable, installed alone."""

from desflow import IcebergStorageEgress

IcebergStorageEgress(catalog=object(), default_table="sim.events")

# The connector takes the catalog from the caller and imports pyarrow
# lazily, so constructing it proves neither is installed. The extra has
# to carry the Arrow FileIO and every catalog the docs list: REST, SQL
# on SQLite or PostgreSQL, and Glue.
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO
import psycopg2
load_catalog("local", type="sql", uri="sqlite:///:memory:", warehouse="file:///tmp/warehouse")
load_catalog("aws", type="glue", **{"glue.region": "us-east-1"})
