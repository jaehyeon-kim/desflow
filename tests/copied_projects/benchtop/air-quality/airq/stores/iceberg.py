"""Connects to the Iceberg catalog and derives the table schemas from the pydantic models."""

import logging
import warnings
from datetime import date

import pyarrow as pa
from pydantic import AwareDatetime, BaseModel
from pyiceberg.catalog import Catalog, load_catalog

from airq.core.config import CATALOG, NAMESPACE, PREDICTIONS, TABLES
from airq.stores import s3 as s3_store

logger = logging.getLogger(__name__)

_WAREHOUSE_BUCKET = "warehouse"

# An overwrite of a day or an as-of date that has no rows yet is expected.
warnings.filterwarnings(
    "ignore", "Delete operation did not match any records", UserWarning
)

_TS = pa.timestamp("us", tz="UTC")
_ARROW = {
    str: pa.string(),
    float: pa.float64(),
    int: pa.int32(),
    bool: pa.bool_(),
    date: pa.date32(),
    AwareDatetime: _TS,
}


def arrow_schema(model: type[BaseModel]) -> pa.Schema:
    """
    Derives the Arrow schema of a table from its pydantic model.

    PyIceberg creates the Iceberg schema from the same Arrow schema. Every field is
    required.

    Args:
        model (type[BaseModel]): The pydantic model of the table's rows.

    Returns:
        pa.Schema: One non-nullable field per model field.
    """
    return pa.schema(
        pa.field(name, _ARROW[info.annotation], nullable=False)
        for name, info in model.model_fields.items()
    )


def catalog() -> Catalog:
    """
    Returns the Iceberg REST catalog.

    The PYICEBERG_CATALOG__ODCTL__* variables configure it. `airq.core.config` sets them on
    the host.

    Returns:
        Catalog: The PyIceberg catalog that `CATALOG` names.
    """
    return load_catalog(CATALOG)


def recreate_tables(cat, properties: dict[str, str]) -> None:
    """
    Drops and recreates the four tables of the feature pipeline.

    It also drops the predictions table, because its predictions were made from the data
    being replaced. It first creates the `airq` namespace if it does not exist.

    Args:
        cat (Catalog): The Iceberg catalog.
        properties (dict[str, str]): The table properties set on each new table.
    """
    cat.create_namespace_if_not_exists(NAMESPACE)
    if cat.table_exists(PREDICTIONS):
        cat.drop_table(PREDICTIONS)
    for model, identifier in TABLES.items():
        if cat.table_exists(identifier):
            cat.drop_table(identifier)
        cat.create_table(identifier, schema=arrow_schema(model), properties=properties)


def to_arrow(model: type[BaseModel], rows: list[BaseModel]) -> pa.Table:
    """
    Converts rows to an Arrow table with the schema of their model.

    Args:
        model (type[BaseModel]): The pydantic model of the rows.
        rows (list[BaseModel]): The rows to convert.

    Returns:
        pa.Table: The rows, with the schema from `arrow_schema`.
    """
    return pa.Table.from_pylist(
        [r.model_dump() for r in rows], schema=arrow_schema(model)
    )


def drop_namespace(s3) -> None:
    """
    Drops every table in the namespace and the namespace, then deletes their files.

    Dropping a table removes it from the catalog. The files are deleted separately, so
    none are left in SeaweedFS whatever the catalog does with them.

    Args:
        s3 (boto3.resources.base.ServiceResource): The S3 resource.
    """
    cat = catalog()
    tables = cat.list_tables(NAMESPACE) if cat.namespace_exists(NAMESPACE) else []
    for identifier in tables:
        cat.drop_table(identifier)
    if cat.namespace_exists(NAMESPACE):
        cat.drop_namespace(NAMESPACE)
    prefix = f"{NAMESPACE}/"
    removed = s3_store.delete_prefix(s3, _WAREHOUSE_BUCKET, prefix)
    logger.info(
        "Iceberg: dropped %d tables and %d files from s3://%s/%s",
        len(tables),
        removed,
        _WAREHOUSE_BUCKET,
        prefix,
    )
