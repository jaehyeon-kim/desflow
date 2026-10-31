"""Python for iceberg_tables.yaml: the order record each packed order publishes. The catalog
is that of iceberg_tables_catalog.py, which must be beside the blueprint."""


def order_record(task_id, context, entity):
    """The finished event of a packed order: the order record."""
    return {"order_id": task_id, "amount": 19.99}
