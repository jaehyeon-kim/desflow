"""Python for cloud_storage.yaml: the order record each packed order publishes. The store
is that of cloud_storage.py, which must be beside the blueprint."""


def order_record(task_id, context, entity):
    """The finished event of a packed order: the order record."""
    return {"order_id": task_id, "amount": 19.99}
