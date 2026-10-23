"""Python for the Streaming Out blueprint beside it: the file sink's predicate."""


# --8<-- [start:packed]
def packed_only(record: dict) -> bool:
    """Keeps the finished orders: events whose value says order_packed."""
    return (
        record["stream_type"] == "event"
        and record["value"].get("event_type") == "order_packed"
    )
    # --8<-- [end:packed]
