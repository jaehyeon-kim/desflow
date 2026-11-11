"""The original of known case DX-18 (tests/known_cases/dx_18): a sink filter written in
Python. DX-18's model.yaml writes the same filter as a `when` expression, as the
Streaming Out blueprint beside this file does, and DX-18 checks that both keep the
same records."""


def packed_only(record: dict) -> bool:
    """Keeps the finished orders: events whose value says order_packed."""
    return (
        record["stream_type"] == "event"
        and record["value"].get("event_type") == "order_packed"
    )
