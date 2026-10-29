"""The Route Records to Topics guide: the router, the topics and the payload that the
blueprint, the builder and the low-level script share.
"""

from typing import Any, Dict

from desflow import KafkaAdminConnector

TOPICS = ["shop-events", "shop-telemetry", "system-alarms"]


# --8<-- [start:router]
def custom_kafka_router(data: Dict[str, Any]) -> str:
    """Picks a topic for each record by its kind and its value."""
    if data.get("stream_type") == "telemetry":
        return "shop-telemetry"

    value = data.get("value")
    if isinstance(value, dict) and (
        value.get("status") == "packing_failed" or value.get("level") == "ERROR"
    ):
        return "system-alarms"

    # Every other event, including a payload that is not a dict.
    return "shop-events"


# --8<-- [end:router]


# --8<-- [start:topics]
def create_topics() -> None:
    """With a router, KafkaEgress creates no topics, so create them first."""
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    admin.create_topics([{"name": topic} for topic in TOPICS])


# --8<-- [end:topics]


def packed(task_id: int, context=None) -> Dict[str, str]:
    """Every tenth order fails, for system-alarms."""
    if task_id % 10 == 9:
        return {"status": "packing_failed"}
    return {"status": "packed"}
