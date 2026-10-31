"""Python for json_schema_and_protobuf.yaml: the order record each packed order publishes,
and the topics the run needs. The serializers and the router are those of
json_schema_and_protobuf_serializers.py, which must be beside the blueprint."""

from desflow import KafkaAdminConnector

TOPICS = ["sim-events", "sim-telemetry", "orders-json", "orders-protobuf"]


def order_record(task_id, context, entity):
    """The finished event of a packed order: the order record."""
    return {"order_id": task_id, "amount": 19.99}


def create_topics():
    """With a router, KafkaEgress creates no topics, so create them first."""
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    admin.create_topics([{"name": topic} for topic in TOPICS])
