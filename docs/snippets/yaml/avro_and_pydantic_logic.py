"""Python for avro_and_pydantic.yaml: the prediction each packed order publishes, and
the topics the run needs. The models, the router and the serializer are those of
avro_and_pydantic.py, which must be beside the blueprint."""

from avro_and_pydantic import MLPrediction

from desflow import KafkaAdminConnector

TOPICS = ["sim-events", "sim-telemetry", "ml-predictions"]


# --8<-- [start:prediction]
def prediction(task_id, context, entity):
    """The finished event of a packed order: a prediction, published as the model."""
    return MLPrediction(task_id=entity.key, confidence=0.9)


# --8<-- [end:prediction]


def create_topics():
    """With a router, KafkaEgress creates no topics, so create them first."""
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    admin.create_topics([{"name": topic} for topic in TOPICS])
