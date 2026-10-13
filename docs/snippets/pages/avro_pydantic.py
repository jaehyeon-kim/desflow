"""The Pydantic models, topic router and Avro serializers of the Avro and Pydantic page.

Building the serializers and the egress opens no connection, so this file runs without
a broker or a schema registry.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

# --8<-- [start:model]
from pydantic import BaseModel

from dynamic_des import DynamicRealtimeEnvironment


class TaskEvent(BaseModel):
    path_id: str
    status: str


def work_task(env: DynamicRealtimeEnvironment, task_id: int):
    # The model is passed as it is; publish_event turns it into a dict.
    event = TaskEvent(path_id="Shop.packers", status="queued")
    env.publish_event(f"task-{task_id}", event)
    yield env.timeout(0)


# --8<-- [end:model]


# --8<-- [start:router]
from dynamic_des import KafkaEgress
from dynamic_des.connectors.egress.kafka import ConfluentAvroSerializer

# An Avro serializer for the predictions topic. The schema describes the whole record
# KafkaEgress sends, with the prediction as its value.
prediction_serializer = ConfluentAvroSerializer(
    registry_url="http://127.0.0.1:8081",
    schema_str="""{
      "type": "record", "name": "PredictionEvent", "namespace": "com.dynamic_des.ml",
      "fields": [
        {"name": "key", "type": "string"},
        {"name": "sim_ts", "type": "double"},
        {"name": "timestamp", "type": "string"},
        {"name": "value", "type": {
          "type": "record", "name": "MLPrediction",
          "fields": [
            {"name": "event_type", "type": "string"},
            {"name": "task_id", "type": "string"},
            {"name": "confidence", "type": "double"}
          ]
        }}
      ]
    }""",
)


# A topic router that sends predictions to their own topic.
def ml_topic_router(data: dict) -> str:
    if data.get("stream_type") == "telemetry":
        return "sim-telemetry"
    value = data.get("value", {})
    if value.get("event_type") == "prediction":
        return "ml-predictions"
    return "sim-events"


egress = KafkaEgress(
    bootstrap_servers="localhost:9092",
    topic_router=ml_topic_router,
    # Only ml-predictions is Avro; sim-telemetry and sim-events use JSON.
    topic_serializers={"ml-predictions": prediction_serializer},
)
# --8<-- [end:router]


# --8<-- [start:schema]
from dataclasses_avroschema.pydantic import AvroBaseModel
from pydantic import Field


class MLPrediction(AvroBaseModel):
    """The prediction, published as the event value."""

    event_type: str = "prediction"
    task_id: str = Field(...)
    confidence: float = Field(...)


class PredictionRecord(AvroBaseModel):
    """The record KafkaEgress serializes for an event."""

    key: str
    sim_ts: float
    timestamp: str
    value: MLPrediction

    class Meta:
        namespace = "com.dynamic_des.ml"
        schema_name = "PredictionEvent"


schema_string = PredictionRecord.avro_schema()
# --8<-- [end:schema]


# --8<-- [start:generated]
generated_serializer = ConfluentAvroSerializer(
    registry_url="http://127.0.0.1:8081",
    schema_str=PredictionRecord.avro_schema(),  # follows the model
)

egress = KafkaEgress(
    bootstrap_servers="localhost:9092",
    topic_router=ml_topic_router,
    topic_serializers={"ml-predictions": generated_serializer},
)
# --8<-- [end:generated]


if __name__ == "__main__":
    import json

    schema = json.loads(schema_string)
    assert [f["name"] for f in schema["fields"]] == [
        "key",
        "sim_ts",
        "timestamp",
        "value",
    ]
    assert (
        ml_topic_router({"stream_type": "event", "value": {"event_type": "prediction"}})
        == "ml-predictions"
    )
    assert ml_topic_router({"stream_type": "telemetry"}) == "sim-telemetry"

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.process(work_task(env, 1))
    env.run(until=1)
