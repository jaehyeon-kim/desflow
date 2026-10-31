"""The Pydantic models, topic router and Avro serializers of the Use Avro and Pydantic page.

Every form of the shop imports the models, the router and the serializers from this file:
avro_and_pydantic.py (builder), avro_and_pydantic.yaml and avro_and_pydantic_low_level.py.
It builds no simulation. Install `dataclasses-avroschema[pydantic]` for the models.

After a shop has run, `python avro_and_pydantic_models.py` prints the first prediction
of ml-predictions.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

# --8<-- [start:router]
from desflow import KafkaEgress
from desflow.connectors.egress.kafka import ConfluentAvroSerializer

# An Avro serializer for the predictions topic. The schema describes the whole record
# KafkaEgress sends, with the prediction as its value.
prediction_serializer = ConfluentAvroSerializer(
    registry_url="http://127.0.0.1:8081",
    schema_str="""{
      "type": "record", "name": "PredictionEvent", "namespace": "com.desflow.ml",
      "fields": [
        {"name": "key", "type": "string"},
        {"name": "task", "type": ["null", "string"], "default": null},
        {"name": "instance", "type": ["null", "long"], "default": null},
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
    bootstrap_servers="127.0.0.1:9092",
    topic_router=ml_topic_router,
    # Only ml-predictions is Avro; sim-telemetry and sim-events use JSON.
    topic_serializers={"ml-predictions": prediction_serializer},
)
# --8<-- [end:router]


# --8<-- [start:schema]
from typing import Optional

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
    task: Optional[str] = None
    instance: Optional[int] = None
    sim_ts: float
    timestamp: str
    value: MLPrediction

    class Meta:
        namespace = "com.desflow.ml"
        schema_name = "PredictionEvent"


schema_string = PredictionRecord.avro_schema()
# --8<-- [end:schema]


# --8<-- [start:generated]
generated_serializer = ConfluentAvroSerializer(
    registry_url="http://127.0.0.1:8081",
    schema_str=PredictionRecord.avro_schema(),  # follows the model
)

egress = KafkaEgress(
    bootstrap_servers="127.0.0.1:9092",
    topic_router=ml_topic_router,
    topic_serializers={"ml-predictions": generated_serializer},
)
# --8<-- [end:generated]


# --8<-- [start:check]
import asyncio

from aiokafka import AIOKafkaConsumer

from desflow.connectors.ingress.kafka import ConfluentAvroDeserializer


async def show_first_prediction() -> None:
    """Prints the first message of ml-predictions, decoded with the schema it names."""
    consumer = AIOKafkaConsumer(
        "ml-predictions",
        bootstrap_servers="127.0.0.1:9092",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=10)
        reader = ConfluentAvroDeserializer(registry_url="http://127.0.0.1:8081")
        print(reader.deserialize("ml-predictions", message.value))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(show_first_prediction())
# --8<-- [end:check]
