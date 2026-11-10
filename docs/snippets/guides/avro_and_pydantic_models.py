"""The Pydantic models and the Avro serializer of the Use Avro and Pydantic page.

Every code form of the shop imports the models and the serializer from this file:
avro_and_pydantic.py (builder) and avro_and_pydantic_low_level.py. It builds no
simulation. Install `dataclasses-avroschema[pydantic]` for the models.

`python avro_and_pydantic_models.py` prints the Avro schema of the models, which is the
file prediction_event.avsc of the blueprint.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

import json


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


# --8<-- [start:serializer]
from desflow.connectors.egress.kafka import ConfluentAvroSerializer

generated_serializer = ConfluentAvroSerializer(
    registry_url="http://127.0.0.1:8081",
    schema_str=PredictionRecord.avro_schema(),  # follows the model
)
# --8<-- [end:serializer]


if __name__ == "__main__":
    print(json.dumps(json.loads(schema_string), indent=2))
