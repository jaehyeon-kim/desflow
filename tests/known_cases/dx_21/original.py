"""DX-21: Kafka serializers for Avro, JSON Schema and Protobuf.

The original is the snippets' own objects: `protobuf_serializer` and `json_serializer` of
docs/snippets/guides/json_schema_and_protobuf_serializers.py, built from the class `protoc`
generated and from the schema text, and `prediction_serializer` below, an Avro serializer
built from a schema written by hand. `check` builds the model and compares each serializer
of its three sinks with the original: the same class and the same schema text, and the
bytes one record encodes to, which need no registry (`serialize` registers the schema
first, so the encoders are called without it).
"""

import sys
from pathlib import Path

from desflow import SimulationContext
from desflow.connectors.egress.kafka import ConfluentAvroSerializer

HERE = Path(__file__).parent
SNIPPETS = HERE.parents[2] / "docs" / "snippets" / "guides"

RECORD = {
    "key": "orders-1",
    "task": "pack_order",
    "instance": 0,
    "sim_ts": 3.5,
    "timestamp": "2026-01-01T00:00:03.500000+00:00",
}

# An Avro serializer with a schema written by hand. The schema describes the whole record
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

ORDER = {**RECORD, "value": {"order_id": 1, "amount": 19.99}}
PREDICTION = {
    **RECORD,
    "value": {"event_type": "prediction", "task_id": "0", "confidence": 0.9},
}


def check(records):
    """The serializer of each sink is the snippet's: its class, its schema and the bytes
    of one record."""
    sys.path.insert(0, str(SNIPPETS))
    try:
        from json_schema_and_protobuf_serializers import (
            json_serializer,
            protobuf_serializer,
        )
    finally:
        sys.path.remove(str(SNIPPETS))
    avro_serializer = prediction_serializer
    app = SimulationContext.from_yaml(HERE / "model.yaml")
    protobuf, avro, json = (
        sink.provider.default_serializer for sink in app._sinks.egress
    )

    assert type(protobuf) is type(protobuf_serializer)
    assert protobuf._schema_str == protobuf_serializer._schema_str
    assert protobuf._message_type.DESCRIPTOR.full_name == (
        protobuf_serializer._message_type.DESCRIPTOR.full_name
    )
    assert protobuf._message_type.DESCRIPTOR.fields_by_name.keys() == (
        protobuf_serializer._message_type.DESCRIPTOR.fields_by_name.keys()
    )
    assert protobuf._indexes == protobuf_serializer._indexes
    made = protobuf._protobuf.encode(protobuf._message_type, ORDER)
    assert made and made == protobuf._protobuf.encode(
        protobuf_serializer._message_type, ORDER
    )

    assert type(avro) is type(avro_serializer)
    assert avro._schema_str == avro_serializer._schema_str
    made = avro._avro.encode(avro._parsed, PREDICTION)
    assert made and made == avro_serializer._avro.encode(
        avro_serializer._parsed, PREDICTION
    )

    assert type(json) is type(json_serializer)
    assert json._schema_str.strip() == json_serializer._schema_str.strip()
    made = json._json_schema.encode(json._validator, ORDER)
    assert made and made == json_serializer._json_schema.encode(
        json_serializer._validator, ORDER
    )

    assert any(r["stream_type"] == "event" for r in records)
