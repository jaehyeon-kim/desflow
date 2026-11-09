"""Imports and constructs what the `confluent` extra exists to enable, installed alone."""

import importlib.util
import json

from desflow.connectors.egress.kafka import ConfluentAvroSerializer
from desflow.connectors.ingress.kafka import ConfluentAvroDeserializer

schema = json.dumps(
    {
        "type": "record",
        "name": "Ping",
        "fields": [{"name": "value", "type": "double"}],
    }
)
ConfluentAvroSerializer(registry_url="http://localhost:8081", schema_str=schema)
ConfluentAvroDeserializer(registry_url="http://localhost:8081")

# The JSON Schema and Protobuf classes need jsonschema and protobuf
from desflow.connectors.egress.kafka import ConfluentJsonSchemaSerializer
from desflow.connectors.ingress.kafka import ConfluentProtobufDeserializer
from google.protobuf import struct_pb2

ConfluentJsonSchemaSerializer(
    registry_url="http://localhost:8081", schema_str='{"type": "object"}'
)
ConfluentProtobufDeserializer("http://localhost:8081", struct_pb2.Struct)

# desflow writes the wire format itself, so the extra brings no confluent-kafka
assert importlib.util.find_spec("confluent_kafka") is None
