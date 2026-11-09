"""Imports and constructs what the `glue` extra exists to enable, installed alone."""

import json

from desflow.connectors.egress.kafka import GlueAvroSerializer
from desflow.connectors.ingress.kafka import GlueAvroDeserializer

schema = json.dumps(
    {
        "type": "record",
        "name": "Ping",
        "fields": [{"name": "value", "type": "double"}],
    }
)
GlueAvroSerializer(registry_name="sim-registry", schema_str=schema)
GlueAvroDeserializer(registry_name="sim-registry")

# The JSON Schema and Protobuf classes need fastjsonschema and protobuf
from desflow.connectors.egress.kafka import GlueJsonSchemaSerializer
from desflow.connectors.ingress.kafka import GlueProtobufDeserializer
from google.protobuf import struct_pb2

GlueJsonSchemaSerializer("sim-registry", '{"type": "object"}')
GlueProtobufDeserializer("sim-registry", struct_pb2.Struct)

# desflow speaks to Glue itself, so the extra brings no Glue client library
import importlib.util

assert importlib.util.find_spec("aws_schema_registry") is None
