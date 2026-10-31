"""The Registry Connection section of the Use Avro and Pydantic page: a serializer for a
schema registry behind HTTPS and basic authentication.

It needs the models of avro_and_pydantic_models.py beside it and builds the serializer when
it is imported, so no shop imports it.
"""

# ruff: noqa: E402

from avro_and_pydantic_models import PredictionRecord

# --8<-- [start:secured-registry]
import ssl

import certifi

from desflow.connectors.egress.kafka import ConfluentAvroSerializer

# A registry behind HTTPS and basic authentication, where each step of a request may take 30 seconds.
secured_serializer = ConfluentAvroSerializer(
    registry_url="https://registry.example.com:8081",
    schema_str=PredictionRecord.avro_schema(),
    auth=("registry-user", "registry-password"),
    # a private CA: cafile="ca.pem"
    verify=ssl.create_default_context(cafile=certifi.where()),
    timeout=30,
)
# --8<-- [end:secured-registry]
