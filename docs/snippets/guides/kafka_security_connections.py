"""The secured Kafka connections of the Connect to Secure Kafka page.

The Builder and Low-level forms of the shop import their egress from this file:
kafka_security.py (builder) and kafka_security_low_level.py use it directly, and the YAML
files set the same keys in the `config` of their sink. It builds no simulation. Each
function builds the egress of one kind of cluster, and `selected_egress()` picks one by the
variable KAFKA_CONNECTION: sasl (the default), mtls or msk. The SASL egress reads
KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET, the mTLS egress reads
KAFKA_BOOTSTRAP_SERVERS, KAFKA_CA_FILE, KAFKA_CERT_FILE and KAFKA_KEY_FILE, and the MSK
egress calls AWS when the sink starts.

`registry_egress()` needs avro_and_pydantic_models.py of Use Avro and Pydantic beside this
file, and the registry variables of kafka_security_registry.yaml.

After a shop has run with the SASL egress, `python kafka_security_connections.py` prints
the first events of sim-events.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402, F811

# --8<-- [start:sasl]
import os
from typing import Any, Dict

from desflow import KafkaEgress


def sasl_connection() -> Dict[str, Any]:
    """The settings of a cluster that uses SASL. The environment holds the credentials."""
    return dict(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        # Passed as they are to the Kafka client
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
        sasl_plain_username=os.environ["KAFKA_API_KEY"],
        sasl_plain_password=os.environ["KAFKA_API_SECRET"],
    )


def sasl_egress() -> KafkaEgress:
    """The egress of a cluster that uses SASL."""
    return KafkaEgress(**sasl_connection())


# --8<-- [end:sasl]


# --8<-- [start:mtls]
import os


def mtls_egress() -> KafkaEgress:
    """The egress of a cluster that requires mutual TLS."""
    return KafkaEgress(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        security_protocol="SSL",
        # The arguments of aiokafka.helpers.create_ssl_context
        ssl={
            "cafile": os.environ["KAFKA_CA_FILE"],
            "certfile": os.environ["KAFKA_CERT_FILE"],
            "keyfile": os.environ["KAFKA_KEY_FILE"],
        },
    )


# --8<-- [end:mtls]


# --8<-- [start:msk]
def msk_egress() -> KafkaEgress:
    """The egress of an Amazon MSK cluster with IAM access control."""
    return KafkaEgress(
        bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        # The token comes from the AWS credentials of your environment or role
        sasl_oauth_token_provider={"type": "aws_msk_iam", "region": "us-east-1"},
    )


# --8<-- [end:msk]


# --8<-- [start:registry]
import os
import ssl

from avro_and_pydantic_models import PredictionRecord

from desflow import KafkaEgress
from desflow.connectors.egress.kafka import ConfluentAvroSerializer


def registry_egress() -> KafkaEgress:
    """The egress of the Avro shop, with a serializer for a secured registry."""
    serializer = ConfluentAvroSerializer(
        registry_url=os.environ["SCHEMA_REGISTRY_URL"],
        schema_str=PredictionRecord.avro_schema(),
        auth=(os.environ["REGISTRY_USER"], os.environ["REGISTRY_PASSWORD"]),
        # True, False, a CA file or directory, or an ssl.SSLContext
        verify=ssl.create_default_context(cafile=os.environ["REGISTRY_CA_FILE"]),
        # A client certificate file and its key file
        cert=(os.environ["REGISTRY_CERT_FILE"], os.environ["REGISTRY_KEY_FILE"]),
        headers={"X-Team": "shop"},  # added to every request
        proxy="http://proxy.example.com:3128",
        timeout=30,  # seconds for each step of a request
        max_retries=5,
    )
    return KafkaEgress(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        topics={"ml-predictions": {"serializer": serializer}},
    )


# --8<-- [end:registry]


# --8<-- [start:select]
import os


def selected_egress() -> KafkaEgress:
    """The egress that KAFKA_CONNECTION names: sasl (the default), mtls or msk."""
    makers = {"sasl": sasl_egress, "mtls": mtls_egress, "msk": msk_egress}
    return makers[os.environ.get("KAFKA_CONNECTION", "sasl")]()


# --8<-- [end:select]


# --8<-- [start:check]
import asyncio

from aiokafka import AIOKafkaConsumer


async def show_events(count: int = 3) -> None:
    """Prints the first events of sim-events, read with the settings of the SASL egress."""
    consumer = AIOKafkaConsumer(
        "sim-events", **sasl_connection(), auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        for _ in range(count):
            message = await asyncio.wait_for(consumer.getone(), timeout=10)
            print(message.value.decode())
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(show_events())
# --8<-- [end:check]
