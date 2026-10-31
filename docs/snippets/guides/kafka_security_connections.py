"""The secured Kafka connections of the Connect to Secure Kafka page.

The Builder and Low-level forms of the shop import the SASL egress from this file:
kafka_security.py (builder) and kafka_security_low_level.py use it directly, and
kafka_security.yaml sets the same keys in its `config`. It builds no simulation. Set
KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET before importing it. The mTLS
and MSK examples read certificate files or call AWS, so they sit in functions this file
defines and does not call.

After a shop has run, `python kafka_security_connections.py` prints the first events of
sim-events.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

# --8<-- [start:sasl]
import os
from typing import Any, Dict

from desflow import KafkaEgress

# The address and the credentials come from the environment, so no secret is in the file
connection: Dict[str, Any] = dict(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    # Passed as they are to the Kafka client
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
    sasl_plain_username=os.environ["KAFKA_API_KEY"],
    sasl_plain_password=os.environ["KAFKA_API_SECRET"],
)
egress = KafkaEgress(**connection)
# --8<-- [end:sasl]


def mtls_ingress():
    # --8<-- [start:mtls]
    from aiokafka.helpers import create_ssl_context

    from desflow import KafkaIngress

    ssl_context = create_ssl_context(
        cafile="/path/to/ca.pem",
        certfile="/path/to/service.cert",
        keyfile="/path/to/service.key",
    )

    ingress = KafkaIngress(
        topic="sim-commands",
        bootstrap_servers="secure-broker.internal.company.com:9093",
        security_protocol="SSL",
        ssl_context=ssl_context,
    )
    # --8<-- [end:mtls]
    return ingress


def msk_connectors():
    # --8<-- [start:msk]
    from aiokafka.abc import AbstractTokenProvider
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    from desflow import KafkaAdminConnector, KafkaEgress

    class MSKTokenProvider(AbstractTokenProvider):
        async def token(self):
            # Uses the AWS credentials of your environment, or the EC2 or EKS role
            token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
            return token

    msk = dict(
        bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
    )

    # KafkaIngress, KafkaEgress and KafkaAdminConnector all use aiokafka
    egress = KafkaEgress(**msk, sasl_oauth_token_provider=MSKTokenProvider())

    admin = KafkaAdminConnector(**msk, sasl_oauth_token_provider=MSKTokenProvider())
    admin.create_topics([{"name": "sim-events", "partitions": 3}])
    # --8<-- [end:msk]
    return egress, admin


# --8<-- [start:check]
import asyncio

from aiokafka import AIOKafkaConsumer


async def show_events(count: int = 3) -> None:
    """Prints the first events of sim-events, read with the settings of the egress."""
    consumer = AIOKafkaConsumer(
        "sim-events", **connection, auto_offset_reset="earliest"
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
