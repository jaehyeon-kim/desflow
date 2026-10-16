"""The secured Kafka connections of the Secure Kafka page, and a shop that uses one.

The shop publishes to a cluster that uses SASL PLAIN, such as Confluent Cloud: put your
broker address and credentials in the `sasl` section, then run `python
kafka_security.py`. The mTLS and MSK examples read certificate files or call AWS, so
they sit in functions this file defines and does not call.
"""

import logging

from desflow import KafkaEgress, SimulationContext


def my_router(data: dict) -> str:
    return "sim-telemetry" if data.get("stream_type") == "telemetry" else "sim-events"


# --8<-- [start:sasl]
egress = KafkaEgress(
    bootstrap_servers="pkc-xxxx.us-east-1.aws.confluent.cloud:9092",
    # Passed as they are to the Kafka client
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
    sasl_plain_username="<YOUR_API_KEY>",
    sasl_plain_password="<YOUR_API_SECRET>",
)
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
    from aiokafka.abc import AbstractTokenProvider as AsyncTokenProvider
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    from kafka.sasl.oauth import AbstractTokenProvider as SyncTokenProvider

    from desflow import KafkaAdminConnector, KafkaEgress

    def msk_token() -> str:
        # Uses the AWS credentials of your environment, or the EC2 or EKS role
        token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
        return token

    class AsyncMSKTokenProvider(AsyncTokenProvider):
        async def token(self):
            return msk_token()

    class SyncMSKTokenProvider(SyncTokenProvider):
        def token(self):
            return msk_token()

    msk = dict(
        bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
    )

    # KafkaIngress and KafkaEgress use aiokafka
    egress = KafkaEgress(
        **msk, topic_router=my_router, sasl_oauth_token_provider=AsyncMSKTokenProvider()
    )

    # create_topics uses kafka-python
    admin = KafkaAdminConnector(**msk, sasl_oauth_token_provider=SyncMSKTokenProvider())
    admin.create_topics([{"name": "sim-events", "partitions": 3}])
    # --8<-- [end:msk]
    return egress, admin


# --8<-- [start:shop]
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(egress)  # the SASL egress above
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 min")
# --8<-- [end:shop]
