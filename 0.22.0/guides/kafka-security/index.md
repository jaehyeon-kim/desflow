# Connecting to Secure Kafka Clusters

**Level:** Advanced

The Kafka connectors pass any keyword argument they do not use themselves to the Kafka client underneath, so SASL, mTLS, OAuth and AWS IAM work without code of your own. `KafkaIngress` and `KafkaEgress` use `aiokafka`. `KafkaAdminConnector` uses `kafka-python` for `create_topics` and `aiokafka` for `send_config` and `collect_data`, so its arguments must suit the library each method uses.

Every example here connects to a managed or secured cluster. No odctl profile runs a secured broker, so the full script at the end needs a cluster of your own that uses SASL PLAIN, such as Confluent Cloud. To try the connectors locally without security, start a broker with `odctl up kafka-lite` and see [Installation](https://jaehyeon.me/desflow/latest/getting-started/index.md).

______________________________________________________________________

## Confluent Cloud (SASL PLAIN)

For Confluent Cloud, or any cluster that uses SASL PLAIN or SCRAM, pass `security_protocol`, `sasl_mechanism` and the credentials:

```python
egress = KafkaEgress(
    bootstrap_servers="pkc-xxxx.us-east-1.aws.confluent.cloud:9092",
    # Passed as they are to the Kafka client
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
    sasl_plain_username="<YOUR_API_KEY>",
    sasl_plain_password="<YOUR_API_SECRET>",
)
```

With no `topic_router`, `KafkaEgress` creates `sim-events` and `sim-telemetry` when it starts, and passes the security settings to the admin client that creates them.

______________________________________________________________________

## On-Premise Cluster (mTLS)

For a cluster that requires mutual TLS, build an SSL context from your certificate files. `aiokafka` takes only `ssl_context`, not the certificate paths, and `kafka-python` accepts the same context, so one context serves every connector.

```python
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
```

______________________________________________________________________

## AWS MSK (IAM)

Amazon MSK with IAM access control uses `sasl_mechanism="OAUTHBEARER"` with a token provider. The token comes from the `aws-msk-iam-sasl-signer-python` package, which you install yourself.

Each library checks the provider's type. `aiokafka` requires a subclass of `aiokafka.abc.AbstractTokenProvider` with an `async` `token()`. `kafka-python` requires a subclass of `kafka.sasl.oauth.AbstractTokenProvider` with a plain `token()`. One object cannot be both, so the connectors that use `aiokafka` get one provider, and `create_topics` gets the other.

```python
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
```

The same `OAUTHBEARER` setting works with a token provider of your own for Okta, Auth0 or another single sign-on provider.

______________________________________________________________________

## Use It in a Simulation

A connector with security settings is added like any other. This simulation publishes its events through the SASL egress above:

```python
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
```

Put your broker address and credentials in `egress`, then run `python kafka_security.py`. It runs for one minute.

______________________________________________________________________

## Full Script

Full script

The complete file. It needs a Kafka cluster that uses SASL PLAIN:

```python
"""The secured Kafka connections of the Secure Kafka page, and a shop that uses one.

The simulation publishes to a cluster that uses SASL PLAIN, such as Confluent Cloud: put your
broker address and credentials in the `sasl` section, then run `python
kafka_security.py`. The mTLS and MSK examples read certificate files or call AWS, so
they sit in functions this file defines and does not call.
"""

import logging

from desflow import KafkaEgress, SimulationContext


def my_router(data: dict) -> str:
    return "sim-telemetry" if data.get("stream_type") == "telemetry" else "sim-events"


egress = KafkaEgress(
    bootstrap_servers="pkc-xxxx.us-east-1.aws.confluent.cloud:9092",
    # Passed as they are to the Kafka client
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
    sasl_plain_username="<YOUR_API_KEY>",
    sasl_plain_password="<YOUR_API_SECRET>",
)


def mtls_ingress():
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
    return ingress


def msk_connectors():
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
    return egress, admin


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
```
