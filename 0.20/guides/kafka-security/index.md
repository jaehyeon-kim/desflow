# Connecting to Secure Kafka Clusters

The Kafka connectors pass any keyword argument they do not use themselves to the Kafka client underneath, so SASL, mTLS, OAuth and AWS IAM work without code of your own. `KafkaIngress` and `KafkaEgress` use `aiokafka`. `KafkaAdminConnector` uses `kafka-python` for `create_topics` and `aiokafka` for `send_config` and `collect_data`, so its arguments must suit the library each method uses.

Every example here connects to a managed or secured cluster, so no local container is involved. To try the connectors locally without security, start a broker with `odctl up kafka-lite` and see [Installation](https://jaehyeon.me/desflow/latest/getting-started/index.md).

______________________________________________________________________

## Confluent Cloud (SASL PLAIN)

For Confluent Cloud, or any cluster that uses SASL PLAIN or SCRAM, pass `security_protocol`, `sasl_mechanism` and the credentials:

docs/snippets/pages/kafka_security.py:sasl

```python
egress = KafkaEgress(
    bootstrap_servers="pkc-xxxx.us-east-1.aws.confluent.cloud:9092",
    topic_router=my_router,
    # Passed as they are to the Kafka client
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
    sasl_plain_username="<YOUR_API_KEY>",
    sasl_plain_password="<YOUR_API_SECRET>",
)
```

______________________________________________________________________

## On-Premise Cluster (mTLS)

For a cluster that requires mutual TLS, build an SSL context from your certificate files. `aiokafka` takes only `ssl_context`, not the certificate paths, and `kafka-python` accepts the same context, so one context serves every connector.

docs/snippets/pages/kafka_security.py:mtls

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

docs/snippets/pages/kafka_security.py:msk

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
