"""DX-23: An SSL context and an MSK IAM token provider.

The original is the guide's own calls (docs/how-to/records-and-kafka/kafka-security.md):
`create_ssl_context` of `aiokafka.helpers`, whose result is the producer's `ssl_context`,
and a subclass of `aiokafka.abc.AbstractTokenProvider` that returns the token
`MSKAuthTokenProvider.generate_auth_token` gives. `check` builds the model and compares
the context its first sink holds with the one the guide makes from the same files (the
loaded CA certificates, the verify mode, the host name check and the minimum version),
and the token provider of its second sink with the guide's. The test writes the CA and
the key pair, and `conftest.py` stands in for `aws_msk_iam_sasl_signer`.
"""

import asyncio
import os
from pathlib import Path

from aiokafka.abc import AbstractTokenProvider
from aiokafka.helpers import create_ssl_context

from desflow import SimulationContext

HERE = Path(__file__).parent


def check(records):
    """The first sink's `ssl_context` is a context made as the guide makes it, from the
    certificate files, and the second sink's token provider gives the guide's token."""
    app = SimulationContext.from_yaml(HERE / "model.yaml")
    tls, msk = (p.producer_config for p in app._egress_providers)
    wanted = create_ssl_context(
        cafile=os.environ["KAFKA_CA_FILE"],
        certfile=os.environ["KAFKA_CERT_FILE"],
        keyfile=os.environ["KAFKA_KEY_FILE"],
        password="changeit",
    )
    made = tls["ssl_context"]
    assert type(made) is type(wanted)
    assert made.get_ca_certs() and made.get_ca_certs() == wanted.get_ca_certs()
    assert (made.verify_mode, made.check_hostname, made.minimum_version) == (
        wanted.verify_mode,
        wanted.check_hostname,
        wanted.minimum_version,
    )
    assert tls["security_protocol"] == "SSL"

    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    class MSKTokenProvider(AbstractTokenProvider):
        async def token(self):
            token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
            return token

    held = msk["sasl_oauth_token_provider"]
    assert isinstance(held, AbstractTokenProvider)
    assert asyncio.run(held.token()) == asyncio.run(MSKTokenProvider().token())
    assert asyncio.run(held.token()) == "token-for-us-east-1"
    assert (msk["security_protocol"], msk["sasl_mechanism"]) == (
        "SASL_SSL",
        "OAUTHBEARER",
    )
    assert any(r["stream_type"] == "event" for r in records)
