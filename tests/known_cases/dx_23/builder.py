"""DX-23: An SSL context and an MSK IAM token provider.

Builder form of `model.yaml`.
"""

import os
from datetime import datetime, timezone

from desflow import KafkaEgress, SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("packing", dist="constant", value=1.5)
    app.add_arrival("orders", "constant", value=2, spawn="pack_order")
    app.add_egress(
        KafkaEgress(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9093"
            ),
            security_protocol="SSL",
            ssl={
                "cafile": os.environ["KAFKA_CA_FILE"],
                "certfile": os.environ["KAFKA_CERT_FILE"],
                "keyfile": os.environ["KAFKA_KEY_FILE"],
                "password": os.environ.get("KAFKA_KEY_PASSWORD", "changeit"),
            },
        )
    )
    app.add_egress(
        KafkaEgress(
            bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider={"type": "aws_msk_iam", "region": "us-east-1"},
        )
    )

    @app.task(name="pack_order", service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)
