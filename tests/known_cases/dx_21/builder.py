"""DX-21: Kafka serializers for Avro, JSON Schema and Protobuf.

Builder form of `model.yaml`.
"""

import os
from datetime import datetime, timezone

from pathlib import Path

from desflow import KafkaEgress, SimulationContext

HERE = Path(__file__).parent


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
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    registry = "http://127.0.0.1:8081"
    for serializer in (
        {
            "type": "confluent_protobuf",
            "registry_url": registry,
            "descriptor_file": str(HERE / "shop_record.desc"),
            "message": "com.desflow.shop.ShopRecord",
        },
        {
            "type": "confluent_avro",
            "registry_url": registry,
            "schema_file": str(HERE / "prediction_event.avsc"),
        },
        {
            "type": "confluent_json_schema",
            "registry_url": registry,
            "schema_file": str(HERE / "order_record.schema.json"),
        },
    ):
        app.add_egress(KafkaEgress(bootstrap_servers=servers, serializer=serializer))

    @app.task(name="pack_order", service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)
