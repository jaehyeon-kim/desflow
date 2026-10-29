"""The serializers, router and order readers of the JSON Schema and Protobuf page.

The shop at the end publishes each packed order as JSON Schema or Protobuf and every other
record as JSON. It needs a Kafka broker and a schema registry: start both with
`odctl up kafka-lite`, then run `python json_schema_and_protobuf.py`.
shop_record.proto and shop_record_pb2.py, generated from it, must be beside this file.
json_schema_and_protobuf.yaml and json_schema_and_protobuf_low_level.py are the same shop in
YAML and the low-level API, and use the serializers and router of this file.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

# --8<-- [start:json-schema]
from desflow.connectors.egress.kafka import ConfluentJsonSchemaSerializer

# The schema describes the whole record KafkaEgress sends, with the order as its value.
json_schema = """{
  "type": "object",
  "properties": {
    "key": {"type": "string"},
    "task": {"type": ["string", "null"]},
    "instance": {"type": ["integer", "null"]},
    "sim_ts": {"type": "number"},
    "timestamp": {"type": "string"},
    "value": {
      "type": "object",
      "properties": {
        "order_id": {"type": "integer"},
        "amount": {"type": "number"}
      },
      "required": ["order_id", "amount"]
    }
  },
  "required": ["key", "sim_ts", "timestamp", "value"]
}"""

json_serializer = ConfluentJsonSchemaSerializer(
    registry_url="http://127.0.0.1:8081", schema_str=json_schema
)
# --8<-- [end:json-schema]


# --8<-- [start:protobuf]
from shop_record_pb2 import ShopRecord

from desflow.connectors.egress.kafka import ConfluentProtobufSerializer

# message_type is the class that protoc generated. The serializer registers its file.
protobuf_serializer = ConfluentProtobufSerializer(
    registry_url="http://127.0.0.1:8081", message_type=ShopRecord
)
# --8<-- [end:protobuf]


# --8<-- [start:glue]
def glue_serializers():
    """The same two formats for an AWS Glue Schema Registry; they need `desflow[glue]`."""
    from pathlib import Path

    from desflow.connectors.egress.kafka import (
        GlueJsonSchemaSerializer,
        GlueProtobufSerializer,
    )

    glue_json = GlueJsonSchemaSerializer(
        registry_name="shop", schema_str=json_schema, region_name="us-east-1"
    )
    # Glue stores the text of the .proto file, so the serializer takes it as schema_str.
    # It must declare the same messages as message_type.
    glue_protobuf = GlueProtobufSerializer(
        registry_name="shop",
        message_type=ShopRecord,
        schema_str=Path(__file__).with_name("shop_record.proto").read_text(),
        region_name="us-east-1",
    )
    return glue_json, glue_protobuf


# --8<-- [end:glue]


# --8<-- [start:router]
from desflow import KafkaEgress


# A topic router that sends orders to their own topics.
def order_topic_router(data: dict) -> str:
    if data.get("stream_type") == "telemetry":
        return "sim-telemetry"
    if "order_id" in data.get("value", {}):
        # Orders alternate between the two formats, so one run shows both
        return "orders-protobuf" if data["instance"] % 2 else "orders-json"
    return "sim-events"


egress = KafkaEgress(
    bootstrap_servers="127.0.0.1:9092",
    topic_router=order_topic_router,
    # Orders are JSON Schema or Protobuf; sim-telemetry and sim-events use JSON.
    topic_serializers={
        "orders-json": json_serializer,
        "orders-protobuf": protobuf_serializer,
    },
)
# --8<-- [end:router]


# --8<-- [start:read-back]
from desflow.connectors.ingress.kafka import (
    ConfluentJsonSchemaDeserializer,
    ConfluentProtobufDeserializer,
)

# A consumer of the order topics reads each one with the deserializer of its format.
order_readers = {
    "orders-json": ConfluentJsonSchemaDeserializer(
        registry_url="http://127.0.0.1:8081"
    ),
    "orders-protobuf": ConfluentProtobufDeserializer(
        registry_url="http://127.0.0.1:8081", message_type=ShopRecord
    ),
}


def read_order(topic: str, payload: bytes) -> dict:
    """Decodes one message of an order topic into the record that was sent."""
    return order_readers[topic].deserialize(topic, payload)


# --8<-- [end:read-back]


# --8<-- [start:shop]
import logging

from desflow import KafkaAdminConnector, SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(egress)  # orders as JSON Schema or Protobuf, the rest as JSON
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, entity):
    # The finished event is the order record
    return {"order_id": task_id, "amount": 19.99}


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    # With a router, KafkaEgress creates no topics, so create them first
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    topics = ["sim-events", "sim-telemetry", "orders-json", "orders-protobuf"]
    admin.create_topics([{"name": topic} for topic in topics])
    app.run(until="1 min")
# --8<-- [end:shop]
