"""The serializers, router and order readers of the JSON Schema and Protobuf page.

Every form of the shop imports the serializers and the router from this file:
json_schema_and_protobuf.py (builder), json_schema_and_protobuf.yaml and
json_schema_and_protobuf_low_level.py. It builds no simulation. shop_record.proto and
shop_record_pb2.py, generated from it, must be beside this file. It needs a Kafka broker
and a schema registry: start both with `odctl up kafka-lite`.

After a shop has run, `python json_schema_and_protobuf_serializers.py` reads the first
order of each order topic back.
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
    MessageDeserializer,
)

# A consumer of the order topics reads each one with the deserializer of its format.
order_readers: dict[str, MessageDeserializer] = {
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


# --8<-- [start:check]
import asyncio

from aiokafka import AIOKafkaConsumer


async def show_first_order(topic: str) -> None:
    """Prints the first order of a topic, decoded by the reader of its format."""
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers="127.0.0.1:9092", auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=10)
        print(topic, read_order(topic, message.value))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    for topic in order_readers:
        asyncio.run(show_first_order(topic))
# --8<-- [end:check]
