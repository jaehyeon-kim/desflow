"""JSON Schema and Protobuf round trips against Karapace (Confluent) and Floci (Glue).

Like the Confluent Avro tests, they publish through `KafkaEgress`, read the raw bytes
from the topic, and decode them with the deserializer. A second test for each Confluent
format writes the bytes by hand, as a producer that is not desflow would, after
registering the schema through the registry's REST API. The Confluent Protobuf message
imports a second file, so registering its dependencies and reading their versions with
`lookup` are checked against Karapace.
"""

import asyncio
import json
import queue
import urllib.parse
import urllib.request
import uuid

import boto3
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from desflow.connectors._schema_registry import protobuf
from desflow.connectors.admin.kafka import KafkaAdminConnector
from desflow.connectors.egress.kafka import (
    ConfluentJsonSchemaSerializer,
    ConfluentProtobufSerializer,
    GlueJsonSchemaSerializer,
    GlueProtobufSerializer,
    KafkaEgress,
)
from desflow.connectors.ingress.kafka import (
    ConfluentJsonSchemaDeserializer,
    ConfluentProtobufDeserializer,
    GlueJsonSchemaDeserializer,
    GlueProtobufDeserializer,
)

pytestmark = pytest.mark.integration

# KafkaEgress publishes the whole record minus `stream_type`.
RECORD = {
    "stream_type": "event",
    "key": "order-1",
    "value": {"order_id": 1, "amount": 100.0},
}
DECODED = {"key": "order-1", "value": {"order_id": 1, "amount": 100.0}}

JSON_SCHEMA = json.dumps(
    {
        "type": "object",
        "properties": {
            "key": {"type": "string"},
            "value": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "integer"},
                    "amount": {"type": "number"},
                },
                "required": ["order_id", "amount"],
            },
        },
        "required": ["key", "value"],
    }
)

PROTO_TEXT = """\
syntax = "proto3";
package desflow.test;
message OrderEvent {
  string key = 1;
  OrderValue value = 2;
}
message OrderValue {
  int32 order_id = 1;
  double amount = 2;
}
"""


def _order_event(with_money: bool):
    """Builds the OrderEvent class without protoc.

    With `with_money` its file imports a second file, which Confluent registers as a
    reference. Glue stores one `.proto` text, so the Glue class has no import.
    """
    field = descriptor_pb2.FieldDescriptorProto
    pool = descriptor_pool.DescriptorPool()

    money = descriptor_pb2.FileDescriptorProto(
        name="desflow/test/money.proto", package="desflow.test", syntax="proto3"
    )
    money.message_type.add(name="Money").field.add(
        name="cents", number=1, type=field.TYPE_INT64, label=field.LABEL_OPTIONAL
    )
    if with_money:
        pool.AddSerializedFile(money.SerializeToString())

    order = descriptor_pb2.FileDescriptorProto(
        name="desflow/test/order.proto",
        package="desflow.test",
        syntax="proto3",
        dependency=["desflow/test/money.proto"] if with_money else [],
    )
    event = order.message_type.add(name="OrderEvent")
    event.field.add(
        name="key", number=1, type=field.TYPE_STRING, label=field.LABEL_OPTIONAL
    )
    event.field.add(
        name="value",
        number=2,
        type=field.TYPE_MESSAGE,
        type_name=".desflow.test.OrderValue",
        label=field.LABEL_OPTIONAL,
    )
    value = order.message_type.add(name="OrderValue")
    value.field.add(
        name="order_id", number=1, type=field.TYPE_INT32, label=field.LABEL_OPTIONAL
    )
    value.field.add(
        name="amount", number=2, type=field.TYPE_DOUBLE, label=field.LABEL_OPTIONAL
    )
    if with_money:
        value.field.add(
            name="total",
            number=3,
            type=field.TYPE_MESSAGE,
            type_name=".desflow.test.Money",
            label=field.LABEL_OPTIONAL,
        )
    pool.AddSerializedFile(order.SerializeToString())
    return message_factory.GetMessageClass(
        pool.FindMessageTypeByName("desflow.test.OrderEvent")
    )


ORDER_EVENT = _order_event(with_money=True)
GLUE_ORDER_EVENT = _order_event(with_money=False)


async def _create_topic(bootstrap_servers: str) -> str:
    topic = f"test-formats-{uuid.uuid4().hex[:8]}"
    KafkaAdminConnector(bootstrap_servers).create_topics(
        [{"name": topic, "partitions": 1}]
    )
    return topic


async def _first_message(bootstrap_servers: str, topic: str, start) -> bytes:
    """Starts a consumer, runs `start`, and returns the first message on the topic.

    The consumer starts before the producer, so it cannot miss the message, and the
    producer task stays alive until the consumer has it.
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=f"group-{topic}",
    )
    await consumer.start()
    task = asyncio.create_task(start())
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=30.0)
        return message.value
    finally:
        task.cancel()
        await consumer.stop()


async def _publish_with_egress(bootstrap_servers: str, topic: str, serializer) -> bytes:
    egress = KafkaEgress(
        bootstrap_servers, event_topic=topic, topic_serializers={topic: serializer}
    )
    q: queue.Queue = queue.Queue()
    q.put([RECORD])
    return await _first_message(bootstrap_servers, topic, lambda: egress.run(q))


async def _publish_bytes(bootstrap_servers: str, topic: str, payload: bytes) -> bytes:
    async def send():
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
        await producer.start()
        try:
            await producer.send_and_wait(topic, payload)
            await asyncio.sleep(60)
        finally:
            await producer.stop()

    return await _first_message(bootstrap_servers, topic, send)


def _registry_get(registry: str, path: str):
    with urllib.request.urlopen(f"{registry}/{path}", timeout=5) as response:
        return json.loads(response.read())


def _registry_register(registry: str, subject: str, body: dict) -> int:
    request = urllib.request.Request(
        f"{registry}/subjects/{urllib.parse.quote(subject, safe='')}/versions",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read())["id"]


@pytest.mark.services
@pytest.mark.asyncio
async def test_confluent_json_schema_round_trip(kafka_container, schema_registry):
    """The egress registers a JSON subject and frames the record; the deserializer reads it."""
    topic = await _create_topic(kafka_container)
    payload = await _publish_with_egress(
        kafka_container,
        topic,
        ConfluentJsonSchemaSerializer(schema_registry, JSON_SCHEMA),
    )

    assert payload[0] == 0
    schema_id = int.from_bytes(payload[1:5], "big")
    registered = _registry_get(schema_registry, f"schemas/ids/{schema_id}")
    assert registered["schemaType"] == "JSON"
    assert f"{topic}-value" in _registry_get(schema_registry, "subjects")
    assert json.loads(payload[5:]) == DECODED
    assert (
        ConfluentJsonSchemaDeserializer(schema_registry).deserialize(topic, payload)
        == DECODED
    )


@pytest.mark.services
@pytest.mark.asyncio
async def test_confluent_json_schema_reads_bytes_of_another_producer(
    kafka_container, schema_registry
):
    """A record framed by hand, after a registration over REST, is read."""
    topic = await _create_topic(kafka_container)
    schema_id = _registry_register(
        schema_registry,
        f"{topic}-value",
        {"schema": JSON_SCHEMA, "schemaType": "JSON"},
    )
    framed = b"\x00" + schema_id.to_bytes(4, "big") + json.dumps(DECODED).encode()
    payload = await _publish_bytes(kafka_container, topic, framed)

    assert (
        ConfluentJsonSchemaDeserializer(schema_registry).deserialize(topic, payload)
        == DECODED
    )


@pytest.mark.services
@pytest.mark.asyncio
async def test_confluent_protobuf_round_trip_registers_the_imported_file(
    kafka_container, schema_registry
):
    """The egress registers money.proto, then order.proto with a reference to it."""
    topic = await _create_topic(kafka_container)
    payload = await _publish_with_egress(
        kafka_container,
        topic,
        ConfluentProtobufSerializer(schema_registry, ORDER_EVENT),
    )

    assert payload[0] == 0
    assert payload[5] == 0  # the index [0]: OrderEvent is the first message
    schema_id = int.from_bytes(payload[1:5], "big")
    registered = _registry_get(schema_registry, f"schemas/ids/{schema_id}")
    assert registered["schemaType"] == "PROTOBUF"
    assert registered["references"] == [
        {
            "name": "desflow/test/money.proto",
            "subject": "desflow/test/money.proto",
            "version": 1,
        }
    ]
    assert ORDER_EVENT.FromString(payload[6:]).value.order_id == 1
    assert (
        ConfluentProtobufDeserializer(schema_registry, ORDER_EVENT).deserialize(
            topic, payload
        )
        == DECODED
    )


@pytest.mark.services
@pytest.mark.asyncio
async def test_confluent_protobuf_reads_bytes_of_another_producer(
    kafka_container, schema_registry
):
    """A message framed by hand, with the index of the second message, is read."""
    topic = await _create_topic(kafka_container)
    money = ORDER_EVENT.DESCRIPTOR.file.dependencies[0]
    _registry_register(
        schema_registry,
        "desflow/test/money.proto",
        {"schema": protobuf.file_descriptor_b64(money), "schemaType": "PROTOBUF"},
    )
    schema_id = _registry_register(
        schema_registry,
        f"{topic}-value",
        {
            "schema": protobuf.file_descriptor_b64(ORDER_EVENT.DESCRIPTOR.file),
            "schemaType": "PROTOBUF",
            "references": [
                {
                    "name": "desflow/test/money.proto",
                    "subject": "desflow/test/money.proto",
                    "version": 1,
                }
            ],
        },
    )
    order_value = ORDER_EVENT.DESCRIPTOR.file.message_types_by_name["OrderValue"]
    value_class = message_factory.GetMessageClass(order_value)
    # [1] is OrderValue, the second message of the file: length 1, then position 1
    framed = (
        b"\x00"
        + schema_id.to_bytes(4, "big")
        + bytes([0x02, 0x02])
        + value_class(order_id=2, amount=5.5).SerializeToString()
    )
    payload = await _publish_bytes(kafka_container, topic, framed)

    assert ConfluentProtobufDeserializer(schema_registry, value_class).deserialize(
        topic, payload
    ) == {"order_id": 2, "amount": 5.5}


def _floci_settings(floci: str) -> dict:
    return {
        "endpoint_url": floci,
        "region_name": "us-east-1",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }


def _glue_registry(floci: str) -> tuple[str, dict]:
    registry = f"desflow-{uuid.uuid4().hex[:8]}"
    settings = _floci_settings(floci)
    boto3.client("glue", **settings).create_registry(RegistryName=registry)
    return registry, settings


@pytest.mark.floci
def test_glue_json_schema_round_trip(floci):
    """The serializer registers a JSON schema on Floci, and the deserializer reads the record."""
    registry, settings = _glue_registry(floci)
    serializer = GlueJsonSchemaSerializer(registry, JSON_SCHEMA, **settings)

    framed = serializer.serialize("events", DECODED)

    assert framed[:2] == b"\x03\x00"
    version = boto3.client("glue", **settings).get_schema_version(
        SchemaVersionId=str(uuid.UUID(bytes=framed[2:18]))
    )
    assert version["DataFormat"] == "JSON"
    assert json.loads(version["SchemaDefinition"]) == json.loads(JSON_SCHEMA)
    assert (
        GlueJsonSchemaDeserializer(registry, **settings).deserialize("events", framed)
        == DECODED
    )


@pytest.mark.floci
def test_glue_protobuf_round_trip(floci):
    """The serializer registers the .proto text on Floci, and the deserializer reads the record."""
    registry, settings = _glue_registry(floci)
    serializer = GlueProtobufSerializer(
        registry, GLUE_ORDER_EVENT, PROTO_TEXT, **settings
    )

    framed = serializer.serialize("events", DECODED)

    assert framed[:2] == b"\x03\x00"
    # OrderEvent sorts before OrderValue by full name, so its index is 0
    assert framed[18] == 0
    version = boto3.client("glue", **settings).get_schema_version(
        SchemaVersionId=str(uuid.UUID(bytes=framed[2:18]))
    )
    assert version["DataFormat"] == "PROTOBUF"
    assert version["SchemaDefinition"] == PROTO_TEXT
    assert (
        GlueProtobufDeserializer(registry, GLUE_ORDER_EVENT, **settings).deserialize(
            "events", framed
        )
        == DECODED
    )
