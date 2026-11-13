import asyncio
import io
import json
import queue
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import fastavro
import httpx
import orjson
import pytest
from pydantic import BaseModel

from desflow.connectors.admin.kafka import KafkaAdminConnector
from desflow.connectors.egress.base import extract_dict
from desflow.connectors.egress.kafka import (
    ConfluentJsonSchemaSerializer,
    ConfluentProtobufSerializer,
    GlueJsonSchemaSerializer,
    GlueProtobufSerializer,
    ConfluentAvroSerializer,
    GlueAvroSerializer,
    KafkaEgress,
)


@pytest.fixture(autouse=True)
def mock_admin():
    """Keeps the topic creation at start from reaching for a broker."""
    with patch("desflow.connectors.egress.kafka.KafkaAdminConnector") as admin:
        yield admin


class DummyModel(BaseModel):
    """A dummy Pydantic model for duck-typing tests."""

    path_id: str
    value: int
    stream_type: str = "telemetry"


def test_extract_dict_helper():
    """Verify duck-typing helper extracts dicts from raw dicts and Pydantic models."""
    # 1. Raw dict
    raw = {"path_id": "A", "value": 1}
    assert extract_dict(raw) == raw

    # 2. Pydantic V2 Model
    model = DummyModel(path_id="B", value=2)
    extracted = extract_dict(model)
    assert extracted["path_id"] == "B"
    assert extracted["value"] == 2
    assert extracted["stream_type"] == "telemetry"


def test_missing_avro_dependencies():
    """Verify that using Avro serializers without the extras raises an explicit ImportError."""
    # Hide fastavro and httpx
    with patch.dict(sys.modules, {"fastavro": None, "httpx": None}):
        with pytest.raises(ImportError, match=r"pip install desflow\[confluent\]"):
            ConfluentAvroSerializer(registry_url="http://mock", schema_str="{}")

    # Hide boto3, then fastavro
    for hidden in ("boto3", "fastavro"):
        with patch.dict(sys.modules, {hidden: None}):
            with pytest.raises(
                ImportError,
                match=r"'boto3' and 'fastavro'.*pip install desflow\[glue\]",
            ):
                GlueAvroSerializer(registry_name="mock", schema_str="{}")


@pytest.mark.parametrize(
    ("hidden", "extra", "build"),
    [
        (
            "jsonschema",
            "confluent",
            lambda: ConfluentJsonSchemaSerializer("http://m", "{}"),
        ),
        (
            "google.protobuf",
            "confluent",
            lambda: ConfluentProtobufSerializer("http://m", None),
        ),
        ("fastjsonschema", "glue", lambda: GlueJsonSchemaSerializer("m", "{}")),
        ("google.protobuf", "glue", lambda: GlueProtobufSerializer("m", None, "")),
    ],
    ids=["confluent json", "confluent protobuf", "glue json", "glue protobuf"],
)
def test_missing_schema_format_dependencies(hidden, extra, build):
    """Each JSON Schema and Protobuf serializer names its extra when a package is missing."""
    with patch.dict(sys.modules, {hidden: None}):
        with pytest.raises(ImportError, match=rf"pip install desflow\[{extra}\]"):
            build()


AVRO_SCHEMA = """
{
  "type": "record",
  "name": "Probe",
  "fields": [{"name": "path_id", "type": "string"}, {"name": "value", "type": "int"}]
}
"""


def test_avro_serializers_construct_with_extras_installed():
    """Verify the extras are sufficient to build both serializers.

    The friendly ImportError above only proves the failure path. It stays green even
    when an extra is missing a transitive dependency, which is how a `confluent`
    extra that could not import its own registry client went unnoticed. Neither
    constructor performs network I/O, so this needs no registry to be reachable.
    """
    confluent = ConfluentAvroSerializer(
        registry_url="http://localhost:65535", schema_str=AVRO_SCHEMA
    )
    assert hasattr(confluent, "serialize")

    glue = GlueAvroSerializer(registry_name="probe", schema_str=AVRO_SCHEMA)
    assert hasattr(glue, "serialize")


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_pluggable_serializers(MockProducer, run_until):
    """Verify that topic_serializers correctly route payloads to custom serializers."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    # Create a mock serializer that returns a specific byte signature
    mock_serializer = MagicMock()
    mock_serializer.serialize.return_value = b"MOCK_AVRO_BYTES"

    mock_egress_queue = queue.Queue()
    # We send two messages: one mapped to custom serializer, one falling back to JSON
    data_custom = {"stream_type": "event", "key": "123", "value": "avro_data"}
    data_json = {"stream_type": "telemetry", "path_id": "Shop", "value": 5}

    mock_egress_queue.put([data_custom, data_json])

    # Instantiate with the custom serializer mapped to "sim-events"
    egress = KafkaEgress(
        bootstrap_servers="localhost:9092",
        telemetry_topic="sim-telemetry",
        event_topic="sim-events",
        topic_serializers={"sim-events": mock_serializer},
    )

    await run_until(
        egress.run(mock_egress_queue),
        lambda: mock_producer_instance.send.called and mock_egress_queue.empty(),
    )

    assert mock_producer_instance.send.call_count == 2

    # 1. Verify custom serializer intercepted the event topic
    call_1 = mock_producer_instance.send.call_args_list[0]
    assert call_1.args[0] == "sim-events"
    assert call_1.kwargs["value"] == b"MOCK_AVRO_BYTES"
    mock_serializer.serialize.assert_called_once()

    # 2. Verify fallback JSON serializer handled the telemetry topic
    call_2 = mock_producer_instance.send.call_args_list[1]
    assert call_2.args[0] == "sim-telemetry"
    assert b"Shop" in call_2.kwargs["value"]  # It should be raw JSON bytes


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_default_telemetry_routing(MockProducer, run_until):
    """Verify default routing for telemetry streams."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    telemetry_topic = "metrics_topic"
    event_topic = "event_topic"
    servers = "localhost:9092"

    mock_egress_queue = queue.Queue()
    data = {
        "stream_type": "telemetry",
        "path_id": "Shop.packers.queue_length",
        "value": 10,
    }
    mock_egress_queue.put([data])

    egress = KafkaEgress(
        telemetry_topic=telemetry_topic,
        event_topic=event_topic,
        bootstrap_servers=servers,
    )

    await run_until(
        egress.run(mock_egress_queue),
        lambda: mock_producer_instance.send.called and mock_egress_queue.empty(),
    )

    mock_producer_instance.start.assert_called_once()
    mock_producer_instance.send.assert_called_once()
    mock_producer_instance.stop.assert_called_once()

    call_args = mock_producer_instance.send.call_args
    target_topic = call_args.args[0]
    sent_key = call_args.kwargs["key"]
    sent_payload_bytes = call_args.kwargs["value"]

    assert target_topic == telemetry_topic
    assert sent_key == b"Shop.packers.queue_length"

    decoded_payload = orjson.loads(sent_payload_bytes)
    assert "stream_type" not in decoded_payload
    assert decoded_payload["path_id"] == "Shop.packers.queue_length"
    assert decoded_payload["value"] == 10


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_default_event_routing(MockProducer, run_until):
    """Verify default routing for event streams."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    mock_egress_queue = queue.Queue()
    data = {
        "stream_type": "event",
        "key": "sim_start_event",
        "value": {"event_type": "lifecycle"},
    }
    mock_egress_queue.put([data])

    egress = KafkaEgress(
        telemetry_topic="metrics",
        event_topic="events",
        bootstrap_servers="localhost:9092",
    )

    await run_until(
        egress.run(mock_egress_queue),
        lambda: mock_producer_instance.send.called and mock_egress_queue.empty(),
    )

    call_args = mock_producer_instance.send.call_args
    target_topic = call_args.args[0]
    sent_key = call_args.kwargs["key"]
    sent_payload_bytes = call_args.kwargs["value"]

    assert target_topic == "events"
    assert sent_key == b"sim_start_event"

    decoded_payload = orjson.loads(sent_payload_bytes)
    assert "stream_type" not in decoded_payload


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_omits_stream_type_by_default(MockProducer, run_until):
    """Verify the default keeps stream_type out of the message and off the record.

    The published payload is the part users depend on, and it must not change. The
    record itself matters too: every egress provider is handed the same dictionaries,
    so a sink that removed the key would take it away from a Parquet or Redis sink
    attached to the same simulation.
    """
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    mock_egress_queue = queue.Queue()
    data = {"stream_type": "telemetry", "path_id": "Shop", "value": 10}
    mock_egress_queue.put([data])

    egress = KafkaEgress(bootstrap_servers="localhost:9092")

    await run_until(
        egress.run(mock_egress_queue),
        lambda: mock_producer_instance.send.called and mock_egress_queue.empty(),
    )

    decoded_payload = orjson.loads(
        mock_producer_instance.send.call_args.kwargs["value"]
    )
    assert "stream_type" not in decoded_payload
    assert data["stream_type"] == "telemetry"


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_include_stream_type(MockProducer, run_until):
    """Verify include_stream_type=True keeps the field in the published message."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    mock_egress_queue = queue.Queue()
    telemetry = {"stream_type": "telemetry", "path_id": "Shop", "value": 10}
    event = {"stream_type": "event", "key": "task-1", "value": {"status": "ok"}}
    mock_egress_queue.put([telemetry, event])

    egress = KafkaEgress(
        bootstrap_servers="localhost:9092",
        telemetry_topic="metrics",
        event_topic="events",
        include_stream_type=True,
    )

    await run_until(
        egress.run(mock_egress_queue),
        lambda: mock_producer_instance.send.called and mock_egress_queue.empty(),
    )

    assert mock_producer_instance.send.call_count == 2

    call_1, call_2 = mock_producer_instance.send.call_args_list

    decoded_telemetry = orjson.loads(call_1.kwargs["value"])
    assert decoded_telemetry["stream_type"] == "telemetry"
    assert call_1.args[0] == "metrics"
    assert call_1.kwargs["key"] == b"Shop"

    decoded_event = orjson.loads(call_2.kwargs["value"])
    assert decoded_event["stream_type"] == "event"
    assert call_2.args[0] == "events"
    assert call_2.kwargs["key"] == b"task-1"


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_include_stream_type_with_topic_router(
    MockProducer, run_until
):
    """Verify the flag also applies when a topic_router chooses the destination."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    mock_egress_queue = queue.Queue()
    mock_egress_queue.put([{"stream_type": "event", "key": "req_1", "value": {}}])

    egress = KafkaEgress(
        bootstrap_servers="localhost:9092",
        topic_router=lambda payload: "mill-lifecycle",
        include_stream_type=True,
    )

    await run_until(
        egress.run(mock_egress_queue),
        lambda: mock_producer_instance.send.called and mock_egress_queue.empty(),
    )

    call_args = mock_producer_instance.send.call_args
    assert call_args.args[0] == "mill-lifecycle"
    assert orjson.loads(call_args.kwargs["value"])["stream_type"] == "event"


def test_avro_drops_stream_type_when_schema_omits_it(registry_transport):
    """Verify an Avro schema without stream_type silently discards the field.

    Avro encodes one field at a time from the registered schema, so switching
    include_stream_type on does not make the field reach an Avro consumer, and the
    send does not fail either. Users need a schema carrying the field, which is why
    the flag docstring states the constraint.
    """
    registry_transport(lambda request: httpx.Response(200, json={"id": 1}))
    serializer = ConfluentAvroSerializer(
        registry_url="http://localhost:65535", schema_str=AVRO_SCHEMA
    )

    with_field = serializer.serialize(
        "probe", {"path_id": "A", "value": 1, "stream_type": "telemetry"}
    )
    without_field = serializer.serialize("probe", {"path_id": "A", "value": 1})

    assert with_field == without_field
    assert b"telemetry" not in with_field
    record = fastavro.schemaless_reader(
        io.BytesIO(with_field[5:]), fastavro.parse_schema(json.loads(AVRO_SCHEMA))
    )
    assert record == {"path_id": "A", "value": 1}


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_custom_topic_router(MockProducer, run_until):
    """Verify that a custom router function correctly overrides default behavior."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance

    def custom_topic_router(payload: dict) -> str:
        value = payload.get("value", {})
        if isinstance(value, dict):
            event_type = value.get("event_type")
            if event_type == "prediction_request":
                return "mill-predictions"
        return "mill-lifecycle"

    mock_egress_queue = queue.Queue()

    data_1 = {
        "stream_type": "event",
        "key": "req_123",
        "value": {"event_type": "prediction_request", "data": [1, 2, 3]},
    }
    data_2 = {
        "stream_type": "event",
        "key": "req_456",
        "value": {"event_type": "random_status", "data": []},
    }

    mock_egress_queue.put([data_1, data_2])

    egress = KafkaEgress(
        bootstrap_servers="localhost:9092", topic_router=custom_topic_router
    )

    await run_until(
        egress.run(mock_egress_queue),
        lambda: mock_producer_instance.send.called and mock_egress_queue.empty(),
    )

    assert mock_producer_instance.send.call_count == 2

    call_1 = mock_producer_instance.send.call_args_list[0]
    assert call_1.args[0] == "mill-predictions"
    assert call_1.kwargs["key"] == b"req_123"

    call_2 = mock_producer_instance.send.call_args_list[1]
    assert call_2.args[0] == "mill-lifecycle"
    assert call_2.kwargs["key"] == b"req_456"

    decoded_payload_1 = orjson.loads(call_1.kwargs["value"])
    assert "stream_type" not in decoded_payload_1


async def _run_until_started(egress, producer):
    """Runs the egress until its producer starts, which follows the topic creation.

    The topics are created in a thread, so a fixed wait can end before the call on a
    slow runner. Waiting on the producer's start, with a time limit, removes that race.
    """
    task = asyncio.create_task(egress.run(queue.Queue()))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5.0
    while not producer.start.called and loop.time() < deadline:
        await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_topics_are_created_at_start(MockProducer, mock_admin):
    """Without a router, the event and telemetry topics are created before producing."""
    MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(
        bootstrap_servers="broker:9092",
        event_topic="ev",
        telemetry_topic="tm",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        linger_ms=5,
        client_id="sim",
    )

    await _run_until_started(egress, MockProducer.return_value)

    # Only settings the admin client knows are passed; linger_ms is producer-only,
    # and the admin connector sets its own client_id.
    mock_admin.assert_called_once_with(
        bootstrap_servers="broker:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
    )
    mock_admin.return_value.create_topics.assert_called_once_with(
        topics_config=[{"name": "ev"}, {"name": "tm"}]
    )


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_one_topic_for_both_streams_is_created_once(MockProducer, mock_admin):
    """The same name for both streams is one topic."""
    MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(
        bootstrap_servers="b:9092", event_topic="all", telemetry_topic="all"
    )

    await _run_until_started(egress, MockProducer.return_value)

    mock_admin.return_value.create_topics.assert_called_once_with(
        topics_config=[{"name": "all"}]
    )


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_a_router_leaves_topics_to_the_caller(MockProducer, mock_admin):
    """Only the router knows its topics, so none are created."""
    MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(bootstrap_servers="b:9092", topic_router=lambda d: "x")

    await _run_until_started(egress, MockProducer.return_value)

    mock_admin.assert_not_called()


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_a_failed_topic_creation_does_not_stop_the_producer(
    MockProducer, mock_admin
):
    """The producer loop has its own retry, so a creation failure is only logged."""
    producer = AsyncMock()
    MockProducer.return_value = producer
    mock_admin.return_value.create_topics.side_effect = RuntimeError("no broker")
    egress = KafkaEgress(bootstrap_servers="b:9092")

    await _run_until_started(egress, MockProducer.return_value)

    producer.start.assert_called_once()


def test_a_topic_error_from_the_broker_is_logged(monkeypatch, caplog):
    """The real admin connector turns a topic error in the response into a log line."""

    class AdminClient:
        def __init__(self, **kwargs):
            pass

        async def start(self):
            pass

        async def create_topics(self, new_topics, validate_only=False):
            # CreateTopicsResponse entries: topic, error code, error message
            error = (new_topics[0].name, 38, "Replication factor is invalid")
            return types.SimpleNamespace(topic_errors=[error])

        async def close(self):
            pass

    monkeypatch.setattr(
        "desflow.connectors.egress.kafka.KafkaAdminConnector", KafkaAdminConnector
    )
    monkeypatch.setattr(
        "desflow.connectors.admin.kafka.AIOKafkaAdminClient", AdminClient
    )

    KafkaEgress(
        bootstrap_servers="b:9092", event_topic="ev", telemetry_topic="tm"
    )._create_topics()

    (message,) = [
        r.getMessage() for r in caplog.records if "Could not create" in r.getMessage()
    ]
    assert "['ev', 'tm']" in message
    assert "InvalidReplicationFactorError" in message
    assert "Topic ev: Replication factor is invalid" in message


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_a_failed_start_closes_the_producer(MockProducer, run_until):
    """aiokafka leaves a producer open when start() fails; the egress closes it."""
    failed = AsyncMock()
    failed.start.side_effect = ConnectionError("no broker")
    MockProducer.return_value = failed

    egress = KafkaEgress(bootstrap_servers="localhost:9092")
    await run_until(egress.run(queue.Queue()), lambda: failed.stop.await_count == 1)

    failed.stop.assert_awaited_once()


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_egress_drops_reserved_keys(MockProducer, run_until, caplog):
    """A payload key such as __table__ belongs to desflow: Kafka writes it nowhere,
    warns once per key, and leaves the shared record as it was."""
    mock_producer_instance = AsyncMock()
    MockProducer.return_value = mock_producer_instance
    mock_egress_queue = queue.Queue()
    value = {"__table__": "orders", "status": "placed"}
    records = [
        {"stream_type": "event", "key": f"orders-{n}", "value": value} for n in (1, 2)
    ]
    mock_egress_queue.put(records)
    egress = KafkaEgress(bootstrap_servers="localhost:9092")

    await run_until(
        egress.run(mock_egress_queue),
        lambda: mock_producer_instance.send.call_count == 2,
    )

    for call in mock_producer_instance.send.call_args_list:
        assert orjson.loads(call.kwargs["value"])["value"] == {"status": "placed"}
    warnings = [r for r in caplog.records if "drops the payload key" in r.getMessage()]
    assert len(warnings) == 1
    assert "KafkaEgress drops the payload key '__table__'" in warnings[0].getMessage()
    assert records[0]["value"] == {"__table__": "orders", "status": "placed"}


# ==========================================
# Batch 3: sink options of a blueprint's Kafka entry
# ==========================================


def _sent(producer):
    """The (topic, key, decoded value) of every message a mocked producer was given."""
    return [
        (call.args[0], call.kwargs["key"], json.loads(call.kwargs["value"]))
        for call in producer.send.call_args_list
    ]


async def _publish(egress, batch, producer, run_until):
    """Runs the egress over one batch until every record has been sent."""
    batches = queue.Queue()
    batches.put(batch)
    await run_until(
        egress.run(batches),
        lambda: batches.empty() and producer.send.call_count >= len(batch),
    )


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_topic_field_selects_the_topic_and_is_removed(MockProducer, run_until):
    """`__topic__` in an event's value picks its topic, and the key is not sent.

    Telemetry keeps `telemetry_topic`, and an event without the field goes to
    `event_topic`.
    """
    producer = MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(bootstrap_servers="b:9092", topic_policy="manual")
    batch = [
        {"stream_type": "event", "key": "k1", "value": {"__topic__": "orders", "n": 1}},
        {"stream_type": "event", "key": "k2", "value": {"n": 2}},
        {"stream_type": "telemetry", "path_id": "p", "value": 3},
    ]

    await _publish(egress, batch, producer, run_until)

    assert _sent(producer) == [
        ("orders", b"k1", {"key": "k1", "value": {"n": 1}}),
        ("sim-events", b"k2", {"key": "k2", "value": {"n": 2}}),
        ("sim-telemetry", b"p", {"path_id": "p", "value": 3}),
    ]
    assert batch[0]["value"] == {"__topic__": "orders", "n": 1}


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_topic_router_wins_over_the_topic_field(MockProducer, run_until):
    """A `topic_router` is the user's explicit function, so it wins; the key is
    still removed from the message."""
    producer = MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(bootstrap_servers="b:9092", topic_router=lambda d: "routed")
    batch = [{"stream_type": "event", "key": "k", "value": {"__topic__": "t", "n": 1}}]

    await _publish(egress, batch, producer, run_until)

    assert _sent(producer) == [("routed", b"k", {"key": "k", "value": {"n": 1}})]


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_a_topic_field_that_is_not_a_name_is_ignored_with_a_warning(
    MockProducer, run_until, caplog
):
    """A number in `__topic__` cannot name a topic: the event goes to the event topic."""
    producer = MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(bootstrap_servers="b:9092", topic_policy="manual")
    batch = [{"stream_type": "event", "key": "k", "value": {"__topic__": 5}}]

    await _publish(egress, batch, producer, run_until)

    assert _sent(producer)[0][0] == "sim-events"
    assert "ignores __topic__ 5" in caplog.text


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_envelope_false_sends_the_value_alone(MockProducer, run_until):
    """With `envelope` off the message is the event's value, keyed by the record key.

    A value that is not a mapping is sent as JSON, and a null value as a tombstone:
    the key with a null value, which no serializer encodes. A ground-truth record is
    sent whole.
    """
    producer = MockProducer.return_value = AsyncMock()
    serializer = MagicMock()
    serializer.serialize.side_effect = lambda topic, data: json.dumps(data).encode()
    egress = KafkaEgress(
        bootstrap_servers="b:9092", topic_policy="manual", serializer=serializer
    )
    egress.envelope = False
    batch = [
        {"stream_type": "event", "key": "k1", "value": {"a": 1}},
        {"stream_type": "event", "key": "k2", "value": "text"},
        {"stream_type": "event", "key": "k4", "value": None},
        {"stream_type": "label", "key": "k3", "path": "p"},
    ]

    await _publish(egress, batch, producer, run_until)

    sent = [
        (call.args[0], call.kwargs["key"], call.kwargs["value"])
        for call in producer.send.call_args_list
    ]
    assert sent == [
        ("sim-events", b"k1", b'{"a": 1}'),
        ("sim-events", b"k2", b'"text"'),
        ("sim-events", b"k4", None),
        ("sim-events", b"k3", b'{"key": "k3", "path": "p"}'),
    ]
    assert len(serializer.serialize.call_args_list) == 3


def _shop_message(tmp_path):
    """A Protobuf class `shop.Row` with a `key` text field and a `value` message
    field `shop.Value` holding `n`, built from a descriptor file."""
    from google.protobuf import descriptor_pb2

    from desflow.connectors.egress.kafka import _message_class

    field = descriptor_pb2.FieldDescriptorProto
    file_proto = descriptor_pb2.FileDescriptorProto(
        name="row.proto", package="shop", syntax="proto3"
    )
    # `Row` comes first, so its Confluent message index is the one byte 0
    row = file_proto.message_type.add(name="Row")
    row.field.add(
        name="key", number=1, type=field.TYPE_STRING, label=field.LABEL_OPTIONAL
    )
    row.field.add(
        name="value",
        number=2,
        type=field.TYPE_MESSAGE,
        type_name=".shop.Value",
        label=field.LABEL_OPTIONAL,
    )
    value = file_proto.message_type.add(name="Value")
    value.field.add(
        name="n", number=1, type=field.TYPE_INT64, label=field.LABEL_OPTIONAL
    )
    descriptor_file = tmp_path / "row.desc"
    descriptor_file.write_bytes(
        descriptor_pb2.FileDescriptorSet(file=[file_proto]).SerializeToString()
    )
    return _message_class(
        {"descriptor_file": str(descriptor_file), "message": "shop.Row"}
    )


_ROW_AVRO = json.dumps(
    {
        "type": "record",
        "name": "Row",
        "fields": [
            {"name": "key", "type": "string"},
            {
                "name": "value",
                "type": {
                    "type": "record",
                    "name": "Value",
                    "fields": [{"name": "n", "type": "long"}],
                },
            },
        ],
    }
)
_ROW_JSON_SCHEMA = json.dumps(
    {"type": "object", "properties": {"value": {"type": "object"}}}
)


@pytest.mark.parametrize(
    "kind, refused",
    [
        pytest.param("json", None, id="json-writes-it"),
        pytest.param("protobuf", None, id="protobuf-writes-it-unset"),
        pytest.param("avro", "on field value", id="avro-refuses-it"),
        pytest.param(
            "json_schema", "None is not of type 'object'", id="json-schema-refuses-it"
        ),
    ],
)
@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_a_null_value_with_the_envelope_on_is_written_or_skipped(
    MockProducer, kind, refused, run_until, registry_transport, tmp_path, caplog
):
    """With `envelope` on a null value stays in the record, which each serializer
    writes or refuses. A refused record is skipped with a warning that names its key
    and the reason, and the rest of the batch is sent."""
    registry_transport(lambda request: httpx.Response(200, json={"id": 1}))
    url = "http://localhost:65535"
    serializer = {
        "json": lambda: None,
        "protobuf": lambda: ConfluentProtobufSerializer(url, _shop_message(tmp_path)),
        "avro": lambda: ConfluentAvroSerializer(url, schema_str=_ROW_AVRO),
        "json_schema": lambda: ConfluentJsonSchemaSerializer(
            url, schema_str=_ROW_JSON_SCHEMA
        ),
    }[kind]()
    producer = MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(
        bootstrap_servers="b:9092", topic_policy="manual", serializer=serializer
    )
    batch = [
        {"stream_type": "event", "key": "k1", "value": {"n": 1}},
        {"stream_type": "event", "key": "k2", "value": None},
        {"stream_type": "event", "key": "k3", "value": {"n": 3}},
    ]
    sends = 2 if refused else 3
    batches = queue.Queue()
    batches.put(batch)

    await run_until(
        egress.run(batches),
        lambda: batches.empty() and producer.send.call_count >= sends,
    )

    sent = {
        call.kwargs["key"]: call.kwargs["value"]
        for call in producer.send.call_args_list
    }
    assert "connection failed" not in caplog.text
    if refused:
        assert list(sent) == [b"k1", b"k3"]
        assert "skipped the record with key k2 for topic sim-events" in caplog.text
        assert refused in caplog.text
    elif kind == "json":
        assert json.loads(sent[b"k2"])["value"] is None
    else:
        message = _shop_message(tmp_path).FromString(sent[b"k2"][6:])
        assert message.key == "k2" and not message.HasField("value")


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(("registry", 422, 42201, "invalid schema"), id="registry-422"),
        pytest.param(
            ("registry", 400, "InvalidInputException", "bad definition"), id="glue-400"
        ),
        pytest.param(OverflowError("int too big for 64 bits"), id="overflow"),
    ],
)
@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_a_registry_or_encoding_error_skips_only_its_record(
    MockProducer, run_until, caplog, error
):
    """A `SchemaRegistryError` with a 4xx status that does not refuse access, or an
    `OverflowError`, skips its record with a warning that names the error, as a
    `ValueError` does. The records after it are sent and the connection is not
    restarted."""
    from desflow.connectors._schema_registry.errors import SchemaRegistryError

    if isinstance(error, tuple):
        error = SchemaRegistryError(*error[1:])

    def serialize(topic, payload):
        if payload["key"] == "k2":
            raise error
        return payload["key"].encode()

    serializer = MagicMock()
    serializer.serialize.side_effect = serialize
    producer = MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(
        bootstrap_servers="b:9092", topic_policy="manual", serializer=serializer
    )
    batches = queue.Queue()
    batches.put(
        [{"stream_type": "event", "key": f"k{n}", "value": {"n": n}} for n in (1, 2, 3)]
    )

    await run_until(
        egress.run(batches),
        lambda: batches.empty() and producer.send.call_count >= 2,
    )

    sent = [call.kwargs["key"] for call in producer.send.call_args_list]
    assert sent == [b"k1", b"k3"]
    assert "skipped the record with key k2" in caplog.text
    assert str(error) in caplog.text
    assert "connection failed" not in caplog.text
    assert MockProducer.call_count == 1


@pytest.mark.parametrize(
    ("status", "code", "cause"),
    [
        pytest.param(401, 40101, None, id="registry-401"),
        pytest.param(403, 40301, None, id="registry-403"),
        pytest.param(400, "AccessDeniedException", None, id="glue-access-denied"),
        pytest.param(-1, -1, "no-credentials", id="glue-no-credentials"),
    ],
)
@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_a_registry_that_refuses_access_ends_the_sink(
    MockProducer, caplog, status, code, cause
):
    """A registry that refuses the sink's credentials or access ends the sink's task
    with that error, so the queue is not drained record by record. Nothing of the
    batch is sent, the producer is stopped and no reconnection is tried."""
    from botocore.exceptions import NoCredentialsError

    from desflow.connectors._schema_registry.errors import SchemaRegistryError

    # A Glue client with no credentials fails before it calls Glue, and
    # `GlueSchemaRegistryClient` wraps botocore's error with the status -1
    origin = NoCredentialsError() if cause == "no-credentials" else None

    def serialize(topic, payload):
        if payload["key"] == "k2":
            raise SchemaRegistryError(status, code, "refused") from origin
        return payload["key"].encode()

    serializer = MagicMock()
    serializer.serialize.side_effect = serialize
    producer = MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(
        bootstrap_servers="b:9092", topic_policy="manual", serializer=serializer
    )
    batches = queue.Queue()
    batches.put(
        [{"stream_type": "event", "key": f"k{n}", "value": {"n": n}} for n in (1, 2, 3)]
    )
    batches.put([{"stream_type": "event", "key": "k4", "value": {"n": 4}}])

    with pytest.raises(SchemaRegistryError) as raised:
        await asyncio.wait_for(egress.run(batches), timeout=5)

    assert raised.value.http_status == status
    producer.send.assert_not_called()
    producer.stop.assert_awaited_once()
    assert batches.qsize() == 1
    assert MockProducer.call_count == 1
    assert "skipped the record" not in caplog.text
    assert "connection failed" not in caplog.text


@pytest.mark.parametrize(
    "kind",
    [
        pytest.param("registry-503", id="registry-503"),
        pytest.param("registry-429", id="registry-429"),
        pytest.param("glue-throttling", id="glue-throttling"),
        pytest.param("timeout", id="timeout"),
        pytest.param("glue-connection", id="glue-connection"),
    ],
)
@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_a_temporary_registry_error_keeps_the_batch_for_a_retry(
    MockProducer, run_until, caplog, monkeypatch, kind
):
    """A registry that answers 5xx, throttles the call, times out or drops the
    connection does not skip its record: the whole batch is encoded again after a
    wait, every record is sent once and in order, and the producer is not
    restarted."""
    import httpx
    from botocore.exceptions import EndpointConnectionError

    from desflow.connectors._schema_registry.errors import SchemaRegistryError

    def temporary():
        if kind == "registry-503":
            return SchemaRegistryError(503, 50301, "unavailable")
        if kind == "registry-429":
            return SchemaRegistryError(429, 42901, "too many requests")
        if kind == "glue-throttling":
            return SchemaRegistryError(400, "ThrottlingException", "Rate exceeded")
        if kind == "timeout":
            return httpx.ConnectTimeout("timed out")
        error = SchemaRegistryError(-1, -1, "could not connect")
        error.__cause__ = EndpointConnectionError(endpoint_url="https://glue")
        return error

    failures = [temporary(), temporary()]

    def serialize(topic, payload):
        if payload["key"] == "k2" and failures:
            raise failures.pop()
        return payload["key"].encode()

    monkeypatch.setattr("desflow.connectors.egress.kafka.FIRST_DELAY", 0.01)
    serializer = MagicMock()
    serializer.serialize.side_effect = serialize
    producer = MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(
        bootstrap_servers="b:9092", topic_policy="manual", serializer=serializer
    )
    batches = queue.Queue()
    batches.put(
        [{"stream_type": "event", "key": f"k{n}", "value": {"n": n}} for n in (1, 2, 3)]
    )
    batches.put([{"stream_type": "event", "key": "k4", "value": {"n": 4}}])

    await run_until(egress.run(batches), lambda: producer.send.call_count >= 4)

    sent = [call.kwargs["key"] for call in producer.send.call_args_list]
    assert sent == [b"k1", b"k2", b"k3", b"k4"]
    assert "skipped the record" not in caplog.text
    assert "could not reach the schema registry" in caplog.text
    assert MockProducer.call_count == 1


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_a_slow_serializer_does_not_hold_the_loop(MockProducer, run_until):
    """A serializer that waits, as a registry call retrying does, runs off the loop,
    so another task on the loop keeps running."""
    import time

    serializer = MagicMock()
    serializer.serialize.side_effect = lambda topic, payload: time.sleep(0.5) or b"x"
    producer = MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(
        bootstrap_servers="b:9092", topic_policy="manual", serializer=serializer
    )
    batches = queue.Queue()
    batches.put([{"stream_type": "event", "key": "k1", "value": {"n": 1}}])
    loop = asyncio.get_running_loop()
    ticks = []

    async def ticker():
        while True:
            ticks.append(loop.time())
            await asyncio.sleep(0.02)

    watcher = asyncio.create_task(ticker())
    await run_until(egress.run(batches), lambda: producer.send.call_count >= 1)
    # The ticker may be cancelled before it wakes after a block, so the last gap ends now
    ticks.append(loop.time())
    watcher.cancel()

    gaps = [later - earlier for earlier, later in zip(ticks, ticks[1:])]
    assert max(gaps) < 0.25


def test_topic_policy_modes_are_read(mock_admin):
    """`topic_policy` takes a bare mode word or a mapping, and refuses the rest."""
    assert KafkaEgress("b:9092", topic_policy="manual").topic_policy["mode"] == "manual"
    egress = KafkaEgress(
        "b:9092",
        topic_policy={
            "mode": "drop_and_create",
            "partitions": 3,
            "replication_factor": 2,
            "topic_configs": {"retention.ms": 1000},
        },
    )
    assert egress.topic_policy == {
        "mode": "drop_and_create",
        "partitions": 3,
        "replication_factor": 2,
        "topic_configs": {"retention.ms": "1000"},
    }
    with pytest.raises(ValueError, match="mode 'sometimes' is not one of"):
        KafkaEgress("b:9092", topic_policy="sometimes")
    with pytest.raises(ValueError, match=r"unknown key\(s\) \['partition'\]"):
        KafkaEgress("b:9092", topic_policy={"partition": 3})
    with pytest.raises(ValueError, match="partitions must be a whole number"):
        KafkaEgress("b:9092", topic_policy={"partitions": 0})


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_topic_policy_creates_with_its_settings(MockProducer, mock_admin):
    """The policy's partitions, replication and settings reach the create request."""
    MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(
        bootstrap_servers="b:9092",
        topic_policy={"partitions": 6, "topic_configs": {"cleanup.policy": "compact"}},
    )

    await _run_until_started(egress, MockProducer.return_value)

    admin = mock_admin.return_value
    admin.drop_topics.assert_not_called()
    admin.create_topics.assert_called_once_with(
        topics_config=[
            {
                "name": name,
                "partitions": 6,
                "topic_configs": {"cleanup.policy": "compact"},
            }
            for name in ("sim-events", "sim-telemetry")
        ]
    )


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_topic_policy_drop_and_create_drops_first(MockProducer, mock_admin):
    """`drop_and_create` deletes the topics before it creates them again."""
    MockProducer.return_value = AsyncMock()
    calls = []
    admin = mock_admin.return_value
    admin.drop_topics.side_effect = lambda names: calls.append(("drop", names))
    admin.create_topics.side_effect = lambda topics_config: calls.append(
        ("create", [c["name"] for c in topics_config])
    )
    egress = KafkaEgress(bootstrap_servers="b:9092", topic_policy="drop_and_create")

    await _run_until_started(egress, MockProducer.return_value)

    assert calls == [
        ("drop", ["sim-events", "sim-telemetry"]),
        ("create", ["sim-events", "sim-telemetry"]),
    ]


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_topic_policy_manual_creates_nothing(MockProducer, mock_admin):
    """`manual` leaves every topic to the operator."""
    MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(bootstrap_servers="b:9092", topic_policy="manual")

    await _run_until_started(egress, MockProducer.return_value)

    mock_admin.return_value.create_topics.assert_not_called()
    mock_admin.return_value.drop_topics.assert_not_called()


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_topics_settings_per_topic(MockProducer, mock_admin):
    """`topics` gives a topic its own settings over the sink's policy, and the
    sink creates it even with a `topic_router`, which leaves the event and telemetry
    topics alone."""
    MockProducer.return_value = AsyncMock()
    egress = KafkaEgress(
        bootstrap_servers="b:9092",
        topic_router=lambda d: "orders",
        topic_policy={"partitions": 2},
        topics={
            "orders": {"partitions": 8},
            "audit": {"mode": "manual"},
            "clicks": {"topic_configs": {"retention.ms": "5"}},
        },
    )

    await _run_until_started(egress, MockProducer.return_value)

    mock_admin.return_value.create_topics.assert_called_once_with(
        topics_config=[
            {"name": "orders", "partitions": 8},
            {"name": "clicks", "partitions": 2, "topic_configs": {"retention.ms": "5"}},
        ]
    )


def test_topics_serializer_applies_to_its_topic_only():
    """`topics.<t>.serializer` serves that topic, and `serializer` serves the rest."""
    egress = KafkaEgress(
        "b:9092",
        serializer={
            "type": "confluent_avro",
            "registry_url": "http://registry:8081",
            "schema": AVRO_SCHEMA,
        },
        topics={
            "orders": {
                "serializer": {
                    "type": "confluent_json_schema",
                    "registry_url": "http://registry:8081",
                    "schema": '{"type": "object"}',
                }
            }
        },
    )

    assert isinstance(egress.default_serializer, ConfluentAvroSerializer)
    assert isinstance(egress.topic_serializers["orders"], ConfluentJsonSchemaSerializer)
    with pytest.raises(ValueError, match="serializer or default_serializer"):
        KafkaEgress(
            "b:9092",
            serializer={
                "type": "glue_avro",
                "registry_name": "r",
                "schema": AVRO_SCHEMA,
            },
            default_serializer=MagicMock(),
        )
    with pytest.raises(ValueError, match="both give a serializer for 'orders'"):
        KafkaEgress(
            "b:9092",
            topic_serializers={"orders": MagicMock()},
            topics={"orders": {"serializer": MagicMock()}},
        )


def test_build_serializer_builds_each_type(tmp_path):
    """Every one of the six types is built from its mapping, and the registry
    arguments reach the serializer."""
    from desflow.connectors.egress.kafka import build_serializer

    schema_file = tmp_path / "schema.avsc"
    schema_file.write_text(AVRO_SCHEMA)
    spec = {
        "type": "confluent_avro",
        "registry_url": "http://registry:8081",
        "schema_file": str(schema_file),
        "auth": ["user", "secret"],
        "timeout": 3,
        "max_retries": 1,
    }
    with patch("desflow.connectors.egress.kafka.ConfluentAvroSerializer") as cls:
        build_serializer(spec)
    cls.assert_called_once_with(
        "http://registry:8081",
        AVRO_SCHEMA,
        auth=("user", "secret"),
        timeout=3,
        max_retries=1,
    )
    assert isinstance(build_serializer(spec), ConfluentAvroSerializer)
    assert isinstance(
        build_serializer(
            {
                "type": "glue_avro",
                "registry_name": "r",
                "schema": AVRO_SCHEMA,
                "boto3": {"region_name": "us-east-1"},
            }
        ),
        GlueAvroSerializer,
    )
    assert isinstance(
        build_serializer(
            {
                "type": "glue_json_schema",
                "registry_name": "r",
                "schema": {"type": "object"},
                "boto3": {"region_name": "us-east-1"},
            }
        ),
        GlueJsonSchemaSerializer,
    )


def test_build_serializer_refuses_what_does_not_belong(tmp_path):
    from desflow.connectors.egress.kafka import build_serializer

    with pytest.raises(ValueError, match="type 'avro' is not one of"):
        build_serializer({"type": "avro"})
    with pytest.raises(ValueError, match=r"unknown key\(s\) \['registry_name'\]"):
        build_serializer(
            {
                "type": "confluent_avro",
                "registry_url": "u",
                "registry_name": "r",
                "schema": AVRO_SCHEMA,
            }
        )
    with pytest.raises(ValueError, match="needs registry_url"):
        build_serializer({"type": "confluent_avro", "schema": AVRO_SCHEMA})
    with pytest.raises(ValueError, match="needs schema or schema_file"):
        build_serializer({"type": "confluent_avro", "registry_url": "u"})
    with pytest.raises(ValueError, match="schema or schema_file, not both"):
        build_serializer(
            {
                "type": "confluent_avro",
                "registry_url": "u",
                "schema": "{}",
                "schema_file": "f",
            }
        )
    with pytest.raises(ValueError, match="needs descriptor_file and message"):
        build_serializer({"type": "confluent_protobuf", "registry_url": "u"})
    with pytest.raises(ValueError, match=r"unknown key\(s\) \['descriptor_file'\]"):
        build_serializer(
            {
                "type": "confluent_avro",
                "registry_url": "u",
                "schema": "{}",
                "descriptor_file": "d",
            }
        )


def test_build_serializer_builds_a_protobuf_class_from_a_descriptor_file(tmp_path):
    """A descriptor set and a message name give the message class, and the
    serializer encodes a message of it with the Confluent framing."""
    from google.protobuf import descriptor_pb2

    from desflow.connectors.egress.kafka import _message_class, build_serializer

    file_proto = descriptor_pb2.FileDescriptorProto(
        name="shop.proto", package="shop", syntax="proto3"
    )
    message = file_proto.message_type.add(name="Order")
    message.field.add(
        name="order_id",
        number=1,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
    )
    descriptor_file = tmp_path / "shop.desc"
    descriptor_file.write_bytes(
        descriptor_pb2.FileDescriptorSet(file=[file_proto]).SerializeToString()
    )
    spec = {"descriptor_file": str(descriptor_file), "message": "shop.Order"}

    cls = _message_class(spec)

    assert cls.DESCRIPTOR.full_name == "shop.Order"
    assert cls(order_id="o-1").SerializeToString() == b"\n\x03o-1"
    assert isinstance(
        build_serializer(
            {"type": "confluent_protobuf", "registry_url": "http://r:8081", **spec}
        ),
        ConfluentProtobufSerializer,
    )
    with pytest.raises(ValueError, match="no message 'shop.Missing'"):
        _message_class({**spec, "message": "shop.Missing"})


def test_ssl_and_token_provider(tmp_path, mock_admin):
    """`ssl` is built by aiokafka's `create_ssl_context` from its four keys, and
    `sasl_oauth_token_provider` builds the MSK IAM provider, or says which package
    is missing."""
    with patch("aiokafka.helpers.create_ssl_context") as create:
        create.return_value = "CONTEXT"
        egress = KafkaEgress(
            "b:9092",
            security_protocol="SSL",
            ssl={
                "cafile": "ca.pem",
                "certfile": "c.pem",
                "keyfile": "k.pem",
                "password": "pw",
            },
        )
    create.assert_called_once_with(
        cafile="ca.pem", certfile="c.pem", keyfile="k.pem", password="pw"
    )
    assert egress.producer_config["ssl_context"] == "CONTEXT"
    assert egress.producer_config["security_protocol"] == "SSL"
    with pytest.raises(ValueError, match=r"ssl has unknown key\(s\) \['cert'\]"):
        KafkaEgress("b:9092", ssl={"cert": "c.pem"})

    signer = types.ModuleType("aws_msk_iam_sasl_signer")
    signer.MSKAuthTokenProvider = MagicMock()
    signer.MSKAuthTokenProvider.generate_auth_token.return_value = ("TOKEN", 1)
    with patch.dict(sys.modules, {"aws_msk_iam_sasl_signer": signer}):
        egress = KafkaEgress(
            "b:9092",
            sasl_oauth_token_provider={"type": "aws_msk_iam", "region": "us-east-1"},
        )
        provider = egress.producer_config["sasl_oauth_token_provider"]
        assert asyncio.run(provider.token()) == "TOKEN"
    signer.MSKAuthTokenProvider.generate_auth_token.assert_called_once_with("us-east-1")

    with patch.dict(sys.modules, {"aws_msk_iam_sasl_signer": None}):
        with pytest.raises(ImportError, match="aws-msk-iam-sasl-signer-python"):
            KafkaEgress(
                "b:9092",
                sasl_oauth_token_provider={"type": "aws_msk_iam", "region": "r"},
            )
    with pytest.raises(ValueError, match="takes {type: aws_msk_iam, region"):
        KafkaEgress("b:9092", sasl_oauth_token_provider={"type": "gcp"})


def test_a_topics_value_that_is_not_a_mapping_names_the_topic(mock_admin):
    with pytest.raises(ValueError, match=r"topics\.orders must be a mapping"):
        KafkaEgress("b:9092", topics={"orders": "manual"})
