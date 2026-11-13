import asyncio
import json
import queue
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from desflow.connectors.ingress.kafka import (
    ConfluentJsonSchemaDeserializer,
    ConfluentProtobufDeserializer,
    GlueJsonSchemaDeserializer,
    GlueProtobufDeserializer,
    ConfluentAvroDeserializer,
    GlueAvroDeserializer,
    KafkaIngress,
)


async def _wait_until(condition, timeout=5.0):
    """Waits until `condition()` holds, or `timeout` seconds pass. The ingress first
    tries to create its topic in a worker thread, which takes longer on a slow runner
    than any fixed wait."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition() and loop.time() < deadline:
        await asyncio.sleep(0.01)


def test_missing_avro_deserializer_dependencies():
    """Verify that using Avro deserializers without the extras raises an explicit ImportError."""
    # Hide fastavro and httpx
    with patch.dict(sys.modules, {"fastavro": None, "httpx": None}):
        with pytest.raises(ImportError, match=r"pip install desflow\[confluent\]"):
            ConfluentAvroDeserializer(registry_url="http://mock")

    # Hide boto3, then fastavro, then fastjsonschema, which only the deserializer needs
    for hidden in ("boto3", "fastavro", "fastjsonschema"):
        with patch.dict(sys.modules, {hidden: None}):
            with pytest.raises(
                ImportError,
                match=r"'boto3', 'fastavro' and 'fastjsonschema'.*pip install desflow\[glue\]",
            ):
                GlueAvroDeserializer(registry_name="mock")


@pytest.mark.parametrize(
    ("hidden", "extra", "build"),
    [
        (
            "jsonschema",
            "confluent",
            lambda: ConfluentJsonSchemaDeserializer("http://m"),
        ),
        (
            "google.protobuf",
            "confluent",
            lambda: ConfluentProtobufDeserializer("http://m", None),
        ),
        ("fastjsonschema", "glue", lambda: GlueJsonSchemaDeserializer("m")),
        ("google.protobuf", "glue", lambda: GlueProtobufDeserializer("m", None)),
    ],
    ids=["confluent json", "confluent protobuf", "glue json", "glue protobuf"],
)
def test_missing_schema_format_dependencies(hidden, extra, build):
    """Each JSON Schema and Protobuf deserializer names its extra when a package is missing."""
    with patch.dict(sys.modules, {hidden: None}):
        with pytest.raises(ImportError, match=rf"pip install desflow\[{extra}\]"):
            build()


@pytest.fixture(autouse=True)
def mock_admin():
    """Keeps the control topic's creation at start from reaching for a broker."""
    with patch("desflow.connectors.ingress.kafka.KafkaAdminConnector") as admin:
        yield admin


@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_kafka_ingress_pluggable_deserializers(MockConsumer):
    """Verify that topic_deserializers correctly route binary payloads to custom deserializers."""

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            class FakeMessage:
                topic = "secure-topic"
                value = b"MOCK_BINARY_AVRO_PAYLOAD"

            yield FakeMessage()
            await asyncio.sleep(999)

    MockConsumer.return_value = FakeConsumer()

    # Create a mock deserializer that returns a specific dictionary
    mock_deserializer = MagicMock()
    mock_deserializer.deserialize.return_value = {
        "path": "Shop.packers.current_cap",
        "value": 10,
    }

    ingress_queue = queue.Queue()
    ingress = KafkaIngress(
        topic="secure-topic",
        bootstrap_servers="localhost:9092",
        topic_deserializers={"secure-topic": mock_deserializer},
    )

    task = asyncio.create_task(ingress.run(ingress_queue))
    await _wait_until(lambda: not ingress_queue.empty())

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Verify the deserializer intercepted the message
    mock_deserializer.deserialize.assert_called_once_with(
        "secure-topic", b"MOCK_BINARY_AVRO_PAYLOAD"
    )

    # Verify the decoded data was placed in the queue
    assert not ingress_queue.empty()
    path, value = ingress_queue.get_nowait()
    assert path == "Shop.packers.current_cap"
    assert value == 10


@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_a_group_commits_an_offset_only_after_its_message_is_handled(
    MockConsumer,
):
    """With a `group_id` the consumer does not commit by itself, because it would
    commit the position of a message that still waits for the registry. The offset is
    committed after the message is put on the queue."""
    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    committed = []
    consumer.commit = AsyncMock(
        side_effect=lambda offsets: committed.append((offsets, ingress_queue.qsize()))
    )

    async def messages():
        yield MagicMock(
            topic="t", partition=2, offset=6, value=b'{"path": "Shop.cap", "value": 1}'
        )
        await asyncio.sleep(999)

    consumer.__aiter__ = lambda self: messages()
    MockConsumer.return_value = consumer
    ingress_queue = queue.Queue()
    ingress = KafkaIngress(topic="t", bootstrap_servers="localhost:9092", group_id="g")

    task = asyncio.create_task(ingress.run(ingress_queue))
    await _wait_until(lambda: bool(committed))
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert MockConsumer.call_args.kwargs["enable_auto_commit"] is False
    [(offsets, queued)] = committed
    assert queued == 1
    assert list(offsets.values()) == [7]


@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_a_slow_deserializer_does_not_hold_the_loop(MockConsumer):
    """A deserializer that waits, as a registry call retrying does, runs off the
    loop, so another task on the loop keeps running."""
    import time

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            class FakeMessage:
                topic = "t"
                value = b"x"

            yield FakeMessage()
            await asyncio.sleep(999)

    MockConsumer.return_value = FakeConsumer()
    deserializer = MagicMock()
    deserializer.deserialize.side_effect = lambda topic, value: (
        time.sleep(0.5) or {"path": "Shop.cap", "value": 1}
    )
    ingress_queue = queue.Queue()
    ingress = KafkaIngress(
        topic="t", bootstrap_servers="localhost:9092", default_deserializer=deserializer
    )
    loop = asyncio.get_running_loop()
    ticks = []

    async def ticker():
        while True:
            ticks.append(loop.time())
            await asyncio.sleep(0.02)

    watcher = asyncio.create_task(ticker())
    task = asyncio.create_task(ingress.run(ingress_queue))
    deadline = loop.time() + 5
    while ingress_queue.empty() and loop.time() < deadline:
        await asyncio.sleep(0.01)
    # The ticker may be cancelled before it wakes after a block, so the last gap ends now
    ticks.append(loop.time())
    task.cancel()
    watcher.cancel()
    await asyncio.gather(task, watcher, return_exceptions=True)

    assert ingress_queue.get_nowait() == ("Shop.cap", 1)
    gaps = [later - earlier for earlier, later in zip(ticks, ticks[1:])]
    assert max(gaps) < 0.25


@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_kafka_ingress_success(MockConsumer):
    """Verify the default JSON fallback deserializes successfully."""

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            class FakeMessage:
                topic = "test-topic"  # Required by the new topic router
                value = json.dumps(
                    {"path": "Shop.resources.packers.current_cap", "value": 5}
                ).encode("utf-8")

            yield FakeMessage()
            await asyncio.sleep(999)

    mock_consumer_instance = FakeConsumer()
    MockConsumer.return_value = mock_consumer_instance

    ingress_queue = queue.Queue()
    ingress = KafkaIngress(topic="test-topic", bootstrap_servers="localhost:9092")

    task = asyncio.create_task(ingress.run(ingress_queue))
    await _wait_until(lambda: not ingress_queue.empty())

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    mock_consumer_instance.start.assert_called_once()
    mock_consumer_instance.stop.assert_called_once()

    assert not ingress_queue.empty()
    path, value = ingress_queue.get_nowait()
    assert path == "Shop.resources.packers.current_cap"
    assert value == 5


@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_kafka_ingress_logs_and_skips_bad_messages(MockConsumer, caplog):
    """A message that is not JSON, uses the old keys or has a null value is logged and skipped."""
    payloads = [
        b"not json",
        json.dumps({"path_id": "Shop.arrival.orders.rate", "value": 2}).encode(),
        json.dumps({"path": "Shop.arrival.orders.rate", "value": None}).encode(),
        json.dumps({"path": "Shop.arrival.orders.rate", "value": 3}).encode(),
    ]

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            for payload in payloads:
                msg = MagicMock(topic="sim-config", value=payload)
                yield msg
            await asyncio.sleep(999)

    MockConsumer.return_value = FakeConsumer()
    ingress_queue = queue.Queue()
    ingress = KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092")

    with caplog.at_level("WARNING", logger="desflow.connectors.ingress.kafka"):
        task = asyncio.create_task(ingress.run(ingress_queue))
        # The valid message is the last, so the three before it have been read
        await _wait_until(lambda: not ingress_queue.empty())
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert ingress_queue.get_nowait() == ("Shop.arrival.orders.rate", 3)
    assert ingress_queue.empty()
    skipped = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(skipped) == 3
    assert all(m.startswith("Skipped a Kafka ingress message") for m in skipped)
    assert "renamed to path and value in desflow 0.21.0" in skipped[1]


@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_kafka_ingress_skips_a_payload_that_is_not_glue_framed(
    MockConsumer, caplog
):
    """A GlueAvroDeserializer rejects JSON bytes, and the ingress logs and skips them."""

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            yield MagicMock(topic="sim-config", value=b'{"path": "A", "value": 1}')
            await asyncio.sleep(999)

    MockConsumer.return_value = FakeConsumer()
    ingress_queue = queue.Queue()
    with patch("boto3.client"):
        deserializer = GlueAvroDeserializer(registry_name="probe")
    ingress = KafkaIngress(
        topic="sim-config",
        bootstrap_servers="localhost:9092",
        default_deserializer=deserializer,
    )

    with caplog.at_level("WARNING", logger="desflow.connectors.ingress.kafka"):
        task = asyncio.create_task(ingress.run(ingress_queue))
        await _wait_until(lambda: any(r.levelname == "WARNING" for r in caplog.records))
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert ingress_queue.empty()
    skipped = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(skipped) == 1
    assert skipped[0].startswith("Skipped a Kafka ingress message")
    assert "Invalid header version byte" in skipped[0]


@pytest.mark.parametrize(
    ("status", "code", "ends"),
    [
        pytest.param(403, 40301, True, id="registry-403"),
        pytest.param(400, "AccessDeniedException", True, id="glue-access-denied"),
        pytest.param(404, 40403, False, id="registry-404"),
    ],
)
@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_a_registry_that_refuses_access_ends_the_source(
    MockConsumer, caplog, status, code, ends
):
    """A registry that refuses the source's credentials or access ends the task with
    that error, so the messages after it are not read and lost one by one. Any other
    registry error skips its message, and the next one is applied."""
    from desflow.connectors._schema_registry.errors import SchemaRegistryError

    payloads = [b"refused", b'{"path": "A", "value": 1}']

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            for payload in payloads:
                yield MagicMock(topic="sim-config", value=payload)
            await asyncio.sleep(999)

    def deserialize(topic, value):
        if value == b"refused":
            raise SchemaRegistryError(status, code, "refused")
        return json.loads(value)

    deserializer = MagicMock()
    deserializer.deserialize.side_effect = deserialize
    consumer = MockConsumer.return_value = FakeConsumer()
    ingress_queue = queue.Queue()
    ingress = KafkaIngress(
        topic="sim-config",
        bootstrap_servers="localhost:9092",
        default_deserializer=deserializer,
        topic_policy="manual",
    )

    task = asyncio.create_task(ingress.run(ingress_queue))
    await _wait_until(lambda: task.done() or not ingress_queue.empty())
    task.cancel()
    outcome = (await asyncio.gather(task, return_exceptions=True))[0]

    if ends:
        assert isinstance(outcome, SchemaRegistryError)
        assert ingress_queue.empty()
        consumer.stop.assert_awaited_once()
        assert MockConsumer.call_count == 1
        assert "Skipped a Kafka ingress message" not in caplog.text
    else:
        assert ingress_queue.get_nowait() == ("A", 1)
        assert "Skipped a Kafka ingress message" in caplog.text


@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
async def test_a_temporary_registry_error_keeps_the_message_for_a_retry(
    MockConsumer, caplog, monkeypatch
):
    """A registry that fails for a time (a 503 status) keeps the message, which is
    decoded again after a wait, so the change is applied and the one after it
    follows in order."""
    from desflow.connectors._schema_registry.errors import SchemaRegistryError

    monkeypatch.setattr(
        "desflow.connectors.ingress.kafka.FIRST_DELAY", 0.01, raising=False
    )

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            for payload in (b"10", b"20"):
                yield MagicMock(topic="sim-config", value=payload)
            await asyncio.sleep(999)

    calls = []

    def deserialize(topic, value):
        calls.append(value)
        if len(calls) == 1:
            raise SchemaRegistryError(503, 50003, "Service Unavailable")
        return {"path": "A", "value": int(value)}

    deserializer = MagicMock()
    deserializer.deserialize.side_effect = deserialize
    MockConsumer.return_value = FakeConsumer()
    ingress_queue = queue.Queue()
    ingress = KafkaIngress(
        topic="sim-config",
        bootstrap_servers="localhost:9092",
        default_deserializer=deserializer,
        topic_policy="manual",
    )

    task = asyncio.create_task(ingress.run(ingress_queue))
    await _wait_until(lambda: ingress_queue.qsize() >= 2)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert [ingress_queue.get_nowait() for _ in range(2)] == [("A", 10), ("A", 20)]
    assert calls == [b"10", b"10", b"20"]
    assert "Skipped a Kafka ingress message" not in caplog.text


def test_topic_policy_creates_the_control_topic(mock_admin, caplog):
    """`create`, the default, asks for the control topic by name. A mapping gives
    it its settings, `drop_and_create` deletes it first, and `manual` touches
    nothing. The consumer's security settings reach the admin client, without its
    `client_id`, which the admin connector sets. A failure is logged."""
    from desflow.connectors.ingress.kafka import KafkaIngress

    KafkaIngress(
        "cfg", "b:9092", security_protocol="SSL", fetch_max_bytes=5, client_id="me"
    )._create_topic()

    mock_admin.assert_called_once_with(
        bootstrap_servers="b:9092", security_protocol="SSL"
    )
    mock_admin.return_value.create_topics.assert_called_once_with(
        topics_config=[{"name": "cfg"}]
    )

    mock_admin.reset_mock()
    KafkaIngress(
        "cfg",
        "b:9092",
        topic_policy={
            "mode": "drop_and_create",
            "partitions": 3,
            "topic_configs": {"a": 1},
        },
    )._create_topic()
    mock_admin.return_value.drop_topics.assert_called_once_with(["cfg"])
    mock_admin.return_value.create_topics.assert_called_once_with(
        topics_config=[{"name": "cfg", "partitions": 3, "topic_configs": {"a": "1"}}]
    )

    mock_admin.reset_mock()
    KafkaIngress("cfg", "b:9092", topic_policy="manual")._create_topic()
    mock_admin.return_value.create_topics.assert_not_called()
    mock_admin.return_value.drop_topics.assert_not_called()

    mock_admin.side_effect = RuntimeError("no broker")
    with caplog.at_level("WARNING"):
        KafkaIngress("cfg", "b:9092")._create_topic()
    assert "Could not create the topic cfg: no broker" in caplog.text


def test_topic_policy_refuses_a_wrong_word_at_construction():
    from desflow.connectors.ingress.kafka import KafkaIngress

    with pytest.raises(ValueError, match="mode 'never' is not one of"):
        KafkaIngress("cfg", "b:9092", topic_policy="never")


def test_ssl_and_token_provider_reach_the_consumer_settings():
    """`ssl` and `sasl_oauth_token_provider` are built as the Kafka egress builds
    them and passed on to the consumer."""
    import types
    import sys

    from desflow.connectors.ingress.kafka import KafkaIngress

    with patch("aiokafka.helpers.create_ssl_context", return_value="CONTEXT") as create:
        ingress = KafkaIngress("cfg", "b:9092", ssl={"cafile": "ca.pem"})
    create.assert_called_once_with(cafile="ca.pem")
    assert ingress.kwargs["ssl_context"] == "CONTEXT"

    signer = types.ModuleType("aws_msk_iam_sasl_signer")
    signer.MSKAuthTokenProvider = MagicMock()
    with patch.dict(sys.modules, {"aws_msk_iam_sasl_signer": signer}):
        ingress = KafkaIngress(
            "cfg",
            "b:9092",
            sasl_oauth_token_provider={"type": "aws_msk_iam", "region": "eu-west-1"},
        )
    assert hasattr(ingress.kwargs["sasl_oauth_token_provider"], "token")
    with patch.dict(sys.modules, {"aws_msk_iam_sasl_signer": None}):
        with pytest.raises(ImportError, match="aws-msk-iam-sasl-signer-python"):
            KafkaIngress(
                "cfg",
                "b:9092",
                sasl_oauth_token_provider={"type": "aws_msk_iam", "region": "r"},
            )
