from desflow.core.sinks import Sink
import asyncio
import json
import queue
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.errors import (
    InvalidReplicationFactorError,
    NotControllerError,
    TopicAlreadyExistsError,
)

from desflow.connectors.admin.kafka import KafkaAdminConnector
from desflow.connectors.egress.base import BaseEgress
from desflow.builder.context import SimulationContext
from desflow.connectors.ingress.kafka import KafkaIngress


def test_admin_process_telemetry():
    """Verify the admin connector properly routes telemetry messages to _vitals."""
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    data = {
        "path_id": "Shop.packers.utilization",
        "value": 99.9,
        "timestamp": "2023-01-01T00:00:00",
    }
    admin._process_message(data)
    assert admin.get_vitals()["Shop.packers.utilization"] == 99.9


def test_admin_process_events_and_pruning():
    """Verify the admin connector routes events and properly prunes old tasks."""
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092", max_tasks=2)

    # Send 3 tasks for the same service
    for i in range(3):
        data = {
            "key": f"task-{i}",
            "value": {
                "path_id": "Shop.service.packing",
                "sim_id": "Shop",
                "service": "packing",
                "status": "started",
            },
            "timestamp": f"2023-01-01T00:00:0{i}",
        }
        admin._process_message(data)

    state = admin.get_state()
    service_tasks = state["Shop"]["packing"]

    # Since max_tasks is 2, task-0 should have been pruned to prevent memory leaks
    assert len(service_tasks) == 2
    assert "task-0" not in service_tasks
    assert "task-1" in service_tasks
    assert "task-2" in service_tasks


@pytest.mark.parametrize(
    "body",
    [
        {"path_id": "Shop.service.pack", "status": "started"},
        {"path_id": ["Shop"], "value": 1},
        {"key": ["a"], "value": {"status": "x", "sim_id": "s", "service": "v"}},
        {"key": "a", "value": {"status": ["x"], "sim_id": "s", "service": "v"}},
        {"key": "a", "value": {"status": "x", "sim_id": {"s": 1}, "service": "v"}},
    ],
    ids=["no-value", "list-path-id", "list-key", "list-status", "mapping-sim-id"],
)
def test_a_message_the_connector_cannot_use_is_passed_over(body):
    """The value alone of an event whose payload has a `path_id`, and a message with a
    list for a key, a status or a name, change neither vitals nor state."""
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")

    admin._process_message(body)

    assert admin.get_vitals() == {} and admin.get_state() == {}


def test_a_status_record_with_only_a_path_id_is_filed_by_its_path_id():
    """A record written before status records named their sim id and service is
    filed under the two sides of the last `.service.` in its path_id."""
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    admin._process_message(
        {
            "key": "o-1",
            "timestamp": "t1",
            "value": {"status": "queued", "path_id": "S.a.service.pack"},
        }
    )
    admin._process_message({"key": "o-1", "timestamp": "t2", "value": {"n": 1}})
    assert admin.get_state()["S.a"]["pack"]["o-1"] == {
        "queued": "t1",
        "finished": "t2",
    }


@pytest.mark.asyncio
@patch("desflow.connectors.admin.kafka.AIOKafkaConsumer")
async def test_collect_data_passes_over_a_value_it_cannot_read(MockConsumer):
    """A null value (a tombstone `KafkaEgress` writes), JSON `null` and a value that is
    not JSON change nothing, and the loop reads the events after them."""

    def event(key):
        body = {
            "key": key,
            "value": {
                "status": "started",
                "path_id": "S.service.pack",
                "sim_id": "S",
                "service": "pack",
            },
        }
        return types.SimpleNamespace(value=json.dumps(body).encode())

    messages = [
        event("a"),
        *(types.SimpleNamespace(value=v) for v in (None, b"null", b"\x00avro")),
        event("c"),
    ]

    async def read():
        for message in messages:
            yield message

    consumer = MockConsumer.return_value
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.__aiter__ = lambda self: read()
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    await admin.collect_data(["events"])
    assert sorted(admin.get_state()["S"]["pack"]) == ["a", "c"]
    assert admin.get_vitals() == {}


class _Capture(BaseEgress):
    """Keeps every record a run sends to its sinks."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


@pytest.mark.asyncio
@patch("desflow.connectors.admin.kafka.AIOKafkaConsumer")
async def test_collect_data_files_events_under_names_that_hold_a_dot(
    MockConsumer, tmp_path
):
    """The events a run with the sim id `Shop.v2` and the services `packing` and
    `pack.big` writes, as `KafkaEgress` sends them, are filed under the whole names,
    beside those of a sim id `Shop`."""

    def events(sim_id):
        path = tmp_path / f"{sim_id}.yaml"
        path.write_text(
            f"simulation: {{sim_id: {sim_id}, factor: 0, random_seed: 1}}\n"
            "services:\n"
            "  packing: {dist: constant, value: 1}\n"
            "  pack.big: {dist: constant, value: 2}\n"
            "arrivals: {a: {initial: 1, spawn: pack}}\n"
            "tasks:\n"
            "  pack: {service: packing, next: big, payload: {e: 1}}\n"
            "  big: {service: pack.big, payload: {e: 2}}\n"
            "run: {until: 10}\n"
        )
        app = SimulationContext.from_yaml(path)
        capture = _Capture()
        app._sinks.ingress = []
        app._sinks.egress = [Sink(capture)]
        app.run()
        return [
            types.SimpleNamespace(
                value=json.dumps(
                    {k: v for k, v in record.items() if k != "stream_type"},
                    default=str,
                ).encode()
            )
            for record in capture.records
            if record.get("stream_type") == "event"
        ]

    messages = [*events("Shop.v2"), *events("Shop")]

    async def read():
        for message in messages:
            yield message

    consumer = MockConsumer.return_value
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.__aiter__ = lambda self: read()
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    await admin.collect_data(["events"])
    state = admin.get_state()
    assert {sim: sorted(services) for sim, services in state.items()} == {
        "Shop.v2": ["pack.big", "packing"],
        "Shop": ["pack.big", "packing"],
    }


@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
@patch("desflow.connectors.admin.kafka.AIOKafkaProducer")
async def test_send_config_message_is_read_by_kafka_ingress(MockProducer, MockConsumer):
    """send_config writes {"path": ..., "value": ...}, and KafkaIngress reads it back."""
    producer = MockProducer.return_value
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock()

    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    await admin.send_config("sim-config", "Shop.resources.packers.current_cap", 4)

    topic, payload = producer.send_and_wait.call_args.args
    assert topic == "sim-config"
    assert json.loads(payload) == {
        "path": "Shop.resources.packers.current_cap",
        "value": 4,
    }

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            yield MagicMock(topic=topic, value=payload)
            await asyncio.sleep(999)

    MockConsumer.return_value = FakeConsumer()
    ingress_queue = queue.Queue()
    task = asyncio.create_task(
        # `manual`: no broker here, and a refused connection is slow on Windows
        KafkaIngress(
            topic, bootstrap_servers="localhost:9092", topic_policy="manual"
        ).run(ingress_queue)
    )
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert ingress_queue.get_nowait() == ("Shop.resources.packers.current_cap", 4)


class FakeAdminClient:
    """Stands in for AIOKafkaAdminClient and records what the connector does with it."""

    instances: list = []
    create_error: Exception | None = None
    topic_errors: list = []
    answers: list = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.events: list = []
        FakeAdminClient.instances.append(self)

    async def start(self):
        self.events.append("start")

    async def create_topics(self, new_topics, validate_only=False):
        self.events.append(("create_topics", new_topics, validate_only))
        if FakeAdminClient.create_error:
            raise FakeAdminClient.create_error
        # aiokafka returns the CreateTopics response and raises no error for a topic.
        answers = FakeAdminClient.answers
        return types.SimpleNamespace(
            topic_errors=answers.pop(0) if answers else FakeAdminClient.topic_errors
        )

    async def close(self):
        self.events.append("close")


@pytest.fixture
def fake_admin_client(monkeypatch):
    FakeAdminClient.instances = []
    FakeAdminClient.create_error = None
    FakeAdminClient.topic_errors = []
    FakeAdminClient.answers = []
    monkeypatch.setattr(
        "desflow.connectors.admin.kafka.AIOKafkaAdminClient", FakeAdminClient
    )
    return FakeAdminClient


def _created(client):
    ((_, new_topics, validate_only),) = [
        e for e in client.events if isinstance(e, tuple)
    ]
    return [
        (t.name, t.num_partitions, t.replication_factor) for t in new_topics
    ], validate_only


def test_create_topics_without_a_running_loop(fake_admin_client):
    admin = KafkaAdminConnector("broker:9092", security_protocol="SSL")

    admin.create_topics(
        [{"name": "a", "partitions": 3}, {"name": "b", "replication": 2}]
    )

    (client,) = fake_admin_client.instances
    assert client.kwargs == {
        "bootstrap_servers": "broker:9092",
        "client_id": "sim_admin",
        "security_protocol": "SSL",
    }
    assert _created(client) == ([("a", 3, 1), ("b", 1, 2)], False)
    assert client.events[0] == "start"
    assert client.events[-1] == "close"


@pytest.mark.asyncio
async def test_create_topics_inside_a_running_loop(fake_admin_client):
    KafkaAdminConnector("broker:9092").create_topics([{"name": "a"}])

    (client,) = fake_admin_client.instances
    assert _created(client)[0] == [("a", 1, 1)]


def test_create_topics_ignores_a_topic_that_exists(fake_admin_client):
    fake_admin_client.topic_errors = [("a", TopicAlreadyExistsError.errno, "exists")]

    KafkaAdminConnector("broker:9092").create_topics([{"name": "a"}])

    assert fake_admin_client.instances[0].events[-1] == "close"


def test_create_topics_raises_any_other_topic_error(fake_admin_client):
    fake_admin_client.topic_errors = [
        ("a", 0, None),
        ("b", InvalidReplicationFactorError.errno, "too many"),
    ]

    with pytest.raises(InvalidReplicationFactorError, match="Topic b: too many"):
        KafkaAdminConnector("broker:9092").create_topics([{"name": "a"}])

    assert fake_admin_client.instances[0].events[-1] == "close"


def test_create_topics_names_a_topic_error_that_has_no_message(fake_admin_client):
    """CreateTopics response version 0 has no message in a topic entry."""
    fake_admin_client.topic_errors = [("b", InvalidReplicationFactorError.errno)]

    with pytest.raises(InvalidReplicationFactorError, match="Topic b: no message"):
        KafkaAdminConnector("broker:9092").create_topics([{"name": "b"}])


def test_create_topics_retries_a_not_controller_error_once_with_a_new_client(
    fake_admin_client,
):
    fake_admin_client.answers = [
        [("a", NotControllerError.errno, "not controller")],
        [("a", 0, None)],
    ]

    KafkaAdminConnector("broker:9092").create_topics([{"name": "a"}])

    first, second = fake_admin_client.instances
    assert first.events[-1] == "close"
    assert second.events[-1] == "close"
    assert _created(first) == _created(second)


def test_create_topics_raises_a_second_not_controller_error(fake_admin_client):
    fake_admin_client.topic_errors = [("a", NotControllerError.errno, "not controller")]

    with pytest.raises(NotControllerError, match="Topic a: not controller"):
        KafkaAdminConnector("broker:9092").create_topics([{"name": "a"}])

    assert len(fake_admin_client.instances) == 2


def test_create_topics_closes_the_client_when_it_fails(fake_admin_client):
    fake_admin_client.create_error = RuntimeError("no broker")

    with pytest.raises(RuntimeError, match="no broker"):
        KafkaAdminConnector("broker:9092").create_topics([{"name": "a"}])

    assert fake_admin_client.instances[0].events[-1] == "close"


def test_create_topics_refuses_a_setting_the_admin_client_lacks():
    admin = KafkaAdminConnector("broker:9092", ssl_cafile="ca.pem")

    with pytest.raises(TypeError, match="ssl_cafile"):
        admin.create_topics([{"name": "a"}])
