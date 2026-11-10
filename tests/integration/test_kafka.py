import asyncio
import json
import queue
import pytest
from aiokafka import AIOKafkaConsumer

from desflow.connectors.egress.kafka import KafkaEgress
from desflow.connectors.admin.kafka import KafkaAdminConnector

pytestmark = [pytest.mark.integration, pytest.mark.services]


@pytest.mark.asyncio
async def test_kafka_egress_and_admin(kafka_container):
    """
    Tests that KafkaEgress successfully writes to a broker,
    and KafkaAdminConnector successfully creates topics and reads them.
    """
    bootstrap_servers = kafka_container

    # 1. Create Topics using Admin Connector
    admin = KafkaAdminConnector(bootstrap_servers)
    admin.create_topics([{"name": "test-events", "partitions": 1}])

    # 2. Write Data using KafkaEgress
    egress = KafkaEgress(bootstrap_servers, event_topic="test-events")
    q = queue.Queue()

    mock_batch = [
        {
            "stream_type": "event",
            "key": "order-1",
            "value": {"order_id": 1, "amount": 100.0},
        },
    ]

    q.put(mock_batch)
    task = asyncio.create_task(egress.run(q))
    await asyncio.sleep(2.0)
    task.cancel()

    # 3. Read Data using a standard consumer to verify it hit the broker
    consumer = AIOKafkaConsumer(
        "test-events",
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id="test-group",
    )

    await consumer.start()
    try:
        # Use a timeout to prevent hanging if it fails
        msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
        payload = json.loads(msg.value.decode("utf-8"))
        assert payload["key"] == "order-1"
        assert payload["value"]["amount"] == 100.0
    finally:
        await consumer.stop()


async def _read_messages(bootstrap_servers, topic, count, timeout=30.0):
    """Reads `count` messages from the start of `topic`."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=_name("group"),
    )
    await consumer.start()
    try:
        messages = []
        while len(messages) < count:
            messages.append(await asyncio.wait_for(consumer.getone(), timeout))
        return messages
    finally:
        await consumer.stop()


async def _publish(egress, batches, topics, bootstrap_servers, count):
    """Runs `egress` over `batches` and reads `count` messages from each topic."""
    q = queue.Queue()
    for batch in batches:
        q.put(batch)
    task = asyncio.create_task(egress.run(q))
    try:
        # A consumer asking for a missing topic makes the broker create it with its
        # defaults, so wait until the sink has made the topic
        for _ in range(150):
            if set(topics) <= set(await _topic_names(bootstrap_servers)):
                break
            await asyncio.sleep(0.1)
        return {
            topic: await _read_messages(bootstrap_servers, topic, count)
            for topic in topics
        }
    finally:
        task.cancel()


async def _partitions(bootstrap_servers, topic):
    """The number of partitions the broker holds for `topic`."""
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        description = (await admin.describe_topics([topic]))[0]
        return len(description["partitions"])
    finally:
        await admin.close()


def _name(prefix):
    import uuid

    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.mark.asyncio
async def test_envelope_false_writes_the_payload_alone(kafka_container):
    """With `envelope` off the message is the event's value, keyed by the event key."""
    topic = _name("env")
    egress = KafkaEgress(kafka_container, event_topic=topic)
    egress.envelope = False
    got = await _publish(
        egress,
        [[{"stream_type": "event", "key": "o-1", "value": {"order_id": 1}}]],
        [topic],
        kafka_container,
        1,
    )
    message = got[topic][0]
    assert json.loads(message.value) == {"order_id": 1}
    assert message.key == b"o-1"


def _tombstone_yaml(tmp_path, bootstrap_servers, topic):
    """A blueprint whose second change of customer `c-7` is a delete: its payload
    gives null, and its Kafka sink has `envelope: false`."""
    from desflow import SimulationContext

    path = tmp_path / "cdc.yaml"
    path.write_text(
        json.dumps(
            {
                "simulation": {"sim_id": "Cdc", "factor": 0, "random_seed": 3},
                "arrivals": {
                    "changes": {"dist": "constant", "value": 1, "spawn": "change"}
                },
                "tasks": {
                    "change": {
                        "vars": {"n": {"gen": "sequence"}},
                        "key": "c-7",
                        "payload": {
                            "gen": "when",
                            "condition": "n == 1",
                            "then": None,
                            "else": {"gen": "object", "fields": {"id": 7}},
                        },
                    }
                },
                "egress": [
                    {
                        "type": "Kafka",
                        "when": "events",
                        "envelope": False,
                        "config": {
                            "bootstrap_servers": bootstrap_servers,
                            "event_topic": topic,
                        },
                    }
                ],
                "run": {"until": 2.5},
            }
        ),
        encoding="utf-8",
    )
    SimulationContext.from_yaml(path).run()


@pytest.mark.asyncio
async def test_a_null_value_is_a_tombstone_with_envelope_false(
    kafka_container, tmp_path
):
    """A CDC delete: with `envelope` off an event whose value is null is a message
    on the event topic with the key and a null value. The Builder and Low-level
    forms reach the same sink call, which the unit tests check."""
    topic = _name("cdc")
    # Read after the run: a consumer started first would race the sink to the topic
    await asyncio.to_thread(_tombstone_yaml, tmp_path, kafka_container, topic)
    insert, delete = await _read_messages(kafka_container, topic, 2)
    assert (insert.key, json.loads(insert.value)) == (b"c-7", {"id": 7})
    assert (delete.key, delete.value) == (b"c-7", None)


# The two projects below send these messages with their own generators, which this
# test cannot run. Their output is fixed here from the code, as read on 2 Nov 2026:
#
# game-leaderboard (benchtop `a72fbf9`, `leaderboard/stores/kafka.py`,
# `leaderboard/simulation/run.py`): `ScoreSerializer` encodes `data["value"]` alone
# with `AVRO_SCHEMA` through `ConfluentAvroSerializer`, and `publish_event(user_id,
# event)` makes the key the user id.
#
# product-recommender (benchtop `a72fbf9`, `recommender/stores/kafka.py`):
# `FeedbackProducer` encodes the `FeedbackEvent` fields alone with `FEEDBACK_SCHEMA`
# through confluent-kafka's `AvroSerializer`, and the Kafka key is
# `product_id.encode("utf-8")`. Both frame the Avro body with the Confluent prefix:
# the byte 0 and the 4-byte schema id.
GAME_SCORE_SCHEMA = {
    "type": "record",
    "name": "ScoreEvent",
    "namespace": "game",
    "fields": [
        {"name": "user_id", "type": "string"},
        {"name": "team_id", "type": "string"},
        {"name": "team_name", "type": "string"},
        {"name": "score", "type": "int"},
        {"name": "event_time_millis", "type": "long"},
        {"name": "event_type", "type": "string"},
    ],
}
GAME_SCORE = {
    "user_id": "USR-000123",
    "team_id": "1234567890",
    "team_name": "Amber-Koala",
    "score": 7,
    "event_time_millis": 1767225600000,
    "event_type": "normal",
}
FEEDBACK_SCHEMA = {
    "namespace": "me.jaehyeon",
    "type": "record",
    "name": "FeedbackEvent",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "product_id", "type": "string"},
        {"name": "reward", "type": "int"},
        {"name": "context_vector", "type": {"type": "array", "items": "double"}},
        {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    ],
}
FEEDBACK = {
    "event_id": "evt-1",
    "product_id": "P-0042",
    "reward": 1,
    "context_vector": [0.5, 0.25],
    "timestamp": 1767225600000,
}


async def _project_message(
    kafka_container, schema_registry, tmp_path, schema, value, key
):
    """Runs a blueprint that finishes `value` with `key_field: key`, a `confluent_avro`
    serializer for `schema` and `envelope: false`, and returns the message and the
    bytes the project's serializer writes for the same value and schema."""
    import io

    import fastavro

    from desflow import SimulationContext

    topic = _name("project")
    (tmp_path / "value.avsc").write_text(json.dumps(schema), encoding="utf-8")
    path = tmp_path / "project.yaml"
    path.write_text(
        json.dumps(
            {
                "simulation": {"sim_id": "Project", "factor": 0, "random_seed": 3},
                "arrivals": {
                    "events": {"dist": "constant", "value": 2, "spawn": "send"}
                },
                "tasks": {"send": {"payload": value, "key_field": key}},
                "egress": [
                    {
                        "type": "Kafka",
                        "when": "events",
                        "envelope": False,
                        "config": {
                            "bootstrap_servers": kafka_container,
                            "event_topic": topic,
                            "serializer": {
                                "type": "confluent_avro",
                                "registry_url": schema_registry,
                                "schema_file": (tmp_path / "value.avsc").as_posix(),
                            },
                        },
                    }
                ],
                "run": {"until": 5},
            }
        ),
        encoding="utf-8",
    )
    # Read after the run: a consumer started first would race the sink to the topic
    await asyncio.to_thread(SimulationContext.from_yaml(path).run)
    [message] = await _read_messages(kafka_container, topic, 1)
    body = io.BytesIO()
    fastavro.schemaless_writer(body, fastavro.parse_schema(schema), value)
    schema_id = int.from_bytes(message.value[1:5], "big")
    expected = b"\x00" + schema_id.to_bytes(4, "big") + body.getvalue()
    return message, expected, schema_id


@pytest.mark.asyncio
async def test_envelope_false_matches_the_project(
    kafka_container, schema_registry, tmp_path
):
    """BT-GL9: the message is the Avro of the score alone, with the Confluent prefix
    and the user id as its key, byte for byte what game-leaderboard's
    `ScoreSerializer` writes for the same event."""
    import urllib.request

    message, expected, schema_id = await _project_message(
        kafka_container,
        schema_registry,
        tmp_path,
        GAME_SCORE_SCHEMA,
        GAME_SCORE,
        "user_id",
    )

    assert message.key == b"USR-000123"
    assert message.value == expected
    with urllib.request.urlopen(
        f"{schema_registry}/schemas/ids/{schema_id}", timeout=5
    ) as response:
        assert json.loads(json.loads(response.read())["schema"]) == GAME_SCORE_SCHEMA


@pytest.mark.asyncio
async def test_key_field_and_avro_match_the_product_recommender(
    kafka_container, schema_registry, tmp_path
):
    """BT-PR9: the feedback is the Avro of the event alone, keyed by the clicked
    product, byte for byte what product-recommender's `FeedbackProducer` writes."""
    message, expected, _ = await _project_message(
        kafka_container,
        schema_registry,
        tmp_path,
        FEEDBACK_SCHEMA,
        FEEDBACK,
        "product_id",
    )

    assert message.key == b"P-0042"
    assert message.value == expected


@pytest.mark.asyncio
async def test_topic_field_routes(kafka_container):
    """`__topic__` in an event's value is its topic, and the key is not written."""
    first, second = _name("route-a"), _name("route-b")
    egress = KafkaEgress(kafka_container, event_topic=_name("fallback"))
    batch = [
        {"stream_type": "event", "key": "a", "value": {"__topic__": first, "n": 1}},
        {"stream_type": "event", "key": "b", "value": {"__topic__": second, "n": 2}},
    ]
    got = await _publish(egress, [batch], [first, second], kafka_container, 1)
    for topic, n in ((first, 1), (second, 2)):
        value = json.loads(got[topic][0].value)["value"]
        assert value == {"n": n}


async def _topic_names(bootstrap_servers):
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        return set(await admin.list_topics())
    finally:
        await admin.close()


async def _topic_setting(bootstrap_servers, topic, name):
    """The value the broker holds for the topic setting `name`."""
    from aiokafka.admin import AIOKafkaAdminClient
    from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        resource = ConfigResource(ConfigResourceType.TOPIC, topic)
        [response] = await admin.describe_configs([resource])
        for _, _, _, _, entries in response.resources:
            for entry in entries:
                if entry[0] == name:
                    return entry[1]
    finally:
        await admin.close()


@pytest.mark.asyncio
async def test_topic_policy_modes(kafka_container):
    """`create` makes a missing topic with the policy's partitions and settings;
    `drop_and_create` empties a topic that holds a message; `manual` creates none."""
    topic = _name("policy")
    egress = KafkaEgress(
        kafka_container,
        event_topic=topic,
        topic_policy={"partitions": 2, "topic_configs": {"retention.ms": 600000}},
    )
    batch = [{"stream_type": "event", "key": "a", "value": {"n": 1}}]
    await _publish(egress, [batch], [topic], kafka_container, 1)
    assert await _partitions(kafka_container, topic) == 2
    assert await _topic_setting(kafka_container, topic, "retention.ms") == "600000"

    again = KafkaEgress(
        kafka_container, event_topic=topic, topic_policy="drop_and_create"
    )
    two = [{"stream_type": "event", "key": "b", "value": {"n": 2}}]
    q = queue.Queue()
    q.put(two)
    task = asyncio.create_task(again.run(q))
    try:
        # Each read is a new consumer, because one started before the drop would keep
        # the deleted topic. Until the drop, the read gets the old message.
        values = []
        for _ in range(10):
            try:
                got = await _read_messages(kafka_container, topic, 1, timeout=5.0)
            except TimeoutError:
                # The topic is new and the message is not there yet
                continue
            values = [json.loads(m.value)["value"] for m in got]
            if values == [{"n": 2}]:
                break
            await asyncio.sleep(1)
    finally:
        task.cancel()
    assert values == [{"n": 2}]

    # `manual` creates nothing: with no record to send, the topic is still absent
    missing = _name("absent")
    manual = KafkaEgress(kafka_container, event_topic=missing, topic_policy="manual")
    task = asyncio.create_task(manual.run(queue.Queue()))
    await asyncio.sleep(5)
    task.cancel()
    assert missing not in await _topic_names(kafka_container)


@pytest.mark.asyncio
async def test_topics_settings_per_topic(kafka_container):
    """`topics` gives a topic its own partitions and settings over the policy's."""
    topic = _name("own")
    egress = KafkaEgress(
        kafka_container,
        event_topic=topic,
        topics={topic: {"partitions": 2}},
    )
    batch = [{"stream_type": "event", "key": "a", "value": {"n": 1}}]
    await _publish(egress, [batch], [topic], kafka_container, 1)
    assert await _partitions(kafka_container, topic) == 2


@pytest.mark.asyncio
async def test_ingress_topic_policy(kafka_container):
    """An ingress creates its topic when it is missing, so a consumer started
    before the producer does not fail."""
    from desflow.connectors.ingress.kafka import KafkaIngress

    topic = _name("control")
    ingress = KafkaIngress(topic, kafka_container, topic_policy={"partitions": 2})
    task = asyncio.create_task(ingress.run(queue.Queue()))
    try:
        # Asking for a missing topic's partitions would make the broker create it
        for _ in range(100):
            if topic in await _topic_names(kafka_container):
                break
            await asyncio.sleep(0.3)
        assert await _partitions(kafka_container, topic) == 2
    finally:
        task.cancel()


@pytest.mark.asyncio
async def test_admin_files_events_of_a_run_under_their_own_names(
    kafka_container, tmp_path
):
    """A run whose sim id and service name hold `.service.` writes its
    events to a broker, and the admin connector files each under the sim id and the
    service, with `finished` beside `queued` and `started`, and nothing under
    `unknown`."""
    from desflow import SimulationContext

    topic = "it-admin-names"
    admin = KafkaAdminConnector(kafka_container)
    admin.create_topics([{"name": topic, "partitions": 1}])
    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: S.service.v2, factor: 0, random_seed: 1}\n"
        "resources: {desk: {current_cap: 1}}\n"
        "services: {front.service.desk: {dist: constant, value: 1}}\n"
        "arrivals: {a: {dist: constant, value: 2, spawn: serve, max_events: 3}}\n"
        "tasks:\n"
        "  serve: {service: front.service.desk, resource: desk, payload: {n: 1}}\n"
        "egress:\n"
        f"  - {{type: Kafka, config: {{bootstrap_servers: '{kafka_container}', "
        f"event_topic: {topic}}}}}\n"
        "run: {until: 20}\n"
    )
    context = SimulationContext.from_yaml(path)
    await asyncio.to_thread(context.run)

    reader = KafkaAdminConnector(kafka_container, max_tasks=100)
    task = asyncio.create_task(reader.collect_data([topic], "earliest"))
    state = {}
    for _ in range(100):
        await asyncio.sleep(0.2)
        state = reader.get_state()
        found = state.get("S.service.v2", {}).get("front.service.desk", {})
        if len(found) == 3 and all("finished" in s for s in found.values()):
            break
    task.cancel()
    assert "unknown" not in state, state
    tasks = state["S.service.v2"]["front.service.desk"]
    assert len(tasks) == 3, tasks
    for statuses in tasks.values():
        assert {"queued", "started", "finished"} <= set(statuses), statuses
