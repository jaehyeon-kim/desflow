"""The How-To pages of Records and Kafka run, and show what they say.

The scripts name a broker, a registry or a connector's service. A test runs each form
unpaced with the Kafka producer replaced by one that keeps its messages, as
how_to_support.py sets out, and asserts what the page shows.
"""

import asyncio
import collections
import io
import json
import re
import runpy
import sys
from pathlib import Path

import pytest
from how_to_support import (
    GUIDES,
    YAML,
    FakeConsumer,
    Registry,
    copy_into,
    created_topics,
    events,
    kafka_messages,
    load_from as _load_from,
)

from desflow import SimulationContext

HOW_TO = Path(__file__).resolve().parents[2] / "docs" / "how-to" / "records-and-kafka"


def _routing_forms(tmp_path, monkeypatch, blueprint, router=False):
    """The messages of the three forms of the Route Records to Topics shop."""
    copy_into(
        tmp_path,
        GUIDES / "complex_routing.py",
        GUIDES / "complex_routing_low_level.py",
        blueprint,
        YAML / "complex_routing_logic.py",
    )
    monkeypatch.chdir(tmp_path)
    forms = {}
    with kafka_messages() as (sent, admin):
        SimulationContext.from_yaml(tmp_path / blueprint.name).run(until=60)
        forms["yaml"] = (list(sent), created_topics(admin))
        sent.clear()
        admin.reset_mock()
        builder = _load_from(tmp_path, "complex_routing")
        egress = builder.router_egress() if router else builder.payload_egress()
        builder.build(egress).run(until=60)
        forms["builder"] = (list(sent), created_topics(admin))
        sent.clear()
        admin.reset_mock()
        low_level = _load_from(tmp_path, "complex_routing_low_level")
        egress = low_level.router_egress() if router else low_level.payload_egress()
        low_level.run(60, egress)
        forms["low_level"] = (list(sent), created_topics(admin))
    return forms


def test_payload_topic_sends_each_record_to_its_topic(tmp_path, monkeypatch):
    """`__topic__` names the topic, and it is not in the message. Telemetry goes to the
    telemetry topic and the other events to the event topic. All three forms send the
    same messages and create the three topics."""
    forms = _routing_forms(tmp_path, monkeypatch, YAML / "complex_routing.yaml")
    wanted = {"shop-events", "shop-telemetry", "system-alarms"}
    for messages, created in forms.values():
        assert created == wanted
        counts = collections.Counter(topic for topic, _, _ in messages)
        assert set(counts) == wanted
        assert counts["system-alarms"] == 2
        alarms = [e for e in events(messages) if e[0] == "system-alarms"]
        assert [(e[1], e[3], e[4]) for e in alarms] == [
            ("orders-10", 9, {"status": "packing_failed"}),
            ("orders-20", 19, {"status": "packing_failed"}),
        ]
        assert all("__topic__" not in e[4] for e in events(messages))
        assert all(t == "shop-telemetry" for t, _, v in messages if "task" not in v)
    assert (
        events(forms["yaml"][0])
        == events(forms["builder"][0])
        == events(forms["low_level"][0])
    )


def test_a_router_sends_the_same_records_to_the_same_topics(tmp_path, monkeypatch):
    """A function that picks the topic gives each message the topic the payload did,
    and the topics it uses are created from `topics`."""
    payload = _routing_forms(tmp_path, monkeypatch, YAML / "complex_routing.yaml")
    router = _routing_forms(
        tmp_path, monkeypatch, YAML / "complex_routing_router.yaml", True
    )
    for form in payload:
        assert router[form][1] == payload[form][1]
        assert [(t, k) for t, k, _ in router[form][0]] == [
            (t, k) for t, k, _ in payload[form][0]
        ]


def _page_output(page, language="text"):
    """The text blocks of a page."""
    text = (HOW_TO / page).read_text(encoding="utf-8")
    return re.findall(rf"```{language}\n(.*?)\n```", text, re.S)


def test_avro_forms_send_the_predictions_as_avro(
    tmp_path, monkeypatch, registry_transport, capsys
):
    """The topic `ml-predictions` carries Avro that fits the model's schema, the other
    topics carry JSON, the schema file is the model's schema, and the three forms send
    the same messages. The read-back prints the first prediction the page shows."""
    fastavro = pytest.importorskip("fastavro")
    pytest.importorskip("dataclasses_avroschema")
    registry = Registry()
    registry_transport(registry)
    copy_into(
        tmp_path,
        GUIDES / "avro_and_pydantic.py",
        GUIDES / "avro_and_pydantic_models.py",
        GUIDES / "avro_and_pydantic_low_level.py",
        GUIDES / "avro_and_pydantic_check.py",
        YAML / "avro_and_pydantic.yaml",
        YAML / "prediction_event.avsc",
    )
    monkeypatch.chdir(tmp_path)
    for name in ("avro_and_pydantic", "avro_and_pydantic_models"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    models = _load_from(tmp_path, "avro_and_pydantic_models")
    schema = json.loads(models.schema_string)
    assert json.loads((tmp_path / "prediction_event.avsc").read_text()) == schema
    parsed = fastavro.parse_schema(schema)

    forms = {}
    with kafka_messages() as (sent, admin):
        yaml_app = SimulationContext.from_yaml(tmp_path / "avro_and_pydantic.yaml")
        serializers = yaml_app._sinks.egress[0].provider.topic_serializers
        assert json.loads(serializers["ml-predictions"]._schema_str) == schema
        yaml_app.run(until=60)
        forms["yaml"] = list(sent)
        sent.clear()
        _load_from(tmp_path, "avro_and_pydantic").app.run(until=60)
        forms["builder"] = list(sent)
        sent.clear()
        module = _load_from(tmp_path, "avro_and_pydantic_low_level")
        env = module.shop([module.egress])
        env.run(until=60)
        env.teardown()
        forms["low_level"] = list(sent)

    def decoded(messages):
        out = []
        for topic, key, value in messages:
            if topic == "ml-predictions":
                assert value[:1] == b"\x00"  # the registry's wire format
                value = fastavro.schemaless_reader(io.BytesIO(value[5:]), parsed)
                value["timestamp"] = None  # the clock time of the run
            elif isinstance(value, dict):
                value = {**value, "timestamp": None}
            out.append((topic, key, value))
        return out

    for messages in forms.values():
        topics = collections.Counter(topic for topic, _, _ in messages)
        assert set(topics) == {"ml-predictions", "sim-events"}
        assert topics["ml-predictions"] == 20
        first = next(m for m in decoded(messages) if m[0] == "ml-predictions")
        assert first[2]["value"] == {
            "event_type": "prediction",
            "task_id": "orders-1",
            "confidence": 0.9,
        }
        assert registry.registered[0]["schema"]
    assert decoded(forms["yaml"]) == decoded(forms["builder"])
    assert decoded(forms["yaml"]) == decoded(forms["low_level"])

    # The read-back decodes a message with the schema the registry holds for it
    check = _load_from(tmp_path, "avro_and_pydantic_check")
    monkeypatch.setattr(check, "AIOKafkaConsumer", FakeConsumer)
    FakeConsumer.messages = [v for t, _, v in forms["yaml"] if t == "ml-predictions"]
    asyncio.run(check.show_first_prediction())
    printed = capsys.readouterr().out.strip()
    assert FakeConsumer.settings["topics"] == ("ml-predictions",)
    shown = _page_output("avro-and-pydantic.md")[0]
    stamp = r"'timestamp': '[^']+'"
    assert re.sub(stamp, "", printed) == re.sub(stamp, "", shown)


def test_json_schema_and_protobuf_forms_send_the_orders_in_two_formats(
    tmp_path, monkeypatch, registry_transport, capsys
):
    """Orders alternate between `orders-json` and `orders-protobuf`; each message reads
    back with the deserializer of its format, the files of the blueprint build the
    serializers the code forms build, and the three forms send the same messages. The
    read-back prints what the page shows."""
    pytest.importorskip("jsonschema")
    pytest.importorskip("google.protobuf")
    registry_transport(Registry())
    copy_into(
        tmp_path,
        GUIDES / "json_schema_and_protobuf.py",
        GUIDES / "json_schema_and_protobuf_serializers.py",
        GUIDES / "json_schema_and_protobuf_low_level.py",
        GUIDES / "shop_record.proto",
        GUIDES / "shop_record_pb2.py",
        YAML / "shop_record.desc",
        YAML / "json_schema_and_protobuf.yaml",
        YAML / "order_record.schema.json",
    )
    monkeypatch.chdir(tmp_path)
    for name in (
        "json_schema_and_protobuf",
        "json_schema_and_protobuf_serializers",
        "json_schema_and_protobuf_low_level",
        "shop_record_pb2",
    ):
        monkeypatch.delitem(sys.modules, name, raising=False)
    serializers = _load_from(tmp_path, "json_schema_and_protobuf_serializers")

    forms = {}
    with kafka_messages() as (sent, admin):
        yaml_app = SimulationContext.from_yaml(
            tmp_path / "json_schema_and_protobuf.yaml"
        )
        built = yaml_app._sinks.egress[0].provider.topic_serializers
        json_made, protobuf_made = built["orders-json"], built["orders-protobuf"]
        assert json.loads(json_made._schema_str) == json.loads(
            serializers.json_serializer._schema_str
        )
        assert protobuf_made._schema_str == serializers.protobuf_serializer._schema_str
        yaml_app.run(until=60)
        forms["yaml"] = list(sent)
        sent.clear()
        _load_from(tmp_path, "json_schema_and_protobuf").app.run(until=60)
        forms["builder"] = list(sent)
        sent.clear()
        module = _load_from(tmp_path, "json_schema_and_protobuf_low_level")
        env = module.shop([module.egress])
        env.run(until=60)
        env.teardown()
        forms["low_level"] = list(sent)

    def orders(messages):
        return [
            (topic, key, {**serializers.read_order(topic, value), "timestamp": None})
            for topic, key, value in messages
            if topic.startswith("orders-")
        ]

    for messages in forms.values():
        topics = collections.Counter(topic for topic, _, _ in messages)
        assert set(topics) == {"orders-json", "orders-protobuf", "sim-events"}
        assert topics["orders-json"] == 10 and topics["orders-protobuf"] == 10
        for topic, key, record in orders(messages):
            number = int(key.split("-")[1])
            assert (topic == "orders-protobuf") == ((number - 1) % 2 == 1)
            shape = str if topic == "orders-protobuf" else int
            assert type(record["value"]["order_id"]) is shape
            assert record["value"]["amount"] == 19.99
    assert (
        orders(forms["yaml"]) == orders(forms["builder"]) == orders(forms["low_level"])
    )

    FakeConsumer.messages = [v for t, _, v in forms["yaml"] if t == "orders-json"][:1]
    monkeypatch.setattr(serializers, "AIOKafkaConsumer", FakeConsumer)
    shown = _page_output("json-schema-and-protobuf.md")[0].splitlines()
    asyncio.run(serializers.show_first_order("orders-json"))
    FakeConsumer.messages = [v for t, _, v in forms["yaml"] if t == "orders-protobuf"][
        :1
    ]
    asyncio.run(serializers.show_first_order("orders-protobuf"))
    printed = capsys.readouterr().out.splitlines()
    stamp = r"'timestamp': '[^']+'"
    assert [re.sub(stamp, "", p) for p in printed] == [
        re.sub(stamp, "", p) for p in shown
    ]


def _security_files(tmp_path):
    copy_into(
        tmp_path,
        GUIDES / "kafka_security.py",
        GUIDES / "kafka_security_connections.py",
        GUIDES / "kafka_security_low_level.py",
        YAML / "kafka_security.yaml",
        YAML / "kafka_security_mtls.yaml",
        YAML / "kafka_security_msk.yaml",
        YAML / "kafka_security_registry.yaml",
        YAML / "prediction_event.avsc",
        GUIDES / "kafka_security_registry.py",
        GUIDES / "kafka_security_registry_low_level.py",
        GUIDES / "avro_and_pydantic_models.py",
        GUIDES / "avro_and_pydantic_low_level.py",
    )
    for name in (
        "avro_and_pydantic_models",
        "avro_and_pydantic_low_level",
        "kafka_security_registry",
        "kafka_security_registry_low_level",
        "kafka_security",
        "kafka_security_connections",
        "kafka_security_low_level",
    ):
        sys.modules.pop(name, None)


def test_sasl_forms_pass_the_credentials_to_the_producer(tmp_path, monkeypatch, capsys):
    """Each form builds its producer with the SASL settings, from the variables, and
    sends the same events. The read-back consumes with the egress's own settings."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9092")
    monkeypatch.setenv("KAFKA_API_KEY", "test-key")
    monkeypatch.setenv("KAFKA_API_SECRET", "test-secret")
    _security_files(tmp_path)
    monkeypatch.chdir(tmp_path)
    forms = {}
    with kafka_messages() as (sent, admin):
        SimulationContext.from_yaml(tmp_path / "kafka_security.yaml").run(until=60)
        forms["yaml"] = list(sent)
        sent.clear()
        connections = _load_from(tmp_path, "kafka_security_connections")
        _load_from(tmp_path, "kafka_security").build(connections.sasl_egress()).run(
            until=60
        )
        forms["builder"] = list(sent)
        sent.clear()
        env = _load_from(tmp_path, "kafka_security_low_level").shop(
            [connections.sasl_egress()]
        )
        env.run(until=60)
        env.teardown()
        forms["low_level"] = list(sent)
        settings = sent.settings
    for producer in settings:
        assert producer["bootstrap_servers"] == "broker.example.com:9092"
        assert producer["security_protocol"] == "SASL_SSL"
        assert producer["sasl_mechanism"] == "PLAIN"
        assert producer["sasl_plain_username"] == "test-key"
        assert producer["sasl_plain_password"] == "test-secret"
    assert len(settings) == 3
    assert {t for t, _, _ in forms["yaml"]} == {"sim-events"}
    assert (
        events(forms["yaml"]) == events(forms["builder"]) == events(forms["low_level"])
    )

    connections = _load_from(tmp_path, "kafka_security_connections")
    monkeypatch.setattr(connections, "AIOKafkaConsumer", FakeConsumer)
    FakeConsumer.messages = [
        json.dumps(value, separators=(",", ":")).encode()
        for topic, _, value in forms["yaml"]
        if topic == "sim-events"
    ]
    asyncio.run(connections.show_events())
    shown = _page_output("kafka-security.md")[0].splitlines()
    stamp = r'"timestamp":"[^"]+"'
    printed = capsys.readouterr().out.splitlines()
    assert [re.sub(stamp, "", p) for p in printed] == [
        re.sub(stamp, "", p) for p in shown
    ]
    assert FakeConsumer.settings["sasl_plain_username"] == "test-key"
    assert FakeConsumer.settings["topics"] == ("sim-events",)


def test_mtls_and_msk_forms_build_the_security_settings(tmp_path, monkeypatch):
    """`ssl` becomes the producer's SSL context, made from the certificate files, and
    `sasl_oauth_token_provider` becomes an MSK IAM token provider, in YAML and in code."""
    import ssl
    import types

    from aiokafka.abc import AbstractTokenProvider
    from tests.known_cases.harness import write_tls_files

    files = write_tls_files(tmp_path)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9093")
    monkeypatch.setenv("KAFKA_CA_FILE", str(files["ca"]))
    monkeypatch.setenv("KAFKA_CERT_FILE", str(files["cert"]))
    monkeypatch.setenv("KAFKA_KEY_FILE", str(files["key"]))
    monkeypatch.setenv("KAFKA_API_KEY", "k")
    monkeypatch.setenv("KAFKA_API_SECRET", "s")
    signer = types.ModuleType("aws_msk_iam_sasl_signer")

    class MSKAuthTokenProvider:
        @staticmethod
        def generate_auth_token(region):
            return f"token-for-{region}", 0

    signer.MSKAuthTokenProvider = MSKAuthTokenProvider
    monkeypatch.setitem(sys.modules, "aws_msk_iam_sasl_signer", signer)
    _security_files(tmp_path)
    monkeypatch.chdir(tmp_path)
    connections = _load_from(tmp_path, "kafka_security_connections")
    builder = _load_from(tmp_path, "kafka_security")
    low_level = _load_from(tmp_path, "kafka_security_low_level")

    def producer_settings(run):
        with kafka_messages() as (sent, _):
            run()
        # every form runs the whole shop: its events reach the topic
        assert {t for t, _, _ in sent} == {"sim-events"}
        assert len(events(sent)) > 3
        return sent.settings[0], events(sent)

    mtls = [
        lambda: SimulationContext.from_yaml(tmp_path / "kafka_security_mtls.yaml").run(
            until=10
        ),
        lambda: builder.mtls_app().run(until=10),
        lambda: _run_env(low_level.mtls_shop()),
    ]
    ran = [producer_settings(run) for run in mtls]
    assert ran[0][1] == ran[1][1] == ran[2][1]
    for settings, _ in ran:
        assert settings["security_protocol"] == "SSL"
        assert isinstance(settings["ssl_context"], ssl.SSLContext)
        assert settings["ssl_context"].get_ca_certs()
    msk = [
        lambda: SimulationContext.from_yaml(tmp_path / "kafka_security_msk.yaml").run(
            until=10
        ),
        lambda: builder.msk_app().run(until=10),
        lambda: _run_env(low_level.msk_shop()),
    ]
    ran = [producer_settings(run) for run in msk]
    assert ran[0][1] == ran[1][1] == ran[2][1]
    for settings, _ in ran:
        assert (settings["security_protocol"], settings["sasl_mechanism"]) == (
            "SASL_SSL",
            "OAUTHBEARER",
        )
        provider = settings["sasl_oauth_token_provider"]
        assert isinstance(provider, AbstractTokenProvider)
        assert asyncio.run(provider.token()) == "token-for-us-east-1"
    assert connections.mtls_egress().producer_config["ssl_context"]
    # KAFKA_CONNECTION picks the cluster the scripts publish to
    monkeypatch.setenv("KAFKA_CONNECTION", "msk")
    assert connections.selected_egress().producer_config["security_protocol"] == (
        "SASL_SSL"
    )
    monkeypatch.setenv("KAFKA_CONNECTION", "mtls")
    assert connections.selected_egress().producer_config["security_protocol"] == "SSL"


def test_registry_forms_pass_the_settings_to_the_registry(
    tmp_path, monkeypatch, registry_transport
):
    """The three forms of the secured-registry step reach the registry with the same
    credentials, headers, timeout and retries, and send the same predictions."""
    import httpx
    from tests.known_cases.harness import write_tls_files

    pytest.importorskip("dataclasses_avroschema")
    files = write_tls_files(tmp_path)
    for name, value in {
        "KAFKA_BOOTSTRAP_SERVERS": "broker.example.com:9092",
        "SCHEMA_REGISTRY_URL": "https://registry.example.com:8081",
        "REGISTRY_USER": "registry-user",
        "REGISTRY_PASSWORD": "registry-password",
        "REGISTRY_CA_FILE": str(files["ca"]),
        "REGISTRY_CERT_FILE": str(files["cert"]),
        "REGISTRY_KEY_FILE": str(files["key"]),
    }.items():
        monkeypatch.setenv(name, value)
    registry = Registry()
    requests = []

    def handler(request):
        requests.append(request)
        return registry(request)

    registry_transport(handler)
    mocked_client = httpx.Client
    proxies = []

    def client(**kwargs):
        # A proxy would bypass the mock transport: keep what the client was given
        proxies.append(kwargs.pop("proxy", None))
        return mocked_client(**kwargs)

    monkeypatch.setattr(httpx, "Client", client)
    _security_files(tmp_path)
    monkeypatch.chdir(tmp_path)
    connections = _load_from(tmp_path, "kafka_security_connections")
    forms = {}
    with kafka_messages() as (sent, _):
        yaml_app = SimulationContext.from_yaml(
            tmp_path / "kafka_security_registry.yaml"
        )
        yaml_serializer = yaml_app._sinks.egress[0].provider.topic_serializers[
            "ml-predictions"
        ]
        yaml_app.run(until=60)
        forms["yaml"] = list(sent)
        sent.clear()
        builder = _load_from(tmp_path, "kafka_security_registry")
        egress = connections.registry_egress()
        builder_serializer = egress.topic_serializers["ml-predictions"]
        builder.build(egress).run(until=60)
        forms["builder"] = list(sent)
        sent.clear()
        # The page runs the Low-level script as a program, so the test does too
        monkeypatch.syspath_prepend(str(tmp_path))
        runpy.run_path(
            str(tmp_path / "kafka_security_registry_low_level.py"), run_name="__main__"
        )
        forms["low_level"] = list(sent)
    assert requests
    assert [p for p in proxies if p] == ["http://proxy.example.com:3128"] * 3
    for request in requests:
        assert request.headers["authorization"].startswith("Basic ")
        assert request.headers["x-team"] == "shop"
    for serializer in (yaml_serializer, builder_serializer):
        client = serializer._client
        assert client._max_retries == 5
        assert client._headers == {"X-Team": "shop"}
        assert client._client.timeout == httpx.Timeout(30)
        assert client._client.auth._auth_header == requests[0].headers["authorization"]
    counts = {name: len(messages) for name, messages in forms.items()}
    assert len(set(counts.values())) == 1 and counts["yaml"] > 20


def _run_env(env):
    """Runs a low-level environment for a few seconds and ends it."""
    env.run(until=10)
    env.teardown()


class _Glue:
    """Registers each definition once and serves its versions back."""

    def __init__(self):
        self.definitions = {}

    def get_schema_by_definition(self, SchemaId, SchemaDefinition):
        version = f"00000000-0000-0000-0000-{len(self.definitions):012d}"
        self.definitions.setdefault(version, SchemaDefinition)
        return {"SchemaVersionId": version, "Status": "AVAILABLE"}

    def get_schema_version(self, SchemaVersionId):
        definition = self.definitions[SchemaVersionId]
        return {
            "SchemaVersionId": SchemaVersionId,
            "Status": "AVAILABLE",
            "DataFormat": "JSON" if definition.startswith("{") else "PROTOBUF",
            "SchemaDefinition": definition,
        }


def test_glue_serializers_round_trip_the_order_record(monkeypatch):
    """The Glue serializers of the Use Schema Serializers page take the same record,
    and the Protobuf one is given the text of shop_record.proto as its schema."""
    pytest.importorskip("boto3")
    pytest.importorskip("fastjsonschema")
    pytest.importorskip("google.protobuf")
    from desflow.connectors.ingress.kafka import (
        GlueJsonSchemaDeserializer,
        GlueProtobufDeserializer,
    )

    glue = _Glue()
    monkeypatch.setattr("boto3.client", lambda service, **kwargs: glue)
    monkeypatch.delitem(
        sys.modules, "json_schema_and_protobuf_serializers", raising=False
    )
    module = _load_from(GUIDES, "json_schema_and_protobuf_serializers")
    glue_json, glue_protobuf = module.glue_serializers()
    record = {
        "key": "orders-1",
        "task": "pack_order",
        "instance": 0,
        "sim_ts": 1.5,
        "timestamp": "2026-01-01T00:00:00Z",
        "value": {"order_id": 0, "amount": 19.99},
    }
    json_payload = glue_json.serialize("orders-json", record)
    protobuf_payload = glue_protobuf.serialize("orders-protobuf", record)
    assert GlueJsonSchemaDeserializer("shop").deserialize("o", json_payload) == record
    read = GlueProtobufDeserializer("shop", module.ShopRecord).deserialize(
        "o", protobuf_payload
    )
    assert read["value"] == {"order_id": "0", "amount": 19.99}
    assert (GUIDES / "shop_record.proto").read_text() in glue.definitions.values()


def test_glue_forms_send_the_orders_through_the_glue_serializers(tmp_path, monkeypatch):
    """The YAML file of the Glue step builds the Glue serializers, with `boto3` setting
    the region, and the shop sends the two order topics through them. The Low-level form,
    which uses `glue_egress()`, sends the same messages."""
    pytest.importorskip("boto3")
    pytest.importorskip("fastjsonschema")
    pytest.importorskip("google.protobuf")
    from desflow.connectors.egress.kafka import (
        GlueJsonSchemaSerializer,
        GlueProtobufSerializer,
    )
    from desflow.connectors.ingress.kafka import (
        GlueJsonSchemaDeserializer,
        GlueProtobufDeserializer,
    )

    glue = _Glue()
    regions = []

    def client(service, **kwargs):
        regions.append(kwargs.get("region_name"))
        return glue

    monkeypatch.setattr("boto3.client", client)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9092")
    copy_into(
        tmp_path,
        YAML / "json_schema_and_protobuf_glue.yaml",
        YAML / "order_record.schema.json",
        YAML / "shop_record.desc",
        GUIDES / "shop_record.proto",
        GUIDES / "shop_record_pb2.py",
        GUIDES / "json_schema_and_protobuf_serializers.py",
        GUIDES / "json_schema_and_protobuf_low_level.py",
        GUIDES / "json_schema_and_protobuf.py",
    )
    monkeypatch.chdir(tmp_path)
    for name in (
        "json_schema_and_protobuf_serializers",
        "json_schema_and_protobuf_low_level",
        "shop_record_pb2",
    ):
        monkeypatch.delitem(sys.modules, name, raising=False)
    forms = {}
    with kafka_messages() as (sent, _):
        app = SimulationContext.from_yaml(
            tmp_path / "json_schema_and_protobuf_glue.yaml"
        )
        serializers = app._sinks.egress[0].provider.topic_serializers
        assert isinstance(serializers["orders-json"], GlueJsonSchemaSerializer)
        assert isinstance(serializers["orders-protobuf"], GlueProtobufSerializer)
        assert regions == ["us-east-1"] * 2
        app.run(until=60)
        assert (tmp_path / "shop_record.proto").read_text() in glue.definitions.values()
        forms["yaml"] = list(sent)
        sent.clear()
        low_level = _load_from(tmp_path, "json_schema_and_protobuf_low_level")
        serializers_module = sys.modules["json_schema_and_protobuf_serializers"]
        env = low_level.shop([serializers_module.glue_egress()])
        env.run(until=60)
        env.teardown()
        forms["low_level"] = list(sent)
    assert regions[2:] == ["us-east-1"] * 2
    for messages in forms.values():
        orders = [(t, v) for t, _, v in messages if t.startswith("orders-")]
        assert {t for t, _ in orders} == {"orders-json", "orders-protobuf"}
        assert all(isinstance(v, bytes) for _, v in orders)
    assert [(t, k) for t, k, _ in forms["yaml"]] == [
        (t, k) for t, k, _ in forms["low_level"]
    ]
    first_json = next(v for t, _, v in forms["yaml"] if t == "orders-json")
    first_protobuf = next(v for t, _, v in forms["yaml"] if t == "orders-protobuf")
    read = GlueJsonSchemaDeserializer("shop", region_name="us-east-1")
    assert read.deserialize("orders-json", first_json)["value"]["amount"] == 19.99
    shop_record = sys.modules["shop_record_pb2"].ShopRecord
    read = GlueProtobufDeserializer("shop", shop_record, region_name="us-east-1")
    assert (
        read.deserialize("orders-protobuf", first_protobuf)["value"]["amount"] == 19.99
    )


def test_shop_record_module_declares_the_fields_of_the_proto_file(tmp_path):
    """shop_record_pb2.py is generated from shop_record.proto; a field the file changed
    and the module did not shows up here. The descriptor file names the same message."""
    pytest.importorskip("google.protobuf")
    from google.protobuf import descriptor_pb2

    text = (GUIDES / "shop_record.proto").read_text()
    copy_into(tmp_path, GUIDES / "shop_record_pb2.py")
    shop_record = _load_from(tmp_path, "shop_record_pb2").ShopRecord
    assert shop_record.DESCRIPTOR.full_name == "com.desflow.shop.ShopRecord"
    for message in shop_record.DESCRIPTOR.file.message_types_by_name.values():
        for field in message.fields:
            assert f"{field.name} = {field.number};" in text
    described = descriptor_pb2.FileDescriptorSet.FromString(
        (YAML / "shop_record.desc").read_bytes()
    )
    assert [m.name for m in described.file[0].message_type] == ["Order", "ShopRecord"]
