"""The Confluent and Glue schema registry code: bytes on the wire, registry calls,
caches and options.

Confluent tests use `httpx.MockTransport` and Glue tests a fake boto3 client, so no
registry is needed. The Avro byte fixtures F1 to F4 follow the Avro 1.12.0
specification, "Binary Encoding": an int is a zigzag varint (27 is 0x36), a string is
its length then UTF-8 ("foo" is 06 66 6f 6f), and a union writes the index of its
branch first (null in ["null","string"] is 00).
"""

import base64
import datetime
import io
import json
import ssl
import types
import uuid

import botocore.exceptions
import httpx
import pytest
from google.protobuf import (
    descriptor_pb2,
    descriptor_pool,
    message_factory,
    timestamp_pb2,
)

from desflow.connectors._schema_registry import (
    confluent_client,
    glue_client,
    glue_json,
    json_schema,
    protobuf,
)
from desflow.connectors._schema_registry.confluent_client import (
    ConfluentRegistryClient,
    build_ssl_context,
)
from desflow.connectors._schema_registry.glue_client import GlueRegistryClient
from desflow.connectors._schema_registry.wire import frame_glue, unframe_glue
from desflow.connectors.egress.kafka import (
    ConfluentAvroSerializer,
    ConfluentJsonSchemaSerializer,
    ConfluentProtobufSerializer,
    GlueAvroSerializer,
    GlueJsonSchemaSerializer,
    GlueProtobufSerializer,
    SchemaRegistryError,
    SerializationError,
)
from desflow.connectors.ingress.kafka import (
    ConfluentAvroDeserializer,
    ConfluentJsonSchemaDeserializer,
    ConfluentProtobufDeserializer,
    GlueAvroDeserializer,
    GlueJsonSchemaDeserializer,
    GlueProtobufDeserializer,
)

PROBE = {
    "type": "record",
    "name": "Probe",
    "fields": [
        {"name": "path_id", "type": "string"},
        {"name": "value", "type": "int"},
    ],
}
PROBE_WITH_TASK = {
    **PROBE,
    "fields": [*PROBE["fields"], {"name": "task", "type": ["null", "string"]}],
}
GUID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# (schema, value, schema id, bytes)
F1 = (PROBE, {"path_id": "foo", "value": 27}, 1, "00 00 00 00 01 06 66 6f 6f 36")
F2 = (
    PROBE,
    {"path_id": "foo", "value": 27},
    0x01020304,
    "00 01 02 03 04 06 66 6f 6f 36",
)
F3 = (
    PROBE_WITH_TASK,
    {"path_id": "foo", "value": 27, "task": None},
    1,
    "00 00 00 00 01 06 66 6f 6f 36 00",
)
F4 = (
    PROBE,
    {"path_id": "foo", "value": 27},
    GUID,
    "01 a1 b2 c3 d4 e5 f6 78 90 ab cd ef 12 34 56 78 90 06 66 6f 6f 36",
)
F5 = ("bytes", b"abc", 1, "00 00 00 00 01 61 62 63")
# A str for a bytes schema is written as UTF-8, as confluent-kafka does.
F5_STR = ("bytes", "abc", 1, F5[3])


class FakeRegistry:
    """Answers registry calls from a table and records every request."""

    def __init__(self, routes=None):
        # (method, path) -> (status, JSON body); a list gives one answer per call.
        self.routes = routes or {}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.routes[(request.method, request.url.raw_path.decode())]
        if isinstance(answer, list):
            answer = answer.pop(0)
        status, body = answer
        if isinstance(body, str):
            return httpx.Response(status, text=body)
        return httpx.Response(status, json=body)

    def paths(self, method=None) -> list[str]:
        return [
            r.url.raw_path.decode() for r in self.requests if method in (None, r.method)
        ]


def schema_answer(schema: dict, **extra) -> tuple[int, dict]:
    return 200, {"schema": json.dumps(schema), **extra}


@pytest.fixture
def no_wait(monkeypatch):
    """Replaces the wait between tries with a recorder; the jitter is at its maximum."""
    waits: list[float] = []
    monkeypatch.setattr(
        confluent_client,
        "time",
        types.SimpleNamespace(sleep=waits.append),
    )
    monkeypatch.setattr(
        confluent_client, "random", types.SimpleNamespace(random=lambda: 1.0)
    )
    return waits


def client_for(
    handler, url="http://registry:8081", **kwargs
) -> ConfluentRegistryClient:
    return ConfluentRegistryClient(
        url, transport=httpx.MockTransport(handler), **kwargs
    )


# Bytes on the wire


@pytest.mark.parametrize(
    "fixture",
    [F1, F2, F3, F5, F5_STR],
    ids=["F1", "F2", "F3", "F5", "F5-str"],
)
def test_serializer_writes_fixture(registry_transport, fixture):
    schema, value, schema_id, expected = fixture
    registry_transport(
        FakeRegistry(
            {("POST", "/subjects/probe-value/versions"): (200, {"id": schema_id})}
        )
    )
    serializer = ConfluentAvroSerializer("http://registry:8081", json.dumps(schema))

    assert serializer.serialize("probe", value) == bytes.fromhex(expected)


@pytest.mark.parametrize(
    "fixture", [F1, F2, F3, F4, F5], ids=["F1", "F2", "F3", "F4", "F5"]
)
def test_deserializer_reads_fixture(registry_transport, fixture):
    schema, value, schema_id, framed = fixture
    path = f"/schemas/{'guids' if isinstance(schema_id, str) else 'ids'}/{schema_id}"
    registry_transport(FakeRegistry({("GET", path): schema_answer(schema)}))
    deserializer = ConfluentAvroDeserializer("http://registry:8081")

    assert deserializer.deserialize("probe", bytes.fromhex(framed)) == value


# Register request, caches and references


def test_register_request(registry_transport):
    registry = FakeRegistry(
        {
            ("POST", "/subjects/orders-value/versions"): (200, {"id": 7}),
            ("POST", "/subjects/prices-value/versions"): (200, {"id": 8}),
            ("POST", "/subjects/a%2Fb-value/versions"): (200, {"id": 9}),
        }
    )
    registry_transport(registry)
    serializer = ConfluentAvroSerializer(
        "http://registry:8081", f"  {json.dumps(PROBE)}\n"
    )

    serializer.serialize("orders", F1[1])
    serializer.serialize("a/b", F1[1])

    request = registry.requests[0]
    assert request.method == "POST"
    assert json.loads(request.content) == {
        "schema": json.dumps(PROBE),
        "schemaType": "AVRO",
        "references": [],
    }
    assert request.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    assert "application/vnd.schemaregistry.v1+json" in request.headers["Accept"]
    assert registry.paths() == [
        "/subjects/orders-value/versions",
        "/subjects/a%2Fb-value/versions",
    ]
    assert "normalize" not in request.url.query.decode()


def test_primitive_schema_is_wrapped(registry_transport):
    registry = FakeRegistry({("POST", "/subjects/t-value/versions"): (200, {"id": 1})})
    registry_transport(registry)

    ConfluentAvroSerializer("http://registry:8081", '"string"').serialize("t", "x")

    assert json.loads(registry.requests[0].content)["schema"] == '{"type":"string"}'


def test_client_is_neutral_about_the_format():
    registry = FakeRegistry(
        {
            ("POST", "/subjects/t-value/versions"): (200, {"id": 1}),
            ("GET", "/schemas/ids/1"): (200, {"schema": "{}"}),
        }
    )
    client = client_for(registry)

    client.register("t-value", "{}", "JSON")
    with pytest.raises(TypeError):
        client.register("t-value", "{}")  # type: ignore[call-arg]

    assert json.loads(registry.requests[0].content)["schemaType"] == "JSON"
    assert client.schema_by_id(1)["schemaType"] == "AVRO"


def test_registration_and_schema_fetch_are_cached(registry_transport):
    registry = FakeRegistry(
        {
            ("POST", "/subjects/a-value/versions"): (200, {"id": 1}),
            ("POST", "/subjects/b-value/versions"): (200, {"id": 2}),
            ("GET", "/schemas/ids/1"): schema_answer(PROBE),
        }
    )
    registry_transport(registry)
    serializer = ConfluentAvroSerializer("http://registry:8081", json.dumps(PROBE))
    deserializer = ConfluentAvroDeserializer("http://registry:8081")

    for topic in ("a", "b", "a", "b", "a", "b"):
        serializer.serialize(topic, F1[1])
    for _ in range(3):
        deserializer.deserialize("a", bytes.fromhex(F1[3]))

    assert registry.paths("POST") == [
        "/subjects/a-value/versions",
        "/subjects/b-value/versions",
    ]
    assert registry.paths("GET") == ["/schemas/ids/1"]


def test_failed_calls_are_not_cached(registry_transport):
    registry = FakeRegistry(
        {
            ("POST", "/subjects/t-value/versions"): [
                (409, {"error_code": 409, "message": "incompatible"}),
                (200, {"id": 1}),
            ],
            ("GET", "/schemas/ids/1"): [
                (404, {"error_code": 40403, "message": "not found"}),
                schema_answer(PROBE),
            ],
        }
    )
    registry_transport(registry)
    serializer = ConfluentAvroSerializer("http://registry:8081", json.dumps(PROBE))
    deserializer = ConfluentAvroDeserializer("http://registry:8081")

    with pytest.raises(SchemaRegistryError):
        serializer.serialize("t", F1[1])
    with pytest.raises(SchemaRegistryError):
        deserializer.deserialize("t", bytes.fromhex(F1[3]))

    assert serializer.serialize("t", F1[1]) == bytes.fromhex(F1[3])
    assert deserializer.deserialize("t", bytes.fromhex(F1[3])) == F1[1]


def test_reference_is_fetched_once_and_decodes(registry_transport):
    address = {
        "type": "record",
        "name": "Address",
        "namespace": "shop",
        "fields": [{"name": "city", "type": "string"}],
    }
    order = {
        "type": "record",
        "name": "Order",
        "namespace": "shop",
        "fields": [{"name": "ship_to", "type": "shop.Address"}],
    }
    reference = {"name": "shop.Address", "subject": "address-value", "version": 2}
    registry = FakeRegistry(
        {
            ("GET", "/schemas/ids/5"): schema_answer(order, references=[reference]),
            ("GET", "/subjects/address-value/versions/2"): schema_answer(address),
        }
    )
    registry_transport(registry)
    deserializer = ConfluentAvroDeserializer("http://registry:8081")
    # A string is its length as a zigzag varint (5 is 0x0a), then UTF-8.
    payload = bytes.fromhex("00 00 00 00 05 0a") + b"Perth"
    assert deserializer.deserialize("orders", payload) == {"ship_to": {"city": "Perth"}}
    deserializer.deserialize("orders", payload)

    assert registry.paths("GET") == [
        "/schemas/ids/5",
        "/subjects/address-value/versions/2",
    ]


# Framing errors and rules


@pytest.mark.parametrize(
    ("payload", "schema_type", "message"),
    [
        (bytes.fromhex("00 00 00 00 01"), "AVRO", "6 bytes or more"),
        (bytes.fromhex("01" + "ab" * 15), "AVRO", "17 bytes or more"),
        (bytes.fromhex("02 00 00 00 01 00"), "AVRO", "Invalid magic byte"),
        (bytes.fromhex("00 00 00 00 01 00"), "PROTOBUF", "PROTOBUF"),
    ],
    ids=["too short", "GUID too short", "magic 2", "other schema type"],
)
def test_deserializer_refuses_what_it_cannot_read(
    registry_transport, payload, schema_type, message
):
    registry_transport(
        FakeRegistry(
            {("GET", "/schemas/ids/1"): schema_answer(PROBE, schemaType=schema_type)}
        )
    )
    deserializer = ConfluentAvroDeserializer("http://registry:8081")

    with pytest.raises(SerializationError, match=message):
        deserializer.deserialize("t", payload)


@pytest.mark.parametrize(
    ("rule_list", "rule", "refused"),
    [
        ("domainRules", {"name": "check", "mode": "READ", "kind": "CONDITION"}, True),
        ("encodingRules", {"name": "check", "mode": "WRITEREAD"}, True),
        ("domainRules", {"name": "check", "mode": "READ", "disabled": True}, False),
        ("encodingRules", {"name": "check", "mode": "WRITE"}, False),
    ],
    ids=["READ", "WRITEREAD", "disabled", "WRITE only"],
)
def test_enabled_read_rules_are_refused(registry_transport, rule_list, rule, refused):
    rule_set = {rule_list: [rule], "migrationRules": [{"name": "m", "mode": "UPGRADE"}]}
    registry_transport(
        FakeRegistry(
            {("GET", "/schemas/ids/1"): schema_answer(PROBE, ruleSet=rule_set)}
        )
    )
    deserializer = ConfluentAvroDeserializer("http://registry:8081")

    if refused:
        with pytest.raises(SerializationError, match="check"):
            deserializer.deserialize("t", bytes.fromhex(F1[3]))
    else:
        assert deserializer.deserialize("t", bytes.fromhex(F1[3])) == F1[1]


# Errors and retries


def test_registry_errors():
    registry = FakeRegistry(
        {
            ("POST", "/subjects/t/versions"): (
                409,
                {"error_code": 409, "message": "Incompatible schema"},
            ),
            ("GET", "/schemas/ids/1"): (400, "<html>bad</html>"),
        }
    )
    client = client_for(registry)

    with pytest.raises(SchemaRegistryError) as conflict:
        client.register("t", "{}", "AVRO")
    with pytest.raises(SchemaRegistryError) as plain:
        client.schema_by_id(1)

    assert (conflict.value.http_status, conflict.value.error_code) == (409, 409)
    assert conflict.value.message == "Incompatible schema"
    assert str(conflict.value) == "Schema registry error 409 (409): Incompatible schema"
    assert (plain.value.http_status, plain.value.error_code) == (400, -1)
    assert "<html>bad</html>" in plain.value.message


def test_retries_a_status_that_may_pass(no_wait):
    registry = FakeRegistry(
        {("GET", "/schemas/ids/1"): [(503, {}), (200, {"schema": "{}"})]}
    )

    client_for(registry).schema_by_id(1)

    assert len(registry.requests) == 2
    assert no_wait == [1.0]


def test_does_not_retry_a_status_that_will_not_pass(no_wait):
    registry = FakeRegistry(
        {
            ("GET", "/schemas/ids/1"): [
                (422, {"error_code": 42201, "message": "invalid"})
            ]
        }
    )

    with pytest.raises(SchemaRegistryError):
        client_for(registry).schema_by_id(1)

    assert len(registry.requests) == 1
    assert no_wait == []


def test_retries_transport_errors_with_growing_waits(no_wait):
    attempts = []

    def handler(request):
        attempts.append(request)
        if len(attempts) <= 3:
            raise httpx.ConnectError("refused")
        return httpx.Response(200, json={"schema": "{}"})

    client_for(handler, max_retries=3).schema_by_id(1)

    assert len(attempts) == 4
    assert no_wait == [1.0, 2.0, 4.0]


def test_gives_up_after_max_retries(no_wait):
    attempts = []

    def handler(request):
        attempts.append(request)
        raise httpx.ConnectError("refused")

    with pytest.raises(httpx.ConnectError):
        client_for(handler, max_retries=2).schema_by_id(1)

    assert len(attempts) == 3


def test_tries_the_next_url_in_order(no_wait):
    hosts = []

    def handler(request):
        hosts.append(request.url.host)
        if request.url.host == "a":
            raise httpx.ConnectError("refused")
        return httpx.Response(200, json={"schema": "{}"})

    client = client_for(handler, url="http://a:8081, http://b:8081/", max_retries=1)

    client.schema_by_id(1)

    assert hosts == ["a", "a", "b"]


# Arguments and old keys


def test_basic_auth_and_headers_are_sent():
    seen = []

    def handler(request):
        seen.append(request.headers)
        return httpx.Response(200, json={"schema": "{}"})

    client_for(
        handler, auth=("user", "pass"), headers={"target-sr-cluster": "lsrc-1"}
    ).schema_by_id(1)

    assert seen[0]["Authorization"] == "Basic dXNlcjpwYXNz"
    assert seen[0]["target-sr-cluster"] == "lsrc-1"


def test_url_credentials_of_the_first_url_apply_to_every_url():
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.host == "a":
            raise httpx.ConnectError("refused")
        return httpx.Response(200, json={"schema": "{}"})

    client_for(
        handler, url="http://u:p%40ss@a:8081,http://b:8081", max_retries=0
    ).schema_by_id(1)

    assert [r.url.host for r in seen] == ["a", "b"]
    assert seen[1].headers["Authorization"] == "Basic dTpwQHNz"
    assert all("@" not in str(r.url) for r in seen)


def test_auth_and_url_credentials_are_exclusive():
    with pytest.raises(ValueError, match="auth"):
        ConfluentRegistryClient("http://u:p@registry:8081", auth=("user", "pass"))


@pytest.mark.parametrize(
    "build",
    [
        lambda **kw: ConfluentAvroSerializer("http://registry:8081", '"string"', **kw),
        lambda **kw: ConfluentAvroDeserializer("http://registry:8081", **kw),
        lambda **kw: ConfluentJsonSchemaSerializer("http://registry:8081", "{}", **kw),
        lambda **kw: ConfluentJsonSchemaDeserializer("http://registry:8081", **kw),
        lambda **kw: ConfluentProtobufSerializer(
            "http://registry:8081", timestamp_pb2.Timestamp, **kw
        ),
        lambda **kw: ConfluentProtobufDeserializer(
            "http://registry:8081", timestamp_pb2.Timestamp, **kw
        ),
    ],
    ids=[
        "serializer",
        "deserializer",
        "json serializer",
        "json deserializer",
        "protobuf serializer",
        "protobuf deserializer",
    ],
)
def test_classes_pass_default_timeout_verify_and_retries_to_the_client(
    monkeypatch, build
):
    built = []
    real = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kw: built.append(kw) or real(**kw))

    default = build()
    unlimited = build(timeout=None)

    assert [kw["timeout"] for kw in built] == [10.0, None]
    assert all(kw["verify"] is True for kw in built)
    assert default._client._max_retries == 3
    assert unlimited._client._max_retries == 3


def test_tls_options_build_a_context(tmp_path):
    insecure = build_ssl_context(False, None)
    assert isinstance(insecure, ssl.SSLContext)
    assert insecure.verify_mode == ssl.CERT_NONE
    assert insecure.check_hostname is False
    assert build_ssl_context(True, None) is True
    context = ssl.create_default_context()
    assert build_ssl_context(context, None) is context
    assert build_ssl_context(str(tmp_path), None).verify_mode == ssl.CERT_REQUIRED
    with pytest.raises(OSError):
        build_ssl_context(True, str(tmp_path / "missing.pem"))


@pytest.mark.parametrize(
    ("key", "value", "expected"),
    [
        ("basic.auth.user.info", "user:pa:ss", {"auth": ("user", "pa:ss")}),
        ("ssl.ca.location", False, {"verify": False}),
        ("max.retries", 5, {"max_retries": 5}),
        ("cache.capacity", 10, {}),
    ],
)
def test_old_keys_warn_and_act_like_their_argument(key, value, expected):
    with pytest.warns(FutureWarning, match=rf"'{key}'.*0\.30\.0"):
        options = confluent_client.resolve_legacy_options({key: value})

    assert {k: options[k] for k in expected} == expected


def test_old_certificate_and_bearer_keys():
    with pytest.warns(FutureWarning):
        cert = confluent_client.resolve_legacy_options(
            {
                "ssl.certificate.location": "c.pem",
                "ssl.key.location": "k.pem",
                "ssl.key.password": "s3cr3t-pass",
            }
        )["cert"]
    with pytest.warns(FutureWarning) as caught:
        headers = confluent_client.resolve_legacy_options(
            {
                "bearer.auth.credentials.source": "STATIC_TOKEN",
                "bearer.auth.token": "s3cr3t-value",
                "bearer.auth.logical.cluster": "lsrc-1",
                "bearer.auth.identity.pool.id": "pool-1",
            }
        )["headers"]

    assert cert == ("c.pem", "k.pem", "s3cr3t-pass")
    assert headers == {
        "Authorization": "Bearer s3cr3t-value",
        "target-sr-cluster": "lsrc-1",
        "Confluent-Identity-Pool-Id": "pool-1",
    }
    assert all("s3cr3t" not in str(w.message) for w in caught)


def test_old_basic_auth_key_authenticates(registry_transport):
    seen = []

    def handler(request):
        seen.append(request.headers["Authorization"])
        return httpx.Response(200, json={"id": 1})

    registry_transport(handler)
    with pytest.warns(FutureWarning):
        serializer = ConfluentAvroSerializer(
            "http://registry:8081",
            json.dumps(PROBE),
            **{"basic.auth.user.info": "user:pass"},
        )

    serializer.serialize("t", F1[1])

    assert seen == ["Basic dXNlcjpwYXNz"]


@pytest.mark.parametrize(
    "options",
    [
        {"url": "http://x"},
        {"retries.wait.ms": 10},
        {"basic.auth.user.info": "no-colon"},
        {"ssl.key.location": "k.pem"},
        {
            "bearer.auth.credentials.source": "OAUTHBEARER",
            "bearer.auth.client.secret": "s3cr3t-value",
        },
        {
            "bearer.auth.credentials.source": "STATIC_TOKEN",
            "bearer.auth.token": "s3cr3t-value",
        },
        {"surprise": 1},
    ],
    ids=[
        "url",
        "retry wait",
        "no colon",
        "key without certificate",
        "OAuth",
        "token without cluster",
        "unknown",
    ],
)
def test_old_keys_without_a_replacement_are_refused(options):
    with pytest.raises(ValueError) as error:
        confluent_client.resolve_legacy_options(options)

    assert "s3cr3t" not in str(error.value)


@pytest.mark.parametrize(
    ("options", "argument"),
    [
        ({"basic.auth.user.info": "u:p"}, {"auth": ("u", "p")}),
        ({"max.retries": 7}, {"max_retries": 3}),
        ({"ssl.ca.location": "/x.pem"}, {"verify": True}),
    ],
    ids=["auth", "max_retries", "verify"],
)
def test_old_key_and_its_argument_are_exclusive(options, argument):
    name = next(iter(argument))

    with pytest.raises(ValueError, match=name):
        confluent_client.resolve_legacy_options(options, **argument)


def test_constructors_make_no_request(registry_transport):
    def handler(request):
        pytest.fail(f"Unexpected request {request.url}")

    registry_transport(handler)

    ConfluentAvroSerializer("http://registry:8081", json.dumps(PROBE))
    ConfluentAvroDeserializer("http://registry:8081")
    ConfluentJsonSchemaSerializer("http://registry:8081", json.dumps(JSON_SCHEMA))
    ConfluentJsonSchemaDeserializer("http://registry:8081")
    ConfluentProtobufSerializer("http://registry:8081", PROBE_MESSAGE)
    ConfluentProtobufDeserializer("http://registry:8081", PROBE_MESSAGE)


def test_guid_payload_of_17_bytes_has_an_empty_rest():
    from desflow.connectors._schema_registry.wire import unframe_confluent

    schema_id, data = unframe_confluent(bytes.fromhex("01" + "ab" * 16))

    assert schema_id == uuid.UUID("ab" * 16)
    assert bytes(data) == b""


# AWS Glue: bytes on the wire
#
# The Glue fixtures were written by aws-glue-schema-registry 1.1.3 (`codec.encode`,
# `AvroSchema.write`, `JsonSchema.write`), so they are what that library reads. The
# prefix is the header version 3, the compression byte (0 off, 5 zlib), then the 16
# bytes of the schema version UUID.

GLUE_VERSION = uuid.UUID("0a1b2c3d-4e5f-6789-abcd-ef0123456789")
GLUE_RECORD = {"path_id": "Shop.packers", "value": 3}
GLUE_AVRO_BODY = bytes.fromhex("1853686f702e7061636b65727306")
# G1: the record as Avro, uncompressed
G1 = bytes.fromhex("03000a1b2c3d4e5f6789abcdef01234567891853686f702e7061636b65727306")
# G2: the same body, zlib compressed
G2 = bytes.fromhex(
    "03050a1b2c3d4e5f6789abcdef0123456789789c9308cec82fd02b484cce4e2d2a660300239e04d0"
)
GLUE_JSON_SCHEMA = {
    "type": "object",
    "properties": {"path_id": {"type": "string"}, "value": {"type": "integer"}},
    "required": ["path_id", "value"],
}
# G3: a record of a JSON-format version, as JSON
G3 = bytes.fromhex(
    "03000a1b2c3d4e5f6789abcdef0123456789"
    "7b22706174685f6964223a2253686f702e7061636b657273222c2276616c7565223a337d"
)


def test_glue_fixtures_hold_the_same_body():
    assert unframe_glue(G1) == (GLUE_VERSION, GLUE_AVRO_BODY)
    assert unframe_glue(G2) == (GLUE_VERSION, GLUE_AVRO_BODY)
    assert unframe_glue(G3) == (
        GLUE_VERSION,
        json.dumps(GLUE_RECORD, separators=(",", ":")).encode(),
    )
    assert frame_glue(GLUE_VERSION, GLUE_AVRO_BODY) == G1


@pytest.mark.parametrize(
    "payload, message",
    [
        (G1[:17], "length 18 bytes or more"),
        (b"\x01" + G1[1:], "Invalid header version byte 1"),
        (G1[:1] + b"\x01" + G1[2:], "Invalid compression byte 1"),
        (G2[:18] + b"not zlib", "Cannot decompress"),
    ],
)
def test_glue_payloads_that_cannot_be_read(payload, message):
    with pytest.raises(SerializationError, match=message):
        unframe_glue(payload)


# AWS Glue: the registry client


def client_error(code: str, message: str, status: int = 400):
    return botocore.exceptions.ClientError(
        {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "GetSchemaByDefinition",
    )


class FakeGlue:
    """Answers the Glue calls from queues of answers and records every call.

    An answer that is an exception is raised. A call with no queued answer fails the
    test, so a call the code should not make is found.
    """

    def __init__(self, **answers):
        self.answers = {name: list(queue) for name, queue in answers.items()}
        self.calls: list[tuple[str, dict]] = []

    def __getattr__(self, name):
        def call(**kwargs):
            self.calls.append((name, kwargs))
            queued = self.answers.get(name)
            if not queued:
                pytest.fail(f"Unexpected Glue call {name}")
            answer = queued.pop(0) if len(queued) > 1 else queued[0]
            if isinstance(answer, Exception):
                raise answer
            return answer

        return call

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


def version_answer(status="AVAILABLE", fmt="AVRO", definition="{}"):
    return {
        "SchemaVersionId": str(GLUE_VERSION),
        "Status": status,
        "DataFormat": fmt,
        "SchemaDefinition": definition,
    }


def create_answer(status="AVAILABLE"):
    """The shape of Glue's CreateSchema response, which names the status differently."""
    return {"SchemaVersionId": str(GLUE_VERSION), "SchemaVersionStatus": status}


VERSION_MISSING = client_error(
    "EntityNotFoundException", "Schema version is not found.", 404
)
SCHEMA_MISSING = client_error("EntityNotFoundException", "Schema is not found.", 404)


class GlueClientFactory:
    """Builds a `GlueRegistryClient` over a FakeGlue and records the `boto3.client` calls."""

    def __init__(self, monkeypatch):
        self.monkeypatch = monkeypatch
        self.created: list[tuple[str, dict]] = []

    def __call__(self, glue: FakeGlue, **boto3_kwargs) -> GlueRegistryClient:
        def fake_client(service, **kwargs):
            self.created.append((service, kwargs))
            return glue

        self.monkeypatch.setattr("boto3.client", fake_client)
        return GlueRegistryClient("probe", boto3_kwargs)


@pytest.fixture
def glue_for(monkeypatch):
    return GlueClientFactory(monkeypatch)


@pytest.fixture
def glue_sleeps(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(glue_client, "time", types.SimpleNamespace(sleep=sleeps.append))
    return sleeps


def test_glue_client_makes_no_call_and_defaults_the_region(glue_for):
    glue = FakeGlue()

    glue_for(glue)
    assert glue_for.created == [("glue", {"region_name": "us-east-1"})]
    assert glue.calls == []

    glue_for.created.clear()
    glue_for(glue, region_name="eu-west-1", endpoint_url="http://floci:4566")
    assert glue_for.created == [
        ("glue", {"region_name": "eu-west-1", "endpoint_url": "http://floci:4566"})
    ]


def test_glue_client_finds_a_known_definition(glue_for):
    glue = FakeGlue(get_schema_by_definition=[version_answer()])

    assert glue_for(glue).version_for("t-value", "{}", "AVRO") == GLUE_VERSION

    assert glue.calls == [
        (
            "get_schema_by_definition",
            {
                "SchemaId": {"SchemaName": "t-value", "RegistryName": "probe"},
                "SchemaDefinition": "{}",
            },
        )
    ]


def test_glue_client_registers_a_new_version_of_a_known_schema(glue_for):
    glue = FakeGlue(
        get_schema_by_definition=[VERSION_MISSING],
        register_schema_version=[version_answer()],
    )

    assert glue_for(glue).version_for("t-value", "{}", "AVRO") == GLUE_VERSION

    assert glue.calls[1] == (
        "register_schema_version",
        {
            "SchemaId": {"SchemaName": "t-value", "RegistryName": "probe"},
            "SchemaDefinition": "{}",
        },
    )


def test_glue_client_creates_a_missing_schema_with_backward_compatibility(glue_for):
    glue = FakeGlue(
        get_schema_by_definition=[SCHEMA_MISSING], create_schema=[create_answer()]
    )

    assert glue_for(glue).version_for("t-value", "{}", "JSON") == GLUE_VERSION

    assert glue.calls[1] == (
        "create_schema",
        {
            "SchemaName": "t-value",
            "RegistryId": {"RegistryName": "probe"},
            "DataFormat": "JSON",
            "Compatibility": "BACKWARD",
            "Description": "",
            "Tags": {},
            "SchemaDefinition": "{}",
        },
    )


def test_glue_client_registers_a_version_when_another_producer_created_the_schema(
    glue_for,
):
    glue = FakeGlue(
        get_schema_by_definition=[SCHEMA_MISSING],
        create_schema=[client_error("AlreadyExistsException", "Schema exists")],
        register_schema_version=[version_answer()],
    )

    assert glue_for(glue).version_for("t-value", "{}", "AVRO") == GLUE_VERSION

    assert glue.names() == [
        "get_schema_by_definition",
        "create_schema",
        "register_schema_version",
    ]


@pytest.mark.parametrize("check", [1, 3, 10])
@pytest.mark.parametrize("path", ["register", "create"])
def test_glue_client_waits_for_a_pending_version(glue_for, glue_sleeps, check, path):
    checks = [version_answer("PENDING")] * (check - 1) + [version_answer()]
    if path == "register":
        first = {
            "get_schema_by_definition": [VERSION_MISSING],
            "register_schema_version": [version_answer("PENDING")],
        }
    else:
        first = {
            "get_schema_by_definition": [SCHEMA_MISSING],
            "create_schema": [create_answer("PENDING")],
        }
    glue = FakeGlue(**first, get_schema_version=checks)

    assert glue_for(glue).version_for("t-value", "{}", "AVRO") == GLUE_VERSION

    assert glue.names().count("get_schema_version") == check
    assert glue_sleeps == [3] * check


def test_glue_client_gives_up_on_a_version_still_pending_after_ten_checks(
    glue_for, glue_sleeps
):
    glue = FakeGlue(
        get_schema_by_definition=[VERSION_MISSING],
        register_schema_version=[version_answer("PENDING")],
        get_schema_version=[version_answer("PENDING")],
    )

    with pytest.raises(SchemaRegistryError, match="still PENDING after 10 checks"):
        glue_for(glue).version_for("t-value", "{}", "AVRO")

    assert glue.names().count("get_schema_version") == 10
    assert glue_sleeps == [3] * 10


@pytest.mark.parametrize("status", ["FAILURE", "DELETING"])
def test_glue_client_stops_at_once_on_a_status_other_than_pending(
    glue_for, glue_sleeps, status
):
    glue = FakeGlue(
        get_schema_by_definition=[VERSION_MISSING],
        register_schema_version=[version_answer("PENDING")],
        get_schema_version=[version_answer(status)],
    )

    with pytest.raises(SchemaRegistryError, match=f"is in {status} status"):
        glue_for(glue).version_for("t-value", "{}", "AVRO")

    assert glue.names().count("get_schema_version") == 1


def test_glue_client_refuses_a_found_version_that_is_not_available(glue_for):
    glue = FakeGlue(get_schema_by_definition=[version_answer("PENDING")])

    with pytest.raises(SchemaRegistryError, match="status is PENDING"):
        glue_for(glue).version_for("t-value", "{}", "AVRO")


def test_glue_client_caches_versions_and_schemas_and_not_failures(glue_for):
    glue = FakeGlue(
        get_schema_by_definition=[
            client_error("InternalServiceException", "x", 500),
            version_answer(),
        ],
        get_schema_version=[version_answer(definition='{"type":"int"}')],
    )
    client = glue_for(glue)

    with pytest.raises(SchemaRegistryError):
        client.version_for("t-value", "{}", "AVRO")
    assert client.version_for("t-value", "{}", "AVRO") == GLUE_VERSION
    assert client.version_for("t-value", "{}", "AVRO") == GLUE_VERSION
    assert glue.names().count("get_schema_by_definition") == 2

    for _ in range(2):
        assert client.schema_version(GLUE_VERSION) == {
            "DataFormat": "AVRO",
            "SchemaDefinition": '{"type":"int"}',
        }
    assert glue.names().count("get_schema_version") == 1


def test_glue_client_errors_carry_the_status_and_code_of_the_answer(glue_for):
    original = client_error("AccessDeniedException", "not allowed", 403)
    client = glue_for(FakeGlue(get_schema_by_definition=[original]))

    with pytest.raises(SchemaRegistryError) as caught:
        client.version_for("t-value", "{}", "AVRO")

    assert (caught.value.http_status, caught.value.error_code) == (
        403,
        "AccessDeniedException",
    )
    assert caught.value.__cause__ is original

    client = glue_for(
        FakeGlue(get_schema_by_definition=[ConnectionError("no endpoint")])
    )
    with pytest.raises(SchemaRegistryError, match="no endpoint") as caught:
        client.version_for("t-value", "{}", "AVRO")
    assert (caught.value.http_status, caught.value.error_code) == (-1, -1)


def test_glue_client_refuses_a_version_that_is_not_available(glue_for):
    client = glue_for(FakeGlue(get_schema_version=[version_answer("PENDING")]))

    with pytest.raises(SchemaRegistryError, match="status is PENDING"):
        client.schema_version(GLUE_VERSION)


# AWS Glue: the serializer and the deserializer


def glue_pair(monkeypatch, glue, schema=None):
    monkeypatch.setattr("boto3.client", lambda service, **kwargs: glue)
    return (
        GlueAvroSerializer("probe", json.dumps(schema or PROBE_GLUE)),
        GlueAvroDeserializer("probe"),
    )


PROBE_GLUE = {
    "type": "record",
    "name": "Probe",
    "fields": [{"name": "path_id", "type": "string"}, {"name": "value", "type": "int"}],
}


def test_glue_serializer_writes_g1_and_registers_once(monkeypatch):
    glue = FakeGlue(
        get_schema_by_definition=[SCHEMA_MISSING, version_answer()],
        create_schema=[create_answer()],
    )
    serializer, deserializer = glue_pair(monkeypatch, glue)
    assert glue.calls == []

    # A key the schema does not have is dropped, as it is in the other Avro classes
    first = serializer.serialize("t", {**GLUE_RECORD, "stream_type": "telemetry"})
    second = serializer.serialize("t", GLUE_RECORD)

    assert first == second == G1
    assert glue.names() == ["get_schema_by_definition", "create_schema"]
    assert glue.calls[1][1]["SchemaName"] == "t-value"
    assert json.loads(glue.calls[1][1]["SchemaDefinition"]) == PROBE_GLUE
    assert glue.calls[1][1]["DataFormat"] == "AVRO"


@pytest.mark.parametrize("payload", [G1, G2])
def test_glue_deserializer_reads_an_avro_version_framed_or_compressed(
    monkeypatch, payload
):
    glue = FakeGlue(
        get_schema_version=[version_answer(definition=json.dumps(PROBE_GLUE))]
    )
    _, deserializer = glue_pair(monkeypatch, glue)

    assert deserializer.deserialize("t", payload) == GLUE_RECORD
    assert deserializer.deserialize("t", payload) == GLUE_RECORD
    assert glue.names() == ["get_schema_version"]


def test_glue_deserializer_reads_a_json_version_and_validates_it(monkeypatch):
    glue = FakeGlue(
        get_schema_version=[
            version_answer(fmt="JSON", definition=json.dumps(GLUE_JSON_SCHEMA))
        ]
    )
    _, deserializer = glue_pair(monkeypatch, glue)

    assert deserializer.deserialize("t", G3) == GLUE_RECORD
    wrong_type = frame_glue(GLUE_VERSION, b'{"path_id":"A","value":"three"}')
    with pytest.raises(SerializationError, match="does not match the schema"):
        deserializer.deserialize("t", wrong_type)
    with pytest.raises(SerializationError, match="not valid JSON"):
        deserializer.deserialize("t", frame_glue(GLUE_VERSION, b"{"))


def _glue_json_reader(monkeypatch, schema):
    glue = FakeGlue(
        get_schema_version=[version_answer(fmt="JSON", definition=json.dumps(schema))]
    )
    _, deserializer = glue_pair(monkeypatch, glue)
    return lambda record: deserializer.deserialize(
        "t", frame_glue(GLUE_VERSION, json.dumps(record).encode())
    )


def test_glue_json_reader_fills_in_the_default_of_a_missing_property(monkeypatch):
    """aws-glue-schema-registry 1.1.3 compiled the schema with fastjsonschema's
    defaults, which include `use_default=True`."""
    read = _glue_json_reader(
        monkeypatch,
        {"properties": {"n": {"type": "integer"}, "a": {"default": ["x"]}}},
    )

    first = read({"n": 1})
    first["a"].append("changed")

    assert first == {"n": 1, "a": ["x", "changed"]}
    assert read({"n": 1}) == {"n": 1, "a": ["x"]}
    assert read({"n": 1, "a": ["y"]}) == {"n": 1, "a": ["y"]}


A_DEFAULT = {"a": {"default": 1}}


@pytest.mark.parametrize(
    ("schema", "record"),
    [
        pytest.param(
            {"type": "array", "items": [{"type": "integer"}], "additionalItems": False},
            [1, 2],
            id="no $schema is Draft 7",
        ),
        pytest.param({"type": "string", "format": "ipv4"}, "999.0.0.1", id="format"),
    ],
)
def test_glue_json_reader_refuses_as_fastjsonschema_does(monkeypatch, schema, record):
    """aws-glue-schema-registry 1.1.3 with fastjsonschema 2.22.2 refused both records."""
    read = _glue_json_reader(monkeypatch, schema)

    with pytest.raises(SerializationError, match="does not match the schema"):
        read(record)


def test_glue_json_reader_compiles_a_version_once(monkeypatch):
    compiled = []
    compile_schema = glue_json.fastjsonschema.compile
    monkeypatch.setattr(
        glue_json.fastjsonschema,
        "compile",
        lambda schema: compiled.append(schema) or compile_schema(schema),
    )
    read = _glue_json_reader(monkeypatch, {"properties": A_DEFAULT})

    assert read({}) == {"a": 1}
    assert read({}) == {"a": 1}
    assert len(compiled) == 1


class _Response(io.BytesIO):
    def info(self):
        return types.SimpleNamespace(get_content_charset=lambda: None)


def test_glue_json_reader_fetches_a_remote_reference_when_it_compiles(monkeypatch):
    """fastjsonschema fetches a remote `$ref` with `urlopen` while it compiles."""
    fetched = []

    def urlopen(uri):
        fetched.append(uri)
        return _Response(json.dumps({"properties": {"a": {"default": 9}}}).encode())

    monkeypatch.setattr("urllib.request.urlopen", urlopen)
    read = _glue_json_reader(
        monkeypatch, {"properties": {"n": {"$ref": "http://x/remote.json"}}}
    )

    assert read({"n": {}}) == {"n": {"a": 9}}
    assert read({"n": {}}) == {"n": {"a": 9}}
    assert fetched == ["http://x/remote.json"]


@pytest.mark.parametrize(
    "definition",
    [
        pytest.param('{"type": 5}', id="unknown type"),
        pytest.param('{"pattern": "("}', id="bad pattern"),
        pytest.param('{"properties": 3}', id="keyword of the wrong type"),
        pytest.param('{"$ref": "#/definitions/missing"}', id="missing reference"),
    ],
)
def test_glue_json_validator_refuses_a_schema_it_cannot_compile(definition):
    with pytest.raises(SerializationError, match="JSON Schema cannot be compiled"):
        glue_json.validator(definition)


def test_glue_json_validator_refuses_a_definition_that_is_not_json():
    with pytest.raises(SerializationError, match="JSON Schema is not valid JSON"):
        glue_json.validator("{")


@pytest.mark.parametrize(
    ("definition", "data"),
    [
        # 1000 levels, under orjson's limit of 1024; json.dumps cannot write it
        pytest.param(
            '{"properties": {"n": {"$ref": "#"}}}',
            b'{"n":' * 1000 + b"{}" + b"}" * 1000,
            id="nested too deeply",
        ),
        pytest.param('{"multipleOf": 0}', b"5", id="multipleOf 0"),
    ],
)
def test_glue_json_decode_refuses_a_record_it_cannot_check(definition, data):
    validate = glue_json.validator(definition)

    with pytest.raises(SerializationError, match="cannot be checked against"):
        glue_json.decode(validate, data)


def test_glue_avro_classes_write_and_read_a_bytes_schema_with_its_length_prefix(
    monkeypatch,
):
    """aws-glue-schema-registry 1.1.3 wrote `b"ab"` as 04 61 62, standard Avro."""
    glue = FakeGlue(
        get_schema_by_definition=[version_answer()],
        get_schema_version=[version_answer(definition='"bytes"')],
    )
    serializer, deserializer = glue_pair(monkeypatch, glue, "bytes")
    framed = frame_glue(GLUE_VERSION, bytes.fromhex("046162"))

    assert serializer.serialize("t", b"ab") == framed
    assert deserializer.deserialize("t", framed) == b"ab"


def test_glue_deserializer_refuses_a_format_it_does_not_read(monkeypatch):
    glue = FakeGlue(get_schema_version=[version_answer(fmt="PROTOBUF")])
    _, deserializer = glue_pair(monkeypatch, glue)

    with pytest.raises(SerializationError, match="data format PROTOBUF"):
        deserializer.deserialize("t", G1)


# JSON Schema and Protobuf
#
# J1 is the JSON record of G3 framed for Confluent. G3 is the same record framed for
# Glue, and is the Glue fixture of the JSON Schema classes. The Protobuf bytes follow
# the Protobuf encoding guide: a field is its number and wire type, then the value
# (field 1, a string, is 0a; field 2, a varint, is 10; 3 is 03). The message index of
# Confluent is the zigzag varints of the path's length and positions (2 is 04, 1 is
# 02, 0 is 00), and the path [0] is the single byte 00.

JSON_SCHEMA = GLUE_JSON_SCHEMA
J1 = bytes.fromhex("00 00 00 00 01") + G3[18:]


def proto_file(name, messages, pool=None, dependencies=()):
    """Builds a .proto file in a descriptor pool without protoc.

    `messages` is a list of `(name, fields, nested messages)`; a field is
    `(name, number, type name)`, where the type is `string` or `int32`.
    """
    pool = pool or descriptor_pool.DescriptorPool()
    types = {
        "string": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        "int32": descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
    }

    def add(container, message_name, fields, nested):
        message = container.add(name=message_name)
        for field_name, number, type_name in fields:
            message.field.add(
                name=field_name,
                number=number,
                type=types[type_name],
                label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
            )
        for child in nested:
            add(message.nested_type, *child)

    proto = descriptor_pb2.FileDescriptorProto(
        name=name, package="shop", syntax="proto3", dependency=list(dependencies)
    )
    for message in messages:
        add(proto.message_type, *message)
    return pool, pool.AddSerializedFile(proto.SerializeToString())


def message_class(pool, full_name):
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(full_name))


PROBE_FIELDS = [("path_id", 1, "string"), ("value", 2, "int32")]
PROBE_POOL, PROBE_FILE = proto_file("probe.proto", [("Probe", PROBE_FIELDS, [])])
PROBE_MESSAGE = message_class(PROBE_POOL, "shop.Probe")
# P1: the record as a message, with the index of the first message of the file
P1_BODY = bytes.fromhex("0a 0c") + b"Shop.packers" + bytes.fromhex("10 03")
P1 = bytes.fromhex("00 00 00 00 01 00") + P1_BODY
# P2: a message nested in the second message of its file, so its path is [1, 0]
NESTED_POOL, NESTED_FILE = proto_file(
    "nested.proto",
    [
        ("First", [], []),
        ("Second", [], [("Inner", [("name", 1, "string")], [])]),
    ],
)
INNER_MESSAGE = message_class(NESTED_POOL, "shop.Second.Inner")
P2 = bytes.fromhex("00 00 00 00 02 04 02 00 0a 02") + b"ab"


def test_protobuf_fixtures_hold_the_message_the_codec_writes():
    assert PROBE_MESSAGE(path_id="Shop.packers", value=3).SerializeToString() == P1_BODY
    assert INNER_MESSAGE(name="ab").SerializeToString() == P2[8:]
    assert protobuf.confluent_indexes(PROBE_MESSAGE.DESCRIPTOR) == P1[5:6]
    assert protobuf.confluent_indexes(INNER_MESSAGE.DESCRIPTOR) == P2[5:8]


def test_confluent_indexes_are_read_back():
    assert protobuf.read_confluent_indexes(P1[5:])[0] == [0]
    path, rest = protobuf.read_confluent_indexes(P2[5:])
    assert (path, bytes(rest)) == ([1, 0], P2[8:])


@pytest.mark.parametrize(
    "buf",
    [b"", b"\x04\x02", b"\x80", bytes.fromhex("ff ff ff ff ff ff ff ff ff ff 01")],
    ids=["empty", "path cut short", "varint cut short", "varint too long"],
)
def test_confluent_indexes_that_cannot_be_read(buf):
    with pytest.raises(SerializationError, match="message index"):
        protobuf.read_confluent_indexes(buf)


def test_confluent_index_length_is_limited():
    # a path length of 100001, as a zigzag varint
    with pytest.raises(SerializationError, match="Invalid message index length"):
        protobuf.read_confluent_indexes(bytes.fromhex("c2 b5 0c"))


def test_glue_indexes_follow_the_sorted_full_names_of_the_java_library():
    """The order of AWS Glue Schema Registry v2.0.1 `MessageIndexFinderTest`.

    Messages are declared here in reverse order, so the sort and not the order of
    declaration gives the index.
    """
    expected = [
        "A",
        "A.B",
        "A.B.C",
        "A.B.C.J",
        "A.B.C.J.K",
        "A.B.C.X",
        "A.B.C.X.D",
        "A.B.C.X.D.F",
        "A.B.C.X.D.F.M",
        "A.B.C.X.D.G",
        "A.B.C.X.L",
        "A.I",
        "A.X",
        "N",
        "O",
        "O.A",
    ]

    def children(parent):
        depth = 0 if parent is None else parent.count(".") + 1
        names = [
            name
            for name in expected
            if name.count(".") == depth
            and (parent is None or name.startswith(parent + "."))
        ]
        return [
            (name.rsplit(".", 1)[-1], [], children(name)) for name in reversed(names)
        ]

    pool, _ = proto_file("complex.proto", children(None))
    for index, name in enumerate(expected):
        descriptor = pool.FindMessageTypeByName(f"shop.{name}")
        assert protobuf.glue_index(descriptor) == bytes([index]), name


def test_glue_index_is_an_unsigned_varint():
    assert protobuf.read_glue_index(bytes.fromhex("ac 02 0a")) == (300, b"\x0a")
    with pytest.raises(SerializationError, match="message index"):
        protobuf.read_glue_index(b"\xac")


# Confluent JSON Schema


def test_confluent_json_serializer_writes_j1_and_registers_the_schema(
    registry_transport,
):
    registry = FakeRegistry(
        {("POST", "/subjects/probe-value/versions"): (200, {"id": 1})}
    )
    registry_transport(registry)
    schema_str = json.dumps(JSON_SCHEMA, indent=1)
    serializer = ConfluentJsonSchemaSerializer("http://registry:8081", schema_str)

    # A key the schema does not forbid is written, as it is on the wire of any producer
    first = serializer.serialize("probe", {**GLUE_RECORD, "extra": True})
    second = serializer.serialize("probe", GLUE_RECORD)

    assert second == J1
    assert json.loads(first[5:]) == {**GLUE_RECORD, "extra": True}
    assert json.loads(registry.requests[0].content) == {
        "schema": schema_str,
        "schemaType": "JSON",
        "references": [],
    }
    assert len(registry.requests) == 1


def test_confluent_json_deserializer_reads_j1(registry_transport):
    registry_transport(
        FakeRegistry(
            {("GET", "/schemas/ids/1"): schema_answer(JSON_SCHEMA, schemaType="JSON")}
        )
    )
    deserializer = ConfluentJsonSchemaDeserializer("http://registry:8081")

    assert deserializer.deserialize("probe", J1) == GLUE_RECORD


def test_confluent_json_checks_records_on_both_sides(registry_transport):
    registry_transport(
        FakeRegistry(
            {
                ("POST", "/subjects/probe-value/versions"): (200, {"id": 1}),
                ("GET", "/schemas/ids/1"): schema_answer(
                    JSON_SCHEMA, schemaType="JSON"
                ),
            }
        )
    )
    serializer = ConfluentJsonSchemaSerializer(
        "http://registry:8081", json.dumps(JSON_SCHEMA)
    )
    deserializer = ConfluentJsonSchemaDeserializer("http://registry:8081")
    wrong_type = {"path_id": "A", "value": "three"}

    with pytest.raises(SerializationError, match="does not match the schema"):
        serializer.serialize("probe", wrong_type)
    with pytest.raises(SerializationError, match="does not match the schema"):
        deserializer.deserialize("probe", J1[:5] + json.dumps(wrong_type).encode())
    with pytest.raises(SerializationError, match="not valid JSON"):
        deserializer.deserialize("probe", J1[:5] + b"{")


def test_confluent_json_serializer_checks_the_wire_form_of_a_value():
    schema = {"type": "object", "properties": {"at": {"type": "string"}}}
    validator = json_schema.validator(schema)

    # orjson writes a datetime as a string, and the string is what is checked
    assert json_schema.encode(validator, {"at": datetime.datetime(2026, 1, 2)}) == (
        b'{"at":"2026-01-02T00:00:00"}'
    )


@pytest.mark.parametrize(
    ("schema", "message"),
    [
        ("{", "not valid JSON"),
        ("[]", "must be a JSON object"),
        ('{"type": 5}', "valid"),
    ],
    ids=["not JSON", "not an object", "not a schema"],
)
def test_confluent_json_serializer_refuses_a_schema_it_cannot_use(schema, message):
    with pytest.raises(SerializationError, match=message):
        ConfluentJsonSchemaSerializer("http://registry:8081", schema)


def test_confluent_json_references_are_fetched_once_and_resolved(registry_transport):
    address = {
        "type": "object",
        "properties": {"city": {"type": "string"}},
        "required": ["city"],
    }
    order = {
        "type": "object",
        "properties": {"ship_to": {"$ref": "address.json"}},
    }
    reference = {"name": "address.json", "subject": "address-value", "version": 2}
    registry = FakeRegistry(
        {
            ("GET", "/schemas/ids/5"): schema_answer(
                order, schemaType="JSON", references=[reference]
            ),
            ("GET", "/subjects/address-value/versions/2"): schema_answer(
                address, schemaType="JSON"
            ),
        }
    )
    registry_transport(registry)
    deserializer = ConfluentJsonSchemaDeserializer("http://registry:8081")
    header = bytes.fromhex("00 00 00 00 05")

    assert deserializer.deserialize("orders", header + b'{"ship_to":{"city":"Perth"}}')
    with pytest.raises(SerializationError, match="does not match the schema"):
        deserializer.deserialize("orders", header + b'{"ship_to":{}}')
    assert registry.paths("GET") == [
        "/schemas/ids/5",
        "/subjects/address-value/versions/2",
    ]


def test_json_reference_that_is_not_in_the_registry_is_not_fetched():
    validator = json_schema.validator({"$ref": "https://example.invalid/a.json"})

    with pytest.raises(SerializationError, match="cannot be checked"):
        json_schema.decode(validator, b"{}")


# Confluent Protobuf


def test_confluent_protobuf_serializer_writes_p1_and_registers_the_file(
    registry_transport,
):
    registry = FakeRegistry(
        {("POST", "/subjects/probe-value/versions"): (200, {"id": 1})}
    )
    registry_transport(registry)
    serializer = ConfluentProtobufSerializer("http://registry:8081", PROBE_MESSAGE)

    # A key the schema does not have is dropped; a message instance is written as it is
    from_dict = serializer.serialize("probe", {**GLUE_RECORD, "stream_type": "event"})
    from_message = serializer.serialize("probe", PROBE_MESSAGE(**GLUE_RECORD))

    assert from_dict == from_message == P1
    body = json.loads(registry.requests[0].content)
    assert body["schemaType"] == "PROTOBUF"
    assert body["references"] == []
    registered = descriptor_pb2.FileDescriptorProto.FromString(
        base64.b64decode(body["schema"])
    )
    assert registered.name == "probe.proto"
    assert len(registry.requests) == 1


def test_confluent_protobuf_deserializer_reads_p1_and_p2(registry_transport):
    registry_transport(
        FakeRegistry(
            {
                ("GET", "/schemas/ids/1"): schema_answer(
                    PROBE_GLUE, schemaType="PROTOBUF"
                ),
                ("GET", "/schemas/ids/2"): schema_answer(
                    PROBE_GLUE, schemaType="PROTOBUF"
                ),
            }
        )
    )

    assert ConfluentProtobufDeserializer(
        "http://registry:8081", PROBE_MESSAGE
    ).deserialize("probe", P1) == {"path_id": "Shop.packers", "value": 3}
    assert ConfluentProtobufDeserializer(
        "http://registry:8081", INNER_MESSAGE
    ).deserialize("nested", P2) == {"name": "ab"}


def test_confluent_protobuf_record_that_does_not_fit_is_refused(registry_transport):
    registry_transport(
        FakeRegistry(
            {
                ("POST", "/subjects/probe-value/versions"): (200, {"id": 1}),
                ("GET", "/schemas/ids/1"): schema_answer(
                    PROBE_GLUE, schemaType="PROTOBUF"
                ),
            }
        )
    )
    serializer = ConfluentProtobufSerializer("http://registry:8081", PROBE_MESSAGE)
    deserializer = ConfluentProtobufDeserializer("http://registry:8081", PROBE_MESSAGE)

    with pytest.raises(SerializationError, match="does not fit Probe"):
        serializer.serialize("probe", {"value": "three"})
    with pytest.raises(SerializationError, match="Expected a Probe or a dict"):
        serializer.serialize("probe", INNER_MESSAGE(name="ab"))
    # field 2 is a varint, and the data ends in the middle of its value
    with pytest.raises(SerializationError, match="not a Probe message"):
        deserializer.deserialize("probe", P1[:6] + bytes.fromhex("10 ff"))


@pytest.mark.parametrize(
    ("own", "other", "build"),
    [
        (
            "JSON",
            "AVRO",
            lambda: ConfluentJsonSchemaDeserializer("http://registry:8081"),
        ),
        (
            "PROTOBUF",
            "JSON",
            lambda: ConfluentProtobufDeserializer(
                "http://registry:8081", PROBE_MESSAGE
            ),
        ),
    ],
    ids=["json", "protobuf"],
)
def test_confluent_format_deserializers_refuse_another_type_and_read_rules(
    registry_transport, own, other, build
):
    rule_set = {"domainRules": [{"name": "check", "mode": "READ"}]}
    registry_transport(
        FakeRegistry(
            {
                ("GET", "/schemas/ids/1"): schema_answer(JSON_SCHEMA, schemaType=other),
                ("GET", "/schemas/ids/2"): schema_answer(
                    JSON_SCHEMA, schemaType=own, ruleSet=rule_set
                ),
            }
        )
    )
    deserializer = build()

    with pytest.raises(SerializationError, match=f"reads {own} only"):
        deserializer.deserialize("t", bytes.fromhex("00 00 00 00 01 00 00"))
    with pytest.raises(SerializationError, match="check"):
        deserializer.deserialize("t", bytes.fromhex("00 00 00 00 02 00 00"))


def test_protobuf_dependencies_are_registered_before_the_file_that_imports_them(
    registry_transport,
):
    pool = descriptor_pool.DescriptorPool()
    pool.AddSerializedFile(timestamp_pb2.DESCRIPTOR.serialized_pb)
    proto_file("base.proto", [("Base", [], [])], pool)
    proto_file("middle.proto", [("Middle", [], [])], pool, ["base.proto"])
    _, top = proto_file(
        "top.proto",
        [("Top", [], [])],
        pool,
        ["middle.proto", "google/protobuf/timestamp.proto"],
    )
    registry = FakeRegistry(
        {
            ("POST", "/subjects/base.proto/versions"): (200, {"id": 10}),
            ("POST", "/subjects/base.proto?normalize=false"): (200, {"version": 3}),
            ("POST", "/subjects/middle.proto/versions"): (200, {"id": 11}),
            ("POST", "/subjects/middle.proto?normalize=false"): (200, {"version": 4}),
            ("POST", "/subjects/t-value/versions"): (200, {"id": 12}),
        }
    )
    registry_transport(registry)
    serializer = ConfluentProtobufSerializer(
        "http://registry:8081", message_class(pool, "shop.Top")
    )

    serializer.serialize("t", {})
    serializer.serialize("t", {})

    assert registry.paths() == [
        "/subjects/base.proto/versions",
        "/subjects/base.proto?normalize=false",
        "/subjects/middle.proto/versions",
        "/subjects/middle.proto?normalize=false",
        "/subjects/t-value/versions",
    ]
    middle, top_request = (registry.requests[2], registry.requests[4])
    assert json.loads(middle.content)["references"] == [
        {"name": "base.proto", "subject": "base.proto", "version": 3}
    ]
    assert json.loads(top_request.content)["references"] == [
        {"name": "middle.proto", "subject": "middle.proto", "version": 4}
    ]
    assert top.name == "top.proto"


def test_lookup_posts_the_schema_once_and_returns_the_version():
    registry = FakeRegistry(
        {
            ("POST", "/subjects/a%2Fb?normalize=false"): [
                (404, {"error_code": 40403, "message": "Schema not found"}),
                (200, {"subject": "a/b", "id": 7, "version": 5, "schema": "x"}),
            ]
        }
    )
    client = client_for(registry)

    with pytest.raises(SchemaRegistryError) as failure:
        client.lookup("a/b", "x", "PROTOBUF")
    assert failure.value.error_code == 40403
    # the failure was not cached, and the answer is
    assert client.lookup("a/b", "x", "PROTOBUF") == 5
    assert client.lookup("a/b", "x", "PROTOBUF") == 5

    assert len(registry.requests) == 2
    assert json.loads(registry.requests[1].content) == {
        "schema": "x",
        "schemaType": "PROTOBUF",
        "references": [],
    }


# AWS Glue JSON Schema and Protobuf


def glue_formats(monkeypatch, glue, *builders):
    monkeypatch.setattr("boto3.client", lambda service, **kwargs: glue)
    return [build() for build in builders]


def test_glue_json_serializer_writes_g3_and_registers_the_definition(monkeypatch):
    glue = FakeGlue(
        get_schema_by_definition=[SCHEMA_MISSING, version_answer(fmt="JSON")],
        create_schema=[create_answer()],
    )
    [serializer] = glue_formats(
        monkeypatch,
        glue,
        lambda: GlueJsonSchemaSerializer("probe", json.dumps(JSON_SCHEMA, indent=2)),
    )
    assert glue.calls == []

    record = dict(GLUE_RECORD)
    first = serializer.serialize("t", record)
    second = serializer.serialize("t", GLUE_RECORD)

    assert first == second == G3
    assert record == GLUE_RECORD
    assert glue.names() == ["get_schema_by_definition", "create_schema"]
    created = glue.calls[1][1]
    assert created["DataFormat"] == "JSON"
    assert created["SchemaName"] == "t-value"
    # the definition is what aws-glue-schema-registry 1.1.3 sent: orjson's compact JSON
    assert created["SchemaDefinition"] == json.dumps(JSON_SCHEMA, separators=(",", ":"))


def test_glue_json_serializer_fills_a_default_and_refuses_a_wrong_record(monkeypatch):
    schema = {
        "type": "object",
        "properties": {"n": {"type": "integer"}, "unit": {"default": "kg"}},
    }
    glue = FakeGlue(get_schema_by_definition=[version_answer(fmt="JSON")])
    [serializer] = glue_formats(
        monkeypatch, glue, lambda: GlueJsonSchemaSerializer("probe", json.dumps(schema))
    )

    assert json.loads(serializer.serialize("t", {"n": 1})[18:]) == {
        "n": 1,
        "unit": "kg",
    }
    calls = len(glue.calls)
    with pytest.raises(SerializationError, match="does not match the schema"):
        serializer.serialize("t", {"n": "one"})
    assert len(glue.calls) == calls


def test_glue_json_serializer_refuses_a_schema_that_is_not_json(monkeypatch):
    glue = FakeGlue()
    with pytest.raises(SerializationError, match="The JSON Schema is not valid JSON"):
        glue_formats(monkeypatch, glue, lambda: GlueJsonSchemaSerializer("probe", "{x"))
    assert glue.calls == []


def test_glue_json_deserializer_reads_g3_and_refuses_another_format(monkeypatch):
    glue = FakeGlue(
        get_schema_version=[
            version_answer(fmt="JSON", definition=json.dumps(JSON_SCHEMA)),
        ]
    )
    [deserializer] = glue_formats(
        monkeypatch, glue, lambda: GlueJsonSchemaDeserializer("probe")
    )

    assert deserializer.deserialize("t", G3) == GLUE_RECORD
    assert deserializer.deserialize("t", G3) == GLUE_RECORD
    assert glue.names() == ["get_schema_version"]
    wrong_type = frame_glue(GLUE_VERSION, b'{"path_id":"A","value":"three"}')
    with pytest.raises(SerializationError, match="does not match the schema"):
        deserializer.deserialize("t", wrong_type)

    other = FakeGlue(get_schema_version=[version_answer(fmt="AVRO")])
    [avro_only] = glue_formats(
        monkeypatch, other, lambda: GlueJsonSchemaDeserializer("probe")
    )
    with pytest.raises(SerializationError, match="data format AVRO"):
        avro_only.deserialize("t", G1)


PROBE_PROTO = (
    'syntax = "proto3";\npackage shop;\nmessage Probe { string path_id = 1; }\n'
)
# GP: a message nested in the second message of its file. Sorted by full name the
# messages are Second, Second.Inner and First, so Inner has the index 2.
GP = frame_glue(GLUE_VERSION, bytes.fromhex("02 0a 02") + b"ab")


def test_glue_protobuf_serializer_writes_gp_and_registers_the_proto_text(monkeypatch):
    glue = FakeGlue(
        get_schema_by_definition=[SCHEMA_MISSING, version_answer(fmt="PROTOBUF")],
        create_schema=[create_answer()],
    )
    [serializer] = glue_formats(
        monkeypatch,
        glue,
        lambda: GlueProtobufSerializer("probe", INNER_MESSAGE, PROBE_PROTO),
    )
    assert glue.calls == []

    assert serializer.serialize("t", {"name": "ab", "stream_type": "event"}) == GP
    assert serializer.serialize("t", INNER_MESSAGE(name="ab")) == GP

    created = glue.calls[1][1]
    assert created["DataFormat"] == "PROTOBUF"
    assert created["SchemaDefinition"] == PROBE_PROTO
    assert glue.names() == ["get_schema_by_definition", "create_schema"]


def test_glue_protobuf_deserializer_reads_gp_and_refuses_another_format(monkeypatch):
    glue = FakeGlue(
        get_schema_version=[version_answer(fmt="PROTOBUF", definition=PROBE_PROTO)]
    )
    [deserializer] = glue_formats(
        monkeypatch, glue, lambda: GlueProtobufDeserializer("probe", INNER_MESSAGE)
    )
    assert glue.calls == []

    assert deserializer.deserialize("t", GP) == {"name": "ab"}
    assert deserializer.deserialize("t", GP) == {"name": "ab"}
    assert glue.names() == ["get_schema_version"]

    other = FakeGlue(get_schema_version=[version_answer(fmt="JSON")])
    [json_only] = glue_formats(
        monkeypatch, other, lambda: GlueProtobufDeserializer("probe", INNER_MESSAGE)
    )
    with pytest.raises(SerializationError, match="data format JSON"):
        json_only.deserialize("t", GP)
