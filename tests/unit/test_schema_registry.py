"""The Confluent Schema Registry code: bytes on the wire, REST calls, caches and options.

Every test uses `httpx.MockTransport`, so no registry and no confluent-kafka is needed.
The byte fixtures follow the Avro 1.12.0 specification, "Binary Encoding": an int is a
zigzag varint (27 is 0x36), a string is its length then UTF-8 ("foo" is 06 66 6f 6f),
and a union writes the index of its branch first (null in ["null","string"] is 00).
"""

import json
import ssl
import types
import uuid

import httpx
import pytest

from desflow.connectors._schema_registry import confluent_client
from desflow.connectors._schema_registry.confluent_client import (
    ConfluentRegistryClient,
    build_ssl_context,
)
from desflow.connectors.egress.kafka import (
    ConfluentAvroSerializer,
    SchemaRegistryError,
    SerializationError,
)
from desflow.connectors.ingress.kafka import ConfluentAvroDeserializer

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
    ],
    ids=["serializer", "deserializer"],
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


def test_guid_payload_of_17_bytes_has_an_empty_rest():
    from desflow.connectors._schema_registry.wire import unframe_confluent

    schema_id, data = unframe_confluent(bytes.fromhex("01" + "ab" * 16))

    assert schema_id == uuid.UUID("ab" * 16)
    assert bytes(data) == b""
