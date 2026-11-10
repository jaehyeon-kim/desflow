"""The files a Kafka connector of a blueprint names are read from the folder of the
blueprint, as a `row` file is, so the working directory does not matter: `schema_file`,
`descriptor_file`, `verify` and `cert` of a `serializer` and of a
`topics.<name>.serializer`, and `cafile`, `certfile` and `keyfile` of `ssl`."""

import shutil
from pathlib import Path

import pytest

from desflow import SimulationContext
from tests.known_cases.harness import write_tls_files

pytest.importorskip("fastavro")
pytest.importorskip("google.protobuf")
pytest.importorskip("httpx")

AVRO = '{"type": "record", "name": "R", "fields": [{"name": "a", "type": "string"}]}'
DESCRIPTOR = Path(__file__).parents[1] / "known_cases" / "dx_21" / "shop_record.desc"


def _document(serializer: str, side: str = "egress") -> str:
    return f"""
simulation: {{sim_id: S, factor: 0}}
arrivals: {{a: {{dist: constant, value: 1, spawn: t}}}}
tasks: {{t: {{payload: {{e: 1}}}}}}
{side}:
  - type: Kafka
    config:
      bootstrap_servers: 127.0.0.1:9092
{serializer}
"""


def _load(
    folder: Path, elsewhere: Path, monkeypatch, serializer: str, side: str = "egress"
):
    (folder / "model.yaml").write_text(_document(serializer, side))
    monkeypatch.chdir(elsewhere)
    return SimulationContext.from_yaml(folder / "model.yaml")


def test_schema_file_is_read_from_the_blueprints_folder(tmp_path, monkeypatch):
    folder, elsewhere = tmp_path / "bp", tmp_path / "cwd"
    folder.mkdir()
    elsewhere.mkdir()
    (folder / "s.avsc").write_text(AVRO)
    app = _load(
        folder,
        elsewhere,
        monkeypatch,
        "      serializer: {type: confluent_avro, registry_url: 'http://r:8081', "
        "schema_file: s.avsc}",
    )
    assert '"name": "R"' in app._sinks.egress[0].provider.default_serializer._schema_str


def test_descriptor_file_is_read_from_the_blueprints_folder(tmp_path, monkeypatch):
    folder, elsewhere = tmp_path / "bp", tmp_path / "cwd"
    folder.mkdir()
    elsewhere.mkdir()
    shutil.copy(DESCRIPTOR, folder / "shop_record.desc")
    app = _load(
        folder,
        elsewhere,
        monkeypatch,
        "      serializer: {type: confluent_protobuf, registry_url: 'http://r:8081', "
        "descriptor_file: shop_record.desc, message: com.desflow.shop.ShopRecord}",
    )
    made = app._sinks.egress[0].provider.default_serializer
    assert made._message_type.DESCRIPTOR.full_name == "com.desflow.shop.ShopRecord"


def test_a_topic_serializer_file_is_read_from_the_blueprints_folder(
    tmp_path, monkeypatch
):
    folder, elsewhere = tmp_path / "bp", tmp_path / "cwd"
    folder.mkdir()
    elsewhere.mkdir()
    (folder / "s.avsc").write_text(AVRO)
    app = _load(
        folder,
        elsewhere,
        monkeypatch,
        "      topics:\n        orders:\n          serializer: {type: confluent_avro, "
        "registry_url: 'http://r:8081', schema_file: s.avsc}",
    )
    made = app._sinks.egress[0].provider.topic_serializers["orders"]
    assert '"name": "R"' in made._schema_str


def test_the_registry_files_of_a_serializer_are_read_from_the_blueprints_folder(
    tmp_path, monkeypatch
):
    folder, elsewhere = tmp_path / "bp", tmp_path / "cwd"
    folder.mkdir()
    elsewhere.mkdir()
    (folder / "s.avsc").write_text(AVRO)
    write_tls_files(folder)
    app = _load(
        folder,
        elsewhere,
        monkeypatch,
        "      serializer: {type: confluent_avro, registry_url: 'https://r:8081', "
        "schema_file: s.avsc, verify: ca.pem, cert: [service.cert, service.key]}",
    )
    assert app._sinks.egress[0].provider.default_serializer._client is not None


@pytest.mark.parametrize("side", ["egress", "ingress"])
def test_the_ssl_files_are_read_from_the_blueprints_folder(side, tmp_path, monkeypatch):
    """The sink's context is also the one its admin connector creates topics with."""
    folder, elsewhere = tmp_path / "bp", tmp_path / "cwd"
    folder.mkdir()
    elsewhere.mkdir()
    write_tls_files(folder)
    app = _load(
        folder,
        elsewhere,
        monkeypatch,
        "      security_protocol: SSL\n"
        "      ssl: {cafile: ca.pem, certfile: service.cert, keyfile: service.key}"
        + ("" if side == "egress" else "\n      topic: control"),
        side,
    )
    if side == "egress":
        context = app._sinks.egress[0].provider.producer_config["ssl_context"]
    else:
        context = app._sinks.ingress[0].kwargs["ssl_context"]
    assert [c["subject"] for c in context.get_ca_certs()] == [
        ((("commonName", "known-cases CA"),),)
    ]
