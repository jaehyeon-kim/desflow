"""The guides' scripts run, unpaced, and behave as their pages say.

The pages show these files, and test_docs_match_examples.py keeps the copies equal to
them. A script that needs a broker runs here with a capturing sink in place of its
Kafka egress, so no service is needed.
"""

import ast
import asyncio
import importlib.util
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from desflow.connectors.egress.base import BaseEgress

ROOT = Path(__file__).resolve().parents[2]
GUIDES = ROOT / "docs" / "snippets" / "guides"


class _Capture(BaseEgress):
    """Keeps every record it receives, in place of a Kafka egress."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _captured(app, until):
    """Runs a builder app unpaced with its one egress replaced by a capture."""
    capture = _Capture()
    app.factor = 0.0
    app._egress_providers = [capture]
    app.run(until=until)
    return capture.records


def _load(name):
    return _load_from(GUIDES, name)


def _load_from(folder, name):
    sys.path.insert(0, str(folder))
    try:
        spec = importlib.util.spec_from_file_location(name, folder / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(folder))


def _events(caplog):
    """(key, value) of every event the console egress logged, in order."""
    events = []
    for record in caplog.records:
        # One log call per batch, one line per record
        for line in record.getMessage().split("\n"):
            if line.startswith("[EVT] "):
                data = ast.literal_eval(line[len("[EVT] ") :])
                events.append((data["key"], data["value"]))
    return events


def test_breakdowns_downtime_matches_the_events():
    """The station of the breakdowns example fails five times and is down 52.5 s of
    300; eight orders are interrupted and all eight resume, as the page says."""
    module = _load("breakdowns")
    examples = ROOT / "examples"
    outcomes = [
        module.yaml_form(str(examples / "yaml" / "breakdowns.yaml")),
        module.builder_form(str(examples / "builder" / "breakdowns_example.py")),
        module.low_level_form(str(examples / "low-level" / "breakdowns_example.py")),
    ]
    assert outcomes[0] == outcomes[1] == outcomes[2]
    assert outcomes[0] == {
        "periods": [
            (84.6, 98.5),
            (150.1, 157.3),
            (238.9, 253.5),
            (272.7, 282.6),
            (285.0, 291.8),
        ],
        "downtime": 52.5,
        "share_down": 0.175,
        "interrupted": 8,
        "resumed": 8,
        "order_packed": 184,
    }


def test_breakdowns_check_runs_from_the_downloaded_files(tmp_path, monkeypatch):
    """The check finds the example where `desflow examples get breakdowns` writes it,
    and prints the lines the page shows."""
    for api, name in (
        ("yaml", "breakdowns.yaml"),
        ("builder", "breakdowns_example.py"),
        ("low-level", "breakdowns_example.py"),
    ):
        (tmp_path / "breakdowns" / api).mkdir(parents=True)
        shutil.copy(ROOT / "examples" / api / name, tmp_path / "breakdowns" / api)
    monkeypatch.chdir(tmp_path)
    module = _load("breakdowns")
    forms = (module.yaml_form, module.builder_form, module.low_level_form)
    printed = "\n".join(str(form()) for form in forms)
    page = (ROOT / "docs/how-to/modelling/breakdowns.md").read_text(encoding="utf-8")
    assert f"```text\n{printed}\n```" in page


def _script_from_page(page, module, script, import_line, tmp_path, import_first=False):
    """Writes `script` from the python blocks of docs/snippets/guides/<module>.py on
    a docs page, in the order the page shows them, with the desflow import line the
    page gives in its text, and returns the page text."""
    text = (ROOT / "docs" / page).read_text(encoding="utf-8")
    assert f"`{import_line}`" in text
    found = re.findall(
        rf'^( *)```python title="docs/snippets/guides/{module}\.py:[\w-]+"[^\n]*\n(.*?)\n\1```$',
        text,
        re.M | re.S,
    )
    parts = [
        "\n".join(line[len(indent) :] for line in code.split("\n"))
        for indent, code in found
    ]
    parts.insert(0 if import_first else 1, import_line)
    (tmp_path / script).write_text("\n\n\n".join(parts) + "\n", encoding="utf-8")
    return text


def _saved_file_from_page(page, name, tmp_path):
    """Writes `name` from the block of docs/snippets/yaml/<name> on a docs page, as a
    reader saves it, so a part the page leaves out fails the run."""
    text = (ROOT / "docs" / page).read_text(encoding="utf-8")
    found = re.findall(
        rf'^( *)```\w+ title="docs/snippets/yaml/{re.escape(name)}[:"][^\n]*\n(.*?)\n\1```$',
        text,
        re.M | re.S,
    )
    assert found, f"{page} shows no block of {name}"
    parts = [
        "\n".join(line[len(indent) :] for line in code.split("\n"))
        for indent, code in found
    ]
    (tmp_path / name).write_text("\n".join(parts) + "\n", encoding="utf-8")


def _printed_on_the_page(text, script):
    """The text block that follows the page's bash block for `script`, and the
    prefix its `grep "^..."` keeps ("" when the command does not filter). The block
    gives the uv command and the pip command, and both must run the script."""
    command, shown = re.search(
        rf"```bash\n((?:(?!```).)*?{re.escape(script)}(?:(?!```).)*?)\n```\n\n```text\n(.*?)\n```",
        text,
        re.S,
    ).groups()
    assert f"uv run --no-project --with desflow {script}" in command
    assert f"pip install desflow\npython {script}" in command
    prefix = re.search(r'grep "\^([^"]*)"', command)
    return (prefix.group(1) if prefix else ""), shown


def _example_files(tmp_path, name, files):
    """Where `desflow examples get <name>` writes the example's forms."""
    for api, file in files:
        (tmp_path / name / api).mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / "examples" / api / file, tmp_path / name / api)


_BREAKDOWNS = (
    "from desflow import DynamicRealtimeEnvironment, Sampler, SimulationContext"
)
_RENEGING = (
    "from desflow import CapacityConfig, DynamicRealtimeEnvironment, "
    "DynamicResource, Sampler, SimParameter, SimulationContext"
)
YAML_SNIPPETS = ROOT / "docs" / "snippets" / "yaml"


@pytest.mark.parametrize(
    "page,module,script,import_line,import_first",
    [
        (
            "how-to/modelling/breakdowns.md",
            "breakdowns",
            "check_breakdowns.py",
            _BREAKDOWNS,
            False,
        ),
        (
            "how-to/modelling/reneging-and-balking.md",
            "reneging_and_balking",
            "check_reneging.py",
            _RENEGING,
            False,
        ),
        (
            "how-to/modelling/wait-for-one-or-all.md",
            "wait_for_one_or_all",
            "wait_for_one_or_all.py",
            "from desflow import Entity, SimulationContext",
            False,
        ),
        (
            "how-to/modelling/wait-for-one-or-all.md",
            "wait_for_one_or_all_low_level",
            "wait_for_one_or_all_low_level.py",
            "from desflow import DynamicRealtimeEnvironment, Signal, SignalFailed",
            True,
        ),
    ],
)
def test_how_to_check_scripts_run_as_the_page_builds_them(
    page, module, script, import_line, import_first, tmp_path
):
    """The script a reader builds from the page's blocks, in the page's order and
    with the page's import line, runs in a folder of the saved files and prints the
    lines the page shows. Importing the snippet file cannot show a function that
    the page leaves out."""
    _example_files(
        tmp_path,
        "breakdowns",
        [
            ("yaml", "breakdowns.yaml"),
            ("builder", "breakdowns_example.py"),
            ("low-level", "breakdowns_example.py"),
        ],
    ) if module == "breakdowns" else None
    if module == "reneging_and_balking":
        _saved_file_from_page(page, "reneging_tie.yaml", tmp_path)
        _example_files(
            tmp_path,
            "patience",
            [
                ("yaml", "patience.yaml"),
                ("builder", "patience_example.py"),
                ("low-level", "patience_example.py"),
            ],
        )
    if module == "wait_for_one_or_all":
        for name in (
            "wait_first.yaml",
            "wait_tie.yaml",
            "wait_all.yaml",
            "wait_for_one_or_all_logic.py",
        ):
            _saved_file_from_page(page, name, tmp_path)
    text = _script_from_page(page, module, script, import_line, tmp_path, import_first)
    run = subprocess.run(
        [sys.executable, script], cwd=tmp_path, capture_output=True, text=True
    )
    assert run.returncode == 0, run.stderr[-800:]
    prefix, shown = _printed_on_the_page(text, script)
    printed = [line for line in run.stdout.splitlines() if line.startswith(prefix)]
    assert "\n".join(printed) == shown


def test_time_varying_arrivals_boundaries():
    """The gap drawn at the step's time uses the new value; a step at until never
    applies."""
    from desflow import SimulationContext

    for module in (
        _load("time_varying_arrivals"),
        _load("time_varying_arrivals_low_level"),
    ):
        assert module.arrivals(50) == ([10.0, 20.0, 30.0, 35.0, 40.0, 45.0], 5.0)
        assert module.arrivals(30) == ([10.0, 20.0], 10.0)
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "time_varying_arrivals.yaml"
    for until, times in (
        (50, [10.0, 20.0, 30.0, 35.0, 40.0, 45.0]),
        (30, [10.0, 20.0]),
    ):
        events = _event_rows(_captured(SimulationContext.from_yaml(blueprint), until))
        assert [row[0] for row in events] == times


def test_handoff_holds_the_picker_until_the_bench_is_free(caplog):
    module = _load("multi_resource_handoffs")
    module.app.factor = 0.0
    with caplog.at_level(logging.INFO, logger="desflow"):
        module.app.run(until=60)
    events = _events(caplog)
    assert any(value.get("event_type") == "order_packed" for _, value in events)

    # Each order goes through its steps in this order
    def step(value):
        if "status" in value:
            return f"{value['path_id'].rsplit('.', 1)[1]} {value['status']}"
        return value["event_type"]

    order = [
        "walking queued",
        "walking started",
        "order_picked",
        "packing queued",
        "packing started",
        "order_packed",
    ]
    seen: dict = {}
    for key, value in events:
        seen.setdefault(key, []).append(step(value))
    for key, steps in seen.items():
        assert steps == order[: len(steps)], key

    # One order at a time holds the picker, from its pick's start until its pack
    # starts, and one at a time is on the bench, from its pack's start until it is
    # packed
    with_picker, on_bench = None, None
    for key, value in events:
        current = step(value)
        if current == "walking started":
            assert with_picker is None, f"{key} picked while {with_picker} held it"
            with_picker = key
        elif current == "packing started":
            assert with_picker == key and on_bench is None
            with_picker, on_bench = None, key
        elif current == "order_packed":
            assert on_bench == key
            on_bench = None


def _event_rows(records):
    """(sim_ts, key, task, instance, value) of every event, in order."""
    return [
        (r["sim_ts"], r["key"], r["task"], r["instance"], r["value"])
        for r in records
        if r.get("stream_type") == "event"
    ]


def test_handoff_forms_publish_the_same_events():
    """The YAML, builder and low-level handoffs publish the same events at the same
    times, with seed 42."""
    from desflow import SimulationContext

    blueprint = ROOT / "docs" / "snippets" / "yaml" / "multi_resource_handoffs.yaml"
    yaml_events = _event_rows(_captured(SimulationContext.from_yaml(blueprint), 30))
    builder_events = _event_rows(_captured(_load("multi_resource_handoffs").app, 30))
    capture = _Capture()
    _load("multi_resource_handoffs_low_level").run(30, 0.0, [capture])
    low_level_events = _event_rows(capture.records)
    assert builder_events and any(
        row[4].get("event_type") == "order_packed" for row in builder_events
    )
    assert yaml_events == builder_events == low_level_events


def test_supervisor_adds_the_wrappers_mid_run(caplog):
    module = _load("dynamic_topology")
    with caplog.at_level(logging.INFO, logger="desflow"):
        resources = module.run(until=20, factor=0.0)
    assert set(resources) == {"packers", "wrappers"}
    assert resources["wrappers"].capacity == 2

    events = _events(caplog)
    added = [value for key, value in events if key == "supervisor"]
    assert added == [{"status": "wrappers_added", "capacity": 2}]
    started = [value["resource"] for _, value in events if value["status"] == "started"]
    assert "packers" in started and "wrappers" in started


def test_supervisor_adds_the_wrappers_in_every_form(tmp_path, monkeypatch, caplog):
    """The blueprint and the builder run the same supervisor as a process, and publish
    the events the low-level script publishes."""
    from desflow import SimulationContext

    # A reader copies the page's files into one folder and runs them there
    for source in (
        GUIDES / "dynamic_topology_builder.py",
        ROOT / "docs" / "snippets" / "yaml" / "dynamic_topology.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "dynamic_topology_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    with caplog.at_level(logging.INFO, logger="desflow"):
        _load("dynamic_topology").run(until=20, factor=0.0)
    low_level = _events(caplog)
    yaml_app = SimulationContext.from_yaml(tmp_path / "dynamic_topology.yaml")
    builder_app = _load_from(tmp_path, "dynamic_topology_builder").app
    for app in (yaml_app, builder_app):
        events = [(r[1], r[4]) for r in _event_rows(_captured(app, 20))]
        assert events == low_level
    assert ("supervisor", {"status": "wrappers_added", "capacity": 2}) in low_level


def test_router_sends_each_record_to_its_topic(tmp_path, monkeypatch):
    """The router picks each record's topic; the three forms send the same events, to
    all three topics, and only failed orders to the alarms."""
    from desflow import SimulationContext

    # A reader copies the page's files into one folder and runs them there
    for source in (
        GUIDES / "complex_routing.py",
        GUIDES / "complex_routing_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "complex_routing.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "complex_routing_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    logic = _load_from(tmp_path, "complex_routing_logic")
    route = logic.custom_kafka_router
    telemetry = {"stream_type": "telemetry", "path_id": "Shop.packers.in_use"}
    assert route({**telemetry, "value": 1}) == "shop-telemetry"
    event = {"stream_type": "event", "key": "orders-1"}
    assert route({**event, "value": {"status": "packing_failed"}}) == "system-alarms"
    assert route({**event, "value": {"level": "ERROR"}}) == "system-alarms"
    assert route({**event, "value": {"status": "finished"}}) == "shop-events"
    assert route({**event, "value": "a plain string"}) == "shop-events"

    # The blueprint creates its topics in run.before; here there is no broker
    import complex_routing_logic

    monkeypatch.setattr(complex_routing_logic, "create_topics", lambda: None)
    yaml_records = _captured(
        SimulationContext.from_yaml(tmp_path / "complex_routing.yaml"), 120
    )
    builder_records = _captured(_load_from(tmp_path, "complex_routing").app, 120)
    capture = _Capture()
    _load_from(tmp_path, "complex_routing_low_level").run(120, 0.0, [capture])
    low_level_records = capture.records
    for records in (yaml_records, builder_records, low_level_records):
        assert {route(r) for r in records} == set(logic.TOPICS)
        alarms = [r for r in records if route(r) == "system-alarms"]
        assert alarms and all(r["instance"] % 10 == 9 for r in alarms)
    assert (
        _event_rows(yaml_records)
        == _event_rows(builder_records)
        == _event_rows(low_level_records)
    )


def test_secure_egress_passes_its_settings_to_the_client(monkeypatch):
    """The SASL settings reach the client in the builder and the blueprint, and the
    three forms publish the same events."""
    from desflow import SimulationContext

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9092")
    monkeypatch.setenv("KAFKA_API_KEY", "test-key")
    monkeypatch.setenv("KAFKA_API_SECRET", "test-secret")
    sys.path.insert(0, str(GUIDES))  # the builder script imports the egress
    try:
        connections = _load("kafka_security_connections")
        module = _load("kafka_security")
    finally:
        sys.path.remove(str(GUIDES))
    blueprint = SimulationContext.from_yaml(
        ROOT / "docs" / "snippets" / "yaml" / "kafka_security.yaml"
    )
    assert connections.connection["sasl_plain_username"] == "test-key"
    for egress in (module.egress, blueprint._egress_providers[0]):
        config = egress.producer_config
        assert config["security_protocol"] == "SASL_SSL"
        assert config["sasl_plain_username"] == "test-key"
        assert egress.topic_router is None  # so it creates its own topics

    records = _captured(module.app, until=30)
    assert any(r.get("value") == {"event_type": "order_packed"} for r in records)
    sys.path.insert(0, str(GUIDES))  # the low-level script imports the egress
    try:
        low_level = _load("kafka_security_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    capture = _Capture()
    env = low_level.shop([capture], 0.0)
    env.run(until=30)
    env.teardown()
    assert (
        _event_rows(_captured(blueprint, until=30))
        == _event_rows(records)
        == _event_rows(capture.records)
    )


def test_predictions_fit_the_avro_schema(tmp_path, monkeypatch):
    """The generated schema describes the record KafkaEgress sends, and the
    predictions of the builder, YAML and low-level forms fit it."""
    fastavro = pytest.importorskip("fastavro")
    from desflow import SimulationContext

    module = _load("avro_and_pydantic_models")
    schema = json.loads(module.schema_string)
    assert [f["name"] for f in schema["fields"]] == [
        "key",
        "task",
        "instance",
        "sim_ts",
        "timestamp",
        "value",
    ]

    # A reader puts the files in one folder; the blueprint's !python tags import
    # from it, and here there is no broker to create the topics on
    for source in (
        GUIDES / "avro_and_pydantic.py",
        GUIDES / "avro_and_pydantic_models.py",
        GUIDES / "avro_and_pydantic_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "avro_and_pydantic.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "avro_and_pydantic_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    import avro_and_pydantic_logic

    monkeypatch.setattr(avro_and_pydantic_logic, "create_topics", lambda: None)
    yaml_records = _captured(
        SimulationContext.from_yaml(tmp_path / "avro_and_pydantic.yaml"), 30
    )
    capture = _Capture()
    env = _load_from(tmp_path, "avro_and_pydantic_low_level").shop([capture], 0.0)
    env.run(until=30)
    env.teardown()

    route = module.ml_topic_router
    parsed = fastavro.parse_schema(schema)
    builder = _load_from(tmp_path, "avro_and_pydantic")
    for records in (_captured(builder.app, until=30), yaml_records, capture.records):
        predictions = [r for r in records if route(r) == "ml-predictions"]
        assert predictions
        assert {"ml-predictions", "sim-events"} <= {route(r) for r in records}
        for record in predictions:
            sent = {k: v for k, v in record.items() if k != "stream_type"}
            assert fastavro.validate(sent, parsed)


def test_secured_registry_serializer_sends_its_credentials(registry_transport):
    """The Registry Connection section builds a serializer whose requests carry `auth`."""
    httpx = pytest.importorskip("httpx")
    sent = []

    def registry(request):
        sent.append(request)
        return httpx.Response(200, json={"id": 1})

    registry_transport(registry)
    sys.path.insert(0, str(GUIDES))  # the registry file imports the models
    try:
        module = _load("avro_and_pydantic_registry")
    finally:
        sys.path.remove(str(GUIDES))

    payload = module.secured_serializer.serialize(
        "ml-predictions",
        {
            "key": "o1",
            "task": None,
            "instance": None,
            "sim_ts": 1.0,
            "timestamp": "2026-01-01T00:00:00Z",
            "value": {"event_type": "prediction", "task_id": "o1", "confidence": 0.9},
        },
    )

    assert sent[0].url.host == "registry.example.com"
    assert sent[0].headers["Authorization"].startswith("Basic ")
    assert payload[:5] == b"\x00\x00\x00\x00\x01"


class _Registry:
    """A schema registry that keeps every schema registered and serves it back by id."""

    def __init__(self):
        self.registered = []

    def __call__(self, request):
        import httpx

        if request.method == "POST":
            self.registered.append(json.loads(request.content))
            return httpx.Response(200, json={"id": len(self.registered)})
        schema_id = int(request.url.path.rsplit("/", 1)[1])
        return httpx.Response(200, json=self.registered[schema_id - 1])


_SHOP_MODULES = (
    "json_schema_and_protobuf",
    "json_schema_and_protobuf_serializers",
    "json_schema_and_protobuf_logic",
    "json_schema_and_protobuf_low_level",
)


def _run_shop_form(form, tmp_path, monkeypatch):
    """Runs one form of the JSON Schema and Protobuf shop for 30 simulated seconds.

    Returns the records it publishes and the serializers its egress holds by topic.
    """
    from desflow import SimulationContext

    # A reader puts the files in one folder; the blueprint's !python tags import from
    # it, and here there is no broker to create the topics on
    for source in (
        GUIDES / "json_schema_and_protobuf.py",
        GUIDES / "json_schema_and_protobuf_serializers.py",
        GUIDES / "json_schema_and_protobuf_low_level.py",
        GUIDES / "shop_record.proto",
        GUIDES / "shop_record_pb2.py",
        ROOT / "docs" / "snippets" / "yaml" / "json_schema_and_protobuf.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "json_schema_and_protobuf_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in _SHOP_MODULES:
        monkeypatch.delitem(sys.modules, name, raising=False)
    try:
        if form == "builder":
            module = _load_from(tmp_path, "json_schema_and_protobuf")
            return _captured(module.app, until=30), module.egress.topic_serializers
        if form == "yaml":
            import json_schema_and_protobuf_logic

            monkeypatch.setattr(
                json_schema_and_protobuf_logic, "create_topics", lambda: None
            )
            app = SimulationContext.from_yaml(
                tmp_path / "json_schema_and_protobuf.yaml"
            )
            serializers = app._egress_providers[0].topic_serializers
            return _captured(app, until=30), serializers
        module = _load_from(tmp_path, "json_schema_and_protobuf_low_level")
        capture = _Capture()
        env = module.shop([capture], 0.0)
        env.run(until=30)
        env.teardown()
        return capture.records, module.egress.topic_serializers
    finally:
        for name in _SHOP_MODULES:
            sys.modules.pop(name, None)


@pytest.mark.parametrize("form", ["builder", "yaml", "low-level"])
def test_orders_round_trip_as_json_schema_and_protobuf(
    form, tmp_path, monkeypatch, registry_transport
):
    """Each form sends the orders to orders-json and orders-protobuf; both formats
    read back what was sent, and the 64 bit integers of a Protobuf record come back
    as strings, as the page says."""
    pytest.importorskip("jsonschema")
    pytest.importorskip("google.protobuf")

    registry = _Registry()
    registry_transport(registry)
    records, serializers = _run_shop_form(form, tmp_path, monkeypatch)
    assert set(serializers) == {"orders-json", "orders-protobuf"}

    # The page's read_order decodes each message, as a consumer of the topics would
    page = _load("json_schema_and_protobuf_serializers")
    route = page.order_topic_router
    assert {"sim-events", "orders-json", "orders-protobuf"} <= {
        route(r) for r in records
    }
    for record in records:
        topic = route(record)
        if topic not in serializers:
            continue
        sent = {k: v for k, v in record.items() if k != "stream_type"}
        payload = serializers[topic].serialize(topic, sent)
        if topic == "orders-json":
            assert page.read_order(topic, payload) == sent
        else:
            as_strings = {
                **sent,
                "instance": str(sent["instance"]),
                "value": {**sent["value"], "order_id": str(sent["value"]["order_id"])},
            }
            assert page.read_order(topic, payload) == as_strings

    # The Confluent JSON serializer registers the schema as given
    kinds = {schema["schemaType"] for schema in registry.registered}
    assert kinds == {"JSON", "PROTOBUF"}
    assert registry.registered[0]["schema"] == page.json_schema


def test_glue_serializers_round_trip_the_order_record(monkeypatch):
    """The Glue serializers of the page take the same record, and the Protobuf one is
    given the text of shop_record.proto as its schema."""
    pytest.importorskip("boto3")
    pytest.importorskip("fastjsonschema")
    pytest.importorskip("google.protobuf")
    from desflow.connectors.ingress.kafka import (
        GlueJsonSchemaDeserializer,
        GlueProtobufDeserializer,
    )

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

    glue = _Glue()
    monkeypatch.setattr("boto3.client", lambda service, **kwargs: glue)
    module = _load("json_schema_and_protobuf_serializers")
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


def test_shop_record_module_declares_the_fields_of_the_proto_file():
    """shop_record_pb2.py is generated from shop_record.proto; a field the file changed
    and the module did not shows up here."""
    pytest.importorskip("google.protobuf")
    text = (GUIDES / "shop_record.proto").read_text()
    shop_record = _load("shop_record_pb2").ShopRecord
    assert shop_record.DESCRIPTOR.full_name == "com.desflow.shop.ShopRecord"
    for message in shop_record.DESCRIPTOR.file.message_types_by_name.values():
        for field in message.fields:
            assert f"{field.name} = {field.number};" in text


def test_reneging_and_balking_ties_and_balance(tmp_path, monkeypatch):
    """At a tie, the order scheduled later is served and the one scheduled first
    reneges, as the page says, in every form; and every order of the patience
    example is packed, reneged, balked or still in the shop, in every form. The
    script runs from the folder a reader works in: the saved blueprint beside it
    and the files `desflow examples get patience` writes."""
    shutil.copy(ROOT / "docs" / "snippets" / "yaml" / "reneging_tie.yaml", tmp_path)
    for api, name in (
        ("yaml", "patience.yaml"),
        ("builder", "patience_example.py"),
        ("low-level", "patience_example.py"),
    ):
        (tmp_path / "patience" / api).mkdir(parents=True)
        shutil.copy(ROOT / "examples" / api / name, tmp_path / "patience" / api)
    monkeypatch.chdir(tmp_path)
    module = _load("reneging_and_balking")
    for tie in (module.tie, module.tie_yaml, module.tie_low_level):
        assert tie(3, 3) == "served"
        assert tie(0, 6) == "reneged"
    forms = (module.yaml_form, module.builder_form, module.low_level_form)
    for form in forms:
        assert form() == {"order_packed": 49, "balked": 17, "reneged": 7, "started": 1}
    printed = [
        "check: arrives at 3, patience 3: " + module.tie(3, 3),
        "check: arrives at 0, patience 6: " + module.tie(0, 6),
        f"check: the same in YAML: {module.tie_yaml(3, 3)} {module.tie_yaml(0, 6)}",
        f"check: the same, low-level: {module.tie_low_level(3, 3)} "
        f"{module.tie_low_level(0, 6)}",
        *(f"check: {dict(form())}" for form in forms),
    ]
    page = (ROOT / "docs/how-to/modelling/reneging-and-balking.md").read_text("utf-8")
    assert "```text\n" + "\n".join(printed) + "\n```" in page


def test_wait_for_one_or_all_first_ties_and_failure():
    """The first branch wins; two signals at one instant are both done; a failed
    branch of an all fails the order at once while the child flow runs on. The
    builder, YAML and low-level forms agree."""
    module = _load("wait_for_one_or_all")
    low_level = _load("wait_for_one_or_all_low_level")
    yaml = ROOT / "docs" / "snippets" / "yaml"
    forms = [
        (module.first, module.two_at_once, module.all_with_a_failure),
        (
            lambda gap: module.first_yaml(gap, yaml / "wait_first.yaml"),
            lambda: module.two_at_once_yaml(yaml / "wait_tie.yaml"),
            lambda: module.all_with_a_failure_yaml(yaml / "wait_all.yaml"),
        ),
        (low_level.first, low_level.two_at_once, low_level.all_with_a_failure),
    ]
    for first, two_at_once, all_with_a_failure in forms:
        assert first(10) == {"stock": True, "limit": False}
        assert first(50) == {"stock": False, "limit": True}
        assert two_at_once() == {"boxes": True, "labels": True}
        assert all_with_a_failure() == [
            (0.0, "order", "queued"),
            (2.0, "order", "failed"),
            (4.0, "inspect", "finished"),
        ]


def test_wait_for_one_or_all_example_packs_and_refunds():
    """The page's example: each restock waits for all of its delivery (10 s) and its
    stock count (20 s), so it comes 20 s after each delivery starts, at 80 s to 560 s;
    27 orders are packed after a restock and 35 are refunded at the limit, in every
    form."""
    expected = {
        "pack": 27,
        "refund": 35,
        "restocked_at": [80.0 + 60 * n for n in range(9)],
    }
    assert _load("wait_for_one_or_all_example").outcome() == expected
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "wait_for_one_or_all_example.yaml"
    assert _load("wait_for_one_or_all").example_yaml(str(blueprint)) == expected
    assert _load("wait_for_one_or_all_example_low_level").outcome() == expected


def test_wait_for_one_or_all_checks_run_from_saved_files(tmp_path, monkeypatch):
    """The script runs in a folder with the saved blueprints and prints the lines the
    page shows; `desflow run --stdout` gives the counts and restock times the page
    reads from the YAML example."""
    from typer.testing import CliRunner

    from desflow.cli import app

    yaml = ROOT / "docs" / "snippets" / "yaml"
    for name in ("wait_first.yaml", "wait_tie.yaml", "wait_all.yaml"):
        shutil.copy(yaml / name, tmp_path)
    shutil.copy(yaml / "wait_for_one_or_all_logic.py", tmp_path)
    monkeypatch.chdir(tmp_path)
    module = _load("wait_for_one_or_all")
    printed = [
        f"check: restock after 10 s: {module.first(10)}",
        f"check: restock after 50 s: {module.first(50)}",
        f"check: two at once: {module.two_at_once()}",
        f"check: all, with a failure: {module.all_with_a_failure()}",
        f"check: the same in YAML: {module.first_yaml(10)} {module.first_yaml(50)}",
        f"check: two at once in YAML: {module.two_at_once_yaml()}",
        f"check: all, with a failure, in YAML: {module.all_with_a_failure_yaml()}",
    ]
    page = (ROOT / "docs/how-to/modelling/wait-for-one-or-all.md").read_text("utf-8")
    assert "```text\n" + "\n".join(printed) + "\n```" in page

    result = CliRunner().invoke(
        app, ["run", str(yaml / "wait_for_one_or_all_example.yaml"), "--stdout"]
    )
    assert result.exit_code == 0, result.output
    records = result.stdout
    counts = re.findall(
        r'"path_id":"Shop.stats.tasks.(?:pack|refund).finished","value":\d+', records
    )[-2:]
    restocks = [
        re.search(r'"sim_ts":[0-9.]+', line).group()
        for line in records.splitlines()
        if '"event_type":"restocked"' in line
    ]
    shown = "\n".join("    " + line for line in counts + restocks)
    assert f"```text\n{shown}\n    ```" in page


def test_performance_tuning_counts_the_records():
    """An hour of the shop publishes 108,688 records at factor 0: no lag readings,
    which would add 3,600, and a rate above 0."""
    module = _load("performance_tuning")
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "performance_tuning.yaml"
    sys.path.insert(0, str(GUIDES))  # the low-level script imports Count from it
    try:
        low_level = _load("performance_tuning_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    for outcome in (
        module.blueprint(str(blueprint)),
        module.throughput(module.shop(), until=3600),
        low_level.throughput(until=3600),
    ):
        assert outcome["records"] == 108_688
        assert outcome["per_second"] > 0


def test_performance_tuning_commands_print_the_page_count(tmp_path):
    """Each command of the Check section, run from a folder that holds the saved
    files, prints the record count the page shows."""
    for source in (
        GUIDES / "performance_tuning.py",
        GUIDES / "performance_tuning_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "performance_tuning.yaml",
    ):
        shutil.copy(source, tmp_path)
    page = (ROOT / "docs/how-to/tools/performance-tuning.md").read_text("utf-8")
    assert "{'records': 108688, 'per_second': " in page
    for command in (
        ["performance_tuning.py", "yaml"],
        ["performance_tuning.py"],
        ["performance_tuning_low_level.py"],
    ):
        ran = subprocess.run(
            [sys.executable, *command], cwd=tmp_path, capture_output=True, text=True
        )
        assert ran.returncode == 0, ran.stderr
        assert ran.stdout.startswith("{'records': 108688, 'per_second': "), ran.stdout


def test_repair_desk_runs_and_explain_names_no_bottleneck(tmp_path):
    """The Use an AI Assistant check: `desflow run repair_desk.yaml` ends without an
    error and logs the lines the page shows, and `desflow explain` names no
    bottleneck."""
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "repair_desk.yaml"
    page = (ROOT / "docs/how-to/tools/use-an-ai-assistant.md").read_text("utf-8")

    def desflow(*args):
        ran = subprocess.run(
            [str(Path(sys.executable).parent / "desflow"), *args],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert ran.returncode == 0, ran.stderr
        return ran.stdout + ran.stderr

    logged = desflow("run", str(blueprint))
    shown = re.findall(r"^\[INFO\] (desflow\.[^\n]*)$", page, re.M)
    assert len(shown) == 6
    assert all(line in logged for line in shown)
    bottleneck = next(
        line
        for line in desflow("explain", str(blueprint)).splitlines()
        if line.startswith("Bottleneck")
    )
    assert f"```text\n{bottleneck}\n```" in page


def test_fit_a_distribution_gaps(tmp_path, capsys):
    """The exponential fits the 299 gaps best, at 0.364 a second; 5 zero gaps skip
    gamma, weibull and lognormal; the file it writes is the one the blueprint
    includes; and one packer packs 1,302 orders in the hour, in every form."""
    pytest.importorskip("scipy")
    from desflow import SimulationContext

    module = _load("fit_a_distribution")
    result = module.fit()
    capsys.readouterr()
    assert result.best == "exponential" and result.settings()["rate"] == 0.36388
    statuses = {row["family"]: (row["status"], row["reason"]) for row in result.table}
    for family in ("gamma", "weibull", "lognormal"):
        assert statuses[family] == (
            "skipped",
            f"{family} needs every value above 0; 5 are not",
        )
    assert statuses["truncated_normal"][0] == "did not fit"
    assert statuses["uniform"][0] == statuses["triangular"][0] == "rejected"
    histogram = module.histogram().settings()
    assert len(histogram["counts"]) == 10 and sum(histogram["counts"]) == 299
    written = module.write(result, tmp_path)
    blueprint = ROOT / "docs" / "snippets" / "yaml"
    assert written.read_text() == (blueprint / "fitted" / "orders.yaml").read_text()
    # The command line route reads the column the script writes
    from typer.testing import CliRunner

    from desflow.cli import app as cli

    column = module.write_gaps(tmp_path)
    assert column.read_text().splitlines()[0] == "gap_seconds"
    assert len(column.read_text().splitlines()) == 300
    out = tmp_path / "fitted" / "cli.yaml"
    ran = CliRunner().invoke(
        cli,
        ["fit", str(column), "--column", "gap_seconds", "--unit", "s", "--lower", "0"]
        + ["--out", str(out)],
    )
    assert ran.exit_code == 0, ran.output
    assert "exponential *" in ran.stdout and "rate=0.36388" in ran.stdout
    shipped = (blueprint / "fitted" / "orders.yaml").read_text().splitlines()[1:]
    assert out.read_text().splitlines()[1:] == shipped
    app = module.shop(result)
    app.run(until="1 h")
    yaml_app = SimulationContext.from_yaml(blueprint / "fit_a_distribution.yaml")
    yaml_app.run()
    for run in (app, yaml_app):
        assert run.env.stats.readings()["Shop.stats.flows.orders.finished"] == 1302
    sys.path.insert(0, str(GUIDES))  # the low-level script imports fit from it
    try:
        low_level = _load("fit_a_distribution_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    assert low_level.run(result) == 1302


def test_test_a_model_tests_pass_and_the_saved_results_are_current(
    tmp_path, monkeypatch
):
    """The three kinds of test pass; the waits fall from 2.055 s with one packer to
    0.111 s with two and 0.016 s with three; saving the results writes the file the
    back test reads; and the YAML and low-level forms give the same results."""
    module = _load("test_a_model")
    module.test_more_packers_shorten_the_wait()
    module.test_results_match_the_saved_run()
    for packers, wait in ((1, 2.055375), (2, 0.110574), (3, 0.016206)):
        assert module.results(packers=packers)["wait_mean"] == wait
    assert module.results() == {"finished": 319, "wait_mean": 0.110574}
    for settings in (
        {"dist": "exponential", "rate": 0},
        {"dist": "constant", "value": -1},
    ):
        module.test_a_distribution_that_cannot_happen_is_refused(settings)
    monkeypatch.setattr(module, "SAVED", tmp_path / "saved.json")
    module.save_results()
    assert (tmp_path / "saved.json").read_text() == (
        GUIDES / "test_a_model_saved.json"
    ).read_text()
    # The YAML and low-level files run the same tests against the same saved results
    for source in (
        GUIDES / "test_a_model_yaml.py",
        GUIDES / "test_a_model_low_level.py",
        GUIDES / "test_a_model_saved.json",
        ROOT / "docs" / "snippets" / "yaml" / "test_a_model.yaml",
    ):
        shutil.copy(source, tmp_path)
    for form in (
        _load_from(tmp_path, "test_a_model_yaml"),
        _load_from(tmp_path, "test_a_model_low_level"),
    ):
        form.test_more_packers_shorten_the_wait()
        form.test_results_match_the_saved_run()
        for packers, wait in ((1, 2.055375), (2, 0.110574), (3, 0.016206)):
            assert form.results(packers=packers)["wait_mean"] == wait
    # The page shows "4 passed" for each form, and each file has four tests
    page = (ROOT / "docs/how-to/analysis/test-a-model.md").read_text("utf-8")
    assert page.count(" 4 passed in ") == 3
    shutil.copy(GUIDES / "test_a_model.py", tmp_path)
    for name in ("test_a_model", "test_a_model_yaml", "test_a_model_low_level"):
        ran = subprocess.run(
            [sys.executable, "-m", "pytest", f"{name}.py", "-p", "no:cacheprovider"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert " 4 passed in " in ran.stdout, ran.stdout


def test_analyse_the_event_log_waits_and_throughput(tmp_path):
    """Two minutes of one packer write 191 event rows, which become 318 log rows for
    64 orders; 63 leave the shop, the mean wait is 1.250 s and 31.5 orders leave each
    minute; every start is the later of the arrival and the last start plus 1.5 s;
    and the YAML and low-level forms write the same events."""
    module = _load("analyse_the_event_log")
    module.run(tmp_path)
    events = module.read_events(tmp_path)
    assert events.num_rows == 191
    statuses = [row for row in events["status"].to_pylist()]
    assert [statuses.count(s) for s in ("queued", "started", "finished")] == [
        64,
        64,
        63,
    ]
    log = module.event_log(events)
    assert log.column_names == ["entity_id", "event_type", "event", "time", "run"]
    assert log.num_rows == 318 and set(log["run"].to_pylist()) == {1}
    per_order = module.waits_and_times(log)
    assert len(per_order) == 64 and len(module.times(log, "depart")) == 63
    assert per_order["orders-1"] == {"wait": 0.0, "in_shop": 1.5}
    assert max(row["wait"] for row in per_order.values()) == 3.66
    assert round(module.mean_wait(per_order), 3) == 1.25
    assert module.throughput_per_minute(log, module.RUN_SECONDS) == 31.5
    arrived = module.times(log, "arrival")
    started = module.times(log, "packing_start")
    last = None
    for order in sorted(started, key=started.get):
        expected = arrived[order] if last is None else max(arrived[order], last + 1.5)
        assert started[order] == pytest.approx(expected)
        last = started[order]
    # The log is sorted by time
    times = log["time"].to_pylist()
    assert times == sorted(times)
    # The YAML and low-level forms write the same events
    sys.path.insert(0, str(GUIDES))  # the low-level script imports from the module
    try:
        low_level = _load("analyse_the_event_log_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "analyse_the_event_log.yaml"
    module.run(tmp_path / "builder")
    module.run_yaml(tmp_path / "yaml", str(blueprint))
    low_level.run(tmp_path / "low_level")

    def rows(out):
        return sorted(
            (row["sim_ts"], row["key"], row["task"], row["status"])
            for row in module.read_events(out).to_pylist()
        )

    builder = rows(tmp_path / "builder")
    assert len(builder) == 191
    assert rows(tmp_path / "yaml") == rows(tmp_path / "low_level") == builder


def test_analyse_the_event_log_commands_print_the_page_counts(tmp_path):
    """Each command of the Check section, run from a folder that holds the saved
    files, prints the counts the page shows."""
    for source in (
        GUIDES / "analyse_the_event_log.py",
        GUIDES / "analyse_the_event_log_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "analyse_the_event_log.yaml",
    ):
        shutil.copy(source, tmp_path)
    page = (ROOT / "docs/how-to/analysis/analyse-the-event-log.md").read_text("utf-8")
    for command in (
        ["analyse_the_event_log.py", "yaml"],
        ["analyse_the_event_log.py"],
        ["analyse_the_event_log_low_level.py"],
    ):
        ran = subprocess.run(
            [sys.executable, *command], cwd=tmp_path, capture_output=True, text=True
        )
        assert ran.returncode == 0, ran.stderr
        assert f"```text\n{ran.stdout}```" in page


def test_animate_a_run_frames_and_entities(tmp_path):
    """The animation of the two-minute run has 61 frames, one for every 2 seconds
    from 0 to 120, and draws the 64 orders of Analyse the Event Log, in every form."""
    import vidigi  # noqa: F401  (a missing dev dependency fails the test)

    module = _load("animate_a_run")
    figure = module.make(tmp_path / "builder")
    assert len(figure.frames) == 61
    assert [frame.name for frame in figure.frames][:3] == ["0", "2", "4"]
    assert figure.frames[-1].name == "120"
    assert module.vidigi_log(tmp_path / "builder")["entity_id"].nunique() == 64
    # The YAML and low-level runs give the same animation
    analyse = _load("analyse_the_event_log")
    sys.path.insert(0, str(GUIDES))  # the low-level script imports from analyse
    try:
        low_level = _load("analyse_the_event_log_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    blueprint = str(ROOT / "docs" / "snippets" / "yaml" / "analyse_the_event_log.yaml")
    for out, run_shop in (
        (tmp_path / "yaml", lambda out: analyse.run_yaml(out, blueprint)),
        (tmp_path / "low_level", low_level.run),
    ):
        other = module.make(out, run_shop)
        assert [frame.name for frame in other.frames] == [
            frame.name for frame in figure.frames
        ]
        assert module.vidigi_log(out).equals(module.vidigi_log(tmp_path / "builder"))


def test_share_by_weight_grants_by_finish_tag_in_every_form(tmp_path, monkeypatch):
    """Nine orders arrive at time 0: tier 2, six of tier 1, two of tier 2. Tier 1
    weighs 3 and tier 2 weighs 1, and the first order takes the packer, so the
    finish tags are worked out by hand here.

    The first order of tier 2 is granted at once with tag 1, and the queue's clock
    becomes 1. Tier 1 then gets 4/3, 5/3, 2, 7/3, 8/3 and 3 (1/3 more each time);
    tier 2 gets 2 and 3 (1 more each time). The smallest tag goes first, and at the
    tags 2 and 3 the order that arrived first goes first, which is tier 1. So the
    tags are granted 4/3, 5/3, 2 (tier 1), 2 (tier 2), 7/3, 8/3, 3 (tier 1), 3 (tier 2),
    and the tiers in grant order, including the first order, are 2, 1, 1, 1, 2, 1, 1, 1, 2.
    First in, first out would give
    2, 1, 1, 1, 1, 1, 1, 2, 2.
    """
    # A reader copies the page's files into one folder and runs the script there
    for source in (
        GUIDES / "share_by_weight.py",
        GUIDES / "share_by_weight_low_level.py",
        *(ROOT / "docs" / "snippets" / "yaml").glob("share_by_weight*"),
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    module = _load_from(tmp_path, "share_by_weight")
    low_level = _load_from(tmp_path, "share_by_weight_low_level")
    by_hand = [2, 1, 1, 1, 2, 1, 1, 1, 2]
    fifo = [2, 1, 1, 1, 1, 1, 1, 2, 2]
    assert module.TIERS == fifo  # the orders arrive in the first in, first out order
    assert by_hand != fifo
    for form in (module.yaml_form, module.builder_form, low_level.low_level_form):
        assert form() == by_hand


def test_editor_settings_hold_the_schema_and_the_three_tags():
    """Set Up Your Editor shows one settings object: the schema for a folder of
    blueprints and the three tags a blueprint uses."""
    settings = json.loads((ROOT / "docs/snippets/editor/settings.json").read_text())
    assert settings["yaml.customTags"] == [
        "!python scalar:string",
        "!include scalar:object",
        "!include mapping:object",
    ]
    assert list(settings["yaml.schemas"].values()) == ["blueprints/*.yaml"]


def test_check_against_theory_agrees_with_the_formulas(capsys, monkeypatch):
    """The wait by hand is 3 s for one packer and 1.286 s for two; explain gives the
    same values; 10 runs of the blueprint agree on every measure; and two packers
    with constant packing have no wait estimate, with D3's reason."""
    from desflow import SimulationContext

    module = _load("check_against_theory")
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "check_against_theory.yaml"
    assert module.BLUEPRINT == blueprint
    assert round(module.by_hand(1, 0.75), 3) == 3.0
    assert round(module.by_hand(2, 1.5), 3) == 1.286
    packers = SimulationContext.from_yaml(blueprint).analyze().resources[0]
    assert packers.wait == pytest.approx(module.by_hand(1, 0.75), abs=1e-9)
    with monkeypatch.context() as patch:
        patch.setenv("PACKERS", "2")
        patch.setenv("RATE", "1.5")
        two = SimulationContext.from_yaml(blueprint).analyze().resources[0]
    assert two.wait == pytest.approx(module.by_hand(2, 1.5), abs=1e-9)
    explanation = module.verify()
    assert [c.differs for c in explanation.verification.comparisons] == [False] * 4
    shop = _load("check_against_theory_shop").app.analyze().resources[0]
    assert shop.wait == pytest.approx(module.by_hand(1, 0.75), abs=1e-9)
    # The builder file is the same model, so its runs give the same measures
    builder = module.verify_builder()
    assert [
        (c.measure, c.measured, c.half_width) for c in builder.verification.comparisons
    ] == [
        (c.measure, c.measured, c.half_width)
        for c in explanation.verification.comparisons
    ]
    reason = "no closed formula for c > 1 with non-exponential times; run --verify to measure it"
    unsupported = module.unsupported()
    assert (unsupported.utilization, unsupported.wait) == (0.75, None)
    assert unsupported.wait_reason == reason
    constant = (
        ROOT / "docs" / "snippets" / "yaml" / "check_against_theory_constant.yaml"
    )
    from_yaml = SimulationContext.from_yaml(constant).analyze().resources[0]
    assert (from_yaml.utilization, from_yaml.wait, from_yaml.wait_reason) == (
        0.75,
        None,
        reason,
    )
    capsys.readouterr()


def test_write_a_connector_applies_the_change_and_writes_the_events(
    tmp_path, monkeypatch
):
    """Nineteen orders arrive, one a second from 1 s, and each takes 1.5 s, so the
    18 that arrive by 18 s are packed by 20 s. The file's change gives the packers 2
    units; the ingress applies it within the first moments of the run, so from then
    on each order starts when it arrives. The three forms write the same events."""
    # A reader copies the page's files into one folder and runs the script there
    for source in (
        GUIDES / "write_a_connector.py",
        GUIDES / "write_a_connector_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "write_a_connector.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "write_a_connector_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    module = _load_from(tmp_path, "write_a_connector")
    low_level = _load_from(tmp_path, "write_a_connector_low_level")
    written = []
    for form in (module.yaml_form, module.builder_form, low_level.low_level_form):
        capacity, rows = form()
        written.append(rows)
        assert capacity == 2
        statuses = [row["status"] for row in rows if row["status"]]
        assert statuses.count("queued") == statuses.count("started") == 19
        packed = [row for row in rows if row["event_type"] == "order_packed"]
        assert len(packed) == 18
        last = [row for row in rows if row["key"] == "orders-19"]
        assert [(row["sim_ts"], row["status"]) for row in last] == [
            ("19.0", "queued"),
            ("19.0", "started"),
        ]
    assert written[0] == written[1] == written[2]
    # The page's YAML check writes the same change with `echo`, and shows these rows
    page = (ROOT / "docs/how-to/records-and-kafka/write-a-connector.md").read_text(
        "utf-8"
    )
    assert f"echo '{json.dumps(module.CHANGE)}' > changes.jsonl" in page
    shown = [
        f"{form.__name__:15} packers: 2, rows in events.csv: {len(rows)}"
        for form, rows in zip(
            (module.yaml_form, module.builder_form, low_level.low_level_form), written
        )
    ]
    assert all(line in page for line in shown)
    first_rows = ["sim_ts,key,task,status,event_type"] + [
        f"{r['sim_ts']},{r['key']},{r['task']},{r['status']},{r['event_type']}"
        for r in written[0][:5]
    ]
    assert "\n".join(first_rows) in page


_STORAGE_MODULES = (
    "cloud_storage",
    "cloud_storage_stores",
    "cloud_storage_logic",
    "cloud_storage_low_level",
)


def _run_storage_form(form, tmp_path, monkeypatch):
    """Runs one form of the Write to Cloud Storage shop on the local disk, in tmp_path.

    Returns the Parquet and the JSON Lines tables, read back with the page's
    read_events.
    """
    from pyarrow import fs

    from desflow import SimulationContext

    for source in (
        GUIDES / "cloud_storage.py",
        GUIDES / "cloud_storage_stores.py",
        GUIDES / "cloud_storage_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "cloud_storage.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "cloud_storage_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("STORAGE", "local")
    monkeypatch.setenv("STORAGE_BUCKET", "output")
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in _STORAGE_MODULES:
        monkeypatch.delitem(sys.modules, name, raising=False)
    try:
        if form == "builder":
            module = _load_from(tmp_path, "cloud_storage")
            module.app.run(until=600)
            module = sys.modules["cloud_storage_stores"]
        elif form == "yaml":
            SimulationContext.from_yaml(tmp_path / "cloud_storage.yaml").run(until=600)
            module = sys.modules["cloud_storage_stores"]
        else:
            low_level = _load_from(tmp_path, "cloud_storage_low_level")
            env = low_level.shop(low_level.sinks)
            env.run(until=600)
            env.teardown()
            module = sys.modules["cloud_storage_stores"]
        store = fs.LocalFileSystem()
        folder = str(tmp_path / "output")
        return (
            module.read_events(store, f"{folder}/parquet"),
            module.read_events(store, f"{folder}/jsonl", format="json"),
        )
    finally:
        for name in _STORAGE_MODULES:
            sys.modules.pop(name, None)


@pytest.mark.parametrize("form", ["builder", "yaml", "low-level"])
def test_cloud_storage_shop_writes_the_same_events_as_parquet_and_jsonl(
    form, tmp_path, monkeypatch
):
    """Each form writes every event to both folders, and read_events reads them back:
    queued, started and finished events, the finished ones with the order record."""
    parquet, jsonl = _run_storage_form(form, tmp_path, monkeypatch)
    assert parquet.num_rows == jsonl.num_rows > 0
    rows = parquet.to_pylist()
    assert {row["status"] for row in rows} == {"queued", "started", None}
    finished = [row for row in rows if row["status"] is None]
    assert finished and all(row["amount"] == 19.99 for row in finished)
    assert {row["key"] for row in rows} == {row["key"] for row in jsonl.to_pylist()}


def test_cloud_storage_forms_write_the_same_rows(tmp_path, monkeypatch):
    """The seeded shop gives the same rows in YAML, Builder and Low-level."""
    rows = []
    for form in ("builder", "yaml", "low-level"):
        folder = tmp_path / form
        folder.mkdir()
        parquet, _ = _run_storage_form(form, folder, monkeypatch)
        columns = ["sim_ts", "key", "status", "order_id"]
        rows.append(sorted(parquet.select(columns).to_pylist(), key=str))
    assert rows[0] == rows[1] == rows[2]


def test_cloud_storage_stores_make_their_filesystems(monkeypatch):
    """Each mapping of the page builds the PyArrow class of its store."""
    from pyarrow import fs

    from desflow.connectors.egress.storage import _build_filesystem

    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "myaccount")
    sys.modules.pop("cloud_storage_stores", None)
    try:
        stores = _load("cloud_storage_stores").STORES
    finally:
        sys.modules.pop("cloud_storage_stores", None)
    classes = {name: type(_build_filesystem(m, fs)) for name, m in stores.items()}
    assert classes == {
        "local": fs.LocalFileSystem,
        "s3": fs.S3FileSystem,
        "gcs": fs.GcsFileSystem,
        "azure": fs.AzureFileSystem,
    }


_ICEBERG_MODULES = (
    "iceberg_tables",
    "iceberg_tables_catalog",
    "iceberg_tables_logic",
    "iceberg_tables_low_level",
)


def _run_iceberg_form(form, tmp_path, monkeypatch):
    """Runs one form of the Write to Iceberg Tables shop on the SQLite catalog, in
    tmp_path, and returns the rows of shop.events, read back with the page's
    read_events."""
    from desflow import SimulationContext

    for source in (
        GUIDES / "iceberg_tables.py",
        GUIDES / "iceberg_tables_catalog.py",
        GUIDES / "iceberg_tables_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "iceberg_tables.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "iceberg_tables_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ICEBERG_CATALOG", raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in _ICEBERG_MODULES:
        monkeypatch.delitem(sys.modules, name, raising=False)
    try:
        if form == "builder":
            module = _load_from(tmp_path, "iceberg_tables")
            module.app.run(until=600)
            module = sys.modules["iceberg_tables_catalog"]
        elif form == "yaml":
            SimulationContext.from_yaml(tmp_path / "iceberg_tables.yaml").run(until=600)
            module = sys.modules["iceberg_tables_catalog"]
        else:
            low_level = _load_from(tmp_path, "iceberg_tables_low_level")
            env = low_level.shop(low_level.sink)
            env.run(until=600)
            env.teardown()
            module = sys.modules["iceberg_tables_catalog"]
        assert (
            module.catalog["uri"]
            == f"sqlite:///{tmp_path.resolve()}/iceberg/catalog.db"
        )
        return module.read_events(module.catalog)
    finally:
        for name in _ICEBERG_MODULES:
            sys.modules.pop(name, None)


def test_iceberg_shop_writes_the_same_rows_in_every_form(tmp_path, monkeypatch):
    """Each form appends every event to shop.events in one commit, and the seeded shop
    gives the same rows in YAML, Builder and Low-level."""
    rows = []
    for form in ("builder", "yaml", "low-level"):
        folder = tmp_path / form
        folder.mkdir()
        table = _run_iceberg_form(form, folder, monkeypatch)
        assert table.num_rows > 0
        columns = ["sim_ts", "key", "status", "order_id", "amount"]
        rows.append(sorted(table.select(columns).to_pylist(), key=str))
    assert rows[0] == rows[1] == rows[2]
    statuses = {row["status"] for row in rows[0]}
    assert statuses == {"queued", "started", None}
    assert all(row["amount"] == 19.99 for row in rows[0] if row["status"] is None)


def test_iceberg_catalogs_name_their_types(monkeypatch, tmp_path):
    """Each catalog of the page is the PyIceberg type its comment names, and
    ICEBERG_CATALOG picks one, with its name."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ICEBERG_CATALOG", "glue")
    sys.modules.pop("iceberg_tables_catalog", None)
    try:
        module = _load("iceberg_tables_catalog")
    finally:
        sys.modules.pop("iceberg_tables_catalog", None)
    types = {name: c["type"] for name, c in module.CATALOGS.items()}
    assert types == {"sqlite": "sql", "rest": "rest", "postgres": "sql", "glue": "glue"}
    assert module.catalog == {"name": "glue", **module.CATALOGS["glue"]}


def test_storage_checks_print_the_row_counts_the_pages_show(tmp_path):
    """After a full hour of the builder shop, the stores file and the catalog file
    print the counts that the Check the Result sections show."""
    for name in (
        "cloud_storage",
        "cloud_storage_stores",
        "iceberg_tables",
        "iceberg_tables_catalog",
    ):
        shutil.copy(GUIDES / f"{name}.py", tmp_path)

    def python(script):
        ran = subprocess.run(
            [sys.executable, script],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            env={k: v for k, v in os.environ.items() if k != "STORAGE"},
        )
        assert ran.returncode == 0, ran.stderr
        return ran.stdout

    python("cloud_storage.py")
    cloud = python("cloud_storage_stores.py")
    python("iceberg_tables.py")
    iceberg = python("iceberg_tables_catalog.py")
    docs = ROOT / "docs" / "how-to" / "storage-and-tables"
    assert f"```text\n{cloud}```" in (docs / "cloud-storage.md").read_text("utf-8")
    assert f"```text\n{iceberg}```" in (docs / "iceberg-tables.md").read_text("utf-8")


class _FakeConsumer:
    """Stands in for AIOKafkaConsumer: serves the messages given, as a broker would."""

    messages: list = []
    settings: dict = {}

    def __init__(self, *topics, **settings):
        type(self).settings = {"topics": topics, **settings}
        self._messages = iter(type(self).messages)

    async def start(self):
        pass

    async def stop(self):
        pass

    async def getone(self):
        from types import SimpleNamespace

        return SimpleNamespace(value=next(self._messages))


def test_kafka_checks_read_the_topics_with_the_connection_they_were_given(
    monkeypatch, capsys, registry_transport
):
    """The read-back of the Check the Result sections consumes the topic of the page
    with the settings of the egress, and prints what the page shows."""
    import asyncio

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker.example.com:9092")
    monkeypatch.setenv("KAFKA_API_KEY", "test-key")
    monkeypatch.setenv("KAFKA_API_SECRET", "test-secret")
    security = _load("kafka_security_connections")
    monkeypatch.setattr(security, "AIOKafkaConsumer", _FakeConsumer)
    events = [b'{"key":"orders-1"}', b'{"key":"orders-2"}', b'{"key":"orders-3"}']
    _FakeConsumer.messages = events
    asyncio.run(security.show_events())
    assert capsys.readouterr().out.splitlines() == [e.decode() for e in events]
    # The consumer gets the egress's own security settings
    assert _FakeConsumer.settings["topics"] == ("sim-events",)
    assert _FakeConsumer.settings["security_protocol"] == "SASL_SSL"
    assert _FakeConsumer.settings["sasl_plain_username"] == "test-key"
    assert _FakeConsumer.settings["bootstrap_servers"] == "broker.example.com:9092"

    registry = _Registry()
    registry_transport(registry)
    serializers = _load("json_schema_and_protobuf_serializers")
    monkeypatch.setattr(serializers, "AIOKafkaConsumer", _FakeConsumer)
    record = {
        "key": "orders-1",
        "task": "pack_order",
        "instance": 0,
        "sim_ts": 1.5,
        "timestamp": "2026-01-01T00:00:00Z",
        "value": {"order_id": 0, "amount": 19.99},
    }
    pytest.importorskip("jsonschema")
    payload = serializers.json_serializer.serialize("orders-json", record)
    _FakeConsumer.messages = [payload]
    asyncio.run(serializers.show_first_order("orders-json"))
    assert capsys.readouterr().out == f"orders-json {record}\n"
    assert _FakeConsumer.settings["topics"] == ("orders-json",)


def test_avro_check_prints_the_prediction_it_reads(
    monkeypatch, capsys, registry_transport
):
    """The read-back of the Avro page decodes a prediction with the schema the
    registry holds for it."""
    import asyncio

    pytest.importorskip("fastavro")
    registry_transport(_Registry())
    models = _load("avro_and_pydantic_models")
    monkeypatch.setattr(models, "AIOKafkaConsumer", _FakeConsumer)
    record = {
        "key": "orders-1",
        "task": "pack_order",
        "instance": 0,
        "sim_ts": 4.052,
        "timestamp": "2026-01-01T00:00:00Z",
        "value": {"event_type": "prediction", "task_id": "orders-1", "confidence": 0.9},
    }
    _FakeConsumer.messages = [
        models.generated_serializer.serialize("ml-predictions", record)
    ]
    asyncio.run(models.show_first_prediction())
    assert capsys.readouterr().out == f"{record}\n"
    assert _FakeConsumer.settings["topics"] == ("ml-predictions",)
