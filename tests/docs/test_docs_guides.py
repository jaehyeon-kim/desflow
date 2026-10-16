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
import queue
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
    sys.path.insert(0, str(GUIDES))
    try:
        spec = importlib.util.spec_from_file_location(name, GUIDES / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(GUIDES))


def _events(caplog):
    """(key, value) of every event the console egress logged, in order."""
    events = []
    for record in caplog.records:
        message = record.getMessage()
        if message.startswith("[EVT] "):
            data = ast.literal_eval(message[len("[EVT] ") :])
            events.append((data["key"], data["value"]))
    return events


def test_jams_stop_the_printer_until_repaired(caplog):
    module = _load("preemptive_breakdowns")
    module.app.factor = 0.0
    with caplog.at_level(logging.INFO, logger="desflow"):
        module.app.run(until=200)
    events = _events(caplog)
    statuses = [value["status"] for _, value in events]
    assert "interrupted" in statuses
    assert "printer_repaired" in statuses
    assert "finished" in statuses

    # Between a jam and its repair, no other order starts on the printer, and the
    # interrupted order finishes after the repair.
    down = None
    for key, value in events:
        if value["status"] == "interrupted":
            down = key
            assert value["cause"] == "paper_jam"
        elif value["status"] == "printer_repaired":
            assert key == down
            down = None
        elif value["status"] == "started":
            assert down is None


def test_handoff_holds_the_picker_until_the_bench_is_free(caplog):
    module = _load("multi_resource_handoffs")
    module.app.factor = 0.0
    with caplog.at_level(logging.INFO, logger="desflow"):
        module.app.run(until=60)
    events = _events(caplog)
    assert any(value["status"] == "finished" for _, value in events)

    # Each order goes queued, picked, at_bench, finished, in that order.
    seen = {}
    for key, value in events:
        seen.setdefault(key, []).append(value["status"])
    order = ["queued", "picked", "at_bench", "finished"]
    for statuses in seen.values():
        assert statuses == order[: len(statuses)]

    # One order at a time holds the picker (from picked to at_bench) and the bench
    # (from at_bench to finished).
    with_picker, on_bench = None, None
    for key, value in events:
        if value["status"] == "picked":
            assert with_picker is None
            with_picker = key
        elif value["status"] == "at_bench":
            assert with_picker == key and on_bench is None
            with_picker, on_bench = None, key
        elif value["status"] == "finished":
            assert on_bench == key
            on_bench = None


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


def test_router_sends_each_record_to_its_topic():
    module = _load("complex_routing")
    route = module.custom_kafka_router
    telemetry = {"stream_type": "telemetry", "path_id": "Shop.packers.in_use"}
    assert route({**telemetry, "value": 1}) == "shop-telemetry"
    event = {"stream_type": "event", "key": "orders-1"}
    assert route({**event, "value": {"status": "packing_failed"}}) == "system-alarms"
    assert route({**event, "value": {"level": "ERROR"}}) == "system-alarms"
    assert route({**event, "value": {"status": "finished"}}) == "shop-events"
    assert route({**event, "value": "a plain string"}) == "shop-events"

    # The shop sends records to all three topics, and only failed orders to alarms.
    records = _captured(module.app, until=120)
    assert {route(r) for r in records} == set(module.TOPICS)
    alarms = [r for r in records if route(r) == "system-alarms"]
    assert alarms and all(r["instance"] % 10 == 9 for r in alarms)


def test_secure_egress_passes_its_settings_to_the_client():
    module = _load("kafka_security")
    config = module.egress.producer_config
    assert config["security_protocol"] == "SASL_SSL"
    assert config["sasl_plain_username"] == "<YOUR_API_KEY>"
    assert module.egress.topic_router is None  # so it creates its own topics

    records = _captured(module.app, until=30)
    assert any(r.get("value") == {"event_type": "order_packed"} for r in records)


def test_predictions_fit_the_avro_schema():
    fastavro = pytest.importorskip("fastavro")
    module = _load("avro_and_pydantic")
    schema = json.loads(module.schema_string)
    assert [f["name"] for f in schema["fields"]] == [
        "key",
        "task",
        "instance",
        "sim_ts",
        "timestamp",
        "value",
    ]

    records = _captured(module.app, until=30)
    route = module.ml_topic_router
    predictions = [r for r in records if route(r) == "ml-predictions"]
    assert predictions
    assert {"ml-predictions", "sim-events"} <= {route(r) for r in records}
    parsed = fastavro.parse_schema(schema)
    for record in predictions:
        sent = {k: v for k, v in record.items() if k != "stream_type"}
        assert fastavro.validate(sent, parsed)
