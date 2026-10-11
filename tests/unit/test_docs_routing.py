"""The Complex Routing guide's router sends each record to the topic the page says."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SNIPPET = ROOT / "docs" / "snippets" / "pages" / "complex_routing.py"


def _router():
    spec = importlib.util.spec_from_file_location("complex_routing", SNIPPET)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.custom_kafka_router


def test_router_sends_each_record_to_its_topic():
    route = _router()
    telemetry = {"stream_type": "telemetry", "path_id": "Factory_A.lathe.in_use"}
    assert route({**telemetry, "value": 1}) == "factory-telemetry"
    event = {"stream_type": "event", "key": "task-1"}
    assert route({**event, "value": {"status": "machine_failed"}}) == "system-alarms"
    assert route({**event, "value": {"level": "ERROR"}}) == "system-alarms"
    assert route({**event, "value": {"status": "finished"}}) == "factory-events"
    assert route({**event, "value": "a plain string"}) == "factory-events"
