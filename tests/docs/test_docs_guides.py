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
    outcome = module.availability(str(ROOT / "examples" / "yaml" / "breakdowns.yaml"))
    assert outcome == {
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


def test_time_varying_arrivals_boundaries():
    """The gap drawn at the step's time uses the new value; a step at until never
    applies."""
    module = _load("time_varying_arrivals")
    assert module.arrivals(50) == ([10.0, 20.0, 30.0, 35.0, 40.0, 45.0], 5.0)
    assert module.arrivals(30) == ([10.0, 20.0], 10.0)


def test_handoff_holds_the_picker_until_the_bench_is_free(caplog):
    module = _load("multi_resource_handoffs")
    module.app.factor = 0.0
    with caplog.at_level(logging.INFO, logger="desflow"):
        module.app.run(until=60)
    events = _events(caplog)
    assert any(value.get("event_type") == "order_packed" for _, value in events)

    # One order at a time holds the picker: from its pick's start until its pack
    # starts, so no pick starts in between.
    with_picker = None
    for key, value in events:
        task_start = value.get("status") == "started"
        if task_start and value["path_id"] == "Shop.service.walking":
            assert with_picker is None, f"{key} picked while {with_picker} held it"
            with_picker = key
        elif task_start and value["path_id"] == "Shop.service.packing":
            assert with_picker == key
            with_picker = None


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


def test_reneging_and_balking_ties_and_balance():
    """At a tie, the order scheduled later is served and the one scheduled first
    reneges, as the page says; and every order of the patience example is packed,
    reneged, balked or still in the shop."""
    module = _load("reneging_and_balking")
    assert module.tie(3, 3) == "served"
    assert module.tie(0, 6) == "reneged"
    outcome = module.balance(str(ROOT / "examples" / "yaml" / "patience.yaml"))
    assert outcome == {"order_packed": 49, "balked": 17, "reneged": 7, "started": 1}


def test_wait_for_one_or_all_first_ties_and_failure():
    """The first branch wins; two signals at one instant are both done; a failed
    branch of an all fails the order at once while the child flow runs on."""
    module = _load("wait_for_one_or_all")
    assert module.first(10) == {"stock": True, "limit": False}
    assert module.first(50) == {"stock": False, "limit": True}
    assert module.two_at_once() == {"boxes": True, "labels": True}
    assert module.all_with_a_failure() == [
        (0.0, "order", "queued"),
        (2.0, "order", "failed"),
        (4.0, "inspect", "finished"),
    ]


def test_performance_tuning_counts_the_records():
    """An hour of the shop publishes 108,688 records at factor 0: no lag readings,
    which would add 3,600, and a rate above 0."""
    module = _load("performance_tuning")
    outcome = module.throughput(module.shop(), until=3600)
    assert outcome["records"] == 108_688
    assert outcome["per_second"] > 0
