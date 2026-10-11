"""The Custom Logic guides' scripts run, unpaced, and behave as their pages say.

The pages show these files, and test_docs_match_examples.py keeps the copies equal to
them.
"""

import ast
import importlib.util
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GUIDES = ROOT / "docs" / "snippets" / "guides"


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


def test_breakdowns_stop_the_lathe_until_repaired(caplog):
    module = _load("preemptive_breakdowns")
    module.app.factor = 0.0
    with caplog.at_level(logging.INFO, logger="dynamic_des"):
        module.app.run(until=200)
    events = _events(caplog)
    statuses = [value["status"] for _, value in events]
    assert "interrupted" in statuses
    assert "machine_repaired" in statuses
    assert "finished" in statuses

    # Between a breakdown and its repair, no other part starts on the lathe, and the
    # interrupted part finishes after the repair.
    down = None
    for key, value in events:
        if value["status"] == "interrupted":
            down = key
            assert value["cause"] == "overheating"
        elif value["status"] == "machine_repaired":
            assert key == down
            down = None
        elif value["status"] == "started":
            assert down is None


def test_handoff_holds_the_crane_until_the_mill_is_free(caplog):
    module = _load("multi_resource_handoffs")
    module.app.factor = 0.0
    with caplog.at_level(logging.INFO, logger="dynamic_des"):
        module.app.run(until=60)
    events = _events(caplog)
    assert any(value["status"] == "finished" for _, value in events)

    # Each part goes queued, crane_loaded, mill_entered, finished, in that order.
    seen = {}
    for key, value in events:
        seen.setdefault(key, []).append(value["status"])
    order = ["queued", "crane_loaded", "mill_entered", "finished"]
    for statuses in seen.values():
        assert statuses == order[: len(statuses)]

    # One part at a time holds the crane (from crane_loaded to mill_entered) and the
    # mill (from mill_entered to finished).
    on_crane, in_mill = None, None
    for key, value in events:
        if value["status"] == "crane_loaded":
            assert on_crane is None
            on_crane = key
        elif value["status"] == "mill_entered":
            assert on_crane == key and in_mill is None
            on_crane, in_mill = None, key
        elif value["status"] == "finished":
            assert in_mill == key
            in_mill = None


def test_supervisor_adds_the_molder_mid_run(caplog):
    module = _load("dynamic_topology")
    with caplog.at_level(logging.INFO, logger="dynamic_des"):
        resources = module.run(until=20, factor=0.0)
    assert set(resources) == {"lathe", "molder"}
    assert resources["molder"].capacity == 2

    events = _events(caplog)
    added = [value for key, value in events if key == "supervisor"]
    assert added == [{"status": "molder_added", "capacity": 2}]
    started = [value["resource"] for _, value in events if value["status"] == "started"]
    assert "lathe" in started and "molder" in started
