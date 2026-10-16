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
