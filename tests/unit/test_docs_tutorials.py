"""The First Simulation tutorial's code runs, in every tab.

The page shows sections of these files, and test_docs_match_examples.py keeps the
copies equal to them. This runs the files: the first shop and the step 2 shop in
the builder and low-level versions, unpaced, checking the events the page describes.
The YAML versions are under docs/snippets/yaml/, where test_docs_snippets.py runs
them. The Kafka versions need a broker, so they are only imported and built here.
"""

import importlib.util
import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
TUTORIALS = ROOT / "docs" / "snippets" / "tutorials"


def _load(name):
    sys.path.insert(0, str(TUTORIALS))
    try:
        spec = importlib.util.spec_from_file_location(name, TUTORIALS / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(TUTORIALS))


def _events(caplog):
    return [r.getMessage() for r in caplog.records if "[EVT]" in r.getMessage()]


def test_low_level_shop_publishes_order_events(caplog):
    module = _load("first_shop_low_level")
    with caplog.at_level(logging.INFO, logger="desflow"):
        module.run(until=30, factor=0.0)
    events = _events(caplog)
    assert any("'status': 'queued'" in line for line in events)
    assert any("'status': 'started'" in line for line in events)
    assert any("'value': {'order_id': 0}" in line for line in events)


def test_builder_shop_publishes_order_events(caplog):
    module = _load("first_shop")
    module.app.factor = 0.0
    with caplog.at_level(logging.INFO, logger="desflow"):
        module.app.run(until=30)
    assert any("'value': {'order_id': 0}" in line for line in _events(caplog))


def test_low_level_changes_end_with_two_packers():
    packers = _load("shop_changes_low_level").run(until=30, factor=0.0)
    assert packers.capacity == 2


def test_builder_changes_end_with_two_packers():
    module = _load("shop_changes")
    module.app.factor = 0.0
    module.app.run(until=30)
    assert module.app.get_resource("packers").capacity == 2


@pytest.mark.parametrize("name", ["shop_kafka", "shop_kafka_low_level"])
def test_kafka_versions_build(name):
    pytest.importorskip("aiokafka")
    _load(name)
