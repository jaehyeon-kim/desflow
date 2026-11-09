"""The Live Runs and Connectors pages: what their examples do."""

import runpy
from pathlib import Path

import pytest

from desflow import ParamRejected, SimulationContext

ROOT = Path(__file__).resolve().parents[2]
PAGES = ROOT / "docs" / "snippets" / "pages"

ON_ENTRY = """\
simulation: {sim_id: S, factor: 0, random_seed: 1}
resources: {packers: {current_cap: 1}}
services: {s: {dist: constant, value: 1}}
arrivals: {a: {dist: constant, value: 1, spawn: t}}
tasks:
  t:
    service: s
    resource: packers
    payload: {kind: x}
    on:
      - {path: S.resources.packers.current_cap, value: many}
run: {until: 5}
"""


def test_resource_paths_and_the_rules_of_a_path():
    """Live Parameters: a resource has a `queue_limit` path, the paths check their
    values by their own rules, and the limit is a read-only property."""
    app = SimulationContext(sim_id="Shop", factor=0.0).add_resource(
        "packers", current_cap=1, max_cap=10
    )
    assert app.registry_paths()["Shop.resources.packers.queue_limit"] is None
    assert "queue_limit=None" in str(app.compile_parameters().resources["packers"])
    namespace = runpy.run_path(str(PAGES / "registry.py"), run_name="__main__")
    registry = namespace["env"].registry
    # the `rules` block ran: "6" is 6, a limit was set and cleared, 2.5 and nan stayed out
    assert registry.get("Shop.resources.packers.max_cap").value == 6
    assert registry.get("Shop.resources.packers.queue_limit").value is None
    assert registry.get("Shop.containers.stock.level").value == 100
    for path, value in [
        ("Shop.resources.packers.current_cap", 2.5),
        ("Shop.resources.packers.queue_limit", -1),
        ("Shop.containers.stock.level", float("nan")),
    ]:
        with pytest.raises(ParamRejected):
            registry.change(path, value)
    with pytest.raises(AttributeError):
        namespace["packers"].queue_limit = 3


def test_an_on_entry_with_a_value_the_path_refuses_is_refused_at_load(tmp_path):
    """Live Parameters: the YAML tab of Know What a Path Accepts."""
    path = tmp_path / "bad.yaml"
    path.write_text(ON_ENTRY, encoding="utf-8")
    with pytest.raises(
        Exception, match=r"bad.yaml:11.*whole number of 0 or more.*'many'"
    ):
        SimulationContext.from_yaml(path)


def _connectors():
    return SimulationContext.from_yaml(
        ROOT / "docs" / "snippets" / "yaml_build" / "connectors.yaml"
    )._sinks.egress


def test_egress_entries_of_the_egress_page_load_with_the_options_they_show():
    """Egress: topic_policy and topics, envelope: false, table_policy and the three
    `when` forms are read as the page says."""
    sinks = _connectors()
    by_policy = [s.provider for s in sinks if getattr(s.provider, "topics", None)]
    assert len(by_policy) == 1
    kafka = by_policy[0]
    assert kafka.topic_policy == {
        "mode": "create",
        "partitions": 3,
        "replication_factor": 1,
        "topic_configs": {"retention.ms": "3600000"},
    }
    assert kafka.topics == {"sim-audit": {"partitions": 1}}
    assert [s.provider.event_topic for s in sinks if not s.envelope] == ["sim-orders"]
    assert "drop_and_create" in [
        getattr(s.provider, "table_policy", None) for s in sinks
    ]
    events, telemetry, expr = [s.predicate for s in sinks[-3:]]
    event = {"stream_type": "event", "key": "k", "task": "t", "value": {"amount": 150}}
    small = {**event, "value": {"amount": 50}}
    reading = {"stream_type": "telemetry", "value": 3}
    assert (events(event), events(reading)) == (True, False)
    assert (telemetry(event), telemetry(reading)) == (False, True)
    assert (expr(event), expr(small), expr(reading)) == (True, False, False)
    with pytest.raises(Exception):
        expr(
            {**event, "value": {}}
        )  # an ordering comparison with a missing field fails


def test_low_level_reads_print_what_the_page_says(capsys):
    """Low-level API, Read a Value, an Expression and a Trace: the printed lines."""
    runpy.run_path(str(PAGES / "low_level_reads.py"), run_name="__main__")
    assert capsys.readouterr().out.splitlines() == [
        "Shop.resources.packers.current_cap 2",
        "True",
        "1 0.5 {'id': 'a'}",
        "2 2.0 {'id': 'b'}",
    ]
