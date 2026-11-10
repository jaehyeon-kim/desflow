"""The Results pages: what their examples publish."""

import asyncio
import queue
import runpy
from pathlib import Path

from desflow import SimulationContext
from desflow.connectors.egress.base import BaseEgress

ROOT = Path(__file__).resolve().parents[2]
YAML = ROOT / "docs" / "snippets" / "yaml"
PAGES = ROOT / "docs" / "snippets" / "pages"


class _Capture(BaseEgress):
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _run(context, until):
    """Runs a built context unpaced, each sink replaced by a capture, and returns
    what each sink received, in the order the sinks were added."""
    from dataclasses import replace

    context.factor = 0.0
    context.go_live_at = None
    context._sinks.egress = [
        replace(sink, provider=_Capture()) for sink in context._sinks.egress
    ]
    context.run(until=until)
    # A sink receives batches (lists of records); flatten them
    return [
        [
            r
            for batch in sink.provider.records
            for r in (batch if isinstance(batch, list) else [batch])
        ]
        for sink in context._sinks.egress
    ]


def _paths(records, kind="telemetry"):
    return [r["path_id"] for r in records if r.get("stream_type") == kind]


def test_readings_in_yaml_and_builder_publish_the_same_values():
    """Statistics, Add Readings of Your Own: the YAML and the Builder form publish
    both readings every 30 seconds from the start."""
    yaml_records = _run(
        SimulationContext.from_yaml(YAML / "statistics_readings.yaml"), 120
    )[0]
    app = runpy.run_path(str(PAGES / "statistics_options.py"))["readings"]
    builder_records = _run(app, 120)[0]
    for records in (yaml_records, builder_records):
        waiting = [r for r in records if r.get("path_id") == "Shop.waiting_per_packer"]
        rate = [r for r in records if r.get("path_id") == "Shop.arrival_rate"]
        assert [r["sim_ts"] for r in waiting] == [0.0, 30.0, 60.0, 90.0]
        assert {r["value"] for r in rate} == {0.6}


def test_on_change_publishes_a_resource_at_each_request_and_release():
    """Statistics, Publish at Each Change: more `in_use` records than the interval
    alone gives, in YAML and in the Builder."""
    plain = SimulationContext.from_yaml(YAML / "statistics_readings.yaml")
    plain._sinks.stats_on_change = False
    base = _paths(_run(plain, 120)[0])
    assert base.count("Shop.stats.resources.packers.in_use") == 12  # every 10 s
    yaml_paths = _paths(
        _run(SimulationContext.from_yaml(YAML / "statistics_readings.yaml"), 120)[0]
    )
    app = runpy.run_path(str(PAGES / "statistics_options.py"))["on_change"]
    builder_paths = _paths(_run(app, 120)[0])
    assert yaml_paths.count("Shop.stats.resources.packers.in_use") > 100
    assert builder_paths.count("Shop.stats.resources.packers.in_use") > 100


def test_trace_reaches_only_a_sink_with_when_trace():
    """Statistics, Trace the Events: the sink gets `trace` records and nothing
    else, in YAML and in the Builder."""
    yaml_sinks = _run(SimulationContext.from_yaml(YAML / "statistics_trace.yaml"), 10)
    app = runpy.run_path(str(PAGES / "statistics_options.py"))["trace"]
    for records in (yaml_sinks[0], _run(app, 10)[0]):
        assert records
        assert {r["stream_type"] for r in records} == {"trace"}
        assert {r["path_id"] for r in records} == {"trace"}
        assert set(records[0]["value"]) == {"priority", "event_id", "event"}
        assert records[0]["value"]["event"] == "Initialize"
        # Records and Telemetry shows the first trace record of this run
        assert records[0]["sim_ts"] == 0.0
        assert records[0]["value"] == {
            "priority": 0,
            "event_id": 2,
            "event": "Initialize",
        }


def _low_level(**options):
    """Runs the low-level shop of statistics_options.py into one capture."""
    module = runpy.run_path(str(PAGES / "statistics_options.py"))
    sink = _Capture()
    module["low_level"](120 if "on_change" in options else 10, sink, **options)
    return [
        r
        for batch in sink.records
        for r in (batch if isinstance(batch, list) else [batch])
    ]


def test_the_low_level_forms_publish_what_the_other_two_do():
    """Statistics: `env.stats.on_change` and a trace sink in the low-level API."""
    changes = _low_level(on_change=True)
    assert _paths(changes).count("Shop.stats.resources.packers.in_use") > 100
    traces = _low_level(trace=True)
    assert traces
    assert {r["stream_type"] for r in traces} == {"trace"}
