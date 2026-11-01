"""Known cases of the YAML-only work: a blueprint and the plain SimPy program that
does the same, compared at sizes 1, 3 and 10, where n entities start at one instant
and the order of their events matters.

Each case runs a blueprint in `yaml_only/` and its program in `yaml_only/programs/`.
Each case has one creation order: the rows of a replay file are read in file order,
and the program starts its entities in that same order, so there is no reversed order
to run.
The blueprints are the smallest models of their documents, because the full documents
use keys that a later batch ships: GD-E1, GD-E3 and ST-RT6 start their entity with
`initial` (Batch 6), and ST-R11 and ST-R17 read through `{gen: expr}` (Batch 4).
"""

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from desflow import DynamicRealtimeEnvironment
from tests.conformance.harness import run_yaml_text
from tests.conformance.yaml_only.programs import (
    cv_032_034_steps,
    cv_040_segments,
)

FOLDER = Path(__file__).parent / "yaml_only"
SIZES = [1, 3, 10]
TASKS = ("ticking", "looking")


def _blueprint(name: str) -> str:
    return (FOLDER / name).read_text(encoding="utf-8")


def _published(records: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    """The tick and look events and the segment readings, in publication order, as
    the program lists them: (time, key, started or done) and ("segment", i, now, peek).
    """
    found: List[Tuple[Any, ...]] = []
    for record in records:
        if record["stream_type"] == "event" and record["task"] in TASKS:
            value = record["value"]
            started = isinstance(value, dict) and value.get("status") == "started"
            found.append(
                (record["sim_ts"], record["key"], "started" if started else "done")
            )
        elif record.get("path_id") == "system.simulation.segment":
            reading = record["value"]
            found.append(("segment", reading["index"], reading["now"], reading["peek"]))
    return found


def _steps(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        record["value"]
        for record in records
        if record.get("path_id") == "system.simulation.step"
    ]


@pytest.mark.parametrize("n", SIZES)
def test_cv_032_and_cv_034_run_steps_reads_peek_and_step(tmp_path, n):
    """CV-032 `env.peek()` and CV-034 `env.step()` from a driver: one reading
    `{step, now, peek}` after each model event, up to `until`, with `peek` null where
    SimPy's is infinity, and the events happen in SimPy's order."""
    readings, published = cv_032_034_steps.reference(n)
    outcome = run_yaml_text(
        tmp_path, _blueprint("cv_032_034_steps.yaml"), {"a": [1] * n}
    )
    assert outcome.error is None
    assert _steps(outcome.records) == readings
    assert _published(outcome.records) == published


@pytest.mark.parametrize("n", SIZES)
def test_cv_040_run_segments_with_a_look(tmp_path, n):
    """CV-040 running in segments: to a time with a look that reads before the events
    at the stop, to the next firing of a signal, to a time, then to the end."""
    outcome = run_yaml_text(
        tmp_path, _blueprint("cv_040_segments.yaml"), {"a": [1] * n}
    )
    assert outcome.error is None
    assert _published(outcome.records) == cv_040_segments.with_look(n)


@pytest.mark.parametrize("n", SIZES)
def test_cv_040_run_segments_every(tmp_path, n):
    """CV-040 with `every`: a segment at each multiple of 2 and one at the end."""
    outcome = run_yaml_text(tmp_path, _blueprint("cv_040_every.yaml"), {"a": [1] * n})
    assert outcome.error is None
    assert _published(outcome.records) == cv_040_segments.every(n)


def test_cv_099_run_sync_calls_sync_where_it_is_asked(tmp_path, monkeypatch):
    """CV-099 `RealtimeEnvironment.sync()`: `run.sync` calls it as the run starts, and
    `sync` on a segment as that segment starts. With neither, a run that is not strict
    does not call it."""
    calls: List[float] = []
    original = DynamicRealtimeEnvironment.sync

    def spy(self):
        calls.append(self.now)
        return original(self)

    monkeypatch.setattr(DynamicRealtimeEnvironment, "sync", spy)
    text = _blueprint("cv_099_sync.yaml")

    run_yaml_text(tmp_path, text.replace("factor: 1", "factor: 0"), {"a": [1]})
    assert calls == [0]

    calls.clear()
    run_yaml_text(
        tmp_path,
        text.replace("factor: 1", "factor: 0").replace(", sync: true", ""),
        {"a": [1]},
    )
    assert calls == []

    calls.clear()
    segments = text.replace("factor: 1", "factor: 0").replace(
        "run: {until: 3, sync: true}",
        "run: {until: 3, segments: [{until: 1}, {until: 2, sync: true}]}",
    )
    run_yaml_text(tmp_path, segments, {"a": [1]})
    assert calls == [1]
