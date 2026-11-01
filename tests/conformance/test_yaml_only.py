"""Known cases of the YAML-only work: a blueprint and the plain SimPy program that
does the same, compared at sizes 1, 3 and 10, where n entities start at one instant
and the order of their events matters.

Each case runs a blueprint in `yaml_only/` and its program in `yaml_only/programs/`.
Each case has one creation order: the rows of a replay file are read in file order,
and the program starts its entities in that same order, so there is no reversed order
to run.
The blueprints are the smallest models of their documents, because the full documents
use keys that a later batch ships: GD-E1, GD-E3 and ST-RT6 start their entity with
`initial`, and ST-R11 and ST-R17 also use `initial`, `held()`, `waiting()` and
comprehensions (all Batch 6). V3 and V4 are the generated values of Batch 4.
"""

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from desflow import DynamicRealtimeEnvironment
from tests.conformance.harness import run_yaml_text
from tests.conformance.yaml_only.programs import (
    cv_032_034_steps,
    cv_040_segments,
    v3_values,
    v4_draws,
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


def _kinds(records: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    """The events of the V3 model as the program lists them: the finished payloads by
    `event_type`, and an interrupted record with its cause. Statuses such as queued and
    started are left out."""
    found: List[Tuple[Any, ...]] = []
    for record in records:
        if record["stream_type"] != "event":
            continue
        value = record["value"]
        time, key = round(record["sim_ts"], 3), record["key"]
        if value.get("status") == "interrupted":
            found.append((time, key, "interrupted", value["cause"]))
        elif value.get("event_type") == "timed":
            found.append((time, key, "timed", value["value"]))
        elif value.get("event_type") == "report":
            found.append((time, key, "report", value["total"]))
        elif value.get("event_type") in ("written", "cut"):
            found.append((time, key, value["event_type"]))
    return found


@pytest.mark.parametrize("n", SIZES)
def test_v3_computed_values_in_a_write_a_cause_and_a_timeout_value(tmp_path, n):
    """V3 a value computed where it is used: an `on:` write reads the registry and
    changes it, an interrupt's cause is read before the state changes in the same
    step, and a timeout's value is fixed when the wait starts."""
    outcome = run_yaml_text(tmp_path, _blueprint("v3_values.yaml"), v3_values.times(n))
    assert outcome.error is None
    expected = v3_values.reference(n)
    assert _kinds(outcome.records) == [
        (round(float(item[0]), 3), *item[1:]) for item in expected
    ]
    assert ("reading-1", "report", 10 - 3 * n) in [item[1:] for item in expected]


def _v4_events(records: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    found: List[Tuple[Any, ...]] = []
    for record in records:
        if record["stream_type"] != "event":
            continue
        value = record["value"]
        status = value.get("status") or "done"
        found.append((round(record["sim_ts"], 3), record["key"], status))
    return found


@pytest.mark.parametrize("n", SIZES)
def test_v4_a_value_drawn_for_each_entity_orders_a_priority_queue(tmp_path, n):
    """V4 a value drawn once for each entity when it starts, in creation order, and
    read by a priority: the jobs are served in the order of their draws."""
    outcome = run_yaml_text(tmp_path, _blueprint("v4_draws.yaml"), {"a": [0.0] * n})
    assert outcome.error is None
    expected = [
        (round(float(t), 3), key, status) for t, key, status in v4_draws.reference(n)
    ]
    assert _v4_events(outcome.records) == expected
    served = [key for _, key, status in expected if status == "started"]
    assert sorted(served) == sorted(f"a-{i}" for i in range(1, n + 1))
