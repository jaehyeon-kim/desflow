"""Known cases of the YAML-only work: a blueprint and the plain SimPy program that
does the same, compared at sizes 1, 3 and 10, where n entities start at one instant
and the order of their events matters.

Each case runs a blueprint in `yaml_only/` and its program in `yaml_only/programs/`.
Each case has one creation order: the rows of a replay file are read in file order,
and the program starts its entities in that same order, so there is no reversed order
to run.
The blueprints of the cases before Batch 6 are the smallest models of their documents,
because the full documents used keys that a later batch ships. GD-E3, ST-RT6, ST-R11 and
ST-R17 run as their full documents since Batch 6 (`initial`, `held()`, `waiting()`,
`putting()`, `getting()` and comprehensions). GD-E1 starts its entity with `initial` and
still runs as its smallest model. V3 and V4 are the generated values of Batch 4.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from desflow import DynamicRealtimeEnvironment
from desflow.core.task import TaskFailed
from tests.conformance.harness import run_yaml_text
from tests.conformance.yaml_only import fixed_draws
from tests.conformance.yaml_only.programs import (
    a1_count,
    a1_entities,
    a4_gap,
    batch6,
    batch8,
    batch9,
    batch11,
    cv_032_034_steps,
    cv_040_segments,
    known8,
    known9,
    known10,
    known11,
    v1_service_time,
    v2_timeout,
    v3_values,
    v4_draws,
    v10_weights,
)

FOLDER = Path(__file__).parent / "yaml_only"
KNOWN_CASES = Path(__file__).parents[1] / "known_cases"
SIZES = [1, 3, 10]
TASKS = ("ticking", "looking")


def _blueprint(name: str) -> str:
    """A document of `yaml_only/`, or, for a row of the known-case list that moved, the
    `model.yaml` of its folder in `tests/known_cases/`."""
    moved = (
        KNOWN_CASES / {"bank_renege": "ex_1"}.get(name[:-5], name[:-5]) / "model.yaml"
    )
    if moved.exists():
        return moved.read_text(encoding="utf-8")
    return (FOLDER / name).read_text(encoding="utf-8")


def _known_document(folder: str, document: str) -> Path:
    """The document of a known-case row: in its folder of `tests/known_cases/`, or
    still in `yaml_only/<folder>/` for a row that part 12b moves."""
    moved = KNOWN_CASES / document / "model.yaml"
    return moved if moved.exists() else FOLDER / folder / f"{document}.yaml"


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


def _csv(folder: Path, name: str, columns: List[str], rows: List[Tuple[Any, ...]]):
    (folder / f"{name}.csv").write_text(
        ",".join(columns) + "\n" + "".join(",".join(map(str, r)) + "\n" for r in rows)
    )


def _of_task(records: List[Dict[str, Any]], task: str) -> List[Tuple[Any, ...]]:
    """The (time, key, status) of a task's events; a finished payload has the status
    `done`."""
    found = []
    for record in records:
        if record["stream_type"] == "event" and record["task"] == task:
            value = record["value"]
            found.append(
                (round(record["sim_ts"], 3), record["key"], value.get("status", "done"))
            )
    return found


def _v1_run(tmp_path, fuels, cut_at, policy):
    _csv(tmp_path, "cars", ["time", "fuel"], [(0, fuel) for fuel in fuels])
    _csv(tmp_path, "cuts", ["time"], [(0,)])
    text = _blueprint("v1_service_time.yaml").replace(
        "on_interrupt: resume", f"on_interrupt: {policy}"
    )
    if cut_at is None:
        text = text.replace("cuts: {replay: {path: cuts.csv}, spawn: cut}", "")
    return run_yaml_text(tmp_path, text, {})


@pytest.mark.parametrize("n", SIZES)
def test_v1_a_service_time_computed_for_each_entity_on_one_unit(tmp_path, n):
    """V1 a service time read from the entity: the cars are served one after the other,
    each for its fuel / 2, and the ties at time 0 keep their order."""
    fuels = [2 * i for i in range(1, n + 1)]
    outcome = _v1_run(tmp_path, fuels, None, "resume")
    assert outcome.error is None
    expected = v1_service_time.reference(fuels)
    assert _of_task(outcome.records, "car") == [
        (round(float(t), 3), key, status) for t, key, status in expected
    ]
    assert len(expected) == 3 * n and expected[-1][0] == n * (n + 1) / 2


@pytest.mark.parametrize("n", SIZES)
def test_v1_a_service_time_of_zero_takes_no_time(tmp_path, n):
    """V1 with a fuel of 0 every car is served in no time, and all finish at time 0."""
    outcome = _v1_run(tmp_path, [0] * n, None, "resume")
    assert outcome.error is None
    expected = v1_service_time.reference([0] * n)
    assert {t for t, _, status in expected if status == "done"} == {0}
    assert _of_task(outcome.records, "car") == [
        (round(float(t), 3), key, status) for t, key, status in expected
    ]


@pytest.mark.parametrize("policy", ["resume", "restart"])
@pytest.mark.parametrize("n", SIZES)
def test_v1_an_interrupt_during_the_service_time(tmp_path, n, policy):
    """V1 interrupted during the computed service time: `resume` keeps the time left
    (a fuel of 6 is served for 3, cut at 1, so 2 are left) and `restart` serves all 3
    again."""
    fuels = [6] + [2] * (n - 1)
    outcome = _v1_run(tmp_path, fuels, 1, policy)
    assert outcome.error is None
    expected = v1_service_time.reference(fuels, cut_at=1, policy=policy)
    assert ("interrupted" in {status for _, _, status in expected}) is True
    done = [t for t, key, status in expected if key == "cars-1" and status == "done"]
    assert done == ([3] if policy == "resume" else [4])
    assert _of_task(outcome.records, "car") == [
        (round(float(t), 3), key, status) for t, key, status in expected
    ]


def test_v1_a_negative_service_time_fails_the_run(tmp_path):
    """V1 with a negative result: SimPy raises `ValueError` for a negative delay, and
    the run fails with a message that names the task and the key."""
    outcome = _v1_run(tmp_path, [-2], None, "resume")
    assert isinstance(outcome.error, ValueError)
    assert "service_time" in str(outcome.error) and "below 0" in str(outcome.error)
    with pytest.raises(ValueError):
        v1_service_time.reference([-2])


def _v2_run(tmp_path, n, limit, at, listed_first, cut_at=None):
    _csv(tmp_path, "sender", ["time"], [(0,)])
    _csv(tmp_path, "waiters", ["time", "limit"], [(0, limit)] * n)
    branches = {
        "signal": "go: {signal: go}",
        "limit": 'limit: {timeout: "attr.limit"}',
    }
    order = ["limit", "signal"] if listed_first == "limit" else ["signal", "limit"]
    text = (
        _blueprint("v2_timeout.yaml")
        .replace("wait: {timeout: 1}", f"wait: {{timeout: {at}}}")
        .replace(
            'wait: {any: {go: {signal: go}, limit: {timeout: "attr.limit"}}}',
            "wait: {any: {" + ", ".join(branches[k] for k in order) + "}}",
        )
    )
    if cut_at is not None:
        text = text.replace(
            "arrivals:\n",
            "arrivals:\n  cuts: {replay: {path: cuts.csv}, spawn: cut}\n",
        ).replace(
            "run:",
            "tasks_cut: x\nrun:",
        )
        text = text.replace("tasks_cut: x\n", "").replace(
            "  wait_for:",
            f"  cut:\n    wait: {{timeout: {cut_at}}}\n    payload: {{event_type: cut}}\n"
            f"    interrupt: {{task: wait_for}}\n  wait_for:",
        )
        _csv(tmp_path, "cuts", ["time"], [(0,)])
    return run_yaml_text(tmp_path, text, {})


def _v2_events(records):
    found = []
    for record in records:
        if record["stream_type"] != "event" or record["task"] == "cut":
            continue
        value = record["value"]
        if record["task"] == "send" and value.get("event_type") == "sent":
            found.append((round(record["sim_ts"], 3), record["key"], "sent"))
        elif record["task"] == "wait_for":
            if value.get("event_type") == "woke":
                found.append(
                    (
                        round(record["sim_ts"], 3),
                        record["key"],
                        "woke",
                        value["signal"],
                        value["limit"],
                    )
                )
            elif value.get("status") == "interrupted":
                found.append((round(record["sim_ts"], 3), record["key"], "interrupted"))
    return found


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize(
    "limit, at, timeout_first",
    [
        (5, 1, False),
        (1, 5, False),
        (2, 2, False),
        (2, 2, True),
        (0, 1, False),
    ],
    ids=["event first", "limit first", "tie", "tie timeout listed first", "zero"],
)
def test_v2_a_wait_timeout_computed_for_each_entity(
    tmp_path, n, limit, at, timeout_first
):
    """V2 a timeout read from the entity, listed with a signal: which of the two is done
    when the waiter wakes, at sizes 1, 3 and 10, with ties in both listing orders and a
    limit of 0."""
    outcome = _v2_run(tmp_path, n, limit, at, "limit" if timeout_first else "signal")
    assert outcome.error is None
    expected = v2_timeout.reference(n, limit, at, timeout_first)
    assert len(expected) == n + 1
    assert _v2_events(outcome.records) == [
        (round(float(item[0]), 3), *item[1:]) for item in expected
    ]


@pytest.mark.parametrize("n", SIZES)
def test_v2_an_interrupt_while_the_computed_timeout_runs(tmp_path, n):
    """V2 interrupted while the timeout runs: every waiter ends at the interrupt."""
    outcome = _v2_run(tmp_path, n, 5, 9, "signal", cut_at=1)
    assert outcome.error is None
    expected = v2_timeout.reference(n, 5, 9, False, cut_at=1)
    assert [e for e in expected if e[2] == "interrupted"]
    assert _v2_events(outcome.records)[:n] == [
        (round(float(item[0]), 3), *item[1:])
        for item in expected
        if item[2] == "interrupted"
    ]


def test_v2_a_negative_timeout_fails_the_run(tmp_path):
    """V2 with a negative result: SimPy raises `ValueError`, and so does the run."""
    outcome = _v2_run(tmp_path, 1, -1, 1, "signal")
    assert isinstance(outcome.error, ValueError)
    assert "below 0" in str(outcome.error)
    with pytest.raises(ValueError):
        v2_timeout.reference(1, -1, 1)


def _v10_run(tmp_path, weights, arrivals=None):
    arrivals = arrivals if arrivals is not None else [0] * len(weights)
    _csv(
        tmp_path,
        "jobs",
        ["time", "fast", "slow"],
        [(at, fast, slow) for at, (fast, slow) in zip(arrivals, weights)],
    )
    text = _blueprint("v10_weights.yaml")
    return run_yaml_text(tmp_path, text, {})


def _routed(records):
    return [
        (round(record["sim_ts"], 3), record["task"])
        for record in records
        if record["stream_type"] == "event"
        and record["task"] in ("fast", "slow")
        and record["value"].get("event_type") in ("fast", "slow")
    ]


_V10_PAIRS = [(1, 0), (0, 1), (0.5, 0.5), (3, 1), (1, 3), (2, 2)]


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize(
    "pair",
    [(1, 0), (0, 1), (0.5, 0.5), (3, 1), None],
    ids=["all", "none", "even", "uneven", "mixed"],
)
def test_v10_weights_from_an_expression_make_one_draw_each(tmp_path, n, pair):
    """V10 weights read from the entity: each task that finishes makes one draw, and
    the shares are the weights over their sum. The tasks and times the SimPy program
    routes to, drawing from the same stream, are the same, for entities that finish
    at one instant and at different times. A weight of 0 never receives an entity."""
    weights = [pair or _V10_PAIRS[i % len(_V10_PAIRS)] for i in range(n)]
    arrivals = [0] * n if pair else [i // 2 for i in range(n)]
    outcome = _v10_run(tmp_path, weights, arrivals)
    assert outcome.error is None
    expected = v10_weights.reference(weights, arrivals)
    assert _routed(outcome.records) == [(float(t), task) for t, task in expected]
    if pair == (1, 0):
        assert {task for _, task in expected} == {"fast"}
    if pair == (0, 1):
        assert {task for _, task in expected} == {"slow"}


@pytest.mark.parametrize("pair", [(0, 0), (-1, -1), (2, -1)])
def test_v10_weights_that_add_up_to_nothing_or_below_0_fail_the_run(tmp_path, pair):
    """V10 refused weights: all 0 and any below 0 fail the run. This is the design's
    stricter rule and not SimPy's behaviour: `random.choices` accepts (2, -1)."""
    outcome = _v10_run(tmp_path, [pair])
    assert isinstance(outcome.error, ValueError)


_SOURCE = "  s1: {gap: 2, max_events: 3, spawn: t}"
_SOURCE_A4 = "  s1: {gap: '1 + t / 4', spawn: t}"


def _sources(names: List[str], body: str) -> str:
    return "\n".join(f"  {name}: {{{body}, spawn: t}}" for name in names)


def _starts(records):
    return [
        (round(record["sim_ts"], 3), record["key"])
        for record in records
        if record["stream_type"] == "event"
        and record["task"] == "t"
        and record["value"].get("event_type") == "ran"
    ]


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize(
    "count, first_at",
    [(3, None), (0, None), (1, None), (3, 0), (3, 2), (10, None)],
    ids=["three", "zero", "one", "first at 0", "first at 2", "ten"],
)
def test_a1_a_fixed_number_of_arrivals(tmp_path, n, count, first_at):
    """A1 `max_events` and `first_at`: n sources each start the task `max_events`
    times, two seconds apart, the first at `first_at` when it is given (also at 0, which
    waits a timeout of 0 in the program), and the sources tie in the order written."""
    names = [f"s{i}" for i in range(1, n + 1)]
    body = "gap: 2, max_events: %d" % count
    if first_at is not None:
        body += f", first_at: {first_at}"
    text = _blueprint("a1_count.yaml").replace(_SOURCE, _sources(names, body))
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    expected = a1_count.reference(names, count, first_at)
    assert len(expected) == n * count
    assert _starts(outcome.records) == [(round(float(t), 3), k) for t, k in expected]


@pytest.mark.parametrize("n", SIZES)
def test_a1_a_computed_count_must_be_a_whole_number(tmp_path, n):
    """A1 with a computed count: `range(2.0)` raises `TypeError` in the program, and a
    `max_events` that gives 2.0 fails the run; 2 works, a negative one starts none (as
    `range(-1)` does) and `true` counts as 1."""
    names = [f"s{i}" for i in range(1, n + 1)]

    def go(expression):
        text = _blueprint("a1_count.yaml").replace(
            _SOURCE,
            _sources(names, f"gap: 2, max_events: {{gen: expr, expr: '{expression}'}}"),
        )
        return run_yaml_text(tmp_path, text, {})

    assert go("1 + 1").error is None
    with pytest.raises(TypeError):
        range(2.0)  # type: ignore[call-overload]
    failed = go("2.0")
    assert isinstance(failed.error, TypeError) and "whole number" in str(failed.error)
    assert _starts(go("0 - 1").records) == []
    assert len(_starts(go("1 == 1").records)) == n


def _entities_text(arrival: str, until: int = 100, drain: bool = False, service=0):
    text = _blueprint("a1_count.yaml").replace(_SOURCE, f"  a: {{{arrival}, spawn: t}}")
    if service:
        text = text.replace("t: {payload", f"t: {{service_time: {service}, payload")
    return text.replace(
        "run: {until: 100}",
        f"run: {{until: {until}{', drain: true' if drain else ''}}}",
    )


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize(
    "stagger, first_at",
    [(0, None), (1, None), (0, 0), (0, 2), (1, 2), (3, 1)],
    ids=["no stagger", "stagger 1", "first at 0", "first at 2", "both", "late stagger"],
)
@pytest.mark.parametrize("by", ["count", "keys"])
def test_a1_count_and_keys_make_entities_that_start_the_task_again(
    tmp_path, n, stagger, first_at, by
):
    """A1 `count` or `keys`, `stagger` and `first_at`: n entities, each a process that
    starts the task twice, two seconds apart, created `stagger` apart; the first start
    waits until `first_at`. The starts and their order are the SimPy program's."""
    keys = (
        [f"a-{i}" for i in range(1, n + 1)]
        if by == "count"
        else [f"k{i}" for i in range(n)]
    )
    who = f"count: {n}" if by == "count" else f"keys: [{', '.join(keys)}]"
    body = f"gap: 2, {who}, max_events: 2, stagger: {stagger}"
    if first_at is not None:
        body += f", first_at: {first_at}"
    outcome = run_yaml_text(tmp_path, _entities_text(body), {})
    assert outcome.error is None
    expected = a1_entities.reference(keys, 2, 2, stagger, first_at)
    assert len(expected) == 2 * n
    assert _starts(outcome.records) == [(round(float(t), 3), k) for t, k in expected]


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize(
    "stop_at, stagger",
    [(4.5, 3), (2, 3), (6, 1.5), (9, 3)],
    ids=["in a stagger", "before any start", "in a gap", "after the starters"],
)
def test_a1_a_drain_during_a_gap_or_a_stagger_ends_the_entities(
    tmp_path, n, stop_at, stagger
):
    """A1 with `count`: the arrivals stop at `until` while an entity waits in a gap or
    the starter waits in a stagger. The entities not yet created are never created,
    a gap in progress starts nothing, and the tasks already started finish (3 s)."""
    keys = [f"a-{i}" for i in range(1, n + 1)]
    body = f"gap: 4, count: {n}, max_events: 3, stagger: {stagger}"
    text = _entities_text(body, until=stop_at, drain=True, service=3)
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    expected = a1_entities.reference(keys, 3, 4, stagger, None, stop_at, 3)
    assert _starts(outcome.records) == [(round(float(t), 3), k) for t, k in expected]


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("stop_at", [3, 5, 7], ids=["gap 2", "gap 3", "gap 4"])
def test_a1_a_drain_during_a_gap_of_a_source_with_max_events(tmp_path, n, stop_at):
    """A1 `max_events` with a drain: the sources stop in the middle of a gap, and the
    tasks already started finish (3 s)."""
    names = [f"s{i}" for i in range(1, n + 1)]
    text = _blueprint("a1_count.yaml").replace(
        _SOURCE, _sources(names, "gap: 2, max_events: 5")
    )
    text = text.replace("t: {payload", "t: {service_time: 3, payload").replace(
        "run: {until: 100}", f"run: {{until: {stop_at}, drain: true}}"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    expected = a1_count.reference(names, 5, None, stop_at, 3)
    assert _starts(outcome.records) == [(round(float(t), 3), k) for t, k in expected]


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize(
    "expression, gap",
    [
        ("1 + t / 4", lambda t, made: 1 + t / 4),
        ("3", lambda t, made: 3),
        ("0 if made_even else 2", None),
    ],
    ids=["from t", "constant", "cycle"],
)
def test_a4_a_gap_computed_from_the_time(tmp_path, n, expression, gap):
    """A4 a gap read when each gap starts: arrivals at the times the program reaches,
    with n sources tying in the order written."""
    names = [f"s{i}" for i in range(1, n + 1)]
    if gap is None:
        body = "gap: {gen: sequence, start: 0, step: 2, loop_after: 2}"
        gap = lambda t, made: 0 if made % 2 == 0 else 2  # noqa: E731
    else:
        body = f"gap: '{expression}'"
    text = _blueprint("a4_gap.yaml").replace(_SOURCE_A4, _sources(names, body))
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    trace, error = a4_gap.reference(names, gap)
    assert error == "" and trace
    assert _starts(outcome.records) == [(round(float(t), 3), k) for t, k in trace]


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("stop_at", [2.5, 4.2], ids=["early", "late"])
def test_a4_a_drain_during_a_computed_gap_ends_the_sources(tmp_path, n, stop_at):
    """A4 with a drain: the sources stop in the middle of a computed gap, which starts
    nothing, and the tasks already started finish (3 s)."""
    names = [f"s{i}" for i in range(1, n + 1)]
    text = _blueprint("a4_gap.yaml").replace(
        _SOURCE_A4, _sources(names, "gap: '1 + t / 4'")
    )
    text = text.replace("t: {payload", "t: {service_time: 3, payload").replace(
        "run: {until: 8}", f"run: {{until: {stop_at}, drain: true}}"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    trace, error = a4_gap.reference(
        names, lambda t, made: 1 + t / 4, stop_at=stop_at, service=3
    )
    assert error == "" and trace
    assert _starts(outcome.records) == [(round(float(t), 3), k) for t, k in trace]


def test_a4_a_negative_gap_fails_the_run(tmp_path):
    """A4 a gap that goes below 0: SimPy raises `ValueError` once the time has passed
    1, and the run fails at that gap, after the arrivals before it."""
    text = _blueprint("a4_gap.yaml").replace(
        _SOURCE_A4, _sources(["s1"], "gap: '1 - 2 * t'")
    )
    outcome = run_yaml_text(tmp_path, text, {})
    trace, error = a4_gap.reference(["s1"], lambda t, made: 1 - 2 * t)
    assert error == "ValueError"
    assert isinstance(outcome.error, ValueError) and "below 0" in str(outcome.error)
    assert _starts(outcome.records) == [(round(float(t), 3), k) for t, k in trace]


@pytest.mark.parametrize("patience", [1.5, 2, 3])
def test_ex_1_bank_renege_prints_the_lines_of_the_simpy_program(tmp_path, patience):
    """EX-1 the port of SimPy's `bank_renege.py` with the three draws fixed: five
    customers from time 0, each served or reneging as the program does."""
    text = fixed_draws.blueprint(_blueprint("bank_renege.yaml"), patience)
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    expected = fixed_draws.reference(patience)
    assert fixed_draws.render(outcome.records) == expected
    assert sum("Here I am" in line for line in expected) == 5
    assert any("RENEGED" in line for line in expected)
    assert any("Waited" in line for line in expected)


def test_ex_1_bank_renege_runs_with_its_own_draws(tmp_path):
    """EX-1 as written: each of the five customers is served or goes away, and the
    keys are Customer00 to Customer04."""
    outcome = run_yaml_text(tmp_path, _blueprint("bank_renege.yaml"), {})
    assert outcome.error is None
    lines = fixed_draws.render(outcome.records)
    assert [line[8:18] for line in lines if "Here I am" in line] == [
        f"Customer{i:02d}" for i in range(5)
    ]
    assert sum("Finished" in line or "RENEGED" in line for line in lines) == 5


# Batch 6: names, lists, local values and sources.


def _marks(records: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    """The payloads that carry a label `e` as (time, key, label, extra), where extra is
    the payload's `x` (None when there is none), in publication order."""
    return [
        (
            round(record["sim_ts"], 3),
            record["key"],
            record["value"]["e"],
            record["value"].get("x"),
        )
        for record in records
        if record["stream_type"] == "event"
        and isinstance(record["value"], dict)
        and "e" in record["value"]
    ]


def _expect(marks: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
    return [(round(float(t), 3), *rest) for t, *rest in marks]


def _expect_pairs(pairs):
    return [(round(float(t), 3), i) for t, i in pairs]


def _rows(tmp_path, name, columns, rows):
    _csv(tmp_path, name, columns, rows)


def _expr(text: str) -> str:
    return "{gen: expr, expr: " + repr(text) + "}"


def _run6(tmp_path, text):
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    return outcome


@pytest.mark.parametrize("n", SIZES)
def test_v7_cause_names_the_preempter_and_the_time_left(tmp_path, n):
    """V7 `cause.by`, `cause.usage_since`, `cause.left`, `held(m)` and `upstream`: a
    holder that a higher priority job preempts reads who preempted it, since when it
    held the unit, how much of its service time was left, and the holders then."""
    _csv(tmp_path, "low", ["time"], [(0,)] * n)
    _csv(tmp_path, "high", ["time"], [(3,)])
    text = f"""
simulation: {{sim_id: V7, factor: 0, random_seed: 42}}
resources: {{m: {{current_cap: 1, queue: priority, preempt: true}}}}
arrivals:
  low: {{replay: {{path: low.csv}}, spawn: low}}
  high: {{replay: {{path: high.csv}}, spawn: high}}
tasks:
  low: {{resource: m, priority: 5, service_time: 10, on_interrupt: abandon, on_interrupt_next: low_cause, payload: {{e: low_done}}}}
  low_cause: {{payload: {{e: cause, x: {_expr("[cause.by, cause.usage_since, cause.left, [h.entity for h in held(m)]]")}}}}}
  high: {{resource: m, priority: 1, service_time: 2, payload: {{e: high_done}}}}
run: {{until: 100}}
"""
    outcome = _run6(tmp_path, text)
    assert _marks(outcome.records) == _expect(batch6.v7_cause(n))
    first = [m for m in _marks(outcome.records) if m[2] == "cause"][0]
    assert first[3] == ["high-1", 0, 7, ["high-1"]]


@pytest.mark.parametrize("n", SIZES)
def test_v9_args_of_a_task_started_by_wait(tmp_path, n):
    """V9 `args` on a wait task branch, read as `arg.x` by the child: each child gets
    the arguments its parent evaluated for it."""
    _csv(tmp_path, "a", ["time", "k"], [(0, i) for i in range(1, n + 1)])
    text = f"""
simulation: {{sim_id: V9, factor: 0, random_seed: 42}}
arrivals: {{a: {{replay: {{path: a.csv}}, spawn: parent}}}}
tasks:
  parent: {{wait: {{all: {{x: {{task: child, args: {{x: {_expr("attr.k * 3")}}}}}, y: {{task: child, args: {{x: {_expr("attr.k * 3 + 1")}}}}}}}}}, payload: {{e: parent}}}}
  child: {{service_time: 1, payload: {{e: ended, x: {_expr("arg.x + 1")}}}}}
run: {{until: 50}}
"""
    outcome = _run6(tmp_path, text)
    assert _marks(outcome.records) == _expect(batch6.v9_args(n))


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize(
    "done, before", [(1, 5), (2, 5), (2, 2)], ids=["first", "both", "never"]
)
def test_v11_wait_when_over_the_events_done(tmp_path, n, done, before):
    """V11 `wait: {when, of}`: the wait ends when the predicate holds over the events
    done and the time, as SimPy's `Condition` with a function; a predicate that never
    holds never ends the wait."""
    _csv(tmp_path, "a", ["time"], [(0,)] * n)
    text = f"""
simulation: {{sim_id: V11, factor: 0, random_seed: 42}}
arrivals: {{a: {{replay: {{path: a.csv}}, spawn: gate}}}}
tasks:
  gate: {{wait: {{when: "done_count == {done} and t < {before}", of: {{a: {{timeout: 1}}, b: {{timeout: 3}}}}}}, payload: {{e: opened, x: {_expr("t")}}}}}
run: {{until: 10}}
"""
    outcome = _run6(tmp_path, text)
    expected = _expect(batch6.v11_when(n, done, before))
    assert _marks(outcome.records) == expected
    assert bool(expected) is (before > 2)


@pytest.mark.parametrize("n", SIZES)
def test_v12_put_item_and_the_bare_item_name(tmp_path, n):
    """V12 `put.item` and bare `item` with a filter: each entity takes the item it
    wants from a store, puts back the item plus 100, and a later look reads the store."""
    _csv(tmp_path, "p", ["time", "want"], [(0, n - i) for i in range(1, n + 1)])
    _csv(tmp_path, "r", ["time"], [(1,)])
    text = f"""
simulation: {{sim_id: V12, factor: 0, random_seed: 42}}
stores: {{tools: {{items: {list(range(n))}}}}}
arrivals: {{p: {{replay: {{path: p.csv}}, spawn: take}}, r: {{replay: {{path: r.csv}}, spawn: look}}}}
tasks:
  take: {{get: {{store: tools, filter: "item == attr.want"}}, payload: {{e: took, x: {_expr("item")}}}, next: back}}
  back: {{put: {{store: tools, item: {_expr("item + 100")}}}, payload: {{e: back, x: {_expr("item + 100")}}}}}
  look: {{payload: {{e: items, x: {_expr("sorted(items(tools))")}}}}}
run: {{until: 5}}
"""
    outcome = _run6(tmp_path, text)
    assert _marks(outcome.records) == _expect(batch6.v12_filter_put(n))


@pytest.mark.parametrize("n", SIZES)
def test_y6_set_args_and_arg_are_local_to_each_process(tmp_path, n):
    """Y6 `set_args` and `arg.<name>`: two children of one parent count with their own
    `arg.i`, and a task started by wait gets its own copy of the arguments."""
    _csv(tmp_path, "a", ["time"], [(0,)] * n)
    text = f"""
simulation: {{sim_id: Y6, factor: 0, random_seed: 42}}
arrivals: {{a: {{replay: {{path: a.csv}}, spawn: parent}}}}
tasks:
  parent: {{wait: {{all: {{x: {{task: count, args: {{gap: 1}}}}, y: {{task: count, args: {{gap: 1.5}}}}}}}}, payload: {{e: parent}}}}
  count: {{payload: {{e: begin, x: {_expr("arg.gap")}}}, set_args: {{i: 0}}, next: step}}
  step: {{service_time: "arg.gap", payload: {{e: step, x: {_expr("arg.i + 1")}}}, set_args: {{i: {_expr("arg.i + 1")}}}, next: {{when: [{{condition: "arg.i < 2", task: step}}]}}}}
run: {{until: 50}}
"""
    outcome = _run6(tmp_path, text)
    assert _marks(outcome.records) == _expect(batch6.y6_local(n))


@pytest.mark.parametrize("n", SIZES)
def test_y7_running_waiting_held_peek_and_list_forms(tmp_path, n):
    """Y7 and Y5: `running(task, cond)`, `waiting(r)`, `held(r)`, `peek()`, `len`,
    comprehensions and `sorted`: a report reads how many workers run, who waits and
    holds the unit, and when the next event is."""
    _csv(tmp_path, "w", ["time", "cls"], [(0, i % 2) for i in range(1, n + 1)])
    _csv(tmp_path, "r", ["time"], [(1,)])
    text = f"""
simulation: {{sim_id: Y7, factor: 0, random_seed: 42}}
resources: {{m: {{current_cap: 1}}}}
arrivals: {{w: {{replay: {{path: w.csv}}, spawn: work}}, r: {{replay: {{path: r.csv}}, spawn: report}}}}
tasks:
  work: {{resource: m, service_time: 5, payload: {{e: done}}}}
  report: {{payload: {{e: state, x: {_expr("[running(work), sorted([w.entity for w in waiting(m)]), len(held(m)), running(work, attr.cls == 1), peek()]")}}}}}
run: {{until: 50}}
"""
    outcome = _run6(tmp_path, text)
    state = [m for m in _marks(outcome.records) if m[2] == "state"]
    expected = _expect(batch6.y7_state(n))
    expected[0][3][1].sort()
    assert state == expected


@pytest.mark.parametrize("n", SIZES)
def test_a3_group_starts_a_drawn_number_of_members(tmp_path, n):
    """A3 `group`: each arrival starts as many members as the group expression gives;
    0 starts nobody and takes no key number."""
    text = f"""
simulation: {{sim_id: A3, factor: 0, random_seed: 42}}
arrivals:
  g: {{dist: constant, value: 5, spawn: member, group: "{n} * (int(t / 5) - 1)"}}
tasks:
  member: {{payload: {{e: member}}}}
run: {{until: 22}}
"""
    outcome = _run6(tmp_path, text)
    expected = _expect(batch6.a3_group(n))
    assert _marks(outcome.records) == expected
    assert [m[1] for m in expected][:n] == [f"g-{i}" for i in range(1, n + 1)]


def _switch_text(names, mode, off_at, on_at, gap, until):
    sources = "\n".join(
        f"  {name}: {{dist: constant, value: {gap}, spawn: t, off_mode: {mode}}}"
        for name in names
    )
    listed = "[" + ", ".join(names) + "]"
    return f"""
simulation: {{sim_id: A5, factor: 0, random_seed: 42}}
arrivals:
{sources}
  c1: {{replay: {{path: c1.csv}}, spawn: stop}}
  c2: {{replay: {{path: c2.csv}}, spawn: go}}
tasks:
  t: {{payload: {{e: t}}}}
  stop: {{payload: {{e: stopped}}, deactivate: {listed}}}
  go: {{payload: {{e: started}}, activate: {listed}}}
run: {{until: {until}}}
"""


@pytest.mark.parametrize("n", SIZES)
def test_a5_off_mode_drop_loses_the_arrivals_while_off(tmp_path, n):
    """A5 `deactivate`, `activate` and `off_mode: drop`: arrivals that fall while a
    source is off are lost; the others go on at the old rhythm."""
    _csv(tmp_path, "c1", ["time"], [(2.5,)])
    _csv(tmp_path, "c2", ["time"], [(5.5,)])
    names = [f"s{i}" for i in range(1, n + 1)]
    outcome = _run6(tmp_path, _switch_text(names, "drop", 2.5, 5.5, 1, 7.5))
    ticks = [m for m in _marks(outcome.records) if m[2] == "t"]
    expected = _expect(batch6.a5_drop(n))
    assert ticks == expected
    assert {m[0] for m in expected} == {1, 2, 6, 7}


@pytest.mark.parametrize("n", SIZES)
def test_a5_off_mode_restart_starts_a_whole_gap_when_on(tmp_path, n):
    """A5 `off_mode: restart`: going off drops the running gap and going on starts a
    whole new gap, so a gap of 2 off at 3 and on at 7 gives 2, 9, 11, 13."""
    _csv(tmp_path, "c1", ["time"], [(3,)])
    _csv(tmp_path, "c2", ["time"], [(7,)])
    names = [f"s{i}" for i in range(1, n + 1)]
    outcome = _run6(tmp_path, _switch_text(names, "restart", 3, 7, 2, 14))
    ticks = [m for m in _marks(outcome.records) if m[2] == "t"]
    expected = _expect(batch6.a5_restart(n))
    assert ticks == expected
    assert {m[0] for m in expected} == {2, 9, 11, 13}


@pytest.mark.parametrize("n", SIZES)
def test_a7_spawn_count_and_copy_attributes(tmp_path, n):
    """A7 `spawn.count` and `copy_attributes`: a task starts as many children as its
    count gives, and each copies the parent's attributes."""
    _csv(tmp_path, "p", ["time", "tag"], [(1, f"t{i}") for i in range(1, n + 1)])
    text = f"""
simulation: {{sim_id: A7, factor: 0, random_seed: 42}}
arrivals: {{p: {{replay: {{path: p.csv}}, spawn: parent, attributes: {{count: 2}}}}}}
tasks:
  parent: {{payload: {{e: parent}}, spawn: {{task: clone, count: "attr.count", copy_attributes: true}}}}
  clone: {{payload: {{e: clone, x: {_expr("[attr.tag, attr.parent]")}}}}}
run: {{until: 5}}
"""
    outcome = _run6(tmp_path, text)
    assert _marks(outcome.records) == _expect(batch6.a7_spawn(n))


@pytest.mark.parametrize("n", SIZES)
def test_m4_initial_entities_start_before_the_run(tmp_path, n):
    """M4 `initial`: n entities exist at the start, ahead of the first step of the
    source processes, and the source goes on with the next key numbers."""
    _csv(tmp_path, "ctl", ["time"], [(0,)])
    text = f"""
simulation: {{sim_id: M4, factor: 0, random_seed: 42}}
arrivals:
  cars: {{initial: {n}, dist: constant, value: 1, spawn: car}}
  ctl: {{replay: {{path: ctl.csv}}, spawn: control}}
tasks:
  car: {{payload: {{e: car}}}}
  control: {{payload: {{e: car}}}}
run: {{until: 2.5}}
"""
    outcome = _run6(tmp_path, text)
    assert _marks(outcome.records) == _expect(batch6.m4_initial(n))


@pytest.mark.parametrize("n", SIZES)
def test_m6_flow_name_is_the_first_task_of_the_process(tmp_path, n):
    """M6 `process.flow_name`: a later task of a chain reads the name of the first
    task, which stands for SimPy's `Process.name`."""
    _csv(tmp_path, "w", ["time"], [(0,)] * n)
    text = f"""
simulation: {{sim_id: M6, factor: 0, random_seed: 42}}
arrivals: {{w: {{replay: {{path: w.csv}}, spawn: first}}}}
tasks:
  first: {{service_time: 1, payload: {{e: first}}, next: second}}
  second: {{payload: {{e: second, x: {_expr("process.flow_name")}}}}}
run: {{until: 10}}
"""
    outcome = _run6(tmp_path, text)
    got = _marks(outcome.records)
    second = [m for m in got if m[2] == "second"]
    assert second == _expect(batch6.m6_flow_name(n))
    assert {m[3] for m in second} == {"first"}


def _looks(records):
    return [
        r["value"]
        for r in records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and r["value"].get("event_type") == "look"
    ]


def test_st_r11_users_and_queue_read_by_look(tmp_path):
    """ST-R11 (SimPy's test_get_users): `held()` and `waiting()` read at the stop of
    each segment give the users and the queue that SimPy's test reads."""
    outcome = _run6(tmp_path, _blueprint("st_r11.yaml"))
    expected = batch6.users_and_queue((1, 2))
    looks = _looks(outcome.records)
    assert [(v["users"], v["queue"]) for v in looks] == expected
    assert expected == [
        (["users-1"], ["users-2", "users-3"]),
        (["users-2"], ["users-3"]),
    ]


def test_st_r17_container_queues_read_by_look(tmp_path):
    """ST-R17 (SimPy's test_container_get_queued): `putting()` and `getting()` read at
    the stop of each segment give the put queue and the get queue."""
    outcome = _run6(tmp_path, _blueprint("st_r17.yaml"))
    expected = batch6.container_queues((1, 2))
    looks = _looks(outcome.records)
    assert [(v["put_queue"], v["get_queue"]) for v in looks] == expected


def test_gd_e3_peek_and_step_in_a_loop(tmp_path):
    """GD-E3: `run.steps` with an entity that starts with `initial` gives one reading
    per model event, as the loop of `env.peek()` and `env.step()` does."""
    outcome = _run6(tmp_path, _blueprint("gd_e3.yaml"))
    readings, _ = batch6.gd_e3_steps(10)
    # The steps at time 0 start the source and the entity, which SimPy's program does
    # not have; from the first tick on, both give the same readings.
    after = [(r["now"], r["peek"]) for r in _steps(outcome.records) if r["now"] >= 1]
    assert after == [(r["now"], r["peek"]) for r in readings if r["now"] >= 1]
    assert after and after[-1] == (9, 10)


def test_st_rt6_sync_is_called_as_the_run_starts(tmp_path, monkeypatch):
    """ST-RT6 (test_rt_sync): `run.sync` calls `env.sync()` once as the run starts,
    for a model whose entity starts with `initial`."""
    calls: List[float] = []
    original = DynamicRealtimeEnvironment.sync

    def spy(self):
        calls.append(self.now)
        return original(self)

    monkeypatch.setattr(DynamicRealtimeEnvironment, "sync", spy)
    _run6(tmp_path, _blueprint("st_rt6.yaml"))
    assert calls[0] == 0


@pytest.mark.parametrize("n", SIZES)
def test_v11_a_when_group_cancels_the_requests_that_lose(tmp_path, n):
    """V11: when a `when` group ends, a request branch that was not granted is
    cancelled, as a SimPy process cancels the request it no longer wants; the queue
    of the unit holds nobody after the wait."""
    _csv(tmp_path, "h", ["time"], [(0,)])
    _csv(tmp_path, "w", ["time"], [(0,)] * n)
    _csv(tmp_path, "r", ["time"], [(3,)])
    text = f"""
simulation: {{sim_id: V11, factor: 0, random_seed: 42}}
resources: {{m: {{current_cap: 1}}}}
arrivals:
  h: {{replay: {{path: h.csv}}, spawn: hold}}
  w: {{replay: {{path: w.csv}}, spawn: waiter}}
  r: {{replay: {{path: r.csv}}, spawn: look}}
tasks:
  hold: {{resource: m, service_time: 10, payload: {{e: held}}}}
  waiter: {{wait: {{when: "done_count == 1", of: {{unit: {{request: m}}, limit: {{timeout: 1}}}}}}, payload: {{e: gave_up}}}}
  look: {{payload: {{e: look, x: {_expr("[len(waiting(m)), len(held(m))]")}}}}}
run: {{until: 5}}
"""
    outcome = _run6(tmp_path, text)
    marks = _marks(outcome.records)
    assert [m[:3] for m in marks if m[2] == "gave_up"] == [
        (1, f"w-{i}", "gave_up") for i in range(1, n + 1)
    ]
    assert [m[3] for m in marks if m[2] == "look"] == [[0, 1]]


@pytest.mark.parametrize("n", SIZES)
def test_v9_args_on_a_spawn_are_read_for_each_child(tmp_path, n):
    """V9 `args` on a `spawn`: each child gets the arguments read in the parent's step
    for it alone, so a counter that the args read gives each child the next number."""
    _csv(tmp_path, "a", ["time"], [(0,)] * n)
    text = f"""
simulation: {{sim_id: V9, factor: 0, random_seed: 42}}
arrivals: {{a: {{replay: {{path: a.csv}}, spawn: parent}}}}
tasks:
  parent: {{service_time: 1, payload: {{e: parent}}, spawn: {{task: kid, count: 2, args: {{x: {{gen: sequence, start: 1}}}}}}}}
  kid: {{service_time: 1, payload: {{e: kid, x: {_expr("arg.x + 1")}}}}}
run: {{until: 50}}
"""
    outcome = _run6(tmp_path, text)
    kids = [m for m in _marks(outcome.records) if m[2] == "kid"]
    assert kids == _expect(batch6.v9_spawn_args(n))
    assert len(kids) == 2 * n


@pytest.mark.parametrize("n", SIZES)
def test_a7_spawn_count_reads_the_names_of_the_process(tmp_path, n):
    """A7 `count` reads the names of the parent's step: `arg.n` written by an earlier
    task, and `item.n` of the item the parent took."""
    _csv(tmp_path, "p", ["time"], [(0,)] * n)
    text = f"""
simulation: {{sim_id: A7, factor: 0, random_seed: 42}}
stores: {{jobs: {{items: {[{"n": 2}] * n}}}}}
arrivals: {{p: {{replay: {{path: p.csv}}, spawn: pre}}}}
tasks:
  pre: {{set_args: {{n: 3}}, payload: {{e: pre}}, next: by_arg}}
  by_arg: {{payload: {{e: by_arg}}, spawn: {{task: kid, count: "arg.n"}}, next: take}}
  take: {{get: {{store: jobs}}, payload: {{e: take}}, spawn: {{task: kid, count: "item.n"}}}}
  kid: {{payload: {{e: kid}}}}
run: {{until: 5}}
"""
    outcome = _run6(tmp_path, text)
    kids = [m for m in _marks(outcome.records) if m[2] == "kid"]
    assert len(kids) == (3 + 2) * n


@pytest.mark.parametrize("n", SIZES)
def test_v7_a_holder_preempted_at_its_grant_has_its_whole_time_left(tmp_path, n):
    """V7, Tie: a holder that a higher priority job preempts at the instant it was
    granted the unit has `cause.left` equal to its whole service time."""
    _csv(tmp_path, "low", ["time"], [(1,)] * n)
    _csv(tmp_path, "high", ["time"], [(1,)])
    text = f"""
simulation: {{sim_id: V7, factor: 0, random_seed: 42}}
resources: {{m: {{current_cap: 1, queue: priority, preempt: true}}}}
arrivals:
  low: {{replay: {{path: low.csv}}, spawn: low}}
  high: {{replay: {{path: high.csv}}, spawn: high}}
tasks:
  low: {{resource: m, priority: 5, service_time: 10, on_interrupt: abandon, on_interrupt_next: lost, payload: {{e: low_done}}}}
  lost: {{payload: {{e: cause, x: {_expr("[cause.by, cause.left]")}}}}}
  high: {{resource: m, priority: 1, service_time: 2, payload: {{e: high_done}}}}
run: {{until: 100}}
"""
    outcome = _run6(tmp_path, text)
    got = [m for m in _marks(outcome.records) if m[2] == "cause"]
    assert got == _expect(batch6.v7_tie(n))
    assert got == [(1, "low-1", "cause", ["high-1", 10])]


@pytest.mark.parametrize("n", SIZES)
def test_v7_cause_left_of_a_single_timeout_wait(tmp_path, n):
    """V7: `cause.left` is the time left of a single `wait: {timeout}` too."""
    _csv(tmp_path, "w", ["time"], [(0,)] * n)
    _csv(tmp_path, "i", ["time"], [(1,)])
    text = f"""
simulation: {{sim_id: V7, factor: 0, random_seed: 42}}
arrivals: {{w: {{replay: {{path: w.csv}}, spawn: w}}, i: {{replay: {{path: i.csv}}, spawn: poke}}}}
tasks:
  w: {{wait: {{timeout: 10}}, on_interrupt: abandon, on_interrupt_next: lost, payload: {{e: w}}}}
  lost: {{payload: {{e: cause, x: {_expr("cause.left")}}}}}
  poke: {{interrupt: {{task: w, cause: hi}}, payload: {{e: poke}}}}
run: {{until: 30}}
"""
    outcome = _run6(tmp_path, text)
    got = [m for m in _marks(outcome.records) if m[2] == "cause"]
    assert got == _expect(batch6.v7_wait_left(n))


@pytest.mark.parametrize("n", SIZES)
def test_y5_list_mapping_and_function_forms(tmp_path, n):
    """Y5: lists, mappings, comprehensions, slices, `get_key`, `set_key`, `in` and
    `is not None` give what the same Python gives."""
    _csv(tmp_path, "a", ["time", "k"], [(0, k) for k in range(1, n + 1)])
    forms = (
        "[sum([a * 2 for a in [1, 2, 3, attr.k] if a > 1]), "
        "sorted([attr.k, 5, 1])[::-1], len(range(attr.k)), "
        "set_key({'k': attr.k}, 'z', [attr.k, 2])['z'][1], "
        "get_key({'a': 1}, 'b', attr.k), max([attr.k, 2]), 2 in [attr.k, 2], "
        "attr.k is not None, [v for v in [attr.k, attr.k + 1]][-1]]"
    )
    text = f"""
simulation: {{sim_id: Y5, factor: 0, random_seed: 42}}
arrivals: {{a: {{replay: {{path: a.csv}}, spawn: t}}}}
tasks:
  t: {{payload: {{e: forms, x: {_expr(forms)}}}}}
run: {{until: 5}}
"""
    outcome = _run6(tmp_path, text)
    assert _marks(outcome.records) == _expect(batch6.y5_forms(n))


@pytest.mark.parametrize("mode", ["resume", "restart"])
def test_p37_preemption_with_resume_and_restart(tmp_path, mode):
    """P37: ten jobs in pairs at 0, 2, 4, 6 and 8 on a preemptive unit; the first pair
    starts with `initial`. `resume` serves the time left after a preemption and
    `restart` serves the whole time again."""
    _csv(
        tmp_path,
        "r",
        ["time", "pr"],
        [(2 * (i // 2), 1 if i % 3 == 0 else 5) for i in range(2, 10)],
    )
    text = f"""
simulation: {{sim_id: P, factor: 0, random_seed: 42}}
resources: {{m: {{current_cap: 1, queue: priority, preempt: true}}}}
services: {{s: {{dist: constant, value: 1.5}}}}
arrivals:
  a0: {{initial: 1, spawn: job, attributes: {{pr: 1}}}}
  a1: {{initial: 1, spawn: job, attributes: {{pr: 5}}}}
  r: {{replay: {{path: r.csv, fields: [pr]}}, spawn: job}}
tasks:
  job: {{service: s, resource: m, priority: "attr.pr", on_interrupt: {mode}, payload: {{e: done}}}}
run: {{until: 100}}
"""
    outcome = _run6(tmp_path, text)
    keys = {"a0-1": 0, "a1-1": 1, **{f"r-{i - 1}": i for i in range(2, 10)}}
    got = [(m[0], keys[m[1]]) for m in _marks(outcome.records)]
    assert got == _expect_pairs(batch6.p37_preemption(mode))


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("item.n", 4),
        ("item", {"n": 4}),
        ("arg.q", 7),
        ("upstream.e", "pre"),
        ("process.key", "a-1"),
        ("process.flow_name", "pre"),
        ("running(listen)", 1),
        ("peek() >= 0", True),
        ("len(held(m))", 0),
    ],
)
def test_on_entries_read_the_names_of_their_task(tmp_path, expression, expected):
    """An `on:` entry of a task reads the names the rest of the task reads: `item`,
    `arg`, `upstream`, `process`, `running()` and `peek()`."""
    _csv(tmp_path, "l", ["time"], [(0,)])
    _csv(tmp_path, "a", ["time"], [(0,)])
    text = f"""
simulation: {{sim_id: S5, factor: 0, random_seed: 42}}
resources: {{m: {{current_cap: 1}}}}
stores: {{jobs: {{items: [{{n: 4}}]}}}}
signals: {{z: {{once: false}}}}
arrivals: {{l: {{replay: {{path: l.csv}}, spawn: listen}}, a: {{replay: {{path: a.csv}}, spawn: pre}}}}
tasks:
  listen: {{wait: {{signal: z}}, payload: {{e: heard, x: {_expr("signal.z.value")}}}}}
  pre: {{set_args: {{q: 7}}, payload: {{e: pre}}, next: take}}
  take: {{get: {{store: jobs}}, payload: {{e: take}}, on: [{{fire: {{signal: z, value: {_expr(expression)}}}}}]}}
run: {{until: 5}}
"""
    outcome = _run6(tmp_path, text)
    heard = [m for m in _marks(outcome.records) if m[2] == "heard"]
    assert heard == [(0, "l-1", "heard", expected)]


@pytest.mark.parametrize("mode", ["drop", "restart"])
@pytest.mark.parametrize("by", ["count", "keys"])
def test_a5_a_source_with_count_or_keys_obeys_active_and_deactivate(tmp_path, by, mode):
    """A5 on an arrival with `count` or `keys`: `active: false` starts no entity, an
    `activate` at 1.5 lets them start, a `deactivate` at 3.2 stops them, and a second
    `activate` at 3.3 starts them again."""
    _csv(tmp_path, "c1", ["time"], [(1.5,), (3.3,)])
    _csv(tmp_path, "c2", ["time"], [(3.2,)])
    which = "count: 2" if by == "count" else "keys: [x, y]"
    text = f"""
simulation: {{sim_id: A5, factor: 0, random_seed: 42}}
arrivals:
  s: {{dist: constant, value: 1, spawn: t, {which}, active: false, off_mode: {mode}}}
  c1: {{replay: {{path: c1.csv}}, spawn: go}}
  c2: {{replay: {{path: c2.csv}}, spawn: stop}}
tasks:
  t: {{payload: {{e: t}}}}
  go: {{payload: {{e: go}}, activate: s}}
  stop: {{payload: {{e: stop}}, deactivate: s}}
run: {{until: 7.9}}
"""
    outcome = _run6(tmp_path, text)
    times = sorted({m[0] for m in _marks(outcome.records) if m[2] == "t"})
    # drop keeps the old rhythm; restart drops the arrival pending at 3.2 and waits a
    # whole gap from each activation
    assert times == (
        [2, 3, 4, 5, 6, 7] if mode == "drop" else [2.5, 4.3, 5.3, 6.3, 7.3]
    )


def test_a3_a_group_of_zero_draws_no_attribute(tmp_path):
    """A3: a group of 0 draws no attributes, so the first member gets the value a
    source with no group draws for its first arrival."""
    attributes = "attributes: {v: {gen: sample, dist: uniform, low: 0, high: 1}}"

    def first_value(text):
        outcome = _run6(tmp_path, text)
        return next(
            r["value"]["x"]
            for r in outcome.records
            if r["stream_type"] == "event" and r["value"].get("e") == "m"
        )

    body = (
        "simulation: {sim_id: A3, factor: 0, random_seed: 42}\n"
        "arrivals:\n  g: {%s}\n"
        "tasks:\n  m: {payload: {e: m, x: %s}}\n"
        "run: {until: 10}\n"
    )
    payload = _expr("attr.v")
    grouped = first_value(
        body
        % (
            f"dist: constant, value: 1, spawn: m, group: '0 if t < 3 else 1', "
            f"{attributes}",
            payload,
        )
    )
    single = first_value(
        body
        % (f"dist: constant, value: 1, first_at: 3, spawn: m, {attributes}", payload)
    )
    assert grouped == single


def test_a_wait_on_running_ends_when_the_instances_end(tmp_path):
    """Y7: a `wait: {condition}` over `running(task)` is read again when an instance
    ends."""
    _csv(tmp_path, "w", ["time"], [(0,)])
    _csv(tmp_path, "c", ["time"], [(0,)])
    text = """
simulation: {sim_id: Y7, factor: 0, random_seed: 42}
arrivals: {w: {replay: {path: w.csv}, spawn: work}, c: {replay: {path: c.csv}, spawn: chk}}
tasks:
  work: {service_time: 4, payload: {e: worked}}
  chk: {wait: {condition: "running(work) == 0"}, payload: {e: none_left, x: {gen: expr, expr: t}}}
run: {until: 20}
"""
    outcome = _run6(tmp_path, text)
    done = [m for m in _marks(outcome.records) if m[2] == "none_left"]
    assert done == [(4, "c-1", "none_left", 4)]


# ----------------------------------------------------------------- Batch 8: holding


def _trace(
    records: List[Dict[str, Any]],
    statuses: Tuple[str, ...] = ("reneged", "balked", "interrupted"),
) -> List[Tuple[Any, ...]]:
    """The marks a blueprint publishes, in publication order, as (time, key, label,
    extra): a payload with a label `e` (and its `x`), and the statuses `reneged`,
    `balked` and `interrupted`, or those in `statuses`."""
    found: List[Tuple[Any, ...]] = []
    for record in records:
        if record["stream_type"] != "event" or not isinstance(record["value"], dict):
            continue
        value = record["value"]
        if "e" in value:
            found.append(
                (round(record["sim_ts"], 3), record["key"], value["e"], value.get("x"))
            )
        elif value.get("status") in statuses:
            found.append(
                (round(record["sim_ts"], 3), record["key"], value["status"], None)
            )
    return found


def _same(tmp_path, text, expected, statuses=("reneged", "balked", "interrupted")):
    """Runs the blueprint and compares its marks with the program's."""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    assert _trace(outcome.records, statuses) == _expect(expected)
    return outcome


H1_TEXT = """
simulation: {sim_id: H1, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1, queue: priority, preempt: true}}
arrivals:
  job: {replay: {path: job.csv}, spawn: take}
  other: {replay: {path: other.csv}, spawn: waiter}
  cut: {replay: {path: cut.csv}, spawn: cutter}
tasks:
  take: {seize: {resource: m, priority: 5}, payload: {e: got}, next: a}
  a: {service_time: 2, payload: {e: a_done}, next: b}
  b: {service_time: 3, payload: {e: b_done}, release: m}
  waiter: {seize: {resource: m, priority: 9, preempt: false}, payload: {e: waiter_got}, release: m}
  cutter: {resource: m, priority: 1, service_time: 1, payload: {e: cutter_done}}
run: {until: 200}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("cut_at", [None, 1, 3])
def test_h1_a_seized_unit_is_held_across_tasks_until_the_release(tmp_path, n, cut_at):
    """H1 `seize` and `release`: the unit stays with the flow through the next two
    timed tasks and is given back by `release: m`, so the waiter of a lower priority
    comes after every job; a preempter takes it in the middle of a step, and the task
    asks again under the same name and goes on with the time it had left."""
    _csv(tmp_path, "job", ["time"], [(0,)] * n)
    _csv(tmp_path, "other", ["time"], [(0,)])
    _csv(tmp_path, "cut", ["time"], [(cut_at,)] if cut_at is not None else [])
    _same(tmp_path, H1_TEXT, batch8.h1_held_across_steps(n, cut_at))


H5_TEXT = """
simulation: {sim_id: H5, factor: 0, random_seed: 42}
resources: {r1: {current_cap: CAP}, r2: {current_cap: CAP}}
arrivals:
  first: {replay: {path: first.csv}, spawn: one_two}
  second: {replay: {path: second.csv}, spawn: two_one}
tasks:
  one_two: {seize: r1, service_time: 1, payload: {e: got_first}, next: one_two_b}
  one_two_b: {seize: r2, service_time: 1, payload: {e: both}, release: r2, next: one_two_c}
  one_two_c: {payload: {e: done}, release: r1}
  two_one: {seize: r2, service_time: 1, payload: {e: got_first}, next: two_one_b}
  two_one_b: {seize: r1, service_time: 1, payload: {e: both}, release: r1, next: two_one_c}
  two_one_c: {payload: {e: done}, release: r2}
run: {until: 200}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("cap,at", [(1, 0), (2, 0), (1, 5)])
def test_h5_opposite_orders_of_seize_load_with_a_warning_and_run_as_simpy(
    tmp_path, caplog, n, cap, at
):
    """H5: tasks that seize two resources in opposite orders load with a warning that
    names both tasks, and run as SimPy runs the same processes: with one unit each
    and a tie at 0 the two sides wait for each other for ever and the run ends with
    no error."""
    _csv(tmp_path, "first", ["time"], [(0,)] * n)
    _csv(tmp_path, "second", ["time"], [(at,)] * n)
    with caplog.at_level("WARNING"):
        _same(
            tmp_path,
            H5_TEXT.replace("CAP", str(cap)),
            batch8.h5_opposite_orders(n, cap, at),
        )
    assert "task 'one_two_b' holding 'r1' from a seize acquires 'r1' before 'r2'" in (
        caplog.text
    ) or "task 'two_one_b' holding 'r2' from a seize acquires 'r2'" in (caplog.text)
    assert "in one order everywhere" in caplog.text


H6_TEXT = """
simulation: {sim_id: H6, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}}
arrivals:
  holder: {replay: {path: holder.csv}, spawn: hold}
  waiter: {replay: {path: waiter.csv}, spawn: want}
  third: {replay: {path: third.csv}, spawn: free_it}
tasks:
  hold: HOLD
  want: {resource: m, service_time: 10, payload: {e: served}}
  free_it: {service_time: 1, payload: {e: freed}, release: {resource: m, task: hold, where: "process.instance == 0"}}
run: {until: 500}
"""


H6_HOLDS = {
    "seize": "{seize: m, service_time: 5, payload: {e: held}, release: m}",
    "resource": "{resource: m, service_time: 5, payload: {e: held}}",
    "wait request": "{wait: {request: m}, service_time: 5, payload: {e: held}}",
}


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("form", H6_HOLDS)
def test_h6_release_frees_the_unit_another_flow_holds(tmp_path, n, form):
    """H6 `release: {resource, task, where}`: another entity frees the unit that the
    first instance of `hold` holds, whether it asked by `seize`, by `resource` or by a
    `request` in a wait; the holder is not told and its own release does nothing, and
    the next request in the queue gets the unit at once."""
    _csv(tmp_path, "holder", ["time"], [(0,)] * n)
    _csv(tmp_path, "waiter", ["time"], [(0,)] * n)
    _csv(tmp_path, "third", ["time"], [(0,)])
    _same(
        tmp_path,
        H6_TEXT.replace("HOLD", H6_HOLDS[form]),
        batch8.h6_release_of_another_holder(n),
    )


H9_TEXT = """
simulation: {sim_id: H9, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}}
arrivals:
  a: {replay: {path: a.csv}, spawn: first_job}
  b: {replay: {path: b.csv}, spawn: second_job}
tasks:
  first_job: {open: {slot: {request: m}}, wait: {handle: slot}, payload: {e: opened}, next: cancel_it}
  cancel_it: {cancel: {handle: slot}, payload: {e: cancelled_after_grant}, next: hold_on}
  hold_on: {service_time: 3, payload: {e: still_held}, release: {handle: slot}}
  second_job: {resource: m, service_time: 1, payload: {e: got}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
def test_h9_a_cancel_after_the_grant_keeps_the_unit_until_the_release(tmp_path, n):
    """H9 `open`, `cancel` and `release: {handle}`: a request cancelled after its grant
    keeps the unit, and a release of its handle gives it back."""
    _csv(tmp_path, "a", ["time"], [(0,)] * n)
    _csv(tmp_path, "b", ["time"], [(1,)])
    _same(tmp_path, H9_TEXT, batch8.h9_cancel_after_the_grant(n))


I10_TEXT = """
simulation: {sim_id: I10, factor: 0, random_seed: 42}
arrivals:
  a: {replay: {path: a.csv}, spawn: waiter}
  b: {replay: {path: b.csv}, spawn: poke}
tasks:
  waiter: {open: {limit: {timeout: 5}}, wait: {handle: limit}, on_interrupt: POLICY, payload: {e: ended}}
  poke: {payload: {e: poke}, interrupt: {task: waiter, cause: x}}
run: {until: 100}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("policy", ["resume", "restart"])
def test_i10_a_wait_for_a_handle_goes_on_with_the_same_event_after_an_interrupt(
    tmp_path, n, policy
):
    """I10: an interrupted `wait: {handle}` under `resume` or `restart` waits for the
    same event again, as a SimPy process that yields the same timeout again does, so it
    ends at the timeout's own time."""
    _csv(tmp_path, "a", ["time"], [(0,)] * n)
    _csv(tmp_path, "b", ["time"], [(2,)])
    _same(
        tmp_path,
        I10_TEXT.replace("POLICY", policy),
        batch8.i10_wait_again_after_an_interrupt(n),
    )


M7_TEXT = """
simulation: {sim_id: M7, factor: 0, random_seed: 42}
arrivals:
  a: {replay: {path: a.csv}, spawn: relay}
  w: {replay: {path: w.csv}, spawn: watch}
tasks:
  relay: {service_time: 3, payload: {e: relay}, next: more}
  more: {service_time: 3, payload: {e: more}}
  watch: {wait: {running: {task: relay, WHERE whole_flow: true}}, payload: {e: flow_done, x: {gen: expr, expr: t}}}
run: {until: 100}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("only_first", [True, False])
def test_m7_whole_flow_waits_for_the_flows_the_task_belongs_to(tmp_path, n, only_first):
    """M7 `whole_flow` on a `running` wait: the wait ends when the whole flow of the
    selected instances has ended, the tasks after the named one included."""
    _csv(tmp_path, "a", ["time"], [(0,)] * n)
    _csv(tmp_path, "w", ["time"], [(1,)])
    where = 'where: "process.instance == 0", ' if only_first else ""
    _same(
        tmp_path,
        M7_TEXT.replace("WHERE ", where),
        batch8.m7_whole_flow(n, only_first),
    )


T10_TEXT = """
simulation: {sim_id: T10, factor: 0, random_seed: 42}
arrivals: {a: {replay: {path: a.csv}, spawn: begin}}
tasks:
  begin: {open: {ev: {timeout: 1}, t: {timeout: {after: 5, value: late}}}, wait: {any: {a: {handle: ev}, b: {handle: t}}}, payload: {e: first, x: {gen: expr, expr: "handle.t.triggered"}}, next: again}
  again: {wait: {handle: t}, payload: {e: again, x: {gen: expr, expr: "handle.t.value"}}}
run: {until: 100}
"""


@pytest.mark.parametrize("n", SIZES)
def test_losing_timeout_can_be_waited_for_again(tmp_path, n):
    """T10: the timeout that lost an `any` is waited for again through its handle, and
    ends at its own time with its own value, as SimPy's second `yield` of the same
    `Timeout` does."""
    _csv(tmp_path, "a", ["time"], [(0,)] * n)
    _same(tmp_path, T10_TEXT, batch8.t10_wait_again(n))


def _runs(tmp_path, text, csvs, expected, replacements=()):
    """Writes the replay files, runs the blueprint and compares its marks."""
    for name, moments in csvs.items():
        _csv(tmp_path, name, ["time"], [(t,) for t in moments])
    for old, new in replacements:
        text = text.replace(old, new)
    return _same(tmp_path, text, expected)


_PROBE = (
    "  probe: {replay: {path: probe.csv}, spawn: probe}\n"
    '  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}\n'
)

GRANT_TEXT = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}, k: {current_cap: 1}}
arrivals:
  hm: {replay: {path: hm.csv}, spawn: holdm}
  hk: {replay: {path: hk.csv}, spawn: holdk}
  s: {replay: {path: s.csv}, spawn: seizer}
  h: {replay: {path: h.csv}, spawn: opener}
tasks:
  holdm: {resource: m, service_time: 10, payload: {e: held_m}}
  holdk: {resource: k, service_time: 10, payload: {e: held_k}}
  seizer: {seize: m, payload: {e: got}, next: seizer2}
  seizer2: {service_time: 2, payload: {e: s_done}, release: m}
  opener: {open: {q: {request: k}}, service_time: 12, payload: {e: after, x: {gen: expr, expr: "handle.q.triggered"}}, release: {handle: q}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
def test_unit_states_grant(tmp_path, n):
    """Unit states, Grant: a queued `seize` becomes held and the flow goes on; the
    request of a handle becomes held whatever the flow is doing."""
    csvs = {"hm": [0], "hk": [0], "s": [0] * n, "h": [0] * n}
    _runs(tmp_path, GRANT_TEXT, csvs, batch8.unit_grant(n))


PATIENCE_TEXT = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}, o: {current_cap: 1}}
arrivals:
  holder: {replay: {path: holder.csv}, spawn: hold}
  s: {replay: {path: s.csv}, spawn: subject}
  behind: {replay: {path: behind.csv}, spawn: behind}
  probe: {replay: {path: probe.csv}, spawn: probe}
tasks:
  hold: {resource: m, service_time: 10, payload: {e: held}}
  subject: SUBJECT
  behind: {resource: m, service_time: 1, payload: {e: behind}}
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 500}
"""

PATIENCE_HELD = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}, o: {current_cap: 1}}
arrivals:
  blocker: {replay: {path: holder.csv}, spawn: block}
  s: {replay: {path: s.csv}, spawn: subject}
  probe: {replay: {path: probe.csv}, spawn: probe}
tasks:
  block: {resource: o, service_time: 9, payload: {e: blocked}}
  subject: {seize: m, payload: {e: seized}, next: second}
  second: {resource: o, service_time: 1, patience: 2, payload: {e: never}}
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("part", ["seize", "handle", "held"])
def test_unit_states_patience_end(tmp_path, n, part):
    """Unit states, Patience end: a queued `seize` ends and leaves the line; the
    request of a handle stays and is granted later; a held unit stays held."""
    subject = {
        "seize": "{seize: {resource: m, patience: 2}, payload: {e: got}}",
        "handle": "{open: {q: {request: m}}, wait: {handle: q}, patience: 2, payload: {e: got}}",
    }
    csvs = {"holder": [0], "s": [0] * n, "behind": [0.5], "probe": [30]}
    if part == "held":
        text, csvs = PATIENCE_HELD, {k: v for k, v in csvs.items() if k != "behind"}
    else:
        text = PATIENCE_TEXT.replace("SUBJECT", subject[part])
        if part == "handle":
            text = text.replace(
                "  behind: {replay: {path: behind.csv}, spawn: behind}\n", ""
            )
            csvs.pop("behind")
    _runs(tmp_path, text, csvs, batch8.unit_patience(n, part))


PREEMPT_TEXT = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1, queue: priority, preempt: true}}
arrivals:
  v: {replay: {path: v.csv}, spawn: take}
  q: {replay: {path: q.csv}, spawn: queued}
  l: {replay: {path: l.csv}, spawn: looker}
  h: {replay: {path: h.csv}, spawn: high}
  probe: {replay: {path: probe.csv}, spawn: probe}
tasks:
  take: {seize: {resource: m, priority: 1}, payload: {e: taken}, next: work}
  work: {service_time: 5, payload: {e: worked}, release: m, on_interrupt: POLICY, ON_NEXT}
  cleanup: {payload: {e: cleanup}, release: m}
  queued: {seize: {resource: m, priority: 2}, payload: {e: queued_got}, release: m}
  looker: {open: {q: {request: m}}, priority: 2, service_time: 1.5, payload: {e: looked, x: {gen: expr, expr: "handle.q.triggered"}}, cancel: q}
  high: {resource: m, priority: 0, service_time: 2, payload: {e: high_done}}
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("policy", ["resume", "restart", "abandon"])
def test_unit_states_preemption(tmp_path, n, policy):
    """Unit states, Preemption: a request in a line is not told; a held unit is
    ended, and under `resume` or `restart` the task asks again under the same name;
    under `abandon` the task ends and a later `release` frees nothing."""
    csvs = {"v": [0] * n, "q": [0.5], "l": [0.5], "h": [1], "probe": [50]}
    on_next = "on_interrupt_next: cleanup" if policy == "abandon" else "next: null"
    text = PREEMPT_TEXT.replace("POLICY", policy).replace("ON_NEXT", on_next)
    _runs(tmp_path, text, csvs, batch8.unit_preemption(n, policy))


LOSS_TEXT = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}}
arrivals:
  a: {replay: {path: a.csv}, spawn: subject}
  probe: {replay: {path: probe.csv}, spawn: probe}
tasks:
  subject: {open: {q: {request: m}}, wait: {any: {w: {handle: q}, t: {timeout: 1}}}, payload: {e: any_done, x: {gen: expr, expr: "[wait.w.done, users.m, queue.m]"}}, next: later}
  later: {service_time: 2, payload: {e: after, x: {gen: expr, expr: "[handle.q.triggered, users.m, queue.m]"}}, release: {handle: q}}
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
def test_unit_states_loss_in_any_and_release_of_a_queued_request(tmp_path, n):
    """Unit states, Loss in `any` and `release`: a request that loses an `any` stays
    in the line and one that wins stays held until its release; a release of a request
    still in the line does nothing, and it is granted later."""
    _runs(
        tmp_path, LOSS_TEXT, {"a": [0] * n, "probe": [50]}, batch8.unit_loss_in_any(n)
    )


CANCEL_TEXT = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}}
arrivals:
  holder: {replay: {path: holder.csv}, spawn: hold}
  a: {replay: {path: a.csv}, spawn: subject}
tasks:
  hold: {resource: m, service_time: 5, payload: {e: held}}
  subject: {open: {q: {request: m}}, service_time: 1, payload: {e: before_cancel, x: STATE}, next: cancel_it}
  cancel_it: {cancel: q, payload: {e: cancelled, x: STATE}, next: release_it}
  release_it: {release: {handle: q}, payload: {e: released, x: STATE}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
def test_unit_states_cancel_of_a_queued_request_and_release_after_it(tmp_path, n):
    """Unit states, `cancel`: a request in the line leaves it and the release after it
    has nothing to free."""
    state = '{gen: expr, expr: "[users.m, queue.m]"}'
    _runs(
        tmp_path,
        CANCEL_TEXT.replace("STATE", state),
        {"holder": [0], "a": [0] * n},
        batch8.unit_cancel(n),
    )


@pytest.mark.parametrize("n", SIZES)
def test_unit_states_cancel_of_a_request_that_left_the_line_fails_the_run(tmp_path, n):
    """Unit states, `cancel`: a request cancelled twice is a run failure, as SimPy's
    `ValueError` is."""
    text = CANCEL_TEXT.replace("STATE", "null").replace(
        "next: release_it}\n  release_it: {release: {handle: q}, payload: {e: released, x: null}}",
        "next: cancel_again}\n  cancel_again: {cancel: q, payload: {e: again}}",
    )
    for name, moments in {"holder": [0], "a": [0] * n}.items():
        _csv(tmp_path, name, ["time"], [(t,) for t in moments])
    outcome = run_yaml_text(tmp_path, text, {})
    assert isinstance(outcome.error, TaskFailed)
    assert outcome.error.task == "cancel_again"
    assert "cancel: 'q' was cancelled already" in str(outcome.error)
    assert [m for m in _trace(outcome.records) if m[2] == "again"] == []
    # SimPy raises ValueError at the second cancel of the first request
    assert batch8.unit_cancel_twice(n)[:2] == [
        (1, "a-1", "cancelled", n - 1),
        (1, "a-1", "again", "ValueError"),
    ]


RELEASE_TEXT = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: 2}}
arrivals: {a: {replay: {path: a.csv}, spawn: subject}}
tasks:
  subject: {open: {a: {request: m}, b: {request: m}}, wait: {all: {x: {handle: a}, y: {handle: b}}}, payload: {e: both, x: {gen: expr, expr: "users.m"}}, next: first}
  first: {service_time: 1, release: {handle: a}, payload: {e: first}, next: after_a}
  after_a: {payload: {e: after_a, x: {gen: expr, expr: "[users.m, handle.b.triggered]"}}, next: second}
  second: {service_time: 1, release: {handle: b}, payload: {e: second}, next: after_b}
  after_b: {payload: {e: after_b, x: {gen: expr, expr: "users.m"}}, next: third}
  third: {release: {handle: a}, payload: {e: third}, next: again}
  again: {payload: {e: again, x: {gen: expr, expr: "users.m"}}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
def test_unit_states_release_of_held_and_of_ended_requests(tmp_path, n):
    """Unit states, `release` and Y4 two units: a release ends a held request and frees
    its unit; a release of a request that has ended does nothing."""
    _runs(tmp_path, RELEASE_TEXT, {"a": [0] * n}, batch8.unit_release(n))


TASK_FLOW_END_TEXT = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}}
arrivals:
  holder: {replay: {path: holder.csv}, spawn: hold}
  a: {replay: {path: a.csv}, spawn: subject}
  probe: {replay: {path: probe.csv}, spawn: probe}
tasks:
  hold: {resource: m, service_time: 10, payload: {e: held}}
  subject: SUBJECT
  next_task: {payload: {e: te_next, x: {gen: expr, expr: "not handle.q.triggered"}}, next: waits}
  waits: {wait: {handle: q}, payload: {e: te_got}, release: {handle: q}}
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("part", ["task", "flow", "held"])
def test_unit_states_task_end_and_flow_end(tmp_path, n, part):
    """Unit states, Task end and Flow end: a request a task opened stays in the line
    when the task ends, and is still there in the next task; at the end of the flow it
    stays and is granted later; a held unit stays held."""
    subject = {
        "task": "{open: {q: {request: m}}, service_time: 2, payload: {e: te_first}, next: next_task}",
        "flow": "{open: {q: {request: m}}, payload: {e: flow_ended}}",
        "held": "{seize: m, payload: {e: seized}}",
    }[part]
    csvs = {"holder": [0], "a": [0] * n, "probe": [50]}
    text = TASK_FLOW_END_TEXT.replace("SUBJECT", subject)
    if part == "held":
        csvs.pop("holder")
        text = text.replace("  holder: {replay: {path: holder.csv}, spawn: hold}\n", "")
    if part != "task":
        text = "\n".join(
            line
            for line in text.split("\n")
            if not line.startswith(("  next_task:", "  waits:"))
        )
    _runs(tmp_path, text, csvs, batch8.unit_task_and_flow_end(n, part))


def test_h1_preempting_a_unit_that_an_ended_flow_holds_fails_the_requesting_flow(
    tmp_path,
):
    """H1: the flow that seized the unit has ended and keeps it; a request of a higher
    priority that would preempt it fails the run with `RuntimeError`, as SimPy's
    `request()` raises in the process that makes it."""
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1, queue: priority, preempt: true}}
arrivals:
  a: {replay: {path: a.csv}, spawn: holder}
  b: {replay: {path: b.csv}, spawn: high}
tasks:
  holder: {seize: {resource: m, priority: 5}, payload: {e: holder_ended}}
  high: {resource: m, priority: 0, service_time: 1, payload: {e: high_done}}
run: {until: 50}
"""
    _csv(tmp_path, "a", ["time"], [(0,)])
    _csv(tmp_path, "b", ["time"], [(1,)])
    outcome = run_yaml_text(tmp_path, text, {})
    assert isinstance(outcome.error, RuntimeError)
    assert "has terminated and cannot be interrupted" in str(outcome.error)
    assert [m[2] for m in _trace(outcome.records)] == ["holder_ended"]
    assert batch8.h1_preempting_an_ended_flow() == [(1, "RuntimeError")]


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("interrupt", [False, True])
def test_y4_a_child_flow_is_a_handle_that_can_be_read_interrupted_and_waited_for(
    tmp_path, n, interrupt
):
    """Y4 `open: {task}`, `handle.<n>.alive`, `.ok` and `.value`, and `interrupt:
    {handle}`: a child flow is read, interrupted and waited for through its name."""
    text = """
simulation: {sim_id: Y4, factor: 0, random_seed: 42}
arrivals: {a: {replay: {path: a.csv}, spawn: begin}}
tasks:
  begin: {open: {c: {task: w}}, payload: {e: alive, x: {gen: expr, expr: "handle.c.alive"}}, next: wait_c}
  wait_c: {service_time: 1, payload: {e: waited}, INTERRUPT next: join}
  join: {wait: {handle: c}, payload: {e: got, x: {gen: expr, expr: "[handle.c.value.e, handle.c.alive, handle.c.ok]"}}}
  w: {service_time: 5, on_interrupt: abandon, on_interrupt_next: stopped, payload: {e: done}}
  stopped: {payload: {e: stopped_task}}
run: {until: 100}
"""
    text = text.replace(
        "INTERRUPT ", "interrupt: {handle: c, cause: stop}, " if interrupt else ""
    )
    _csv(tmp_path, "a", ["time"], [(0,)] * n)
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    expected = batch8.y4_child_reference(n, interrupt)
    # The program's child returns "stopped" or "done"; the blueprint's child ends with
    # the record of its last task, `stopped_task` or `done`
    expected = [
        (t, k, label, [{"stopped": "stopped_task"}.get(x[0], x[0])] + x[1:])
        if label == "got"
        else (t, k, label, x)
        for t, k, label, x in expected
    ]
    assert _trace(outcome.records) == _expect(expected)


@pytest.mark.parametrize("n", SIZES)
def test_y4_cancel_of_a_get_and_of_a_put_and_a_get_granted_before_the_cancel(
    tmp_path, n
):
    """Y4 `cancel` of a get or a put: a queued get leaves its line, and a put too; a
    get granted before the cancel keeps its item, `ok` and value."""
    text = """
simulation: {sim_id: Y4, factor: 0, random_seed: 42}
stores: {empty: {}, full: {current_cap: 1, items: [1]}, stocked: {items: [x]}}
arrivals:
  g: {replay: {path: g.csv}, spawn: begin}
  k: {replay: {path: k.csv}, spawn: keep}
  p: {replay: {path: p.csv}, spawn: put_open}
tasks:
  begin: {open: {r: {get: {store: empty}}}, service_time: 1, payload: {e: begun}, next: cancel_it}
  cancel_it: {cancel: r, service_time: 1, payload: {e: cancelled, x: {gen: expr, expr: "getters.empty"}}, next: put_it}
  put_it: {put: {store: empty, item: x}, payload: {e: put_done}, next: read}
  read: {payload: {e: items, x: {gen: expr, expr: "level.empty"}}}
  keep: {open: {r: {get: {store: stocked}}}, service_time: 0, payload: {e: granted}, next: cancel_kept}
  cancel_kept: {cancel: r, payload: {e: kept, x: {gen: expr, expr: "[level.stocked, handle.r.ok, handle.r.value]"}}}
  put_open: {open: {r: {put: {store: full, item: 2}}}, payload: {e: put_opened, x: {gen: expr, expr: "putters.full"}}, next: cancel_put}
  cancel_put: {cancel: r, payload: {e: put_cancelled, x: {gen: expr, expr: "putters.full"}}}
run: {until: 100}
"""
    _csv(tmp_path, "g", ["time"], [(0,)] * n)
    _csv(tmp_path, "k", ["time"], [(0,)])
    _csv(tmp_path, "p", ["time"], [(0,)])
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    assert _trace(outcome.records) == _expect(batch8.y4_get_cancel(n))


def test_y4_a_child_reads_a_handle_of_its_parent_and_a_group_is_made_now(tmp_path):
    """Y4 scope and group: a child flow reads the handles of the flow that started it,
    and a group of timeouts made at one step is waited for at a later step."""
    text = """
simulation: {sim_id: Y4, factor: 0, random_seed: 42}
arrivals:
  a: {replay: {path: a.csv}, spawn: parent}
  g: {replay: {path: g.csv}, spawn: grouped}
tasks:
  parent: {open: {ev: {timeout: {after: 2, value: v}}, c: {task: child}}, wait: {handle: c}, payload: null}
  child: {wait: {handle: ev}, payload: {e: child, x: {gen: expr, expr: "handle.ev.value"}}}
  grouped: {open: {g: {all: {a: {timeout: {after: 1, value: 1}}, b: {timeout: {after: 3, value: 3}}}}}, service_time: 2, payload: {e: made}, next: later}
  later: {wait: {handle: g}, payload: {e: got, x: {gen: expr, expr: "handle.g.value"}}}
run: {until: 100}
"""
    _csv(tmp_path, "a", ["time"], [(0,)])
    _csv(tmp_path, "g", ["time"], [(0,)])
    _same(tmp_path, text, batch8.y4_scope_and_group())


@pytest.mark.parametrize("n", SIZES)
def test_y4_an_opened_request_is_ahead_of_a_seize_of_the_same_resource(tmp_path, n):
    """Y4 key order: `open` comes before `seize` in a task, so the opened request is
    ahead in the line, and the seize waits for the unit it holds."""
    text = """
simulation: {sim_id: Y4, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}}
arrivals:
  blocker: {replay: {path: blocker.csv}, spawn: block}
  a: {replay: {path: a.csv}, spawn: flow}
  r: {replay: {path: r.csv}, spawn: free_it}
tasks:
  block: {resource: m, service_time: 1, payload: {e: blocked}}
  flow: {open: {o: {request: m}}, seize: m, payload: {e: seized_got}}
  free_it: {service_time: 2, payload: {e: released}, release: {resource: m, task: flow}}
run: {until: 100}
"""
    _csv(tmp_path, "blocker", ["time"], [(0,)])
    _csv(tmp_path, "a", ["time"], [(0,)] * n)
    _csv(tmp_path, "r", ["time"], [(0,)])
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    got = [m[:3] for m in _trace(outcome.records) if m[2] == "seized_got"]
    expected = [m[:3] for m in batch8.y4_key_order(n) if m[2] == "seized_got"]
    assert got == _expect(expected)


@pytest.mark.parametrize("n", SIZES)
def test_a_seized_unit_that_a_preemption_takes_before_the_task_looked_is_asked_for_again(
    tmp_path, n
):
    """Engine fix 9: a unit granted to a `seize` and preempted in the same instant,
    before the task looked at the grant, is taken from it, and the task asks again
    under the same name, as a SimPy process whose `yield request` is interrupted
    does."""
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue: priority, preempt: true}}
arrivals:
  h: {replay: {path: h.csv}, spawn: hold}
  v: {replay: {path: v.csv}, spawn: victim}
  p: {replay: {path: p.csv}, spawn: high}
  probe: {replay: {path: probe.csv}, spawn: probe}
tasks:
  hold: {resource: m, priority: 1, service_time: 5, payload: {e: held}}
  victim: {seize: {resource: m, priority: 5}, payload: {e: taken}, next: work}
  work: {service_time: 3, payload: {e: worked}, release: m}
  high: {resource: m, priority: 0, service_time: 2, payload: {e: high_done}}
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 500}
"""
    csvs = {"h": [0], "v": [0] * n, "p": [5], "probe": [100]}
    _runs(tmp_path, text, csvs, batch8.granted_then_preempted(n))


# ------------------------------------------------- Batch 8: M5, Y8, Y12 and H8


def _ranked(tmp_path, text, expected):
    """As `_same`, with the statuses `preempted` and `balked` as marks."""
    return _same(tmp_path, text, expected, ("preempted", "balked"))


def _one(name, spawn, at, attributes=None):
    """An arrival of one entity at `at`, whose key is `<name>-1`."""
    extra = ""
    if attributes:
        pairs = ", ".join(f"{k}: {v}" for k, v in attributes.items())
        extra = f", attributes: {{{pairs}}}"
    return (
        f"  {name}: {{dist: constant, value: 1, spawn: {spawn}, max_events: 1, "
        f"first_at: {at}{extra}}}\n"
    )


M5_JOBS = {
    "equal rank at one instant, holder without preempt": [
        ("a", 0, 1, False, 5),
        ("b", 0, 1, True, 5),
    ],
    "equal rank, request later": [("a", 0, 1, False, 5), ("b", 1, 1, True, 5)],
    "equal key, both preempting, no preemption": [
        ("a", 0, 1, True, 5),
        ("b", 0, 1, True, 5),
    ],
    "both without preempt": [("a", 0, 1, False, 5), ("b", 0, 1, False, 5)],
    "lower rank later": [("a", 0, 5, True, 5), ("b", 1, 1, True, 5)],
    "request with preempt false never preempts": [
        ("a", 0, 5, True, 5),
        ("b", 1, 1, False, 5),
    ],
    "queue order by key": [
        ("a", 0, 1, True, 3),
        ("b", 1, 2, False, 1),
        ("c", 1, 2, True, 1),
        ("d", 1, 1, False, 1),
    ],
}
M5_RANKS = [
    ("old", 0, 5, True, 9),
    ("new", 1, 5, True, 9),
    ("hi", 2, 1, True, 9),
]
WHEN_TIME = "request.rank < holder.rank and t - holder.since > 1"


def _m5_text(jobs, copies, cap=1, queue="", preempt="true", until=30):
    arrivals = "".join(
        _one(
            f"{name}{index}",
            "job",
            at,
            {"rank": rank, "pre": str(flag).lower(), "span": span},
        )
        for name, at, rank, flag, span in jobs
        for index in range(1, copies + 1)
    )
    return f"""
simulation: {{sim_id: M5, factor: 0, random_seed: 42}}
resources:
  m: {{current_cap: {cap}, queue: {{rank: "attr.rank"{queue}}}, preempt: {preempt}}}
arrivals:
{arrivals}tasks:
  job: {{resource: m, preempt: "attr.pre", service_time: "attr.span", on_interrupt: abandon, payload: {{e: done}}}}
run: {{until: {until}}}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("case", sorted(M5_JOBS))
def test_m5_default_queue_rule_runs_as_a_preemptive_resource(tmp_path, n, case):
    """M5: a `queue` rank on the request's `attr.rank` with `preempt: true` orders
    and preempts as SimPy's PreemptiveResource does with the same priorities."""
    jobs = M5_JOBS[case]
    plain = batch8.m5_rules(jobs, n)
    outcome = _ranked(tmp_path, _m5_text(jobs, n), plain)
    assert outcome.records


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("ties", ["fifo", "lifo"])
@pytest.mark.parametrize("cap", [1, 2])
def test_m5_ties_change_only_the_order_of_equal_ranks(tmp_path, n, ties, cap):
    """M5 `ties`: of equal ranks the earliest waits first (fifo) or the latest does
    (lifo); a later request of an equal rank preempts nobody in either."""
    jobs = [
        ("a", 0, 1, False, 2),
        ("b", 1, 1, False, 1),
        ("c", 1, 1, False, 1),
        ("d", 1.5, 1, False, 1),
        ("e", 2, 1, True, 1),
    ]
    _ranked(
        tmp_path,
        _m5_text(jobs, n, cap=cap, queue=f", ties: {ties}"),
        batch8.m5_rules(jobs, n, capacity=cap, ties=ties),
    )


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("at", [1, 2])
def test_m5_when_rule_decides_each_preemption(tmp_path, n, at):
    """M5 `preempt: {when}`: a request of a lower rank preempts only when the rule
    holds (here, after the holder has held for more than 1 s)."""
    jobs = [("a", 0, 5, True, 5), ("b", at, 1, True, 1)]
    _ranked(
        tmp_path,
        _m5_text(jobs, n, preempt=f'{{when: "{WHEN_TIME}"}}'),
        batch8.m5_rules(
            jobs,
            n,
            when=lambda request, holder: (
                request.priority < holder.priority and (at - holder.usage_since) > 1
            ),
        ),
    )


@pytest.mark.parametrize("n", SIZES)
def test_m5_victim_rule_picks_the_holder_with_the_largest_value(tmp_path, n):
    """M5 `preempt: {victim}`: of the holders, the one with the largest value is
    the candidate, the first granted among equal values; `holder.rank` here."""
    text = _m5_text(M5_RANKS, n, cap=2, preempt='{victim: "holder.rank"}', until=3)
    _ranked(
        tmp_path,
        text,
        batch8.m5_rules(
            M5_RANKS, n, capacity=2, victim=lambda holder: holder.priority, until=3
        ),
    )


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("p", [1, 3])
def test_y8_a_computed_preempt_flag_is_read_for_each_request(tmp_path, n, p):
    """Y8 computed `preempt`: `preempt: "attr.p < 2"` is a flag per request; of the
    requests at 1 the first of priority 1 preempts the holder and the others wait
    in key order, and with priority 3 none preempts."""
    arrivals = _one("low1", "job", 0, {"p": 5}) + "".join(
        _one(f"high{i}", "job", 1, {"p": p}) for i in range(1, n + 1)
    )
    text = f"""
simulation: {{sim_id: Y8, factor: 0, random_seed: 42}}
resources:
  r: {{current_cap: 1, queue: priority, preempt: true}}
arrivals:
{arrivals}tasks:
  job: {{resource: r, priority: "attr.p", preempt: "attr.p < 2", service_time: 5, on_interrupt: abandon, payload: {{e: done}}}}
run: {{until: 100}}
"""
    _ranked(tmp_path, text, batch8.y8_computed(p, n))


@pytest.mark.parametrize("n", SIZES)
def test_y8_a_request_mapping_gives_each_branch_its_own_priority(tmp_path, n):
    """Y8 `request` mapping: in one `wait: {all: ...}` a flow asks for m at priority
    0 and for n at priority 5, so n goes first to a flow that asks for n alone at
    priority 1."""
    ps = "".join(_one(f"p{i}", "ask", 0.5) for i in range(1, n + 1))
    qs = "".join(_one(f"q{i}", "only_n", 1) for i in range(1, n + 1))
    text = f"""
simulation: {{sim_id: Y8, factor: 0, random_seed: 42}}
resources:
  m: {{current_cap: 1, queue: priority}}
  n: {{current_cap: 1, queue: priority}}
arrivals:
  hm: {{dist: constant, value: 1, spawn: hold_m, max_events: 1, first_at: 0}}
  hn: {{dist: constant, value: 1, spawn: hold_n, max_events: 1, first_at: 0}}
{ps}{qs}tasks:
  hold_m: {{resource: m, service_time: 2, payload: null}}
  hold_n: {{resource: n, service_time: 2, payload: null}}
  ask: {{wait: {{all: {{x: {{request: {{resource: m, priority: 0}}}}, y: {{request: {{resource: n, priority: 5}}}}}}}}, service_time: 1, payload: {{e: got, x: {{gen: expr, expr: "t - 1"}}}}}}
  only_n: {{resource: n, priority: 1, service_time: 1, payload: {{e: got, x: {{gen: expr, expr: "t - 1"}}}}}}
run: {{until: 100}}
"""
    _ranked(tmp_path, text, batch8.y8_branches(n))


@pytest.mark.parametrize("n", SIZES)
def test_y8_a_request_mapping_that_names_no_priority_or_preempt_is_simpys_request(
    tmp_path, n
):
    """Y8: `request: {resource: r}` is `r.request()`, priority 0 and preempt true, so it
    takes the unit of a holder of priority 5 and goes before a request of priority 3."""
    arrivals = (
        _one("low1", "low", 0)
        + "".join(_one(f"viamap{i}", "viamap", 1) for i in range(1, n + 1))
        + "".join(_one(f"plain{i}", "plain", 2) for i in range(1, n + 1))
    )
    text = f"""
simulation: {{sim_id: Y8, factor: 0, random_seed: 42}}
resources:
  r: {{current_cap: 1, queue: priority, preempt: true}}
arrivals:
{arrivals}tasks:
  low: {{resource: r, priority: 5, service_time: 5, on_interrupt: abandon, payload: {{e: done}}}}
  viamap: {{wait: {{request: {{resource: r}}}}, service_time: 5, payload: {{e: done}}}}
  plain: {{resource: r, priority: 3, service_time: 5, on_interrupt: abandon, payload: {{e: done}}}}
run: {{until: 1000}}
"""
    _ranked(tmp_path, text, batch8.y8_omitted(n))


@pytest.mark.parametrize("n", SIZES)
def test_y8_a_request_mapping_may_preempt_in_a_task_that_may_not(tmp_path, n):
    """Y8: in a task with `preempt: false` the request written as a name asks with
    `preempt=False` and waits for the unit, and the mapping that leaves `preempt` out
    is `request()`, which preempts the holder of a lower rank."""
    mixes = "".join(_one(f"mix{i}", "mix", 1) for i in range(1, n + 1))
    text = f"""
simulation: {{sim_id: Y8, factor: 0, random_seed: 42}}
resources:
  a: {{current_cap: 1, queue: priority, preempt: true}}
  b: {{current_cap: 1, queue: priority, preempt: true}}
arrivals:
  hold_a: {{dist: constant, value: 1, spawn: hold_a, max_events: 1, first_at: 0}}
  hold_b: {{dist: constant, value: 1, spawn: hold_b, max_events: 1, first_at: 0}}
{mixes}tasks:
  hold_a: {{resource: a, priority: 5, service_time: 5, on_interrupt: abandon, payload: {{e: done}}}}
  hold_b: {{resource: b, priority: 5, service_time: 5, on_interrupt: abandon, payload: {{e: done}}}}
  mix: {{preempt: false, wait: {{all: {{x: {{request: a}}, y: {{request: {{resource: b}}}}}}}}, service_time: 1, payload: {{e: got, x: {{gen: expr, expr: "t - 1"}}}}}}
run: {{until: 100}}
"""
    _ranked(tmp_path, text, batch8.y8_mapping_preempt(n))


@pytest.mark.parametrize("n", SIZES)
def test_y8_a_request_mapping_beside_a_name_does_not_take_the_task_priority(
    tmp_path, n
):
    """Y8: in a task of priority 3 the request written as a name ranks 3 and the one
    written as a mapping with no priority ranks 0, so it goes before a request of
    priority 1."""
    ps = "".join(_one(f"mix{i}", "mix", 0.5) for i in range(1, n + 1))
    qs = "".join(_one(f"q{i}", "only_n", 1) for i in range(1, n + 1))
    text = f"""
simulation: {{sim_id: Y8, factor: 0, random_seed: 42}}
resources:
  m: {{current_cap: {n}, queue: priority}}
  n: {{current_cap: 1, queue: priority}}
arrivals:
  hn: {{dist: constant, value: 1, spawn: hold_n, max_events: 1, first_at: 0}}
{ps}{qs}tasks:
  hold_n: {{resource: n, service_time: 2, payload: null}}
  mix: {{priority: 3, wait: {{all: {{x: {{request: m}}, y: {{request: {{resource: n}}}}}}}}, service_time: 1, payload: {{e: got, x: {{gen: expr, expr: "t - 1"}}}}}}
  only_n: {{resource: n, priority: 1, service_time: 1, payload: {{e: got, x: {{gen: expr, expr: "t - 1"}}}}}}
run: {{until: 100}}
"""
    _ranked(tmp_path, text, batch8.y8_default_rank(n))


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("ties", ["fifo", "lifo"])
@pytest.mark.parametrize("ranks", [[5, 1, 3], [1, 1, 0], [2, 2, 2]])
def test_y12_waiting_gets_of_a_store_follow_the_rank(tmp_path, n, ranks, ties):
    """Y12 `get_queue`: getters wait in rank order, and among equal ranks in the order
    they asked (`ties: fifo`) or the latest first (`ties: lifo`), and each item goes
    to the first of them."""
    getters = "".join(
        _one(f"g{i}", "get", 0, {"rank": rank})
        for i, rank in enumerate((r for r in ranks for _ in range(n)), 1)
    )
    text = f"""
simulation: {{sim_id: Y12, factor: 0, random_seed: 42}}
stores:
  s: {{get_queue: {{rank: "attr.rank", ties: {ties}}}}}
arrivals:
{getters}  p: {{dist: constant, value: 1, spawn: put, max_events: {len(ranks) * n}, first_at: 1}}
tasks:
  get: {{get: {{store: s}}, payload: {{e: got, x: {{gen: expr, expr: item}}}}}}
  put: {{put: {{store: s, item: {{gen: expr, expr: t}}}}, payload: {{e: put}}}}
run: {{until: 100}}
"""
    _ranked(tmp_path, text, batch8.y12_gets(ranks, n, ties == "lifo"))


@pytest.mark.parametrize("n", SIZES)
def test_y12_waiting_puts_of_a_store_follow_the_rank(tmp_path, n):
    """Y12 `put_queue`: with one place, the puts that wait go in by rank (1 before 5)
    as takers free the place."""
    shelving = ""
    for name, rank in (("first", 0), ("low", 5), ("high", 1)):
        for i in range(1, n + 1):
            shelving += _one(f"{name[0]}{i}", "shelve", 0, {"rank": rank, "name": name})
    text = f"""
simulation: {{sim_id: Y12, factor: 0, random_seed: 42}}
stores:
  shelf: {{current_cap: 1, put_queue: {{rank: "attr.rank"}}}}
arrivals:
{shelving}  tk: {{dist: constant, value: 1, spawn: take, max_events: {3 * n}, first_at: 1}}
tasks:
  shelve: {{put: {{store: shelf, item: {{gen: attr, name: name}}}}, payload: {{e: in}}}}
  take: {{get: {{store: shelf}}, payload: {{e: out, x: {{gen: expr, expr: item}}}}}}
run: {{until: 100}}
"""
    outcome = _ranked(tmp_path, text, batch8.y12_puts(n))
    assert outcome.records


@pytest.mark.parametrize("n", SIZES)
def test_y12_a_container_get_that_cannot_be_met_stops_the_scan(tmp_path, n):
    """Y12 container `get_queue`: a get of 5 at rank 0 that cannot be met holds back
    the gets of 1 at rank 1 behind it, as SimPy's container does."""
    smalls = "".join(
        _one(f"small{i}", "cget", 0, {"rank": 1, "amount": 1}) for i in range(1, n + 1)
    )
    text = f"""
simulation: {{sim_id: Y12, factor: 0, random_seed: 42}}
containers:
  c: {{get_queue: {{rank: "attr.rank"}}, level: 0, current_cap: 100}}
arrivals:
{_one("big", "cget", 0, {"rank": 0, "amount": 5})}{smalls}  cp: {{dist: constant, value: 1, spawn: cput_two, max_events: 1, first_at: 1}}
  cq: {{dist: constant, value: 1, spawn: cput_three, max_events: 1, first_at: 2}}
  cr: {{dist: constant, value: 1, spawn: cput_one, max_events: {n}, first_at: 3}}
tasks:
  cget: {{get: {{container: c, amount: "attr.amount"}}, payload: {{e: got, x: {{gen: expr, expr: "level.c"}}}}}}
  cput_two: {{put: {{container: c, amount: 2}}, payload: null}}
  cput_three: {{put: {{container: c, amount: 3}}, payload: null}}
  cput_one: {{put: {{container: c, amount: 1}}, payload: null}}
run: {{until: 100}}
"""
    _ranked(tmp_path, text, batch8.y12_blocked_head(n))


H8_CASES = {
    "1 then 2 at 4": ([(0, 1), (4, 2)], [0, 1, 2, 5, 6], 10),
    "shrink removes nobody": ([(0, 3), (4, 0)], [0, 1, 2, 3, 5], 2),
    "limit 0 balks at a free unit too": ([(0, 0)], [0, 1], 10),
    "no limit": ([(0, None)], [0, 1, 2], 1),
    "write and request at one instant, write made first": (
        [(0, 0), (1, 1)],
        [0, 1],
        10,
    ),
}


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("case", sorted(H8_CASES))
def test_h8_queue_limit_changes_during_the_run(tmp_path, n, case):
    """H8: `<sim>.resources.m.queue_limit` written during the run applies from the
    next request, removes nobody who already waits, and `null` is no limit; 0 balks
    even at a free unit."""
    limits, times, service = H8_CASES[case]
    first = "null" if limits[0][1] is None else limits[0][1]
    arrivals = "".join(
        _one(f"j{(k * n) + i}", "job", at)
        for k, at in enumerate(times)
        for i in range(1, n + 1)
    )
    steps = ", ".join(
        f"{{at: {at}, path: H8.resources.m.queue_limit, "
        f"value: {'null' if value is None else value}}}"
        for at, value in limits[1:]
    )
    text = f"""
simulation: {{sim_id: H8, factor: 0, random_seed: 42}}
resources: {{m: {{current_cap: 1, queue_limit: {first}}}}}
services: {{s: {{dist: constant, value: {service}}}}}
arrivals:
{arrivals}scenario: [{steps}]
tasks:
  job: {{resource: m, service: s, payload: {{e: done}}}}
run: {{until: 100}}
"""
    _ranked(tmp_path, text, batch8.h8_limit(limits, times, n, service))


# ------------------------------------------------- Batch 8: the known cases that run now


def _not_in_known_cases(table):
    """The rows of a batch's table that `tests/known_cases` does not run: the rows with
    the prefixes EX-, GD-, ST- and P run there in all three APIs, from the same
    documents and programs, so this file runs the other prefixes."""
    return sorted(
        row
        for row in table
        if not row.startswith(("EX-", "GD-", "ST-")) and not re.match(r"P\d", row)
    )


KNOWN8 = {
    # row: (document of known8/, program, files of the document)
    "EX-2": ("ex_2", "EX_2", {}),
    "GD-RS1": ("gd_rs1", "GD_RS1", {}),
    "GD-RS6": (
        "gd_rs6",
        "GD_RS6",
        {"late.csv": "time,prio,can_preempt\n1,-2,0\n1,-1,1\n"},
    ),
    "GD-RS11": ("gd_rs11", "GD_RS11", {}),
    "ST-P08": ("st_p08", "ST_P08", {}),
    "ST-R01": ("st_r01", "ST_R01", {}),
    "ST-R14": ("st_r14", "ST_R14", {}),
    "ST-R15": ("st_r15", "ST_R15", {}),
    "ST-R26": ("st_r26", "ST_R26", {}),
    "ST-R29": ("st_r29", "ST_R29", {}),
    "P30": ("p30", "P30", {}),
    "P50b": ("p50b", "P50b", {}),
    "P76": ("p76", "P76", {}),
    "P77": ("p77", "P77", {}),
    "P79": ("p79", "P79", {}),
    "DS-H1": ("ds_h1", "DS_H1", {}),
    "DS-H5": ("ds_h5", "DS_H5", {}),
    "DS-H6": ("ds_h6", "DS_H6", {}),
    "DS-H8": ("ds_h8", "DS_H8", {}),
    "DS-H9": ("ds_h9", "DS_H9", {}),
    "DS-I10": ("ds_i10", "DS_I10", {}),
    "DS-M5": ("ds_m5", "DS_M5", {}),
    "DS-M7": ("ds_m7", "DS_M7", {}),
    "DS-Y4": ("ds_y4", "DS_Y4", {}),
    "DS-Y8": ("ds_y8", "DS_Y8", {}),
    "DS-Y12": ("ds_y12", "DS_Y12", {}),
    "CV-069": ("cv_069", "CV_069", {}),
}
KNOWN8_SHARED = {
    # row: the row whose document and program it runs in, the same form with the same
    # assertions, so a second case would repeat the first
    "P31": "P30",
    "CV-049": "DS-Y4",
    "CV-050": "DS-Y4",
    "CV-052": "DS-Y4",
    "CV-067": "DS-H1",
    "CV-068": "DS-H1",
    "CV-070": "DS-Y4",
    "CV-071": "DS-H1",
    "CV-073": "DS-Y8",
    "CV-077": "DS-Y8",
    "CV-079": "DS-M5",
    "CV-080": "DS-M5",
    "CV-081": "DS-H8",
    "CV-082": "DS-Y12",
    "CV-089": "DS-Y4",
    "CV-123": "DS-Y4",
}
_KEEP_STATUS = ("interrupted", "preempted", "reneged", "balked", "failed")


def _known8_records(records, grants=False):
    """The event records of a run as the programs log them: (time, key, task, value).
    The engine's own `started`, `queued` and `resumed` records are left out unless the
    program logs them (`grants`), and a record that keeps a status loses `path_id`."""
    found = []
    for record in records:
        if record["stream_type"] != "event":
            continue
        value = record["value"]
        if isinstance(value, dict) and "status" in value:
            if value["status"] in _KEEP_STATUS:
                value = {k: v for k, v in value.items() if k != "path_id"}
            elif set(value) <= {"status", "path_id", "reason"}:
                if not grants:
                    continue
                value = {k: v for k, v in value.items() if k != "path_id"}
        found.append((round(record["sim_ts"], 3), record["key"], record["task"], value))
    return found


def _known_program(module, name, asserts):
    """The program `name` of `module`; without `asserts`, the same source with every
    `assert` removed, because the asserts state the original's own scenario and a run
    of another size is a different one."""
    if asserts:
        return getattr(module, name)
    import ast
    import types

    path = Path(module.__file__)
    tree = ast.parse(path.read_text())

    class _NoAssert(ast.NodeTransformer):
        def visit_Assert(self, node):  # noqa: N802
            return ast.copy_location(ast.Pass(), node)

    tree = ast.fix_missing_locations(_NoAssert().visit(tree))
    plain = types.ModuleType(f"{module.__name__}_without_asserts")
    exec(compile(tree, str(path), "exec"), plain.__dict__)  # noqa: S102
    plain.PROCS = known8.PROCS
    plain.MODE = known8.MODE
    plain.PENDING = known8.PENDING
    return getattr(plain, name)


def _known_sized(module, table, row):
    """Whether the row's program takes a size: it has a `ties` form, or it starts its
    entities with `arrival(initial=...)`."""
    import inspect

    function = getattr(module, table[row][1])
    return (
        bool(getattr(function, "ties", False))
        or function.__name__ in getattr(module, "SIZED", ())
        or "initial=" in inspect.getsource(function)
    )


def _known_run(module, table, folder, tmp_path, row, size=None):
    import re

    document, program, files = table[row]
    text = _known_document(folder, document).read_text()
    for name, content in files.items():
        (tmp_path / name).write_text(content)
    if size is not None:
        text = re.sub(r"initial: \d+", f"initial: {size}", text, count=1)
    function = _known_program(module, program, size is None)
    ties = bool(getattr(function, "ties", False))
    known8.MODE.update(on=size is not None and not ties, n=size, rev=False)
    try:
        log, error = function(size, False) if ties and size else function()
    finally:
        known8.MODE.update(on=False, n=None, rev=False)
        known8.PENDING.clear()
    outcome = run_yaml_text(tmp_path, text, {})
    return outcome, log, error, bool(getattr(function, "grants", False))


def _known_compare(module, table, folder, tmp_path, row, size=None):
    outcome, log, error, grants = _known_run(module, table, folder, tmp_path, row, size)
    produced = _known8_records(outcome.records, grants)
    wanted = [(round(t, 3), k, task, v) for t, k, task, v in log]
    assert produced == wanted
    return outcome, error


def _known8_sized(row):
    return _known_sized(known8, KNOWN8, row)


def _known8_compare(tmp_path, row, size=None):
    return _known_compare(known8, KNOWN8, "known8", tmp_path, row, size)


@pytest.mark.parametrize("row", _not_in_known_cases(KNOWN8))
def test_known_case_of_batch_8_runs_as_its_simpy_program(tmp_path, row):
    """Each known case of Batch 8 that its keys allow now runs from its blueprint (no
    `!python`) and gives the records of the SimPy program written from the original:
    the same times, keys, tasks and payloads. A row with no document of its own runs
    the document of the row whose form it uses."""
    outcome, error = _known8_compare(tmp_path, row)
    assert (outcome.error is None) == (error is None), (outcome.error, error)


@pytest.mark.parametrize("size", [1, 3, 10])
@pytest.mark.parametrize(
    "row",
    [r for r in _not_in_known_cases(KNOWN8) if not r.startswith("CV-")],
)
def test_known_case_of_batch_8_runs_at_other_sizes(tmp_path, row, size):
    """As the base run, with `size` entities in the first arrival that starts a number
    of them at once, in the document and in the program (the rows whose program takes
    a size)."""
    if not _known8_sized(row):
        pytest.skip("the program has no arrival with `initial`")
    _known8_compare(tmp_path, row, size)


KNOWN8_DOCS = {
    # row: (the documentation snippet written with `!python`, the document without it)
    "DX-10": ("simpy_held_unit", "dx_10"),
    "DX-13": ("simpy_wait_again", "dx_13"),
    "DX-14": ("simpy_queue_again", "dx_14"),
    "DX-15": ("simpy_whole_flow", "dx_15"),
}


def _finished(records):
    """The (time, payload) of the finished events, without the statuses a plain
    process does not publish and the fields that name the engine's own parts."""
    skip = {"status", "path_id", "reason", "event_type"}
    return sorted(
        (
            round(record["sim_ts"], 3),
            json.dumps(
                {k: v for k, v in record["value"].items() if k not in skip},
                sort_keys=True,
            ),
        )
        for record in records
        if record["stream_type"] == "event"
        and record["value"].get("status")
        not in ("queued", "started", "interrupted", "resumed")
        and set(record["value"]) - skip
    )


@pytest.mark.parametrize("row", sorted(KNOWN8_DOCS))
def test_known_case_of_batch_8_document_equals_the_docs_snippet(tmp_path, row):
    """DX-10, DX-13, DX-14 and DX-15: the document without `!python` publishes the
    finished events of the documentation snippet that holds the same form in a SimPy
    process (`docs/snippets/yaml/simpy_forms.py`), at the same times."""
    snippet, document = KNOWN8_DOCS[row]
    docs = Path(__file__).parents[2] / "docs" / "snippets" / "yaml"
    original = tmp_path / "original"
    original.mkdir()
    (original / "simpy_forms.py").write_text((docs / "simpy_forms.py").read_text())
    text = (docs / f"{snippet}.yaml").read_text()
    text = text.replace("egress:\n  - type: Console\n", "")
    wanted = run_yaml_text(original, text, {})
    assert wanted.error is None, repr(wanted.error)
    own = tmp_path / "own"
    own.mkdir()
    got = run_yaml_text(own, _known_document("known8", document).read_text(), {})
    assert got.error is None, repr(got.error)
    assert _finished(got.records) == _finished(wanted.records)
    assert _finished(got.records)


BATCH_8_ROWS = (
    "EX-2 GD-RS1 GD-RS6 GD-RS11 ST-P08 ST-R01 ST-R14 ST-R15 ST-R26 ST-R29 P30 P31 "
    "P50b P76 P77 P79 DS-H1 DS-H5 DS-H6 DS-H8 DS-H9 DS-I10 DS-M5 DS-M7 DS-Y4 DS-Y8 "
    "DS-Y12 CV-049 CV-050 CV-052 CV-067 CV-068 CV-069 CV-070 CV-071 CV-073 CV-077 "
    "CV-079 CV-080 CV-081 CV-082 CV-089 CV-123 DX-10 DX-13 DX-14 DX-15"
).split()


def test_every_known_case_of_batch_8_runs_in_a_document_that_a_case_runs():
    """The 47 rows of the batch are each a case, or share the document of a case that
    has the same form and assertions."""
    assert len(BATCH_8_ROWS) == 47
    assert set(KNOWN8_SHARED.values()) <= set(KNOWN8)
    assert not set(KNOWN8_SHARED) & set(KNOWN8)
    assert sorted({*KNOWN8, *KNOWN8_SHARED, *KNOWN8_DOCS}) == sorted(BATCH_8_ROWS)


# ------------------------------------------------- Batch 9: the known cases that run now

KNOWN9: Dict[str, Tuple[str, str, Dict[str, str]]] = {
    # row: (document of known9/, program, files of the document)
    "EX-3": ("ex_3", "EX_3", {}),
    "GD-TT2": ("gd_tt2", "GD_TT2", {}),
    "ST-R10": ("st_r10", "ST_R10", {}),
    "ST-X01": ("st_x01", "ST_X01", {}),
    "P39": ("p39", "P39", {}),
    "P42": ("p42", "P42", {}),
    "P65": ("p65", "P65", {}),
    "P74": ("p74", "P74", {}),
    "DS-I3": ("ds_i3", "DS_I3", {}),
    "DS-I4": ("ds_i4", "DS_I4", {}),
    "DS-I6": ("ds_i6", "DS_I6", {}),
    "CV-110": ("ds_m3", "DS_M3", {}),
}
KNOWN9_SHARED = {
    # row: the row whose document and program it runs in, the same form with the same
    # assertions, so a second case would repeat the first
    "ST-I02": "GD-TT2",
    "CV-054": "DS-I3",
}
# row: the key of a later batch that its blueprint needs, so it does not run yet
KNOWN9_WAITING = {
    "BT-LD12": "wait: {each} (Batch 11), and Postgres ingress and egress",
}
BATCH_9_ROWS = (
    "EX-3 GD-TT2 ST-I02 ST-R10 ST-X01 P39 P42 P65 P74 DS-I3 DS-I4 DS-I6 CV-054 CV-110 "
    "BT-LD12"
).split()


@pytest.mark.parametrize("row", _not_in_known_cases(KNOWN9))
def test_known_case_of_batch_9_runs_as_its_simpy_program(tmp_path, row):
    """Each known case of Batch 9 that its keys allow now runs from its blueprint (no
    `!python`) and gives the records of the SimPy program written from the original:
    the same times, keys, tasks and payloads."""
    outcome, error = _known_compare(known9, KNOWN9, "known9", tmp_path, row)
    assert (outcome.error is None) == (error is None), (outcome.error, error)


@pytest.mark.parametrize("size", [1, 3, 10])
@pytest.mark.parametrize("row", _not_in_known_cases(KNOWN9))
def test_known_case_of_batch_9_runs_at_other_sizes(tmp_path, row, size):
    """As the base run, with `size` entities in the first arrival that starts a number
    of them at once, in the document and in the program (the rows whose program takes
    a size)."""
    if not _known_sized(known9, KNOWN9, row):
        pytest.skip("the program has no arrival with `initial`")
    _known_compare(known9, KNOWN9, "known9", tmp_path, row, size)


def test_every_known_case_of_batch_9_runs_or_names_the_key_it_waits_for():
    """The 15 rows of the batch are each a case, share the document of a case that has
    the same form and assertions, or wait for a later batch's key."""
    assert len(BATCH_9_ROWS) == 15
    assert set(KNOWN9_SHARED.values()) <= set(KNOWN9)
    assert not set(KNOWN9_SHARED) & set(KNOWN9)
    assert sorted({*KNOWN9, *KNOWN9_SHARED, *KNOWN9_WAITING}) == sorted(BATCH_9_ROWS)


# ------------------------------------------------- Batch 9: interrupts of holders

INTERRUPT_TEXT = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {m: {current_cap: CAP}}
arrivals:
  holder: {replay: {path: holder.csv}, spawn: hold}
  s: {replay: {path: s.csv}, spawn: subject}
  behind: {replay: {path: behind.csv}, spawn: behind}
  cut: {replay: {path: cut.csv}, spawn: cutter}
  probe: {replay: {path: probe.csv}, spawn: probe}
tasks:
  hold: {resource: m, service_time: 10, payload: {e: held}}
  subject: SUBJECT
  cleanup: {payload: {e: cleanup}, release: m}
  behind: {resource: m, service_time: 1, payload: {e: behind}}
  cutter: CUTTER
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 500}
"""
HOLDER_FORMS = {
    "seize": "{seize: m, service_time: SERVICE, on_interrupt: POLICY, NEXT payload: {e: s_done}, release: m}",
    "resource": "{resource: m, service_time: SERVICE, on_interrupt: POLICY, NEXT payload: {e: s_done}}",
    "wait request": "{wait: {request: m}, service_time: SERVICE, on_interrupt: POLICY, NEXT payload: {e: s_done}}",
}


def _interrupt_text(form, policy, part):
    subject = HOLDER_FORMS[form].replace("POLICY", policy)
    subject = subject.replace(
        "NEXT", "on_interrupt_next: cleanup," if policy == "abandon" else ""
    )
    subject = subject.replace("SERVICE", "5" if part == "held" else "2")
    cut = {"queued": 3, "held": 2}.get(part)
    cutter = (
        "{wait: {timeout: 5}, payload: null, next: cutter2}\n"
        "  cutter2: {wait: {timeout: 5}, payload: null, next: cutter3}\n"
        "  cutter3: {wait: {timeout: 0}, payload: {e: cut}, interrupt: {task: subject}}"
        if part == "instant"
        else f"{{wait: {{timeout: {cut}}}, payload: {{e: cut}}, interrupt: {{task: subject}}}}"
    )
    return (
        INTERRUPT_TEXT.replace("SUBJECT", subject)
        .replace("CUTTER", cutter)
        .replace("CAP", "N" if part == "held" else "1")
    )


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("policy", ["resume", "restart", "abandon"])
@pytest.mark.parametrize("part", ["queued", "held", "instant"])
@pytest.mark.parametrize("form", sorted(HOLDER_FORMS))
def test_unit_states_interrupt(tmp_path, n, policy, part, form):
    """Unit states, Interrupt and Grant at the instant, in each form that holds a
    unit (`seize`, `resource`, a wait `request`). Queued: `resume` keeps the place,
    `restart` asks again at the back, `abandon` leaves the line. Held: the unit stays
    with the flow under `resume` and `restart`, and `abandon` frees it through the
    task that follows. At the instant the grant is made: the unit is held, `resume`
    goes on, `restart` keeps it, `abandon` frees it. A `resource:` task that is still
    queued asks again at the back under `resume` too, as it did before Batch 9."""
    csvs = {
        "holder": [] if part == "held" else [0],
        "s": [0] * n,
        "behind": [1] if part in ("queued", "instant") else [],
        "cut": [0],
        "probe": [30],
    }
    text = _interrupt_text(form, policy, part).replace("CAP: N", f"CAP: {n}")
    text = text.replace("current_cap: N", f"current_cap: {n}")
    expected = batch9.unit_interrupt(n, policy, part, back=form == "resource")
    _runs(tmp_path, text, csvs, expected)


I3_TEXT = """
simulation: {sim_id: I3, factor: 0, random_seed: 42}
arrivals:
  w: {replay: {path: w.csv}, spawn: work}
  c1: {replay: {path: c.csv}, spawn: cut1}
  c2: {replay: {path: c.csv}, spawn: cut2}
tasks:
  work: {service_time: 10, on_interrupt: POLICY, on_interrupt_run: repair, payload: {e: done}}
  repair: {service_time: 5, payload: {e: repaired}}
  cut1: {wait: {timeout: 2}, payload: null, interrupt: {task: work}}
  cut2: {wait: {timeout: 4}, payload: null, interrupt: {task: work}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("policy", ["resume", "restart"])
def test_i3_a_flow_runs_after_an_interrupt_and_then_the_work_goes_on(
    tmp_path, n, policy
):
    """I3 `on_interrupt_run`: the named task runs as a child flow of the same entity
    when the interrupt arrives, and the work goes on after it with the time left
    (`resume`) or from the start (`restart`). An interrupt that arrives while the child
    runs reaches the child, as a `yield from` of the repair in the same process does."""
    _runs(
        tmp_path,
        I3_TEXT.replace("POLICY", policy),
        {"w": [0] * n, "c": [0]},
        batch9.i3_interrupt_run(n, policy),
    )


I4_TEXT = """
simulation: {sim_id: I4, factor: 0, random_seed: 42}
arrivals:
  s: {replay: {path: s.csv}, spawn: park}
  t: {replay: {path: t.csv}, spawn: other}
  r: {replay: {path: r.csv}, spawn: wake}
tasks:
  park: {wait: {suspend: true}, payload: {e: woke, x: {gen: expr, expr: "wait.suspend.value"}}}
  other: {wait: {suspend: true}, payload: {e: woke, x: {gen: expr, expr: "wait.suspend.value"}}}
  wake: {payload: null, resume: {task: park, value: {gen: sequence}, limit: {gen: expr, expr: "[1, 0, 100][v]", names: {v: {gen: sequence}}}}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
def test_i4_suspend_waits_for_a_resume_that_names_the_task(tmp_path, n):
    """I4 `suspend` and `resume`: instances of the task wait until another flow resumes
    them, oldest first; `limit` caps how many, `limit: 0` wakes none, no `limit` wakes
    all; `value` is evaluated once for each resume, whether it wakes anyone or not,
    and the woken instance reads it as `wait.suspend.value`."""
    csvs = {"s": [0] * n, "t": [1], "r": [2, 4, 6]}
    _runs(tmp_path, I4_TEXT, csvs, batch9.i4_resume(n))


I6_TEXT = """
simulation: {sim_id: I6, factor: 0, random_seed: 42}
resources: {m: {current_cap: 1}}
arrivals:
  holder: {replay: {path: holder.csv}, spawn: hold}
  l: {replay: {path: l.csv}, spawn: late}
tasks:
  hold: {resource: m, service_time: 5, payload: null}
  late: {balk: "queue.m >= 2", on_balk: elsewhere, patience: 2, on_renege: elsewhere, resource: m, service_time: 0, payload: {e: served}}
  elsewhere: {payload: {e: elsewhere, x: {gen: expr, expr: "upstream.status"}}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
def test_i6_a_flow_goes_on_after_it_balks_or_reneges(tmp_path, n):
    """I6 `on_renege` and `on_balk`: a flow that leaves the line because its patience
    ends, or because the line is too long, goes on to the named task, which reads
    `upstream.status` as `reneged` or `balked`."""
    _runs(
        tmp_path,
        I6_TEXT,
        {"holder": [0], "l": [1] * n},
        batch9.i6_leave(n),
    )


M3_TEXTS = {
    "step": """
simulation: {sim_id: M3, factor: 0, random_seed: 42}
arrivals:
  a: {replay: {path: a.csv}, spawn: t}
tasks:
  t: {service_time: {gen: expr, expr: "1 / 0"}, on_fail: handle, payload: {e: fine}}
  handle: {payload: {e: handled, x: {gen: expr, expr: "upstream.status"}}}
run: {until: 500}
""",
    "wait": """
simulation: {sim_id: M3, factor: 0, random_seed: 42}
signals: {may_fail: {once: true}}
arrivals:
  a: {replay: {path: a.csv}, spawn: t}
  b: {replay: {path: b.csv}, spawn: breaker}
tasks:
  t: {wait: {signal: may_fail}, on_fail: handle, payload: {e: fine}}
  handle: {payload: {e: handled, x: {gen: expr, expr: "upstream.reason"}}}
  breaker: {wait: {timeout: 1}, payload: null, fail: "True", fail_reason: event, signal: may_fail, on_fail: broke}
  broke: {payload: {e: broke}}
run: {until: 500}
""",
}


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("part", ["step", "wait"])
def test_m3_a_failed_flow_goes_on_when_on_fail_names_a_task(tmp_path, n, part):
    """M3 `on_fail` with no `fail`: a flow whose own expression fails, or whose wait
    fails, goes on to the named task, which reads `upstream.reason`."""
    for name, moments in {"a": [0] * n, "b": [0]}.items():
        _csv(tmp_path, name, ["time"], [(x,) for x in moments])
    expected = batch9.m3_failure(n, part)
    if part == "wait":
        expected = [(1, "b-1", "failed", None), (1, "b-1", "broke", None)] + expected
    _same(tmp_path, M3_TEXTS[part], expected, ("failed", "reneged", "balked"))


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("form", ["seize", "wait request"])
def test_a_request_asked_again_under_restart_and_interrupted_goes_to_the_back(
    tmp_path, n, form
):
    """Engine fix 7: a `seize` (or a wait `request`) that restart took out of the line
    and asked again goes to the back again when a second interrupt arrives, behind the
    flow that asked meanwhile. The interrupts are at 3 and 5, and flows ask at 1 and 4."""
    text = _interrupt_text(form, "restart", "queued")
    text = text.replace(
        "  probe: {replay",
        "  behind2: {replay: {path: behind2.csv}, spawn: behind2}\n"
        "  cut2: {replay: {path: cut2.csv}, spawn: cutter2}\n  probe: {replay",
    ).replace(
        "  probe: {payload",
        "  behind2: {resource: m, service_time: 1, payload: {e: behind}}\n"
        "  cutter2: {wait: {timeout: 5}, payload: {e: cut}, interrupt: {task: subject}}\n"
        "  probe: {payload",
    )
    csvs = {
        "holder": [0],
        "s": [0] * n,
        "behind": [1],
        "behind2": [4],
        "cut": [0],
        "cut2": [0],
        "probe": [30],
    }
    _runs(tmp_path, text, csvs, batch9.unit_interrupt(n, "restart", "again"))


SUSPEND_TEXT = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
arrivals:
  s: {replay: {path: s.csv}, spawn: park, attributes: {late: 0}}
  late: {replay: {path: late.csv}, spawn: park, attributes: {late: 1}}
  cut: {replay: {path: cut.csv}, spawn: cutter}
  wake: {replay: {path: wake.csv}, spawn: waker}
tasks:
  park: {wait: {suspend: true}, on_interrupt: POLICY, NEXT payload: {e: woke}}
  cleanup: {payload: {e: cleanup}}
  cutter: {wait: {timeout: 2}, payload: null, interrupt: {task: park, where: "attr.late == 0"}}
  waker: {wait: {timeout: 6}, payload: null, resume: {task: park}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("policy", ["resume", "restart", "abandon"])
@pytest.mark.parametrize("wakes", [True, False])
def test_an_interrupt_of_a_suspended_instance_follows_on_interrupt(
    tmp_path, n, policy, wakes
):
    """Engine fix 11: an interrupt of a suspended instance follows `on_interrupt`.
    `resume` keeps it suspended in its place, `restart` suspends it again at the back
    of the order, and `abandon` ends it, so a later `resume` wakes the rest in order."""
    text = SUSPEND_TEXT.replace("POLICY", policy).replace(
        "NEXT", "on_interrupt_next: cleanup," if policy == "abandon" else ""
    )
    csvs = {"s": [0] * n, "late": [1], "cut": [0], "wake": [0] if wakes else []}
    _runs(
        tmp_path,
        text,
        csvs,
        batch9.suspended_interrupt(n, policy, 6 if wakes else None),
    )


@pytest.mark.parametrize("extra", ["breakdown", "release"])
@pytest.mark.parametrize("policy", ["resume", "restart", "abandon"])
@pytest.mark.parametrize("part", ["queued", "held"])
@pytest.mark.parametrize("form", sorted(HOLDER_FORMS))
def test_an_interrupted_holder_leaves_no_unit_or_request_with_a_breakdown_or_a_release(
    tmp_path, form, part, policy, extra
):
    """Every form that holds a unit ends with the unit given back and the line empty
    when an interrupt meets a breakdown of the resource (at 6, repaired at 8) or a unit
    that another flow freed with `release` (at 1). Each of the three flows ends, by its
    work or by its cleanup."""
    n = 3
    text = _interrupt_text(form, policy, part).replace(
        "current_cap: N", f"current_cap: {n}"
    )
    csvs = {
        "holder": [] if part == "held" else [0],
        "s": [0] * n,
        "behind": [],
        "cut": [0],
        "probe": [60],
    }
    if extra == "breakdown":
        text = text.replace(
            "m: {current_cap: 1}",
            "m: {current_cap: 1, breakdown: {time_to_failure: {dist: constant, value: 6}, repair: {dist: constant, value: 2}}}",
        ).replace(
            f"m: {{current_cap: {n}}}",
            f"m: {{current_cap: {n}, breakdown: {{time_to_failure: {{dist: constant, value: 6}}, repair: {{dist: constant, value: 2}}}}}}",
        )
    else:
        text = text.replace(
            "  probe: {replay",
            "  rel: {replay: {path: rel.csv}, spawn: freer}\n  probe: {replay",
        ).replace(
            "  probe: {payload",
            "  freer: {wait: {timeout: 1}, payload: null, release: {resource: m, task: subject}}\n  probe: {payload",
        )
        csvs["rel"] = [0]
    for name, moments in csvs.items():
        _csv(tmp_path, name, ["time"], [(t,) for t in moments])
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    marks = _trace(outcome.records)
    went_down = any(
        r["stream_type"] == "event" and r["value"].get("status") == "down"
        for r in outcome.records
    )
    assert went_down == (extra == "breakdown")
    assert [m[3] for m in marks if m[2] == "end"] == [[0, 0]], marks
    for index in range(1, n + 1):
        ended = [
            m[2]
            for m in marks
            if m[1] == f"s-{index}" and m[2] in ("s_done", "cleanup")
        ]
        assert len(ended) == 1, (index, marks)


# ------------------------------------------------ Batch 10: the known cases that run now

KNOWN10: Dict[str, Tuple[str, str, Dict[str, str]]] = {
    # row: (document of known10/, program, files of the document)
    "DS-B1": ("ds_b1", "DS_B1", {}),
    "P49": ("p49", "P49", {}),
    "P50": ("p50", "P50", {}),
    "DS-B3": ("ds_b3", "DS_B3", {}),
    "P52": ("p52", "P52", {}),
    "P53": ("p53", "P53", {}),
    "DS-M1": ("ds_m1", "DS_M1", {}),
    "GD-M2": ("gd_m2", "GD_M2", {}),
    "GD-M3": ("gd_m3", "GD_M3", {}),
    "GD-M4": ("gd_m4", "GD_M4", {}),
}
KNOWN10_SHARED = {
    # row: the row whose document and program it runs in, the same form with the same
    # assertions
    "CV-043": "DS-M1",
}
# row: the test that runs it, where it is not a SimPy program
KNOWN10_OTHER = {
    "DX-7": "tests/unit/test_blueprint.py::test_readings_match_the_docs_packers_report",
}
BATCH_10_ROWS = (
    "GD-M2 GD-M3 GD-M4 P49 P50 P52 P53 DS-B1 DS-B3 DS-M1 CV-043 DX-7"
).split()


@pytest.mark.parametrize("row", _not_in_known_cases(KNOWN10))
def test_known_case_of_batch_10_runs_as_its_simpy_program(tmp_path, row):
    """Each known case of Batch 10 runs from its blueprint (no `!python`) and gives the
    records of the SimPy program written from the original: the same times, keys, tasks
    and payloads."""
    outcome, error = _known_compare(known10, KNOWN10, "known10", tmp_path, row)
    assert (outcome.error is None) == (error is None), (outcome.error, error)


@pytest.mark.parametrize("size", [1, 3, 10])
@pytest.mark.parametrize("row", _not_in_known_cases(KNOWN10))
def test_known_case_of_batch_10_runs_at_other_sizes(tmp_path, row, size):
    """As the base run, with `size` entities in the first arrival that starts a number
    of them at once, in the document and in the program (the rows whose program takes
    a size)."""
    if not _known_sized(known10, KNOWN10, row):
        pytest.skip("the program has no arrival with `initial`")
    _known_compare(known10, KNOWN10, "known10", tmp_path, row, size)


def test_every_known_case_of_batch_10_runs_or_names_the_key_it_waits_for():
    """The 12 rows of the batch are each a case, share the document of a case that has
    the same form, or run in a test of their own."""
    assert len(BATCH_10_ROWS) == 12
    assert set(KNOWN10_SHARED.values()) <= set(KNOWN10)
    assert not set(KNOWN10_SHARED) & set(KNOWN10)
    assert not (set(KNOWN10_OTHER) & set(KNOWN10)) | (
        set(KNOWN10_OTHER) & set(KNOWN10_SHARED)
    )
    assert sorted({*KNOWN10, *KNOWN10_SHARED, *KNOWN10_OTHER}) == sorted(BATCH_10_ROWS)
    test, _, name = KNOWN10_OTHER["DX-7"].partition("::")
    assert f"def {name}(" in (Path(__file__).parents[2] / test).read_text()


def _reading_series(records, resource="res"):
    """The `in_use` and `queue_length` readings of a resource: (time, field, value)."""
    found = []
    for record in records:
        if record["stream_type"] != "telemetry":
            continue
        path = record["path_id"]
        for field in ("in_use", "queue_length"):
            if path.endswith(f".resources.{resource}.{field}"):
                found.append((round(record["sim_ts"], 3), field, record["value"]))
    return found


def _calls_as_readings(calls):
    """What the readings after each call give: the first call of an instant gives both
    values, and each later call only the one that changed."""
    found, last, previous = [], None, None
    for at, count, queued in calls:
        at = round(at, 3)
        if at != last:
            previous, last = None, at
        if previous is None or previous[0] != count:
            found.append((at, "in_use", count))
        if previous is None or previous[1] != queued:
            found.append((at, "queue_length", queued))
        previous = (count, queued)
    return found


def _run_with_trace(tmp_path, row):
    """Runs the document of a row with a sink that keeps the trace records."""
    from tests.conformance.harness import RecordingEgress

    text = _known_document("known10", KNOWN10[row][0]).read_text()
    sink = RecordingEgress()
    outcome = run_yaml_text(
        tmp_path, text, {}, prepare=lambda app: app.add_egress(sink, when="trace")
    )
    return outcome, sink.records


def _trace_events(records):
    return [
        (
            round(r["sim_ts"], 3),
            r["value"]["priority"],
            r["value"]["event"],
        )
        for r in records
        if r["stream_type"] == "trace"
    ]


@pytest.mark.parametrize("row", ["DS-M1"])
def test_on_change_publishes_the_reading_after_each_call_as_simpy_records_it(
    tmp_path, row
):
    """M1: a resource's `in_use` and `queue_length` are published after each `request`
    and `release` call, the series the patched SimPy resource records (a grant made
    when a release is processed publishes none, as no call is made)."""
    known10.TELE.clear()
    outcome, _ = _known_compare(known10, KNOWN10, "known10", tmp_path, row)
    wanted = _calls_as_readings(known10.TELE)
    assert wanted, row
    assert _reading_series(outcome.records) == wanted
    known10.TELE.clear()


@pytest.mark.parametrize("row", ["DS-M1"])
def test_trace_publishes_each_event_the_run_processes_as_simpy_steps_it(tmp_path, row):
    """M1, MF5: the trace gives one record per processed model event, with its time,
    priority and class, to the sinks that ask for `when: trace` only. The blueprint's
    arrival process is the one extra process SimPy's program starts."""
    known10.TRACE.clear()
    function = getattr(known10, KNOWN10[row][1])
    function()
    wanted = list(known10.TRACE)
    known10.TRACE.clear()
    known8.MODE.update(on=False, n=None, rev=False)
    outcome, records = _run_with_trace(tmp_path, row)
    assert outcome.error is None, repr(outcome.error)
    assert all(r["stream_type"] != "trace" for r in outcome.records)
    produced = _trace_events(records)
    assert [(t, p, c) for t, p, c in produced if c != "Event"] == [
        (round(t, 3), p, c) for t, p, c in wanted if c != "Event"
    ]


# ------------------------------------------------ Batch 11: computed sets and events


def _rows_i(tmp_path, n, name="a", at=0):
    """A replay of `n` entities at one instant, with the attribute `i` from 0."""
    _csv(tmp_path, name, ["time", "i"], [(at, i) for i in range(n)])


Y1_ROUND_ROBIN = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {desk: {family: true, members: 3, current_cap: 1}}
arrivals:
  a: {replay: {path: a.csv}, spawn: job}
tasks:
  job:
    resource: {of: desk, key: "attr.i % 3"}
    service_time: 2
    payload: {e: done, x: {gen: expr, expr: "[users.desk[0], queue.desk[0]]"}}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
def test_y1_members_made_from_a_number_serve_in_turn(tmp_path, n):
    """Y1 `members: 3`: three resources of one unit exist when the run starts, and the
    entity `i` uses the one at `i % 3`, as a list of `Resource` objects does."""
    _rows_i(tmp_path, n)
    _same(tmp_path, Y1_ROUND_ROBIN, batch11.round_robin(n))


@pytest.mark.parametrize("n", SIZES)
def test_y1_a_member_is_made_at_its_first_use_with_the_capacity_of_its_creator(
    tmp_path, n
):
    """Y1: no member exists at the start. The first entity of a key makes the member with
    a capacity read from its attributes, a second user of the key finds that capacity,
    and a read of a key that does not exist gives 0 and makes nothing."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {desk: {family: true, current_cap: "attr.i + 1"}}
arrivals:
  a: {replay: {path: a.csv}, spawn: job}
tasks:
  job:
    resource: {of: desk, key: "attr.i % 2"}
    service_time: 2
    payload: {e: done, x: {gen: expr, expr: "[users.desk[attr.i % 2], queue.desk[attr.i % 2], users.desk[7], members(desk)]"}}
run: {until: 500}
"""
    _same(tmp_path, text, batch11.per_entity(n))


@pytest.mark.parametrize("n", SIZES)
def test_y1_a_store_family_is_made_at_its_first_put(tmp_path, n):
    """Y1 stores: a store for each key, made at time 3, when the first entity puts."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
stores: {q: {family: true}}
arrivals:
  a: {replay: {path: a.csv}, spawn: wait3}
tasks:
  wait3: {wait: {timeout: 3}, payload: {e: begin}, next: put_it}
  put_it: {put: {store: {of: q, key: "attr.i % 2"}, item: {gen: expr, expr: "attr.i"}}, payload: {e: put}, next: wait10}
  wait10: {wait: {timeout: 10}, payload: {e: waited}, next: get_it}
  get_it: {get: {store: {of: q, key: "attr.i % 2"}}, payload: {e: got, x: {gen: expr, expr: "[item, sorted(members(q))]"}}}
run: {until: 500}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    found = [m for m in _trace(outcome.records) if m[2] == "got"]
    assert found == _expect(batch11.stores(n))


@pytest.mark.parametrize("n", SIZES)
def test_y1_a_container_family_starts_each_member_at_its_level(tmp_path, n):
    """Y1 containers: the level is text, read once when the member is made."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
containers: {c: {family: true, level: "10 * (attr.i + 1)", current_cap: 1000}}
arrivals:
  a: {replay: {path: a.csv}, spawn: take}
tasks:
  take: {get: {container: {of: c, key: "attr.i % 2"}, amount: 4}, payload: {e: took, x: {gen: expr, expr: "[level.c[attr.i % 2], members(c)]"}}}
run: {until: 500}
"""
    _same(tmp_path, text, batch11.containers(n))


@pytest.mark.parametrize("n", SIZES)
def test_y1_a_signal_family_wakes_the_waiter_of_its_key(tmp_path, n):
    """Y1 signals: a waiter resumes when the signal of its key fires, and a flow that
    waits after the firing resumes at once, as a triggered event does."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
signals: {reply: {family: true, once: true}}
arrivals:
  a: {replay: {path: a.csv}, spawn: wait_for}
  f: {replay: {path: f.csv}, spawn: fire}
  l: {replay: {path: l.csv}, spawn: later}
tasks:
  wait_for: {wait: {signal: {of: reply, key: "attr.i"}}, payload: {e: heard, x: {gen: expr, expr: "wait.reply.value.v"}}}
  fire: {wait: {timeout: 1}, payload: {v: {gen: expr, expr: "attr.i"}}, signal: {of: reply, key: "attr.i"}}
  later: {wait: {timeout: 2}, payload: {e: late_start}, next: late_wait}
  late_wait: {wait: {signal: {of: reply, key: "attr.i"}}, payload: {e: late, x: {gen: expr, expr: "wait.reply.value.v"}}}
run: {until: 500}
"""
    _rows_i(tmp_path, n, "f")
    _rows_i(tmp_path, n, "l")
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    got = sorted(
        (m[0], m[2], m[3]) for m in _trace(outcome.records) if m[2] in ("heard", "late")
    )
    expected = sorted((t, label, v) for t, _, label, v in _expect(batch11.signals(n)))
    assert got == expected


@pytest.mark.parametrize("n", SIZES)
def test_y2_the_member_with_the_lowest_value_is_taken_at_the_step(tmp_path, n):
    """Y2 `lowest`: the pick and the request are one step, so two entities at one instant
    pick in turn and the second sees the first's request."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {desk: {family: true, members: 2, current_cap: 1}}
arrivals:
  a: {replay: {path: a.csv}, spawn: job}
tasks:
  job:
    resource: {of: desk, lowest: "member.queue + member.users"}
    service_time: 2
    payload: {e: done, x: {gen: expr, expr: "0 if process.key in [h.entity for h in held(desk[0])] else 1"}}
run: {until: 500}
"""
    _same(tmp_path, text, batch11.lowest(n))


@pytest.mark.parametrize("n", SIZES)
def test_y2_a_list_of_names_takes_the_one_the_key_gives(tmp_path, n):
    """Y2 `of: [left, right]`: `key` gives one of the declared names."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {left: {current_cap: 1}, right: {current_cap: 1}}
arrivals:
  a: {replay: {path: a.csv}, spawn: job}
tasks:
  job:
    resource: {of: [left, right], key: "['left', 'right'][attr.i % 2]"}
    service_time: 2
    payload: {e: done, x: {gen: expr, expr: "['left', 'right'][0 if process.key in [h.entity for h in held(left)] else 1]"}}
run: {until: 500}
"""
    _same(tmp_path, text, batch11.by_name(n))


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize(
    "delays,group,wait_group",
    [
        ([3, 1, 2], "all", "all"),
        ([3, 1, 2], "any", "any"),
        ([1, 1, 2], "any", "any"),
        ([], "all", "all"),
        ([], "any", "any"),
        ([3, 1, 2], "at_least_2", "{at_least: 2}"),
        ([], "at_least_2", "{at_least: 2}"),
    ],
)
def test_y3_each_timeout_of_a_list_under_every_group(
    tmp_path, n, delays, group, wait_group
):
    """Y3: one timeout for each element of a list, under `all`, `any` and `at_least`, the
    ties of two timeouts at one instant, and an empty list that is done at once."""
    _rows_i(tmp_path, n)
    text = f"""
simulation: {{sim_id: S, factor: 0, random_seed: 42}}
arrivals:
  a: {{replay: {{path: a.csv}}, spawn: job}}
tasks:
  job:
    wait: {{each: {delays}, group: {wait_group}, branch: {{timeout: {{after: "each.value", value: {{gen: expr, expr: "each.value * 10"}}}}}}}}
    payload: {{e: done, x: {{gen: expr, expr: "[wait.each.values, wait.each.flags]"}}}}
run: {{until: 500}}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    assert _trace(outcome.records) == _expect(batch11.each_timeouts(n, delays, group))


@pytest.mark.parametrize("n", SIZES)
def test_y3_a_when_group_is_done_when_its_expression_holds(tmp_path, n):
    """Y3 `group: {when: ...}`: the wait is done when `done_count >= 2` holds, which for
    three timeouts is what `at_least: 2` gives."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
arrivals:
  a: {replay: {path: a.csv}, spawn: job}
tasks:
  job:
    wait: {each: [3, 1, 2], group: {when: "done_count >= 2"}, branch: {timeout: {after: "each.value", value: {gen: expr, expr: "each.value * 10"}}}}
    payload: {e: done, x: {gen: expr, expr: "[wait.each.values, wait.each.flags]"}}
run: {until: 500}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    assert _trace(outcome.records) == _expect(
        batch11.each_timeouts(n, [3, 1, 2], "at_least_2")
    )


@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("count", [0, 1, 3])
def test_y3_each_child_flow_of_a_number_returns_its_value(tmp_path, n, count):
    """Y3: a child flow for each index of a number, in order, each waited for under
    `all`; the values are in element order, with the arguments `each.index` gives."""
    _rows_i(tmp_path, n)
    text = f"""
simulation: {{sim_id: S, factor: 0, random_seed: 42}}
arrivals:
  a: {{replay: {{path: a.csv}}, spawn: job}}
tasks:
  job:
    wait: {{each: {count}, branch: {{task: child, args: {{j: {{gen: expr, expr: each.index}}}}}}}}
    payload: {{e: done, x: {{gen: expr, expr: "wait.each.values"}}}}
  child:
    wait: {{timeout: "{count} - arg.j"}}
    payload: {{r: {{gen: expr, expr: "arg.j * 2"}}}}
run: {{until: 500}}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    found = _trace(outcome.records)
    expected = _expect(batch11.each_children(n, count))
    assert [(m[0], m[1], m[2]) for m in found] == [(t, k, e) for t, k, e, _ in expected]
    assert [[v["r"] for v in m[3]] for m in found] == [x for _, _, _, x in expected]


@pytest.mark.parametrize("n", SIZES)
def test_y3_each_request_of_a_computed_number_keeps_the_first_unit(tmp_path, n):
    """Y3: a request for each of three resources under `any`: the first granted is
    kept to the task's end, and the others leave their queues, as in SimPy."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {r: {family: true, members: 3, current_cap: 1}}
arrivals:
  h: {replay: {path: h.csv}, spawn: hold}
  a: {replay: {path: a.csv}, spawn: wait_half}
tasks:
  hold: {resource: {of: r, key: "attr.i"}, service_time: "3 - attr.i", payload: {e: held}}
  wait_half: {wait: {timeout: 0.5}, payload: {e: half}, next: job}
  job:
    wait: {each: 3, group: any, branch: {request: {of: r, key: each.index}}}
    payload: {e: done, x: {gen: expr, expr: "[wait.each.flags, [queue.r[0], queue.r[1], queue.r[2]]]"}}
run: {until: 500}
"""
    _csv(tmp_path, "h", ["time", "i"], [(0, j) for j in range(3)])
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    found = [m for m in _trace(outcome.records) if m[2] == "done"]
    expected = _expect(batch11.each_requests(n, 3))
    assert [(m[0], m[1], m[3]) for m in found] == [(t, k, x) for t, k, _, x in expected]


@pytest.mark.parametrize("n", SIZES)
def test_y4_an_opened_each_of_requests_is_named_one_by_one_by_its_index(tmp_path, n):
    """Y4 with Y3's `each`: a request for each of three members is opened under one name,
    a wait names the last by `index`, `cancel` of the name cancels each in order and
    leaves the granted one, `handle.pool.flags` reads which are done, and `release`
    names one unit by `index`."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
resources: {r: {family: true, members: 3, current_cap: 1}}
arrivals:
  h: {replay: {path: h.csv}, spawn: hold}
  a: {replay: {path: a.csv}, spawn: begin}
tasks:
  hold: {resource: {of: r, key: "attr.i"}, service_time: "3 - attr.i", payload: {e: held}}
  begin: {wait: {timeout: 0.5}, payload: {e: half}, next: opener}
  opener: {open: {pool: {each: 3, branch: {request: {of: r, key: each.index}}}}, payload: {e: opened}, next: waiter}
  waiter: {wait: {handle: pool, index: 2}, payload: {e: got}, next: closer}
  closer:
    cancel: {handle: pool}
    payload: {e: done, x: {gen: expr, expr: "[handle.pool.flags, [queue.r[0], queue.r[1], queue.r[2]]]"}}
    release: {handle: pool, index: 2}
run: {until: 500}
"""
    _csv(tmp_path, "h", ["time", "i"], [(0, j) for j in range(3)])
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    found = [m for m in _trace(outcome.records) if m[2] == "done"]
    expected = _expect(batch11.open_requests(n))
    assert [(m[0], m[1], m[3]) for m in found] == [(t, k, x) for t, k, _, x in expected]


# --------------------------------------------- Batch 11: the known cases that run now

KNOWN11: Dict[str, Tuple[str, str, Dict[str, str]]] = {
    # row: (document of known11/, program, files of the document)
    "DS-Y1": ("ds_y1", "DS_Y1", {}),
    "DS-Y2": ("ds_y2", "DS_Y2", {}),
    "DS-Y3": ("ds_y3", "DS_Y3", {}),
    "ST-U07": ("st_u07", "ST_U07", {}),
    "ST-U08": ("st_u08", "ST_U08", {}),
    "ST-U09": ("st_u09", "ST_U09", {}),
    "P64": ("p64", "P64", {}),
    "CV-074": ("cv_074", "CV_074", {}),
    "CV-083": ("cv_083", "CV_083", {}),
    "EX-7b": ("ex_7b", "EX_7b", {}),
    "EX-4": ("ex_4", "EX_4", {}),
    "GD-V2": ("gd_v2", "GD_V2", {}),
    "ST-C06": ("st_c06", "ST_C06", {}),
    "ST-C09": ("st_c09", "ST_C09", {}),
    "ST-U03": ("st_u03", "ST_U03", {}),
    "ST-V02": ("st_v02", "ST_V02", {}),
    "ST-X03": ("st_x03", "ST_X03", {}),
    "ST-X04": ("st_x04", "ST_X04", {}),
    "ST-X05": ("st_x05", "ST_X05", {}),
    "P78": ("p78", "P78", {}),
    "DS-M3": ("ds_m3", "DS_M3", {}),
    "DS-Y9": ("ds_y9", "DS_Y9", {}),
    "DS-Y10": ("ds_y10", "DS_Y10", {}),
    "DS-Y11": ("ds_y11", "DS_Y11", {}),
    "CV-014": ("cv_014", "CV_014", {}),
    "CV-112": ("cv_112", "CV_112", {}),
}
KNOWN11_SHARED = {
    # row: the row whose document and program it runs in, the same form with the same
    # assertions
    "CV-121": "DS-Y1",
    "CV-122": "DS-Y1",
    "CV-125": "DS-Y1",
    "CV-129": "DS-Y2",
    "CV-022": "DS-Y3",
    "CV-118": "DS-Y3",
    "CV-120": "DS-Y3",
    "ST-C07": "ST-C06",
    "ST-C08": "ST-C06",
    "CV-006": "DS-Y9",
    "CV-009": "DS-Y9",
    "CV-010": "DS-Y9",
    "CV-012": "DS-Y11",
    "CV-013": "DS-Y11",
    "CV-031": "DS-Y10",
}
# row: the test that runs it, where it is not a document of known11/ with a SimPy program
KNOWN11_OTHER = {
    "CV-124": "tests/conformance/test_yaml_only.py::test_y1_a_member_is_made_at_its_first_use_with_the_capacity_of_its_creator",
    "CV-126": "tests/conformance/test_yaml_only.py::test_y1_a_member_is_made_at_its_first_use_with_the_capacity_of_its_creator",
    "CV-134": "tests/conformance/test_yaml_only.py::test_y4_an_opened_each_of_requests_is_named_one_by_one_by_its_index",
    "BT-AQ6": "tests/benchtop/test_each.py::test_each_hour_has_seven_forecasts_and_a_reading",
    "BT-AQ7": "tests/benchtop/test_each.py::test_each_hour_has_seven_forecasts_and_a_reading",
    "BT-LD9": "tests/benchtop/test_each.py::test_an_order_has_one_row_for_each_item_and_each_item_is_a_different_product",
    "BT-LD10": "tests/benchtop/test_each.py::test_an_order_has_one_row_for_each_item_and_each_item_is_a_different_product",
    "BT-LD12": "tests/benchtop/test_each.py::test_an_order_no_picker_takes_in_time_is_cancelled_with_each_of_its_items",
    "BT-WL4": "tests/benchtop/test_each.py::test_a_request_is_made_for_each_serving_service",
    "DX-1": "tests/unit/test_families.py::test_an_order_of_one_to_five_items_with_a_total_needs_no_python",
}
# The 50 rows of the Batch 11 design, and BT-LD12 (a part-1 row outside the 50). The rows
# from GD-V2 on use fire.fail, defused, fail_reason, fire.priority, listen or `when`.
BATCH_11_ROWS = (
    "EX-4 EX-7b ST-U07 ST-U08 ST-U09 P64 DS-Y1 DS-Y2 DS-Y3 CV-022 CV-074 CV-083 CV-118 "
    "CV-120 CV-121 CV-122 CV-124 CV-125 CV-126 CV-129 CV-134 BT-AQ6 BT-AQ7 BT-LD9 "
    "BT-LD10 BT-WL4 DX-1 BT-LD12 GD-V2 ST-C06 ST-C07 ST-C08 ST-C09 ST-U03 ST-V02 "
    "ST-X03 ST-X04 ST-X05 P78 DS-M3 DS-Y9 DS-Y10 DS-Y11 CV-006 CV-009 CV-010 CV-012 "
    "CV-013 CV-014 CV-031 CV-112"
).split()


@pytest.mark.parametrize("row", _not_in_known_cases(KNOWN11))
def test_known_case_of_batch_11_runs_as_its_simpy_program(tmp_path, row):
    """Each known case of Batch 11 that its keys allow runs from its blueprint (no
    `!python`) and gives the records of the SimPy program written from the original:
    the same times, keys, tasks and payloads."""
    outcome, error = _known_compare(known11, KNOWN11, "known11", tmp_path, row)
    assert (outcome.error is None) == (error is None), (outcome.error, error)


@pytest.mark.parametrize("size", [1, 3, 10])
@pytest.mark.parametrize("row", _not_in_known_cases(KNOWN11))
def test_known_case_of_batch_11_runs_at_other_sizes(tmp_path, row, size):
    """As the base run, with `size` entities in the first arrival that starts a number
    of them at once, in the document and in the program (the rows whose program takes
    a size)."""
    if not _known_sized(known11, KNOWN11, row):
        pytest.skip("the program has no arrival with `initial`")
    _known_compare(known11, KNOWN11, "known11", tmp_path, row, size)


Y9_WAITERS = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
signals: {ev: {once: true}}
arrivals:
  a: {replay: {path: a.csv}, spawn: w}
  d: {initial: 1, spawn: drive}
tasks:
  w: {wait: {signal: ev}, on_fail: caught, payload: {e: fine}}
  caught: {payload: {e: caught, x: {gen: expr, expr: "upstream.reason"}}}
  drive: {wait: {timeout: 1}, payload: null, on: [{fire: {signal: ev, fail: true, value: boom}}]}
run: {until: 500}
"""


@pytest.mark.parametrize("n", SIZES)
def test_y9_a_failed_signal_fails_each_waiter_with_its_reason(tmp_path, n):
    """Y9 `fire.fail`: the waiters of a one-shot signal fail with the reason, and each
    catches it with `on_fail`, as the waiters of a failed event do."""
    _rows_i(tmp_path, n)
    outcome = run_yaml_text(tmp_path, Y9_WAITERS, {})
    assert outcome.error is None, repr(outcome.error)
    got = [m for m in _trace(outcome.records) if m[2] in ("caught", "fine")]
    assert got == _expect(batch11.y9_waiters_fail(n))


@pytest.mark.parametrize("n", SIZES)
def test_y9_a_computed_fail_reason_is_read_when_the_task_fails(tmp_path, n):
    """Y9 `fail_reason` as a generator: it is read for each failure, with the entity's
    attributes, so each entity's reason is its own."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
arrivals:
  a: {replay: {path: a.csv}, spawn: w}
tasks:
  w: {wait: {timeout: 1}, fail: "True", fail_reason: {gen: expr, expr: "'bad-' + str(attr.i)"}, on_fail: caught, payload: {e: unreachable}}
  caught: {payload: {e: caught, x: {gen: expr, expr: "upstream.reason"}}}
run: {until: 500}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    got = [m for m in _trace(outcome.records) if m[2] == "caught"]
    assert got == _expect(batch11.y9_computed_reason(n))


def test_y9_a_defused_fire_ends_the_run_with_no_error_where_a_plain_one_stops_it(
    tmp_path,
):
    """Y9 `fire.defused`: a one-shot signal's failure with no waiter raises from `run()`
    as SimPy's failed event does (the plain case is ST-X03, and the signal's own
    `defused` is ST-X04), and `defused: true` on the firing ends the run with no error."""
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
signals: {ev: {once: true}}
arrivals:
  d: {initial: 1, spawn: drive}
tasks:
  drive: {wait: {timeout: 1}, payload: null, on: [{fire: {signal: ev, fail: true, value: boom, defused: true}}]}
run: {until: 500}
"""
    assert batch11.y9_unhandled(True)[1] is None
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    assert "boom" in str(batch11.y9_unhandled(False)[1])
    plain = text.replace(", defused: true", "")
    outcome = run_yaml_text(tmp_path, plain, {})
    assert outcome.error is not None and "boom" in str(outcome.error)


@pytest.mark.parametrize("n", SIZES)
def test_y10_events_at_one_time_run_by_priority_then_by_creation(tmp_path, n):
    """Y10 `fire.priority`: three signals fired at 1 with the priorities 2, 1 (also the
    default) and 0 wake their waiters in the order 0, 1, 2, whatever order they were
    fired in, as `env.schedule(event, priority)` orders them."""
    for name in "abc":
        _rows_i(tmp_path, n, name)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
signals: {a: {once: true}, b: {once: true}, c: {once: true}}
arrivals:
  wa: {replay: {path: a.csv}, spawn: hear_a}
  wb: {replay: {path: b.csv}, spawn: hear_b}
  wc: {replay: {path: c.csv}, spawn: hear_c}
  d: {initial: 1, spawn: drive}
tasks:
  hear_a: {wait: {signal: a}, payload: {e: heard}}
  hear_b: {wait: {signal: b}, payload: {e: heard}}
  hear_c: {wait: {signal: c}, payload: {e: heard}}
  drive:
    wait: {timeout: 1}
    payload: null
    on: [{fire: {signal: a, priority: 2}}, {fire: {signal: b, priority: 1}}, {fire: {signal: c, priority: 0}}]
run: {until: 500}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    assert _trace(outcome.records) == _expect(batch11.y10_priorities(n))


@pytest.mark.parametrize("n", SIZES)
def test_y11_a_listen_runs_when_the_event_fires_and_its_when_decides_each_entry(
    tmp_path, n
):
    """Y11 `listen` and `when`: each entity attaches a listener when its task starts,
    before its own wait; the listener runs in attach order with the waiters, and an
    entry whose `when` is false does nothing."""
    _rows_i(tmp_path, n)
    text = """
simulation: {sim_id: S, factor: 0, random_seed: 42}
variables: {count: 0}
signals: {go: {once: true}}
arrivals:
  a: {replay: {path: a.csv}, spawn: l}
  d: {initial: 1, spawn: drive}
tasks:
  l:
    listen: {signal: go, on: [{when: "attr.i % 2 == 0", path: S.variables.count, value: {gen: expr, expr: "registry['S.variables.count'] + 1"}}]}
    wait: {signal: go}
    payload: {e: woke, x: {gen: expr, expr: "registry['S.variables.count']"}}
    next: report
  report: {wait: {timeout: 1}, payload: {e: report, x: {gen: expr, expr: "registry['S.variables.count']"}}}
  drive: {wait: {timeout: 2}, payload: null, on: [{fire: {signal: go}}]}
run: {until: 500}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    assert _trace(outcome.records) == _expect(batch11.y11_listen(n))


def test_every_known_case_of_batch_11_runs_or_shares_or_has_its_own_test():
    """The 51 rows (the 50 of the design, and BT-LD12) are each a case, share the document
    of a case that has the same form, or run in a test of their own, and every such test
    is in its file."""
    assert len(BATCH_11_ROWS) == 51
    assert set(KNOWN11_SHARED.values()) <= set(KNOWN11)
    sets = [set(KNOWN11), set(KNOWN11_SHARED), set(KNOWN11_OTHER)]
    assert sum(len(s) for s in sets) == len(set().union(*sets))
    assert sorted(set().union(*sets)) == sorted(BATCH_11_ROWS)
    for test in KNOWN11_OTHER.values():
        path, _, name = test.partition("::")
        assert f"def {name}(" in (Path(__file__).parents[2] / path).read_text()
