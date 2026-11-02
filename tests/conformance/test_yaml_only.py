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

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from desflow import DynamicRealtimeEnvironment
from tests.conformance.harness import run_yaml_text
from tests.conformance.yaml_only import fixed_draws
from tests.conformance.yaml_only.programs import (
    a1_count,
    a1_entities,
    a4_gap,
    batch6,
    cv_032_034_steps,
    cv_040_segments,
    v1_service_time,
    v2_timeout,
    v3_values,
    v4_draws,
    v10_weights,
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
