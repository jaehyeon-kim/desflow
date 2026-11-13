"""Interrupts and failures (Batch 9): `on_interrupt_run`, `suspend` and `resume`,
`on_renege` and `on_balk`, `on_fail` with no `fail`, `upstream.reason` and
`upstream.status`: the refusals each key makes at load, the same keys through the
builder, the names they read, and what a run leaves behind."""

import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pytest

from desflow import SimulationContext, replicate
from desflow.builder.task import TaskFailed
from desflow.blueprint.loader import BlueprintError
from desflow.values.models import Expr
from tests.conformance.harness import run_builder, run_yaml_text

BASE = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
signals: [sig]
arrivals: {a: {dist: constant, value: 1, spawn: t}}
tasks:
  t: TASK
  u: {payload: {e: u}}
run: {until: 5}
"""


def load(tmp_path: Path, task: str) -> SimulationContext:
    path = tmp_path / "model.yaml"
    path.write_text(BASE.replace("TASK", task))
    return SimulationContext.from_yaml(path)


REFUSED = [
    (
        "{service_time: 1, on_interrupt: abandon, on_interrupt_run: u, payload: {e: 1}}",
        "on_interrupt_run runs a flow and then goes on with the task",
    ),
    (
        "{resource: m, service_time: 1, on_interrupt_run: u, payload: {e: 1}}",
        "on_interrupt_run is for work that holds no unit of its own",
    ),
    (
        "{wait: {request: m}, service_time: 1, on_interrupt_run: u, payload: {e: 1}}",
        "and a request in its wait holds one",
    ),
    (
        "{service_time: 1, on_interrupt_run: nosuch, payload: {e: 1}}",
        "task 't' names task 'nosuch', which is not defined under tasks",
    ),
    (
        "{service_time: 1, on_renege: u, payload: {e: 1}}",
        "on_renege is where a reneging task goes, so it needs patience",
    ),
    (
        "{service_time: 1, on_balk: u, payload: {e: 1}}",
        "on_balk is where a balking task goes, so it needs a balk, a resource, a seize or an open",
    ),
    (
        "{resource: m, service_time: 1, patience: 2, on_renege: nosuch, payload: {e: 1}}",
        "task 't' names task 'nosuch', which is not defined under tasks",
    ),
    (
        "{resource: m, service_time: 1, on_balk: nosuch, balk: 'queue.m > 1', payload: {e: 1}}",
        "task 't' names task 'nosuch', which is not defined under tasks",
    ),
    (
        "{wait: {suspend: false}, payload: {e: 1}}",
        "a wait suspend branch is suspend: true, got False",
    ),
    (
        "{open: {a: {suspend: true}}, payload: {e: 1}}",
        "open 'a' has a suspend branch",
    ),
    (
        "{resume: {task: nosuch}, payload: {e: 1}}",
        "task 't' names task 'nosuch', which is not defined under tasks",
    ),
    (
        "{on: [{resume: {task: nosuch}}], payload: {e: 1}}",
        "the on: action of task 't' resumes task 'nosuch'",
    ),
    (
        "{resume: {task: u, limit: 1.5}, payload: {e: 1}}",
        "tasks.t.resume.limit",
    ),
    (
        "{resume: {task: u, limit: true}, payload: {e: 1}}",
        "tasks.t.resume.limit",
    ),
    (
        "{resume: {task: u, where: 'attr.x +'}, payload: {e: 1}}",
        "tasks.t.resume.where",
    ),
    (
        "{on: [{resume: {task: u, where: 'nosuch.x == 1'}}], payload: {e: 1}}",
        "tasks.t.on.0.resume.where",
    ),
    (
        "{on: [{resume: {task: u, limit: 'nosuch + 1'}}], payload: {e: 1}}",
        "tasks.t.on.0.resume.limit",
    ),
    ("{resume: {task: u, nope: 1}, payload: {e: 1}}", "tasks.t.resume.nope"),
    ("{resume: {}, payload: {e: 1}}", "tasks.t.resume.task: Field required"),
    (
        "{on: [{resume: {task: u}, fire: {signal: sig}}], payload: {e: 1}}",
        "an on: entry takes exactly one of fire, spawn, interrupt, resume",
    ),
    (
        "{fail_reason: x, on_fail: u, payload: {e: 1}}",
        "fail_reason needs a fail condition",
    ),
    (
        "{on_fail: nosuch, payload: {e: 1}}",
        "task 't' names task 'nosuch', which is not defined under tasks",
    ),
    (
        "{payload: {e: 1}, resume: {task: u, value: {gen: expr, expr: 'handle.x.ok'}}}",
        "handle.x names no handle, which no task opens",
    ),
    (
        "{payload: {e: 1}, resume: {task: u, limit: {gen: expr, expr: 'handle.x.ok'}}}",
        "handle.x names no handle, which no task opens",
    ),
    (
        "{payload: {e: 1}, on: [{resume: {task: u, value: {gen: expr, expr: 'handle.x.ok'}}}]}",
        "handle.x names no handle, which no task opens",
    ),
]


@pytest.mark.parametrize("task,message", REFUSED)
def test_the_blueprint_refuses_each_misuse_of_the_new_keys(tmp_path, task, message):
    with pytest.raises(BlueprintError, match=re.escape(message)):
        load(tmp_path, task)


def test_on_fail_alone_loads_and_a_wait_suspend_loads(tmp_path):
    """`on_fail` needs no `fail` (M3), and a `suspend` branch is `true`, alone or in a
    group; `on_renege` and `on_balk` load with their cause."""
    for task in (
        "{on_fail: u, payload: {e: 1}}",
        "{wait: {suspend: true}, payload: {e: 1}}",
        "{wait: {any: {a: {suspend: true}, b: {timeout: 2}}}, payload: {e: 1}}",
        "{resource: m, service_time: 1, patience: 2, on_renege: u, payload: {e: 1}}",
        "{resource: m, service_time: 1, balk: 'queue.m > 1', on_balk: u, payload: {e: 1}}",
        "{seize: m, on_balk: u, payload: {e: 1}}",
        "{open: {q: {request: m}}, service_time: 1, balk: 'queue.m > 1', on_balk: u, payload: {e: 1}}",
        "{service_time: 1, on_interrupt_run: u, payload: {e: 1}}",
        "{service_time: 1, on_interrupt: restart, on_interrupt_run: u, payload: {e: 1}}",
    ):
        load(tmp_path, task)


# ----------------------------------------------------------------- the builder


def builder() -> SimulationContext:
    app = SimulationContext("S", factor=0)
    app.add_resource("m", current_cap=1)
    app.add_signal("sig")
    return app


def named_tasks(app: SimulationContext, **tasks: Dict[str, Any]) -> None:
    for name, keys in tasks.items():
        app.task(name=name, **keys)(lambda task_id, **_kw: {"e": name})


def test_the_builder_refuses_what_the_blueprint_refuses():
    app = builder()
    with pytest.raises(ValueError, match="runs a flow and then goes on with the task"):
        app.task(name="a", on_interrupt="abandon", on_interrupt_run="u")
    with pytest.raises(ValueError, match="holds no unit of its own"):
        app.task(name="b", resource_id="m", on_interrupt_run="u")
    with pytest.raises(ValueError, match="holds no unit of its own"):
        app.task(name="c", wait={"request": "m"}, on_interrupt_run="u")
    with pytest.raises(ValueError, match="needs patience"):
        app.task(name="d", on_renege="u")
    with pytest.raises(
        ValueError, match="needs a balk, a resource, a seize or an open"
    ):
        app.task(name="e", on_balk="u")
    with pytest.raises(ValueError, match="suspend branch is suspend: true"):
        app.task(name="f", wait={"suspend": False})
    with pytest.raises(ValueError, match="a resume action takes a task"):
        app.task(name="g", resume={"nope": 1})
    with pytest.raises(ValueError, match="a resume limit is a whole number"):
        app.task(name="h", resume={"task": "u", "limit": 1.5})
    with pytest.raises(ValueError, match="a resume limit is a whole number"):
        app.task(name="i", resume={"task": "u", "limit": True})


@pytest.mark.parametrize(
    "keys,message",
    [
        ({"on_interrupt_run": "nosuch"}, "names task 'nosuch'"),
        (
            {
                "patience": 1,
                "resource_id": "m",
                "service_time": 1,
                "on_renege": "nosuch",
            },
            "'nosuch'",
        ),
        (
            {
                "balk": "queue.m > 1",
                "resource_id": "m",
                "service_time": 1,
                "on_balk": "nosuch",
            },
            "'nosuch'",
        ),
        ({"resume": {"task": "nosuch"}}, "names task 'nosuch'"),
        ({"on": [{"resume": {"task": "nosuch"}}]}, "resumes task 'nosuch'"),
        (
            {"resume": {"task": "u", "value": Expr(gen="expr", expr="handle.x.ok")}},
            "resume names 'x', which no task opens",
        ),
        (
            {
                "resume": {
                    "task": "u",
                    "limit": Expr(gen="expr", expr="1 if handle.x.ok else 2"),
                }
            },
            "resume names 'x', which no task opens",
        ),
    ],
)
def test_the_builder_checks_the_names_before_the_run(keys, message):
    app = builder()
    named_tasks(app, u={}, t=keys)
    with pytest.raises(ValueError, match=re.escape(message)):
        app.run(until=1)


def test_the_builder_resumes_as_the_blueprint_does(tmp_path):
    """I4 through the Builder, with NumPy numbers in the limit and the value, gives the
    records of the blueprint; `resume` is a task key and an `on:` entry."""
    yaml = """
simulation: {sim_id: S, factor: 0}
arrivals:
  p: {dist: constant, value: 1000, initial: 3, spawn: park}
  w: {dist: constant, value: 2, spawn: wake}
  x: {dist: constant, value: 4, spawn: wake_on}
tasks:
  park: {wait: {suspend: true}, payload: {e: woke, x: {gen: expr, expr: "wait.suspend.value"}}}
  wake: {payload: {e: wake}, resume: {task: park, where: "process.instance == 1", value: 9, limit: 1}}
  wake_on: {payload: {e: wake_on}, on: [{resume: {task: park, value: 7}}]}
run: {until: 10}
"""
    outcome = run_yaml_text(tmp_path, yaml, {})

    def marks(records):
        return [
            (round(r["sim_ts"], 3), r["key"], r["value"]["e"], r["value"].get("x"))
            for r in records
            if r["stream_type"] == "event" and "e" in r["value"]
        ]

    expected = marks(outcome.records)
    assert [m for m in expected if m[2] == "woke"] == [
        (2.0, "p-2", "woke", 9),
        (4.0, "p-1", "woke", 7),
        (4.0, "p-3", "woke", 7),
    ]
    for limit, value in [(1, 9), (np.int64(1), np.float64(9.0))]:
        app = SimulationContext("S", factor=0)
        app.add_arrival("p", dist="constant", value=1000, initial=3, spawn="park")
        app.add_arrival("w", dist="constant", value=2, spawn="wake")
        app.add_arrival("x", dist="constant", value=4, spawn="wake_on")
        app.task(
            name="park",
            wait={"suspend": True},
        )(lambda task_id, **_kw: {"e": "woke"})
        app.task(
            name="wake",
            resume={
                "task": "park",
                "where": "process.instance == 1",
                "value": value,
                "limit": limit,
            },
        )(lambda task_id, **_kw: {"e": "wake"})
        app.task(name="wake_on", on=[{"resume": {"task": "park", "value": 7}}])(
            lambda task_id, **_kw: {"e": "wake_on"}
        )
        got = marks(run_builder(app, 10))
        assert [(t, k, e) for t, k, e, _ in got] == [
            (t, k, e) for t, k, e, _ in expected
        ]


# ------------------------------------------------------------- what a run does


def run_marks(tmp_path: Path, text: str, until: float = 50) -> List[Tuple[Any, ...]]:
    outcome = run_yaml_text(tmp_path, text.replace("UNTIL", str(until)), {})
    assert outcome.error is None, repr(outcome.error)
    return [
        (
            round(r["sim_ts"], 3),
            r["key"],
            r["value"].get("e", r["value"].get("status")),
            r["value"].get("x"),
        )
        for r in outcome.records
        if r["stream_type"] == "event"
        and (
            "e" in r["value"]
            or r["value"].get("status")
            in ("failed", "reneged", "balked", "interrupted")
        )
    ]


def test_a_resume_with_a_limit_of_zero_or_nobody_suspended_wakes_nobody(tmp_path):
    yaml = """
simulation: {sim_id: S, factor: 0}
arrivals:
  w0: {dist: constant, value: 1, spawn: wake, max_events: 1, first_at: 0}
  p: {dist: constant, value: 1, spawn: park, max_events: 1, first_at: 1}
  w1: {dist: constant, value: 1, spawn: none, max_events: 1, first_at: 2}
  w2: {dist: constant, value: 1, spawn: all, max_events: 1, first_at: 3}
tasks:
  park: {wait: {suspend: true}, payload: {e: woke, x: {gen: expr, expr: "wait.suspend.value"}}}
  wake: {payload: {e: wake}, resume: {task: park, value: 1}}
  none: {payload: {e: none}, resume: {task: park, value: 2, limit: 0}}
  all: {payload: {e: all}, resume: {task: park, value: 3, limit: 0}}
run: {until: UNTIL}
"""
    marks = run_marks(tmp_path, yaml)
    assert [m for m in marks if m[2] == "woke"] == []
    assert [m[2] for m in marks] == ["wake", "none", "all"]


def test_a_resume_value_is_read_once_for_each_resume(tmp_path):
    yaml = """
simulation: {sim_id: S, factor: 0}
arrivals:
  p: {dist: constant, value: 1000, initial: 4, spawn: park}
  w: {dist: constant, value: 1, spawn: wake, max_events: 2, first_at: 1}
tasks:
  park: {wait: {suspend: true}, payload: {e: woke, x: {gen: expr, expr: "wait.suspend.value"}}}
  wake: {payload: {e: wake}, resume: {task: park, value: {gen: sequence}, limit: 2}}
run: {until: UNTIL}
"""
    woke = [m for m in run_marks(tmp_path, yaml) if m[2] == "woke"]
    assert woke == [
        (1.0, "p-1", "woke", 0),
        (1.0, "p-2", "woke", 0),
        (2.0, "p-3", "woke", 1),
        (2.0, "p-4", "woke", 1),
    ]


def test_a_suspend_in_a_group_ends_with_another_branch_and_wakes_nobody_later(tmp_path):
    """A suspension whose wait ended by its timeout is no longer suspended: a later
    `resume` does not wake it and nothing is left in the list."""
    yaml = """
simulation: {sim_id: S, factor: 0}
arrivals:
  p: {initial: 1, spawn: park}
  w: {dist: constant, value: 5, spawn: wake, max_events: 1, first_at: 5}
tasks:
  park: {wait: {any: {s: {suspend: true}, t: {timeout: 2}}}, payload: {e: left, x: {gen: expr, expr: "wait.t.done"}}}
  wake: {payload: {e: wake}, resume: {task: park}}
run: {until: UNTIL}
"""
    marks = run_marks(tmp_path, yaml)
    assert [(m[0], m[2]) for m in marks] == [(2.0, "left"), (5.0, "wake")]
    app = SimulationContext.from_yaml(_write(tmp_path, yaml.replace("UNTIL", "20")))
    run_builder(app, 20)
    assert app._run.suspended.get("park", []) == []


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "again.yaml"
    path.write_text(text)
    return path


def test_an_abandoned_or_ended_instance_leaves_nothing_suspended(tmp_path):
    yaml = """
simulation: {sim_id: S, factor: 0}
arrivals:
  p: {dist: constant, value: 1000, initial: 3, spawn: park}
  c: {dist: constant, value: 1, spawn: cut, max_events: 1, first_at: 1}
tasks:
  park: {wait: {suspend: true}, on_interrupt: abandon, payload: {e: woke}}
  cut: {payload: {e: cut}, interrupt: {task: park}}
run: {until: 10}
"""
    app = SimulationContext.from_yaml(_write(tmp_path, yaml))
    records = run_builder(app, 10)
    assert app._run.suspended.get("park", []) == []
    statuses = [
        r["value"].get("status") for r in records if r["stream_type"] == "event"
    ]
    assert statuses.count("interrupted") == 3


def _is(record, label):
    return isinstance(record["value"], dict) and record["value"].get("e") == label


def test_the_run_of_a_blueprint_with_suspensions_starts_clean_each_time(tmp_path):
    """The first run ends with an instance still suspended. A second run of the same
    context starts with no suspended instance, so its first `resume` with a limit of 1
    wakes the instance that suspended in that run, and replications give the same."""
    yaml = """
simulation: {sim_id: S, factor: 0, random_seed: 1}
arrivals:
  p: {initial: 1, spawn: park}
  w: {dist: constant, value: 8, spawn: wake, max_events: 1, first_at: 8}
tasks:
  park: {wait: {suspend: true}, payload: {e: woke}}
  wake: {payload: {e: wake}, resume: {task: park, limit: 1}}
stats: typical
run: {until: 5}
"""
    path = _write(tmp_path, yaml)
    app = SimulationContext.from_yaml(path)
    first = run_builder(app, 5)
    assert not [r for r in first if _is(r, "woke")]
    assert len(app._run.suspended["park"]) == 1
    second = run_builder(app, 10)
    woke = [r["sim_ts"] for r in second if _is(r, "woke")]
    assert woke == [8.0]
    result = replicate(str(path), 3, seed=1, measures=["flows.p.started"])
    assert result.values["flows.p.started"] == [1, 1, 1]


FAIL_TEXT = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals: {a: {initial: 1, spawn: t}}
tasks:
  t: TASK
  handle: {payload: {e: handled, x: {gen: expr, expr: "[upstream.status, upstream.reason]"}}}
run: {until: 5}
"""


@pytest.mark.parametrize(
    "task,reason",
    [
        (
            "{service_time: {gen: expr, expr: '1 / 0'}, on_fail: handle, payload: {e: t}}",
            "division by zero",
        ),
        (
            "{wait: {timeout: 1}, payload: {e: {gen: expr, expr: '1 / 0'}}, on_fail: handle}",
            "division by zero",
        ),
        (
            "{wait: {timeout: 1}, fail: 'True', fail_reason: nope, on_fail: handle, payload: {e: t}}",
            "nope",
        ),
    ],
)
def test_a_failure_goes_to_on_fail_and_the_task_reads_upstream_status_and_reason(
    tmp_path, task, reason
):
    path = tmp_path / "model.yaml"
    path.write_text(FAIL_TEXT.replace("TASK", task))
    outcome = run_yaml_text(tmp_path, FAIL_TEXT.replace("TASK", task), {})
    assert outcome.error is None, repr(outcome.error)
    handled = [
        r["value"]["x"]
        for r in outcome.records
        if r["stream_type"] == "event" and r["value"].get("e") == "handled"
    ]
    assert len(handled) == 1
    assert handled[0][0] == "failed" and reason in handled[0][1]


UNEVALUABLE = {
    "an each of -3": "wait: {each: '1 - 4', branch: {timeout: 1}}",
    "a wait timeout": "wait: {timeout: 'attr.zz'}",
    "a service_time": "service_time: 'attr.zz'",
    "a key that is a fraction": "key: {gen: expr, expr: '1.5'}",
    "a key that reads a missing attribute": "key: {gen: expr, expr: 'attr.zz'}",
}


@pytest.mark.parametrize("step", UNEVALUABLE)
def test_an_expression_that_cannot_be_evaluated_stops_the_run_under_stop_and_fails_the_task_under_continue(
    tmp_path, step
):
    """Under stop the error stops the run with no `failed` record, as in plain SimPy.
    Under continue `failed` is published with the error as its reason, and only the
    flow ends."""
    text = (
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "arrivals: {a: {dist: constant, value: 1, spawn: t, max_events: 2}}\n"
        f"tasks: {{t: {{{UNEVALUABLE[step]}, payload: {{e: done}}}}}}\n"
        "run: {until: 10%s}\n"
    )
    stopped = run_yaml_text(tmp_path, text % "", {})
    assert isinstance(stopped.error, ValueError)
    assert not any(
        r["value"].get("status") == "failed"
        for r in stopped.records
        if r["stream_type"] == "event"
    )
    kept = run_yaml_text(tmp_path, text % ", on_unhandled_failure: continue", {})
    assert kept.error is None
    reasons = [
        r["value"]["reason"]
        for r in kept.records
        if r["stream_type"] == "event" and r["value"].get("status") == "failed"
    ]
    assert len(reasons) == 2 and all(
        reason.startswith(("ValueError", "ValueGenerationError")) for reason in reasons
    )


def test_on_renege_and_on_balk_leave_nothing_in_the_line(tmp_path):
    yaml = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  h: {initial: 1, spawn: hold}
  l: {initial: 4, spawn: late}
  probe: {dist: constant, value: 20, spawn: probe, first_at: 20, max_events: 1}
tasks:
  hold: {resource: m, service_time: 5, payload: null}
  late: {balk: "queue.m >= 2", on_balk: away, patience: 2, on_renege: away, resource: m, service_time: 1, payload: {e: served}}
  away: {payload: {e: away, x: {gen: expr, expr: "upstream.status"}}}
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 30}
"""
    marks = run_marks(tmp_path, yaml, 30)
    away = [(m[0], m[3]) for m in marks if m[2] == "away"]
    assert sorted(away) == [
        (0.0, "balked"),
        (0.0, "balked"),
        (2.0, "reneged"),
        (2.0, "reneged"),
    ]
    assert [m[3] for m in marks if m[2] == "end"] == [[0, 0]]


def test_explain_reads_a_blueprint_with_the_new_keys(tmp_path):
    yaml = """
simulation: {sim_id: S, factor: 0}
arrivals:
  p: {dist: constant, value: 1, spawn: park}
  w: {dist: constant, value: 2, spawn: wake}
tasks:
  park: {wait: {suspend: true}, on_interrupt_run: fix, payload: {e: woke}}
  fix: {service_time: 1, payload: {e: fix}}
  wake: {payload: {e: wake}, resume: {task: park, limit: 1}}
run: {until: 10}
"""
    app = SimulationContext.from_yaml(_write(tmp_path, yaml))
    explanation = app.analyze()
    features = {node.id: node.features for node in explanation.graph.nodes}
    assert "resume" in features["task:wake"]


CHILD_FAILS = """
simulation: {sim_id: S, factor: 0}
arrivals:
  w: {initial: 1, spawn: work}
  c: {initial: 1, spawn: cutter}
tasks:
  work: {service_time: 10, on_interrupt_run: repair, ON_FAIL payload: {e: done}}
  repair: {service_time: 1, fail: "True", fail_reason: broken, payload: {e: repaired}}
  handle: {payload: {e: handled, x: {gen: expr, expr: "[upstream.status, upstream.reason]"}}}
  cutter: {wait: {timeout: 2}, payload: null, interrupt: {task: work}}
run: {until: 30}
"""


def test_a_failing_on_interrupt_run_flow_fails_the_task_and_goes_to_its_on_fail(
    tmp_path,
):
    outcome = run_yaml_text(
        tmp_path, CHILD_FAILS.replace("ON_FAIL", "on_fail: handle,"), {}
    )
    assert outcome.error is None, repr(outcome.error)
    events = [
        (
            r["sim_ts"],
            r["task"],
            r["value"].get("status") or r["value"].get("e"),
            r["value"].get("x"),
        )
        for r in outcome.records
        if r["stream_type"] == "event"
    ]
    assert (3.0, "work", "failed", None) in events
    assert (3.0, "handle", "handled", ["failed", "broken"]) in events
    assert not any(e[2] == "done" for e in events)


def test_a_failing_on_interrupt_run_flow_with_no_on_fail_stops_the_run(tmp_path):
    outcome = run_yaml_text(tmp_path, CHILD_FAILS.replace("ON_FAIL", ""), {})
    assert outcome.error is not None
    assert "broken" in str(outcome.error)


def test_a_loop_of_tasks_with_no_time_in_an_on_interrupt_run_flow_stops_the_run(
    tmp_path,
):
    """The hand-off loop of the repair flow counts instances at one instant as a flow
    does, and its error is not a failure of the child that goes to `on_fail`."""
    yaml = """
simulation: {sim_id: S, factor: 0}
arrivals:
  a: {initial: 1, spawn: work}
  c: {initial: 1, spawn: cutter}
tasks:
  work: {service_time: 10, on_interrupt_run: ping, on_fail: handle, payload: {e: 1}}
  ping: {payload: {e: 2}, next: pong}
  pong: {payload: {e: 3}, next: ping}
  handle: {payload: {e: handled}}
  cutter: {wait: {timeout: 2}, payload: null, interrupt: {task: work}}
run: {until: 30}
"""
    outcome = run_yaml_text(tmp_path, yaml, {})
    assert "ran 10000 times in one flow at time 2" in str(outcome.error)


def test_a_loop_of_tasks_with_no_time_while_waiting_for_a_unit_stops_the_run(
    tmp_path,
):
    """An interrupt that reaches a task in a line runs its `on_interrupt_run` flow in
    the holding step; a loop there stops the run and does not go to `on_fail`."""
    yaml = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  b: {initial: 1, spawn: blocker}
  a: {initial: 1, spawn: work}
  c: {initial: 1, spawn: cutter}
tasks:
  blocker: {service_time: 100, resource: m, payload: {e: 0}}
  work: {service_time: 10, seize: m, on_interrupt_run: ping, on_fail: handle, payload: {e: 1}}
  ping: {payload: {e: 2}, next: pong}
  pong: {payload: {e: 3}, next: ping}
  handle: {payload: {e: handled}}
  cutter: {wait: {timeout: 2}, payload: null, interrupt: {task: work}}
run: {until: 30}
"""
    outcome = run_yaml_text(tmp_path, yaml, {})
    assert "ran 10000 times in one flow at time 2" in str(outcome.error)


def test_a_unit_an_earlier_seize_took_stays_held_while_the_on_interrupt_run_flow_runs(
    tmp_path,
):
    """The repair flow runs in the process of the task that was interrupted, and gives
    back none of the units that task's flow holds: the unit of an earlier `seize` is
    held through the repair and the rest of the work, and freed by the `release`."""
    yaml = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  a: {initial: 1, spawn: take}
  c: {initial: 1, spawn: cutter}
  p1: {dist: constant, value: 3, spawn: probe, first_at: 3, max_events: 1}
  p2: {dist: constant, value: 20, spawn: probe, first_at: 20, max_events: 1}
tasks:
  take: {seize: m, payload: {e: took}, next: work}
  work: {service_time: 10, on_interrupt_run: repair, payload: {e: done}, release: m}
  repair: {service_time: 5, payload: {e: repaired}}
  cutter: {wait: {timeout: 2}, payload: null, interrupt: {task: work}}
  probe: {payload: {e: probe, x: {gen: expr, expr: "users.m"}}}
run: {until: 30}
"""
    marks = run_marks(tmp_path, yaml, 30)
    assert [(m[0], m[3]) for m in marks if m[2] == "probe"] == [(3.0, 1), (20.0, 0)]
    assert [(m[0], m[2]) for m in marks if m[2] in ("repaired", "done")] == [
        (7.0, "repaired"),
        (15.0, "done"),
    ]


SEIZE_FAILS = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  h: {initial: 1, spawn: hold}
  s: {dist: constant, value: 0.5, spawn: subject, max_events: 1}
  c: {dist: constant, value: 3, spawn: cut, max_events: 1}
  b: {dist: constant, value: 4, spawn: behind, max_events: 1}
  p: {dist: constant, value: 30, spawn: probe, max_events: 1}
tasks:
  hold: {resource: m, service_time: 10, payload: {e: held}}
  subject: {seize: m, service_time: 2, on_interrupt: resume, on_interrupt_run: rep, on_fail: h2, payload: {e: s_done}, release: m}
  rep: {payload: {e: rep, x: {gen: expr, expr: "1 / 0"}}}
  h2: {payload: {e: handled}}
  cut: {payload: {e: cut}, interrupt: {task: subject}}
  behind: {resource: m, service_time: 1, payload: {e: behind}}
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 50}
"""


def test_a_queued_seize_leaves_the_line_when_the_on_interrupt_run_flow_fails(tmp_path):
    """The failed task goes to `on_fail` with no request left in the line, so the flow
    that asks later is served when the holder ends."""
    marks = run_marks(tmp_path, SEIZE_FAILS)
    assert (3.0, "s-1", "handled", None) in marks
    assert [m[3] for m in marks if m[2] == "end"] == [[0, 0]]
    assert [m[0] for m in marks if m[2] == "behind"] == [11.0]


def test_a_step_that_raises_leaves_no_unit_or_request_with_on_fail(tmp_path):
    """A `resource:` task, a wait `request` and a `seize` whose step raises each go to
    `on_fail` and leave nothing held or queued."""
    for task in (
        "{resource: m, service_time: {gen: expr, expr: '1 / 0'}, on_fail: h, payload: {e: t}}",
        "{wait: {request: m}, service_time: {gen: expr, expr: '1 / 0'}, on_fail: h, payload: {e: t}}",
        "{seize: m, service_time: {gen: expr, expr: '1 / 0'}, on_fail: h, payload: {e: t}}",
    ):
        text = f"""
simulation: {{sim_id: S, factor: 0}}
resources: {{m: {{current_cap: 1}}}}
arrivals:
  a: {{initial: 1, spawn: t}}
  p: {{dist: constant, value: 5, spawn: probe, max_events: 1}}
tasks:
  t: {task}
  h: {{payload: {{e: handled}}, release: m}}
  probe: {{payload: {{e: end, x: {{gen: expr, expr: "[users.m, queue.m]"}}}}}}
run: {{until: 20}}
"""
        marks = run_marks(tmp_path, text)
        assert [m[3] for m in marks if m[2] == "end"] == [[0, 0]], task


def _failing_task(app: SimulationContext, fail: Any) -> None:
    app.add_arrival("a", dist="constant", value=1, spawn="t", max_events=3, first_at=0)
    app.task(name="t", service_time=0.5, fail=fail)(lambda task_id: {"e": "done"})


def _raises(task_id, payload):
    raise ZeroDivisionError("x")


def test_a_builder_fail_function_that_raises_is_the_failure_of_the_task():
    """As a task function that raises is: `failed` with the error as its reason, a
    TaskFailed under stop, and only the flow ends under continue."""
    app = SimulationContext("S", factor=0)
    _failing_task(app, _raises)
    stopped = run_builder(app, 10, raises=True)
    assert isinstance(stopped.error, TaskFailed)
    assert stopped.error.reason == "ZeroDivisionError: x"
    failed = [
        r["value"]
        for r in stopped.records
        if r["stream_type"] == "event" and r["value"].get("status") == "failed"
    ]
    assert failed[0]["reason"] == "ZeroDivisionError: x"

    app = SimulationContext("S", factor=0)
    _failing_task(app, _raises)
    kept = run_builder(app, 10, raises=True, on_unhandled_failure="continue")
    assert kept.error is None
    reasons = [
        r["value"]["reason"]
        for r in kept.records
        if r["stream_type"] == "event" and r["value"].get("status") == "failed"
    ]
    assert reasons == ["ZeroDivisionError: x"] * 3


def test_a_step_that_raises_fails_the_signal_of_the_task(tmp_path):
    """The error goes through the path a `fail` condition takes, so a flow that waits
    for the task's `signal` sees it fail, once."""
    text = """
simulation: {sim_id: S, factor: 0}
signals: [sig]
arrivals:
  a: {dist: constant, value: 1, spawn: t, max_events: 1}
  w: {initial: 1, spawn: w}
tasks:
  t: {service_time: {gen: expr, expr: '1 / 0'}, signal: sig, on_fail: h, payload: {e: t_done}}
  h: {payload: {e: handled}}
  w: {wait: {signal: sig}, on_fail: h2, payload: {e: w_done}}
  h2: {payload: {e: w_told}}
run: {until: 10}
"""
    marks = run_marks(tmp_path, text)
    assert [(m[0], m[2]) for m in marks if m[2] in ("handled", "w_told")] == [
        (1.0, "handled"),
        (1.0, "w_told"),
    ]


def test_a_granted_request_branch_is_kept_when_an_interrupt_restarts_the_wait(tmp_path):
    """Fix 8: under `restart` a wait keeps the `request` branch that holds its unit,
    granted at the instant of the interrupt or long before, inside an `all`. A flow
    that asked later waits for it."""
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  a: {initial: 1, spawn: t}
  b: {dist: constant, value: 0.5, spawn: other, max_events: 1}
  h: {dist: constant, value: 2, spawn: hit, max_events: 1}
tasks:
  t: {wait: {all: {r: {request: m}, d: {timeout: 5}}}, service_time: 1, on_interrupt: restart, payload: {e: t_done}}
  other: {wait: {request: m}, service_time: 1, payload: {e: other_done}}
  hit: {payload: {e: hit}, interrupt: {task: t}}
run: {until: 40}
"""
    marks = run_marks(tmp_path, text)
    assert [(m[0], m[2]) for m in marks if m[2] in ("t_done", "other_done")] == [
        (8.0, "t_done"),
        (9.0, "other_done"),
    ]


def test_on_balk_follows_a_seize_or_an_open_on_a_full_line(tmp_path):
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue_limit: 1}, k: {current_cap: 1, queue_limit: 1}}
arrivals:
  a: {initial: 3, spawn: s}
  b: {initial: 3, spawn: o}
tasks:
  s: {seize: m, on_balk: away, service_time: 1, payload: {e: s_done}, release: m}
  o: {open: {q: {request: k}}, on_balk: away, payload: {e: o_open}}
  away: {payload: {e: away, x: {gen: expr, expr: "upstream.status"}}}
run: {until: 10}
"""
    marks = run_marks(tmp_path, text)
    away = [(m[1], m[3]) for m in marks if m[2] == "away"]
    assert sorted(away) == [("a-3", "balked"), ("b-3", "balked")]


def test_a_restart_on_a_line_that_is_full_by_then_balks(tmp_path):
    """B-U2: an interrupt with `on_interrupt: restart` asks again at the back of the
    line; the scenario lowered the line's limit to 0, so the task balks."""
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue_limit: 2}}
arrivals:
  a: {replay: {path: a.csv}, spawn: h}
  b: {replay: {path: b.csv}, spawn: w}
scenario:
  - {at: 2, path: S.resources.m.queue_limit, value: 0}
  - {at: 3, interrupt: {task: w}}
tasks:
  h: {seize: m, service_time: 10, payload: {e: h_done}, release: m}
  w: {seize: m, on_interrupt: restart, service_time: 1, on_balk: away, payload: {e: w_done}, release: m}
  away: {payload: {e: away, x: {gen: expr, expr: "upstream.status"}}}
run: {until: 20}
"""
    outcome = run_yaml_text(tmp_path, text, {"a": [0.0], "b": [1.0]})
    assert outcome.error is None, repr(outcome.error)
    found = [
        (r["sim_ts"], r["task"], r["value"])
        for r in outcome.records
        if r["stream_type"] == "event" and r["task"] in ("w", "away")
    ]
    assert found == [
        (1.0, "w", {"status": "queued"}),
        (3.0, "w", {"status": "interrupted", "cause": None}),
        (3.0, "w", {"status": "balked", "resource": "m"}),
        (3.0, "away", {"e": "away", "x": "balked"}),
    ]


def test_the_open_row_of_the_schema_leaves_out_the_suspend_branch():
    import json

    from tests.unit.test_schema import _generator

    task = json.loads(_generator().text())["$defs"]["Task"]["properties"]
    assert "SuspendWait" not in json.dumps(task["open"])
    assert "SuspendWait" in json.dumps(task["wait"])


def test_a_step_that_raises_after_the_signal_fired_goes_to_on_fail_and_fires_it_once(
    tmp_path,
):
    """The signal fired as succeeded before the `on:` action raised. The task goes to
    `on_fail`, the waiters of the signal are not failed a second time, and the run
    ends with no error."""
    text = """
simulation: {sim_id: S, factor: 0}
signals: [sig]
arrivals:
  a: {dist: constant, value: 1, spawn: t, max_events: 1}
  w: {initial: 2, spawn: w}
tasks:
  t: {service_time: 1, signal: sig, on_fail: h, payload: {e: t_done}, on: [{spawn: {task: u, count: "1 / 0"}}]}
  u: {payload: {e: u}}
  h: {payload: {e: handled}}
  w: {wait: {signal: sig}, on_fail: h2, payload: {e: w_done}}
  h2: {payload: {e: w_told}}
run: {until: 10}
"""
    outcome = run_yaml_text(
        tmp_path, text.replace("signals: [sig]", "stats: basic\nsignals: [sig]"), {}
    )
    assert outcome.error is None, repr(outcome.error)
    labels = [
        r["value"].get("e") for r in outcome.records if r["stream_type"] == "event"
    ]
    assert labels.count("handled") == 1
    assert labels.count("w_done") == 2
    assert "w_told" not in labels
    readings = outcome.app_or_env.env.stats.readings()
    counts = {
        k.split(".tasks.")[-1]: v for k, v in readings.items() if ".tasks." in k and v
    }
    assert counts["t.finished"] == 1
    assert "t.failed" not in counts


PREEMPTED_SEIZE = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue: priority, preempt: true}}
arrivals:
  s: {initial: 1, spawn: subject}
  x: {dist: constant, value: 3, spawn: boss, max_events: 1}
  p: {dist: constant, value: 30, spawn: probe, max_events: 1}
tasks:
  subject: {seize: {resource: m, priority: 5}, service_time: 10, on_interrupt: resume, on_interrupt_run: rep, on_fail: h2, payload: {e: s_done}, release: m}
  rep: {payload: {e: rep, x: {gen: expr, expr: "1 / 0"}}}
  h2: HANDLER
  boss: {resource: m, priority: 1, preempt: true, service_time: 4, payload: {e: boss}}
  probe: {payload: {e: end, x: {gen: expr, expr: "[users.m, queue.m]"}}}
run: {until: 50}
"""


@pytest.mark.parametrize(
    "handler,ends",
    [
        ("{service_time: 1, payload: {e: handled}}", 4.0),
        ("{payload: {e: handled}}", 3.0),
    ],
)
def test_a_seize_unit_a_preemption_took_is_not_asked_for_again_after_the_flow_fails(
    tmp_path, handler, ends
):
    marks = run_marks(tmp_path, PREEMPTED_SEIZE.replace("HANDLER", handler))
    assert [m[0] for m in marks if m[2] == "handled"] == [ends]
    assert [m[3] for m in marks if m[2] == "end"] == [[0, 0]]


def test_the_upgrading_line_of_fix_8_gives_the_old_records_for_two_interrupts(
    tmp_path,
):
    """A `restart` of a wait that holds its unit inside an `all` gave the unit back
    at 63efb85: `other` ran from 2 to 3 and the flow ended at 10 after two
    interrupts. The model of the Upgrading line (`again` repeats the wait, has
    `on_interrupt: abandon` and `on_interrupt_next: again`, and every `interrupt`
    names both tasks) gives the same finished records."""
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  a: {initial: 1, spawn: t}
  b: {dist: constant, value: 0.5, spawn: other, max_events: 1}
  h: {dist: constant, value: 2, spawn: hit, max_events: 2}
tasks:
  t: {wait: {all: {r: {request: m}, d: {timeout: 5}}}, service_time: 1, on_interrupt: abandon, on_interrupt_next: again, payload: {e: t_done}}
  again: {wait: {all: {r: {request: m}, d: {timeout: 5}}}, service_time: 1, on_interrupt: abandon, on_interrupt_next: again, payload: {e: t_done}}
  other: {wait: {request: m}, service_time: 1, payload: {e: other_done}}
  hit: {payload: {e: hit}, on: [{interrupt: {task: t}}, {interrupt: {task: again}}]}
run: {until: 40}
"""
    marks = run_marks(tmp_path, text)
    assert [(m[0], m[2]) for m in marks if m[2] in ("t_done", "other_done")] == [
        (3.0, "other_done"),
        (10.0, "t_done"),
    ]


def test_restart_during_the_service_keeps_the_unit_as_at_63efb85(tmp_path):
    """Fix 8 changes only an interrupt that reaches a task while it waits. An
    interrupt during the service of a `restart` task keeps the granted unit and
    draws the service again, as at 63efb85: `t` ends at 7 and `other` at 8."""
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  x: {initial: 1, spawn: holder}
  a: {dist: constant, value: 0.1, spawn: t, max_events: 1}
  b: {dist: constant, value: 0.5, spawn: other, max_events: 1}
  h: {dist: constant, value: 4, spawn: hit, max_events: 1}
tasks:
  holder: {resource: m, service_time: 2, payload: {e: holder_done}}
  t: {wait: {request: m}, service_time: 3, on_interrupt: restart, payload: {e: t_done}}
  other: {wait: {request: m}, service_time: 1, payload: {e: other_done}}
  hit: {payload: {e: hit}, interrupt: {task: t}}
run: {until: 40}
"""
    marks = run_marks(tmp_path, text)
    assert [(m[0], m[2]) for m in marks if m[2] in ("t_done", "other_done")] == [
        (7.0, "t_done"),
        (8.0, "other_done"),
    ]


PATIENCE_AT_A_TIE = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  h: {dist: constant, value: 1, spawn: hog, max_events: 1, first_at: 0}
  k: {dist: constant, value: 1, spawn: kick, max_events: 1, first_at: 1.0}
  a: {dist: constant, value: 1, spawn: job, max_events: 1, first_at: 1.0}
tasks:
  hog: {resource: m, service_time: 5, payload: {e: h}}
  kick: {service_time: 0.6, interrupt: {task: job, cause: cut}, payload: {e: k}}
  job: {BATCHresource: m, service_time: 1, patience: 0.6, on_interrupt: resume, payload: {e: done}}
run: {until: 10}
"""

BATCH_CLOSES_AS_PATIENCE_ENDS = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  a: {dist: constant, value: 1, spawn: first, max_events: 1, first_at: 0}
  b: {dist: constant, value: 1, spawn: second, max_events: 1, first_at: 1.0}
tasks:
  first: {batch: {size: 3, timeout: 1.6, name: s}, resource: m, service_time: 1, patience: 1.2, payload: {e: done}}
  second: {batch: {size: 3, timeout: 1.6, name: s}, resource: m, service_time: 1, patience: 0.6, payload: {e: done}}
run: {until: 10}
"""


@pytest.mark.parametrize(
    "text, key, last",
    [
        (PATIENCE_AT_A_TIE.replace("BATCH", ""), "a-1", (1.6, "reneged")),
        (
            PATIENCE_AT_A_TIE.replace("BATCH", "batch: {size: 2}, "),
            "a-1",
            (1.6, "reneged"),
        ),
        (BATCH_CLOSES_AS_PATIENCE_ENDS, "b-1", (1.6, "started")),
    ],
    ids=["interrupt in a queue", "interrupt in a batch", "batch closes"],
)
def test_patience_used_up_at_a_tie_does_not_stop_the_run(tmp_path, text, key, last):
    """0.6 s of patience from 1.0 ends at 1.6, where 1.0 + 0.6 - 1.6 is -1.1e-16 in
    floats: an interrupt or a batch close at that instant leaves no patience, and the
    next wait starts with none, rather than with a negative delay SimPy refuses."""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None
    statuses = [
        (round(r["sim_ts"], 3), r["value"]["status"])
        for r in outcome.records
        if r["stream_type"] == "event" and r["key"] == key and "status" in r["value"]
    ]
    assert statuses[-1] == last


WAIT_ERRORS = {
    "condition": (
        "containers: {stock: {max_cap: 10, level: 0}}\n"
        "arrivals:\n"
        "  w: {dist: constant, value: 0, spawn: waiter, max_events: 1}\n"
        "  p: {dist: constant, value: 1, spawn: putter, max_events: 1}\n"
        'tasks:\n  waiter: {wait: {condition: "level.stock >= 1 and attr.missing > 0"},'
        " payload: {e: waited}%s}\n"
        "  putter: {put: {container: stock, amount: 1}, payload: {e: put}}\n"
    ),
    "when": (
        "arrivals: {w: {dist: constant, value: 0, spawn: waiter, max_events: 1}}\n"
        'tasks:\n  waiter: {wait: {when: "done_count >= 1 and attr.missing > 0",'
        " of: {a: {timeout: 1}, b: {timeout: 5}}}, payload: {e: waited}%s}\n"
    ),
    "filter": (
        "stores: {box: {max_cap: 10}}\n"
        "arrivals:\n"
        "  g: {dist: constant, value: 0, spawn: waiter, max_events: 1}\n"
        "  p: {dist: constant, value: 1, spawn: putter, max_events: 1}\n"
        'tasks:\n  waiter: {get: {store: box, filter: "item.size > 1"},'
        " payload: {e: got}%s}\n"
        "  putter: {put: {store: box}, payload: {other: 1}}\n"
    ),
}


@pytest.mark.parametrize("place", WAIT_ERRORS)
def test_an_expression_of_a_wait_that_cannot_be_evaluated_fails_the_waiting_task(
    tmp_path, place
):
    """A `condition`, a `when` predicate or a store `filter` is evaluated in another
    task's step or a callback, but the failure is the waiting task's: under stop the
    run stops with the error, under continue the waiter publishes `failed` and goes
    to its `on_fail`, and the task that changed the state does not fail."""
    head = "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
    tail = "  handler: {payload: {e: handled}}\nrun: {until: 10%s}\n"
    body = WAIT_ERRORS[place]
    stopped = run_yaml_text(tmp_path, head + body % "" + tail % "", {})
    assert isinstance(stopped.error, ValueError)
    kept = run_yaml_text(
        tmp_path,
        head + body % ", on_fail: handler" + tail % ", on_unhandled_failure: continue",
        {},
    )
    assert kept.error is None
    events = [
        (r.get("task") or r.get("source"), r["value"])
        for r in kept.records
        if r["stream_type"] == "event"
    ]
    failed = [task for task, value in events if value.get("status") == "failed"]
    assert failed == ["waiter"]
    assert ("handler", {"e": "handled"}) in events
