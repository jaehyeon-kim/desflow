"""Holding resources (Batch 8, part 1): `seize`, `release`, `open`, `cancel`, the
`handle` names, `interrupt: {handle}`, `whole_flow`, the refusals each key makes at
load, and the same keys through the builder and the low-level resource."""

import re
from pathlib import Path
from typing import Any, List, Tuple

import numpy as np
import pytest
import simpy

from desflow import SimulationContext, replicate
from desflow.blueprint.loader import BlueprintError
from desflow.resources.resource import DynamicResource
from desflow.values.models import Expr
from tests.conformance.harness import run_builder, run_yaml_text

BASE = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}, p: {current_cap: 1, queue: priority}}
stores: {s: {}}
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
    ("{seize: nosuch, payload: {e: 1}}", "task 't' uses resource 'nosuch'"),
    (
        "{seize: {resource: m, priority: 2}, payload: {e: 1}}",
        "has a priority on its seize, but resource 'm' does not set queue: priority",
    ),
    (
        "{seize: {resource: p, patience: 3}, patience: 4, payload: {e: 1}}",
        "patience is on the task and on its seize",
    ),
    ("{seize: 3, payload: {e: 1}}", "tasks.t.seize"),
    (
        "{seize: {resource: m, nope: 1}, payload: {e: 1}}",
        "tasks.t.seize.nope: unknown key",
    ),
    ("{open: {}, payload: {e: 1}}", "open takes a mapping of names to wait branches"),
    (
        "{open: {a: {timeout: 1, each: x}}, payload: {e: 1}}",
        "a wait branch is one of",
    ),
    ("{open: {'1a': {timeout: 1}}, payload: {e: 1}}", "an open name is read as"),
    (
        "{open: {a: {all: {r: {request: m}}}}, payload: {e: 1}}",
        "opened under its own name, not inside a group",
    ),
    ("{open: {a: {request: nosuch}}, payload: {e: 1}}", "uses resource 'nosuch'"),
    ("{open: {a: {task: nosuch}}, payload: {e: 1}}", "names task 'nosuch'"),
    ("{open: {a: {signal: nosuch}}, payload: {e: 1}}", "names signal 'nosuch'"),
    (
        "{open: {a: {get: {store: nosuch}}}, payload: {e: 1}}",
        "opens a get of store 'nosuch'",
    ),
    ("{open: {a: {request: m}}, priority: 3, payload: {e: 1}}", "does not set queue"),
    ("{seize: m, priority: 3, payload: {e: 1}}", "does not set queue: priority"),
    (
        "{seize: m, balk: 'queue.m > 1', payload: {e: 1}}",
        "needs a resource or an open",
    ),
    ("{release: nosuch, payload: {e: 1}}", "releases 'nosuch'"),
    (
        "{release: {handle: x}, payload: {e: 1}}",
        "release names 'x', which no task opens",
    ),
    (
        "{release: {resource: m, task: nosuch}, payload: {e: 1}}",
        "names task 'nosuch'",
    ),
    ("{release: {resource: m}, payload: {e: 1}}", "tasks.t.release"),
    (
        "{open: {a: {timeout: 1}}, release: {handle: a}, payload: {e: 1}}",
        "opened as a timeout in task 't', and release needs a request",
    ),
    ("{cancel: x, payload: {e: 1}}", "cancel names 'x', which no task opens"),
    (
        "{open: {a: {timeout: 1}}, cancel: a, payload: {e: 1}}",
        "opened as a timeout in task 't', and cancel needs a request, get or put",
    ),
    (
        "{open: {a: {task: u}}, cancel: {handle: a}, payload: {e: 1}}",
        "opened as a task in task 't', and cancel needs a request, get or put",
    ),
    (
        "{open: {a: {request: m}}, interrupt: {handle: a}, payload: {e: 1}}",
        "opened as a request in task 't', and interrupt needs a child flow",
    ),
    (
        "{open: {a: {task: u}}, interrupt: {handle: a, task: u}, payload: {e: 1}}",
        "interrupt takes a task or a handle, and not both",
    ),
    ("{interrupt: {cause: x}, payload: {e: 1}}", "has neither"),
    (
        "{open: {a: {task: u}}, interrupt: {handle: a, where: 'queue.m > 0'}, "
        "payload: {e: 1}}",
        "takes no where with a handle",
    ),
    ("{interrupt: {handle: zz}, payload: {e: 1}}", "interrupt names 'zz'"),
    ("{wait: {handle: zz}, payload: {e: 1}}", "wait names 'zz', which no task opens"),
    (
        "{payload: {e: 1, x: {gen: expr, expr: 'handle.zz.alive'}}}",
        "handle.zz names no handle",
    ),
    (
        "{service_time: {gen: expr, expr: '1 if handle.zz.ok else 2'}, payload: {e: 1}}",
        "handle.zz names no handle",
    ),
    (
        "{open: {a: {task: u, args: {k: {gen: expr, expr: 'handle.zz.ok'}}}}, "
        "payload: {e: 1}}",
        "handle.zz names no handle",
    ),
    (
        "{open: {a: {timeout: 1}}, payload: {e: 1, x: {gen: expr, expr: 'handle.a'}}}",
        "write handle.<name>.triggered, .processed, .ok, .value, .alive",
    ),
    (
        "{open: {a: {timeout: 1}}, payload: {e: 1, x: {gen: expr, expr: 'handle.a.no'}}}",
        "write handle.<name>.triggered",
    ),
    (
        "{wait: {running: {task: u, whole_flow: 3}}, payload: {e: 1}}",
        "whole_flow of a wait running branch is true or false",
    ),
    (
        "{wait: {signal: sig, whole_flow: true}, payload: {e: 1}}",
        "a wait branch is one of",
    ),
]


@pytest.mark.parametrize("task,message", REFUSED)
def test_a_bad_holding_key_is_refused_at_load_with_its_line(tmp_path, task, message):
    with pytest.raises(BlueprintError, match=re.escape(message)) as error:
        load(tmp_path, task)
    assert "model.yaml:7" in str(error.value)


@pytest.mark.parametrize(
    "task",
    [
        "{seize: m, payload: {e: 1}}",
        "{seize: {resource: p, priority: 2, preempt: true, patience: 3}, payload: {e: 1}}",
        "{open: {a: {request: p}}, priority: 3, wait: {handle: a}, payload: {e: 1}}",
        "{open: {a: {task: u, args: {x: 1}}}, balk: 'queue.m > 3', payload: {e: 1}}",
        "{open: {a: {request: m}}, cancel: a, release: {handle: a}, payload: {e: 1}}",
        "{open: {a: {task: u}}, interrupt: {handle: a, cause: x}, payload: {e: 1}}",
        "{open: {g: {any: {x: {timeout: 1}, y: {handle: g}}}}, payload: {e: 1}}",
        "{wait: {running: {task: u, whole_flow: true}}, payload: {e: 1}}",
        "{release: {resource: m, task: u, where: 'process.instance == 0'}, "
        "payload: {e: 1}}",
    ],
)
def test_a_good_holding_key_loads(tmp_path, task):
    if "g: {any" in task:
        # a handle in a group refers to a name opened before it
        task = "{open: {y: {timeout: 2}, g: {any: {x: {timeout: 1}, z: {handle: y}}}}, payload: {e: 1}}"
    load(tmp_path, task)


def test_an_open_name_used_in_a_group_must_be_opened_first(tmp_path):
    with pytest.raises(BlueprintError, match="no task opens"):
        load(
            tmp_path,
            "{open: {g: {any: {x: {timeout: 1}, z: {handle: nope}}}}, payload: {e: 1}}",
        )


def test_a_handle_that_two_tasks_open_differently_is_refused_for_a_use_that_needs_one(
    tmp_path,
):
    text = BASE.replace(
        "  u: {payload: {e: u}}",
        "  u: {open: {a: {timeout: 3}}, payload: {e: u}}",
    ).replace("TASK", "{open: {a: {request: m}}, cancel: a, payload: {e: 1}}")
    path = tmp_path / "two.yaml"
    path.write_text(text)
    with pytest.raises(BlueprintError, match="opened as a timeout in task 'u'"):
        SimulationContext.from_yaml(path)


# ------------------------------------------------------------------ the builder


def builder() -> SimulationContext:
    app = SimulationContext("S", factor=0)
    app.add_resource("m", current_cap=1)
    app.add_resource("p", current_cap=1, queue="priority")
    app.add_store("s")
    return app


def test_the_builder_refuses_what_the_blueprint_refuses():
    app = builder()
    with pytest.raises(ValueError, match="patience is on the task and on its seize"):
        app.task(name="a", seize={"resource": "p", "patience": 2}, patience=3)
    with pytest.raises(ValueError, match="interrupt takes a task or a handle"):
        app.task(name="b", interrupt={"task": "a", "handle": "h"})
    with pytest.raises(ValueError, match="interrupt takes a task or a handle"):
        app.task(name="c", interrupt={"cause": "x"})
    with pytest.raises(ValueError, match="open takes a mapping"):
        app.task(name="d", open={})
    with pytest.raises(ValueError, match="cancel is a name"):
        app.task(name="e", cancel=3)
    with pytest.raises(ValueError, match="needs a resource, a seize or a request"):
        app.task(name="f", priority=1)
    with pytest.raises(ValueError, match="needs a wait, a seize, a resource or a get"):
        app.task(name="g", patience=1)
    with pytest.raises(ValueError, match="needs a resource or an open"):
        app.task(name="h", balk="queue.m > 1")


def named_tasks(app: SimulationContext, **tasks: dict) -> None:
    for name, keys in tasks.items():
        app.task(name=name, **keys)(lambda task_id, **_kw: {"e": name})


@pytest.mark.parametrize(
    "keys,message",
    [
        ({"seize": "nosuch"}, "task 't' uses resource 'nosuch'"),
        ({"open": {"a": {"request": "nosuch"}}}, "uses resource 'nosuch'"),
        ({"open": {"a": {"get": {"store": "nosuch"}}}}, "gets from store 'nosuch'"),
        ({"open": {"a": {"task": "nosuch"}}}, "names task 'nosuch'"),
        ({"release": "nosuch"}, "uses resource 'nosuch'"),
        ({"release": {"resource": "m", "task": "nosuch"}}, "names task 'nosuch'"),
        ({"seize": {"resource": "m", "priority": 1}}, "does not queue by priority"),
        ({"cancel": "x"}, "cancel names 'x', which no task opens"),
        ({"wait": {"handle": "x"}}, "wait names 'x', which no task opens"),
        ({"release": {"handle": "x"}}, "release names 'x', which no task opens"),
        ({"interrupt": {"handle": "x"}}, "interrupt names 'x', which no task opens"),
        (
            {"open": {"x": {"timeout": 5}}, "cancel": "x"},
            "'x' is opened as a timeout in task 't', and cancel needs",
        ),
        (
            {"open": {"x": {"timeout": 5}}, "release": {"handle": "x"}},
            "'x' is opened as a timeout in task 't', and release needs",
        ),
        (
            {"open": {"x": {"timeout": 5}}, "interrupt": {"handle": "x"}},
            "'x' is opened as a timeout in task 't', and interrupt needs",
        ),
        (
            {"open": {"y": {"timeout": 5}}, "fail": "handle.x.ok"},
            "names 'x', which no task opens",
        ),
        (
            {"vars": {"v": Expr(gen="expr", expr="handle.x.triggered")}},
            "vars names 'x', which no task opens",
        ),
        (
            {"vars": {"v": {"gen": "expr", "expr": "handle.x.triggered"}}},
            "vars names 'x', which no task opens",
        ),
        (
            {"vars_once": {"v": Expr(gen="expr", expr="handle.x.triggered")}},
            "vars_once names 'x', which no task opens",
        ),
        (
            {"finish_vars": {"v": Expr(gen="expr", expr="handle.x.triggered")}},
            "finish_vars names 'x', which no task opens",
        ),
        (
            {"on_start": {"v": Expr(gen="expr", expr="handle.x.triggered")}},
            "on_start names 'x', which no task opens",
        ),
        (
            {
                "on": [
                    {
                        "path": "S.resources.m.current_cap",
                        "value": Expr(gen="expr", expr="1 if handle.x.ok else 2"),
                    }
                ]
            },
            "on names 'x', which no task opens",
        ),
        (
            {"interrupt": {"task": "t", "cause": Expr(gen="expr", expr="handle.x.ok")}},
            "interrupt names 'x', which no task opens",
        ),
        (
            {"set": {"k": Expr(gen="expr", expr="handle.x.ok")}},
            "set names 'x', which no task opens",
        ),
        (
            {"set_args": {"k": Expr(gen="expr", expr="1 if handle.x.ok else 2")}},
            "set_args names 'x', which no task opens",
        ),
        (
            {"service_time": Expr(gen="expr", expr="1 if handle.x.ok else 2")},
            "service_time names 'x', which no task opens",
        ),
        (
            {
                "put": {
                    "store": "s",
                    "item": Expr(gen="expr", expr="1 if handle.x.ok else 2"),
                }
            },
            "put.item names 'x', which no task opens",
        ),
        (
            {
                "wait": {
                    "task": "u",
                    "args": {"k": Expr(gen="expr", expr="1 if handle.x.ok else 2")},
                }
            },
            "wait names 'x', which no task opens",
        ),
        (
            {
                "open": {
                    "y": {
                        "task": "u",
                        "args": {"k": Expr(gen="expr", expr="1 if handle.x.ok else 2")},
                    }
                }
            },
            "open names 'x', which no task opens",
        ),
        (
            {
                "spawn": {
                    "task": "t",
                    "args": {"k": Expr(gen="expr", expr="1 if handle.x.ok else 2")},
                }
            },
            "spawn names 'x', which no task opens",
        ),
    ],
)
def test_the_builder_checks_the_names_before_the_run(keys, message):
    app = builder()
    named_tasks(app, t=keys)
    with pytest.raises(ValueError, match=re.escape(message)):
        app.run(until=1)


def run_records(app: SimulationContext, until: float) -> List[Tuple[Any, ...]]:
    records = run_builder(app, until)
    assert isinstance(records, list)
    return [
        (round(r["sim_ts"], 3), r["key"], r["value"]["e"])
        for r in records
        if r["stream_type"] == "event" and "e" in r["value"]
    ]


def test_the_builder_seizes_and_releases_as_the_blueprint_does(tmp_path):
    yaml = """
simulation: {sim_id: H, factor: 0}
resources: {m: {current_cap: 1, queue: priority}}
arrivals: {a: {dist: constant, value: 1000, initial: 3, spawn: take}}
tasks:
  take: {seize: {resource: m, priority: 5, patience: 9}, payload: {e: took}, next: work}
  work: {service_time: 2.5, payload: {e: worked}, release: m}
run: {until: 30}
"""
    outcome = run_yaml_text(tmp_path, yaml, {})
    expected = [
        (round(r["sim_ts"], 3), r["key"], r["value"]["e"])
        for r in outcome.records
        if r["stream_type"] == "event" and "e" in r["value"]
    ]
    assert [e[2] for e in expected].count("worked") == 3
    # NumPy numbers stand for the plain ones in the priority and the patience
    for priority, patience in [(5, 9), (np.int64(5), np.float64(9.0)), (5.0, 9)]:
        app = SimulationContext("H", factor=0)
        app.add_resource("m", current_cap=1, queue="priority")
        app.add_arrival("a", dist="constant", value=1000, initial=3, spawn="take")
        app.task(
            name="take",
            seize={"resource": "m", "priority": priority, "patience": patience},
            next="work",
        )(lambda task_id: {"e": "took"})
        app.task(name="work", service_time=2.5, release="m")(
            lambda task_id: {"e": "worked"}
        )
        assert run_records(app, 30) == expected


def test_numpy_numbers_are_accepted_by_seize_and_open_places():
    app = SimulationContext("H", factor=0)
    app.add_resource("m", current_cap=1, queue="priority")
    app.add_arrival("a", dist="constant", value=1000, initial=2, spawn="take")
    app.task(
        name="take",
        seize={"resource": "m", "priority": np.float32(2), "preempt": True},
        patience=np.int64(7),
        next="work",
    )(lambda task_id: {"e": "took"})
    app.task(name="work", service_time=np.float64(2), release="m")(
        lambda task_id: {"e": "worked"}
    )
    labels = [m[2] for m in run_records(app, 20)]
    assert labels.count("worked") == 2


# ------------------------------------------------------------- engine behaviour


def test_a_handle_read_by_a_task_that_no_task_of_the_flow_opened_fails_the_run(
    tmp_path,
):
    """Handles belong to the instance and its `next` chain: a task started for another
    entity does not see them, and the run fails with a message that names the task and
    the name."""
    text = """
simulation: {sim_id: S, factor: 0}
arrivals: {a: {dist: constant, value: 1, spawn: opener, max_events: 1}, b: {dist: constant, value: 2, spawn: reader, max_events: 1}}
tasks:
  opener: {open: {q: {timeout: 50}}, service_time: 5, payload: {e: opened}}
  reader: {payload: {e: read, x: {gen: expr, expr: "handle.q.alive"}}}
run: {until: 10}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is not None
    assert "no event is open under the name 'q'" in str(outcome.error)
    assert "task 'reader'" in str(outcome.error)


def test_an_interrupt_of_a_child_that_has_ended_fails_the_run(tmp_path):
    """`interrupt: {handle}` of a child flow that has ended raises, as SimPy's
    `process.interrupt()` does."""
    text = """
simulation: {sim_id: S, factor: 0}
arrivals: {a: {dist: constant, value: 1, spawn: parent, max_events: 1}}
tasks:
  parent: {open: {c: {task: child}}, service_time: 3, payload: {e: waited}, next: poke}
  poke: {interrupt: {handle: c, cause: x}, payload: {e: poked}}
  child: {service_time: 1, payload: {e: child_done}}
run: {until: 10}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert isinstance(outcome.error, RuntimeError)
    assert "has terminated and cannot be interrupted" in str(outcome.error)

    simpy_env = simpy.Environment()
    ended = simpy_env.process(iter(()) if False else (x for x in ()))
    simpy_env.run()
    with pytest.raises(RuntimeError, match="has terminated and cannot be interrupted"):
        ended.interrupt("x")


def test_a_release_of_a_resource_the_flow_does_not_hold_frees_nothing(tmp_path):
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals: {a: {dist: constant, value: 2, spawn: other, max_events: 1}, b: {dist: constant, value: 1, spawn: hold, max_events: 1}}
tasks:
  hold: {resource: m, service_time: 5, payload: {e: held}}
  other: {release: m, payload: {e: nothing, x: {gen: expr, expr: "users.m"}}}
run: {until: 10}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    nothing = [
        r["value"]["x"]
        for r in outcome.records
        if r["stream_type"] == "event" and r["value"].get("e") == "nothing"
    ]
    assert nothing == [1]


def test_a_flow_that_leaves_by_balk_renege_or_the_end_of_the_run_leaves_no_request(
    tmp_path,
):
    """A balk, a renege and the end of the run leave no request in the line and no
    unit held by a flow that gave up."""
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue_limit: 1}}
arrivals: {a: {dist: constant, value: 1, spawn: take, max_events: 6}}
tasks:
  take: {seize: {resource: m, patience: 1}, payload: {e: took}, next: work}
  work: {service_time: 4, payload: {e: worked}, release: m}
run: {until: 40}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    resource = outcome.app_or_env._run.holders.resources["m"]
    assert not resource.queue and not resource.users
    statuses = {
        r["value"].get("status") for r in outcome.records if r["stream_type"] == "event"
    }
    assert {"reneged", "balked"} <= statuses


def test_replicate_runs_a_model_that_seizes_and_releases(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(
        """
simulation: {sim_id: S, factor: 0, random_seed: 3}
resources: {m: {current_cap: 1}}
arrivals: {a: {dist: exponential, rate: 1, spawn: take}}
services: {s: {dist: exponential, rate: 2}}
tasks:
  take: {seize: m, payload: {event_type: took}, next: work}
  work: {service: s, payload: {event_type: worked}, release: m}
stats: typical
run: {until: 100}
"""
    )
    result = replicate(str(path), 3, seed=1, measures=["resources.m.utilization_avg"])
    values = result.values["resources.m.utilization_avg"]
    assert len(values) == 3 and all(0 < v <= 100 for v in values)
    assert len(set(values)) == 3


# ------------------------------------------------------------- the low-level path


def test_a_preemption_of_a_unit_that_an_ended_process_holds_raises(
    env, registry, sample_params
):
    """A process ends while it holds a unit; a request that would preempt it raises
    SimPy's RuntimeError in the process that asks, as a `PreemptiveResource` does."""
    registry.register_sim_parameter(sample_params)
    registry.update("Shop.resources.packers.current_cap", 1)
    res = DynamicResource(env, "Shop", "packers", queue="priority", preempt=True)
    holder_request = []

    def holder():
        request = res.request(priority=5)
        holder_request.append(request)
        yield request

    env.process(holder())
    env.run(until=1)
    assert res.users == holder_request
    with pytest.raises(RuntimeError, match="has terminated and cannot be interrupted"):
        res.request(priority=0, preempt=True)
    assert holder_request[0].preempted is True

    plain = simpy.Environment()
    unit = simpy.PreemptiveResource(plain, 1)

    def plain_holder():
        yield unit.request(priority=5)

    plain.process(plain_holder())
    plain.run(until=1)
    with pytest.raises(RuntimeError, match="has terminated and cannot be interrupted"):
        unit.request(priority=0)


def test_a_preempted_request_is_marked_and_a_live_holder_is_interrupted(
    env, registry, sample_params
):
    registry.register_sim_parameter(sample_params)
    registry.update("Shop.resources.packers.current_cap", 1)
    res = DynamicResource(env, "Shop", "packers", queue="priority", preempt=True)
    seen = []

    def holder():
        request = res.request(priority=5)
        yield request
        try:
            yield env.timeout(10)
        except simpy.Interrupt as interrupt:
            seen.append((env.now, request.preempted, interrupt.cause.usage_since))

    env.process(holder())
    env.run(until=2)

    def taker():
        with res.request(priority=0, preempt=True) as request:
            yield request

    env.process(taker())
    env.run(until=5)
    assert seen == [(2, True, 0)]
    plain = res.request(priority=3)
    assert plain.preempted is False


def test_explain_counts_a_seized_and_an_opened_resource_as_used_and_leaves_them_out(
    tmp_path,
):
    """The queueing formulas hold when a task holds a unit through its own service, so
    a task that seizes or opens a request is named among the features they leave out,
    and its resources have users."""
    from typer.testing import CliRunner

    from desflow.cli import app

    path = tmp_path / "m.yaml"
    path.write_text(
        """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}, k: {current_cap: 1}}
arrivals: {a: {dist: exponential, rate: 0.1, spawn: take}}
services: {s: {dist: exponential, rate: 1}}
tasks:
  take: {seize: m, open: {q: {request: k}}, payload: {event_type: took}, next: work}
  work: {service: s, payload: {event_type: worked}, release: m}
run: {until: 100}
"""
    )
    result = CliRunner().invoke(app, ["explain", str(path)])
    assert result.exit_code == 0, result.output
    assert "it has seize, open" in result.output or "seize, open" in result.output
    assert "no task uses it" not in result.output


def test_a_task_that_only_open_starts_counts_as_started_for_a_lookup(tmp_path):
    """`open: {task}` starts a child flow, so a `lookup` of that task is not refused
    for being a task nothing starts."""
    path = tmp_path / "m.yaml"
    path.write_text(
        """
simulation: {sim_id: S, factor: 0}
arrivals: {a: {dist: constant, value: 1000, initial: 1, spawn: top}, b: {dist: constant, value: 1000, initial: 1, first_at: 2, spawn: reader}}
tasks:
  top: {open: {c: {task: child}}, service_time: 3, payload: {e: top}}
  child: {service_time: 1, payload: {e: child, x: 5}}
  reader: {payload: {e: read, x: {gen: lookup, task: child, path: x}}}
run: {until: 10}
"""
    )
    outcome = SimulationContext.from_yaml(path)
    assert outcome is not None


@pytest.mark.parametrize(
    "interrupt,message",
    [
        ({"handle": "h"}, "a handle is the interrupt key of a task"),
        ({"where": "attr.x > 1"}, "names the task to interrupt"),
        ({}, "names the task to interrupt"),
    ],
)
def test_the_builder_refuses_a_scenario_interrupt_without_a_task(interrupt, message):
    """The Builder refuses `add_step(interrupt={"handle": ...})` as the blueprint does,
    in place of a `KeyError` when the step is due."""
    app = builder()
    with pytest.raises(ValueError, match=re.escape(message)):
        app.add_step(1, interrupt=interrupt)


WHOLE_FLOW_TASKS = [
    # (tasks, warned)
    (
        "t: {payload: {e: t}, next: c}\n  c: {wait: {task: u}, payload: {e: c}}\n  u: {wait: {running: {task: t, whole_flow: true}}, payload: {e: u}}",
        True,
    ),
    (
        "t: {payload: {e: t}, next: c}\n  c: {payload: {e: c}}\n  u: {wait: {running: {task: t, whole_flow: true}}, payload: {e: u}}",
        False,
    ),
    (
        "t: {payload: {e: t}, next: u}\n  u: {wait: {running: {task: t, whole_flow: true}}, payload: {e: u}}",
        True,
    ),
    (
        "t: {payload: {e: t}, next: v}\n  v: {payload: {e: v}, next: u}\n  u: {wait: {running: {task: t, whole_flow: true}}, payload: {e: u}}",
        True,
    ),
    (
        "t: {wait: {running: {task: t, whole_flow: true}}, payload: {e: t}, next: t}",
        True,
    ),
    (
        "t: {payload: {e: t}, next: u}\n  u: {wait: {running: {task: t}}, payload: {e: u}}",
        False,
    ),
    (
        "t: {payload: {e: t}}\n  u: {wait: {running: {task: t, whole_flow: true}}, payload: {e: u}}",
        False,
    ),
    (
        "t: {open: {h: {task: c}}, payload: {e: t}, next: d}\n  d: {wait: {handle: h}, payload: {e: d}}\n  c: {wait: {running: {task: t, whole_flow: true}}, payload: {e: c}}",
        True,
    ),
    (
        "t: {open: {h: {task: c}}, payload: {e: t}}\n  c: {wait: {running: {task: t, whole_flow: true}}, payload: {e: c}}",
        False,
    ),
    (
        "t: {payload: {e: t}, spawn: {task: u}}\n  u: {wait: {running: {task: t, whole_flow: true}}, payload: {e: u}}",
        False,
    ),
    (
        "t: {wait: {running: {task: u, whole_flow: true}}, payload: {e: t}, next: u}\n  u: {payload: {e: u}}",
        False,
    ),
]


@pytest.mark.parametrize("tasks,warned", WHOLE_FLOW_TASKS)
def test_a_whole_flow_wait_that_can_wait_for_its_own_flow_is_warned_of(
    tmp_path, caplog, tasks, warned
):
    """A flow is one process, so `running: {task: t, whole_flow: true}` in a task that
    `t` leads to waits for the flow it runs in. The load logs one warning that names
    both tasks; a wait for a flow that does not lead to the waiting task logs none."""
    text = (
        "simulation: {sim_id: S, factor: 0}\n"
        "arrivals: {a: {dist: constant, value: 1, spawn: t}}\n"
        f"tasks:\n  {tasks}\nrun: {{until: 3}}\n"
    )
    path = tmp_path / "model.yaml"
    path.write_text(text)
    with caplog.at_level("WARNING", logger="desflow.core.context"):
        app = SimulationContext.from_yaml(path)
        app.run(until=3)
    found = [r.getMessage() for r in caplog.records if "whole flow" in r.getMessage()]
    assert len(found) == (1 if warned else 0)
    if warned:
        assert "waits for the whole flow of task" in found[0]
        assert "which ends only after the wait does" in found[0]


def test_the_builder_warns_of_a_whole_flow_wait_for_its_own_flow(caplog):
    app = SimulationContext("S", factor=0)
    app.add_arrival("a", dist="constant", value=1, spawn="t")
    named_tasks(
        app,
        t={"next": "u"},
        u={"wait": {"running": {"task": "t", "whole_flow": True}}},
    )
    with caplog.at_level("WARNING", logger="desflow.core.context"):
        app.run(until=3)
    assert any(
        "task 'u' waits for the whole flow of task 't', and 't' leads to 'u'"
        in r.getMessage()
        for r in caplog.records
    )


RELEASED_HOLDS = {
    "seize": "{seize: m, service_time: 5, payload: {e: held}, release: m}",
    "resource": "{resource: m, service_time: 5, payload: {e: held}}",
    "wait request": "{wait: {request: m}, service_time: 5, payload: {e: held}}",
}


@pytest.mark.parametrize("form", RELEASED_HOLDS)
def test_a_holder_whose_unit_another_flow_released_does_not_ask_again_when_interrupted(
    tmp_path, form
):
    """The holder is not told of a `release: {resource, task}`, so an interrupt with
    `resume` goes on with the service and asks for nothing: the flow that took the unit
    is not preempted, as under `seize`."""
    text = f"""
simulation: {{sim_id: S, factor: 0}}
resources: {{m: {{current_cap: 1, queue: priority, preempt: true}}}}
arrivals:
  h: {{replay: {{path: h.csv}}, spawn: hold}}
  f: {{replay: {{path: f.csv}}, spawn: free_it}}
  w: {{replay: {{path: w.csv}}, spawn: want}}
  i: {{replay: {{path: i.csv}}, spawn: poke}}
tasks:
  hold: {RELEASED_HOLDS[form][:-1]}, on_interrupt: resume}}
  free_it: {{payload: {{e: freed}}, release: {{resource: m, task: hold}}}}
  want: {{resource: m, priority: 5, service_time: 1, payload: {{e: want}}}}
  poke: {{payload: {{e: poke}}, interrupt: {{task: hold}}}}
run: {{until: 100}}
"""
    for name, at in (("h", 0), ("f", 1), ("w", 1.5), ("i", 2)):
        (tmp_path / f"{name}.csv").write_text(f"time\n{at}\n")
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    marks = [
        (round(r["sim_ts"], 2), r["key"], r["value"].get("e") or r["value"]["status"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and (
            r["value"].get("e") or r["value"].get("status") in ("preempted", "resumed")
        )
    ]
    assert not [m for m in marks if m[2] == "preempted"]
    assert (2.5, "w-1", "want") in marks and (5.0, "h-1", "held") in marks


@pytest.mark.parametrize("form", RELEASED_HOLDS)
def test_a_holder_whose_unit_was_released_goes_on_through_a_breakdown_and_an_interrupt(
    tmp_path, form
):
    """The unit was freed at 1 and the resource breaks down at 1.5 for 20 s, so the
    holder owned no unit when it broke. An interrupt at 2 with `resume` goes on with
    the service in each form, and `held` is published at 5."""
    text = f"""
simulation: {{sim_id: S, factor: 0}}
resources: {{m: {{current_cap: 1, queue: priority, preempt: true, breakdown: {{time_to_failure: {{dist: constant, value: 1.5}}, repair: {{dist: constant, value: 20}}}}}}}}
arrivals:
  h: {{replay: {{path: h.csv}}, spawn: hold}}
  f: {{replay: {{path: f.csv}}, spawn: free_it}}
  i: {{replay: {{path: i.csv}}, spawn: poke}}
tasks:
  hold: {RELEASED_HOLDS[form][:-1]}, on_interrupt: resume}}
  free_it: {{payload: {{e: freed}}, release: {{resource: m, task: hold}}}}
  poke: {{payload: {{e: poke}}, interrupt: {{task: hold}}}}
run: {{until: 100}}
"""
    for name, at in (("h", 0), ("f", 1), ("i", 2)):
        (tmp_path / f"{name}.csv").write_text(f"time\n{at}\n")
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    held = [
        round(r["sim_ts"], 2)
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["key"] == "h-1"
        and r["value"].get("e") == "held"
    ]
    assert held == [5.0]
