import re

import numpy
import pytest

from desflow import SimulationContext
from desflow.values.models import Expr
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.builder.signals import Signal, SignalFailed


def test_a_signal_failure_nobody_waits_for_is_defused():
    """A `Signal.fail` with no waiter defuses the failed event, so the run goes on."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    closing = Signal(env, "closing", once=False)

    def process():
        yield env.timeout(1)
        closing.fail("shutdown")

    env.process(process())
    env.run(until=5)
    assert closing.ok is False
    assert closing.count == 1
    assert closing.value == "shutdown"


# Batch 11 part 2: a failing fire, a defused failure, a priority, `listen`, `when`


def _fail_at_one(**signal):
    """A run whose one-shot signal fails at 1 with no waiter."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    ev = Signal(env, "ev", once=True, **signal)

    def driver():
        yield env.timeout(1)
        ev.fail("boom", defused=signal.pop("fire_defused", False))

    env.process(driver())
    return env


def test_a_one_shot_failure_with_no_waiter_stops_the_run_unless_it_is_defused():
    """Y9: with nobody waiting, `Signal.fail` of a one-shot signal raises from `run()`
    as a failed event does; `defused=True` on the call or on the signal does not."""
    env = _fail_at_one()
    with pytest.raises(SignalFailed, match="boom"):
        env.run(until=5)
    env = DynamicRealtimeEnvironment(factor=0.0)
    ev = Signal(env, "ev", once=True)

    def driver():
        yield env.timeout(1)
        ev.fail("boom", defused=True)

    env.process(driver())
    env.run(until=5)
    assert ev.ok is False
    _fail_at_one(defused=True).run(until=5)


def test_a_defused_signal_still_fails_the_waiter_that_waits_for_it():
    """Y9: `defused` only stops an unhandled failure from ending the run; a waiter of the
    signal still receives the failure."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    ev = Signal(env, "ev", once=True, defused=True)
    seen = []

    def waiter():
        try:
            yield ev.wait()
        except SignalFailed as error:
            seen.append(error.reason)

    def driver():
        yield env.timeout(1)
        ev.fail("boom")

    env.process(waiter())
    env.process(driver())
    env.run(until=5)
    assert seen == ["boom"]


def test_events_at_one_time_run_by_priority_then_by_creation():
    """Y10: `fire(priority=...)` schedules the event as `env.schedule(event, priority)`
    does, so a lower number runs first, 1 is the default, and a NumPy integer is
    accepted."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    names = ["a", "b", "c", "d"]
    signals = {name: Signal(env, name, once=True) for name in names}
    heard = []

    def waiter(name):
        yield signals[name].wait()
        heard.append(name)

    def driver():
        yield env.timeout(1)
        signals["a"].fire(priority=2)
        signals["b"].fire()
        signals["c"].fire(priority=numpy.int64(0))
        signals["d"].fire(priority=1)

    for name in names:
        env.process(waiter(name))
    env.process(driver())
    env.run(until=5)
    assert heard == ["c", "b", "d", "a"]


@pytest.mark.parametrize(
    "call,error,fragment",
    [
        (dict(urgent=True, priority=0), ValueError, "takes urgent or priority"),
        (dict(priority=1.5), TypeError, "a priority is a whole number, got 1.5"),
        (dict(priority=True), TypeError, "a priority is a whole number, got True"),
    ],
)
def test_a_bad_priority_is_refused_by_name(call, error, fragment):
    """Y10: `urgent` with a `priority`, and a priority that is not a whole number, are
    refused with a named error."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    with pytest.raises(error, match=re.escape(fragment)):
        Signal(env, "ev", once=True).fire(**call)


@pytest.mark.parametrize("priority", [1, 2, 0])
def test_a_priority_needs_a_one_shot_signal(priority):
    """Y10: a signal that re-arms refuses a `priority` whatever its value, as it refuses
    `urgent` and `after`; with no priority it fires."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    again = Signal(env, "again", once=False)
    again.fire()
    with pytest.raises(ValueError, match=re.escape("`priority` needs a one-shot")):
        again.fire(priority=priority)


def _app_with_task(**task):
    app = SimulationContext("S", factor=0, random_seed=42)
    app.add_signal("go", once=True)
    app.add_signal("bad", once=True)
    app.add_arrival_replay("a", [{"time": 0, "i": 0}], spawn="t")
    app.task(name="t", wait={"timeout": 1}, **task)(lambda task_id: {"e": "t"})
    return app


@pytest.mark.parametrize(
    "task,fragment",
    [
        (
            dict(on=[{"fire": {"signal": "bad", "fail": True, "from": "x"}}]),
            "a fire action takes one of them",
        ),
        (
            dict(on=[{"fire": {"signal": "bad", "defused": True}}]),
            "defused is for a failure, so it needs fail: true",
        ),
        (
            dict(on=[{"fire": {"signal": "bad", "urgent": True, "priority": 0}}]),
            "takes urgent or priority",
        ),
        (dict(on=[{"fire": {"signal": "bad", "priority": 1.5}}]), "whole number"),
        (
            dict(on=[{"fire": {"signal": "bad", "fail": "yes"}}]),
            "a fire fail is true or false, got 'yes'",
        ),
        (
            dict(on=[{"fire": {"signal": "bad", "fail": True, "defused": 1}}]),
            "a fire defused is true or false, got 1",
        ),
        (
            dict(on=[{"when": "1 +", "fire": {"signal": "bad"}}]),
            "not a valid expression",
        ),
        (
            dict(on=[{"wehn": "False", "fire": {"signal": "bad"}}]),
            "an on: action takes fire, spawn, interrupt, resume, activate, deactivate, path, value and when, not wehn",
        ),
        (
            dict(listen={"on": [{"fire": {"signal": "go"}}]}),
            "exactly one of signal and handle",
        ),
        (
            dict(
                listen={
                    "signal": "go",
                    "handle": "h",
                    "on": [{"fire": {"signal": "go"}}],
                }
            ),
            "exactly one of signal and handle",
        ),
        (
            dict(
                listen={"signal": "go", "index": 0, "on": [{"fire": {"signal": "go"}}]}
            ),
            "index picks an element of a handle",
        ),
        (
            dict(listen={"signal": "zz", "on": [{"fire": {"signal": "go"}}]}),
            "uses signal 'zz', which is not added",
        ),
        (dict(listen="go"), "listen is {signal: ..., on: [...]}"),
    ],
)
def test_the_builder_refuses_a_wrong_fire_listen_or_when_by_name(task, fragment):
    """Y9 to Y11: each wrong form is refused when the task is added or the run is built,
    as the YAML refusals are."""
    with pytest.raises((ValueError, TypeError), match=re.escape(fragment)):
        _app_with_task(**task).run(until=5)


def test_the_builder_fails_and_defuses_a_signal():
    """Y9: `fire: {fail: true}` fails a one-shot signal nobody waits for, which stops the
    run; `defused` on the fire, or `add_signal(defused=True)`, does not. `defused` must be
    true or false."""
    fire = {"signal": "bad", "fail": True, "value": "oops"}
    with pytest.raises(SignalFailed, match="oops"):
        _app_with_task(on=[{"fire": fire}]).run(until=5)
    _app_with_task(on=[{"fire": {**fire, "defused": True}}]).run(until=5)
    app = _app_with_task(on=[{"fire": fire}])
    app.add_signal("bad2", once=True, defused=True)
    with pytest.raises(ValueError, match="defused is true or false"):
        app.add_signal("bad3", defused="yes")


def test_a_listen_runs_its_entries_before_the_waiters_added_after_it():
    """Y11: the builder's `listen` attaches when the task starts, so its entries run
    before the waiter of a task that starts later, and a `when` that is false skips an
    entry."""
    app = SimulationContext("S", factor=0, random_seed=42)
    app.add_signal("go", once=True)
    app.add_variable("heard", 0)
    app.add_signal("never", once=True)
    seen = []
    app.add_arrival_replay("a", [{"time": 0, "i": 0}], spawn="t")
    app.add_arrival_replay("b", [{"time": 0, "i": 1}], spawn="w")
    app.add_arrival_replay("c", [{"time": 1, "i": 2}], spawn="d")

    @app.task(
        name="t",
        listen={
            "signal": "go",
            "on": [
                {"when": "attr.i == 1", "fire": {"signal": "never"}},
                {"when": "attr.i == 0", "path": "S.variables.heard", "value": 7},
            ],
        },
        wait={"timeout": 1},
    )
    def t(task_id):
        return {"e": "t"}

    @app.task(name="w", wait={"signal": "go"})
    def w(task_id):
        seen.append(app.env.registry.get("S.variables.heard").value)
        return {"e": "w"}

    @app.task(name="d", on=[{"fire": {"signal": "go"}}])
    def d(task_id):
        return {"e": "d"}

    app.run(until=5)
    assert seen == [7]


HEAD = "simulation: {sim_id: S, factor: 0, random_seed: 42}\n"


def _yaml_events(tmp_path, body):
    from tests.conformance.harness import run_yaml_text

    outcome = run_yaml_text(tmp_path, HEAD + body, {})
    found = [
        (round(r["sim_ts"], 3), r["key"], r["task"], r["value"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["value"].get("status") not in ("queued", "started")
    ]
    return outcome, found


@pytest.mark.parametrize(
    "reason,wanted",
    [
        ('""', ""),
        ("7", 7),
        ('{gen: expr, expr: "None"}', "True"),
    ],
)
def test_a_task_fail_reason_is_used_as_given_and_a_generator_is_read_when_it_fails(
    tmp_path, reason, wanted
):
    """Y9: `fail_reason` text or a number is the reason as written, including an empty
    text; a generator is read when the failure happens, and a result of None falls back to
    the condition text."""
    (tmp_path / "a.csv").write_text("time,i\n0,3\n")
    outcome, found = _yaml_events(
        tmp_path,
        f"""
arrivals: {{a: {{replay: {{path: a.csv}}, spawn: f}}}}
tasks:
  f: {{fail: "True", fail_reason: {reason}, on_fail: c, payload: {{e: x}}}}
  c: {{payload: {{e: caught, x: {{gen: expr, expr: "upstream.reason"}}}}}}
""",
    )
    assert outcome.error is None, repr(outcome.error)
    assert [v for *_, task, v in found if task == "c"] == [{"e": "caught", "x": wanted}]
    assert [v["reason"] for *_, task, v in found if task == "f"] == [wanted]


def test_a_fail_reason_that_cannot_be_read_fails_the_task_and_names_the_place(tmp_path):
    """Y9: an error in a generator `fail_reason` fails the task through its `on_fail`, with
    the type of the error and the place in the reason, and does not stop the run."""
    outcome, found = _yaml_events(
        tmp_path,
        """
arrivals: {a: {initial: 1, spawn: f}}
tasks:
  f: {fail: "True", fail_reason: {gen: expr, expr: "1 / 0"}, on_fail: c, payload: {e: x}}
  c: {payload: {e: caught}}
""",
    )
    assert outcome.error is None, repr(outcome.error)
    reason = [v["reason"] for *_, task, v in found if task == "f"][0]
    assert reason.startswith("ValueGenerationError: ")
    assert "tasks.f.fail_reason" in reason
    assert [v for *_, task, v in found if task == "c"] == [{"e": "caught"}]


def test_a_listen_on_a_signal_firing_or_an_event_that_was_processed_fails_the_task(
    tmp_path,
):
    """Y11: `event.callbacks.append` on a processed event raises, so a `listen` on a
    signal whose firing was processed, or on a handle whose event ran, fails the task
    through `_fail`, with the task and the name in the reason."""
    outcome, found = _yaml_events(
        tmp_path,
        """
signals: {go: {once: true}}
arrivals: {a: {initial: 1, spawn: f}, b: {initial: 1, spawn: l}}
tasks:
  f: {payload: null, on: [{fire: {signal: go}}]}
  l: {wait: {timeout: 1}, payload: null, next: l2}
  l2: {listen: {signal: go, on: [{fire: {signal: go}}]}, payload: {e: x}}
""",
    )
    assert [v["reason"] for *_, task, v in found if task == "l2"] == [
        "RuntimeError: task 'l2' listen: signal 'go' was processed already, so it takes no callback"
    ]
    outcome, found = _yaml_events(
        tmp_path,
        """
signals: {s: {once: false}}
arrivals: {a: {initial: 1, spawn: l}}
tasks:
  l: {open: {p: {timeout: 1}}, wait: {timeout: 2}, payload: null, next: m}
  m: {listen: {handle: p, on: [{fire: {signal: s}}]}, payload: {e: x}}
""",
    )
    assert [v["reason"] for *_, task, v in found if task == "m"] == [
        "RuntimeError: task 'm' listen: the event 'p' was processed already, so it takes no callback"
    ]


def test_a_listen_takes_a_family_member_and_an_element_of_an_each_group(tmp_path):
    """Y11: `listen` names a member of a family of signals by a reference, or one element
    of an opened `each` group by `index`."""
    outcome, found = _yaml_events(
        tmp_path,
        """
variables: {n: 0}
signals: {r: {family: true, once: true}}
arrivals: {a: {initial: 1, spawn: l}, b: {initial: 1, spawn: f}}
tasks:
  l: {listen: {signal: {of: r, key: "3"}, on: [{path: S.variables.n, value: {gen: expr, expr: "signal.r[3].value"}}]}, wait: {timeout: 2}, payload: {e: done, x: {gen: expr, expr: "registry['S.variables.n']"}}}
  f: {wait: {timeout: 1}, payload: null, on: [{fire: {signal: {of: r, key: "3"}, value: 9}}]}
""",
    )
    assert [v for *_, task, v in found if task == "l"] == [{"e": "done", "x": 9}]
    outcome, found = _yaml_events(
        tmp_path,
        """
variables: {n: 0}
arrivals: {a: {initial: 1, spawn: l}}
tasks:
  l: {open: {p: {each: 3, branch: {timeout: {after: "each.index + 1"}}}}, listen: {handle: p, index: 1, on: [{path: S.variables.n, value: {gen: expr, expr: "t"}}]}, wait: {timeout: 5}, payload: {e: done, x: {gen: expr, expr: "registry['S.variables.n']"}}}
""",
    )
    assert [v for *_, task, v in found if task == "l"] == [{"e": "done", "x": 2}]


def test_an_unhandled_fail_reason_error_ends_the_run_with_the_error_as_the_cause(
    tmp_path,
):
    """Y9: a `fail_reason` that cannot be read fails the task through `_fail` with the
    error as the cause, and with no `on_fail` the task's own `on:` actions do not run, as
    for any task that fails on an error."""
    outcome, found = _yaml_events(
        tmp_path,
        """
signals: {s: {once: false}}
arrivals: {a: {initial: 1, spawn: f}}
tasks:
  f: {fail: "True", fail_reason: {gen: expr, expr: "1 / 0"}, payload: {e: x}, on: [{fire: {signal: s}}]}
""",
    )
    from desflow.builder.task import TaskFailed

    chain, error = [], outcome.error
    while error is not None:
        chain.append(error)
        error = error.__cause__
    assert isinstance(chain[0], TaskFailed)
    assert type(chain[-1]).__name__ == "ValueGenerationError"
    assert "tasks.f.fail_reason" in str(chain[-1])
    assert not [v for *_, v in found if v.get("e") == "x"]


def test_a_fail_reason_generator_reads_the_payload_the_task_made(tmp_path):
    """Y9: a generator `fail_reason` is read after the service, so it reads the payload
    the task made, as its `fail` condition does."""
    outcome, found = _yaml_events(
        tmp_path,
        """
arrivals: {a: {initial: 1, spawn: f}}
tasks:
  f: {fail: "True", fail_reason: {gen: expr, expr: "'p=' + str(payload.k)"}, on_fail: c, payload: {e: x, k: 5}}
  c: {payload: {e: caught, x: {gen: expr, expr: "upstream.reason"}}}
""",
    )
    assert outcome.error is None, repr(outcome.error)
    assert [v for *_, task, v in found if task == "c"] == [{"e": "caught", "x": "p=5"}]


def test_the_listen_document_of_the_schema_test_runs(tmp_path):
    """`tests/conformance/yaml_only/y11_listen.yaml`, which the schema test reads, loads
    and runs: a listen on a signal and on a handle, a `when`, a defused failure and a
    priority."""
    from pathlib import Path

    from tests.conformance.harness import run_yaml_text

    text = (
        Path(__file__).parents[1] / "conformance" / "yaml_only" / "y11_listen.yaml"
    ).read_text()
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    done = [
        (round(r["sim_ts"], 3), r["value"]["e"])
        for r in outcome.records
        if r["stream_type"] == "event" and "e" in r["value"]
    ]
    assert done == [(1.0, "first"), (2.0, "later")]


def test_a_when_on_the_on_of_a_signal_skips_the_entry_when_it_is_false(tmp_path):
    """Y11: `when` is on every `on:` entry, also on a signal's own `on`, where it reads the
    value of the firing that runs it: of the values 1 and 5 fired at different times, only
    5 passes `signal.s.value > 2`."""
    outcome, found = _yaml_events(
        tmp_path,
        """
variables: {n: 0}
signals: {s: {once: false, on: [{when: "signal.s.value > 2", path: S.variables.n, value: {gen: expr, expr: "registry['S.variables.n'] + 1"}}]}}
arrivals: {a: {initial: 1, spawn: f}}
tasks:
  f: {wait: {timeout: 1}, payload: null, on: [{fire: {signal: s, value: 1}}], next: f2}
  f2: {wait: {timeout: 1}, payload: null, on: [{fire: {signal: s, value: 5}}], next: g}
  g: {wait: {timeout: 1}, payload: {e: n, x: {gen: expr, expr: "registry['S.variables.n']"}}}
""",
    )
    assert outcome.error is None, repr(outcome.error)
    assert [v for *_, task, v in found if task == "g"] == [{"e": "n", "x": 1}]


def test_the_builder_checks_the_names_a_when_reads_in_the_on_of_a_signal():
    """Y11: `add_signal(on=[{"when": ...}])` reads a signal name that is not added, and
    the build refuses it naming the signal and the entry, as for a task."""
    app = SimulationContext("S", factor=0, random_seed=42)
    app.add_signal("s", on=[{"when": "signal.zz.value == 1", "fire": {"signal": "s"}}])
    app.add_arrival_replay("a", [{"time": 0}], spawn="t")
    app.task(name="t", wait={"timeout": 1})(lambda task_id: {"e": "t"})
    with pytest.raises(
        ValueError, match=re.escape("signal 's' on entry 0 when reads signal.zz")
    ):
        app.run(until=5)


@pytest.mark.parametrize(
    "entry,place",
    [
        ({"when": "wait.zz.ok", "fire": {"signal": "s"}}, "signal 's' on entry 0 when"),
        (
            {"fire": {"signal": "s", "value": Expr(gen="expr", expr="wait.zz.ok")}},
            "signal 's' on entry 0 value",
        ),
    ],
)
def test_the_builder_refuses_a_wait_read_in_the_on_of_a_signal(entry, place):
    """A signal's `on` entries run with no wait in scope: a `when` or a generator
    `value` that reads `wait.<branch>` is refused at start, with the text of YAML."""
    app = SimulationContext("S", factor=0, random_seed=42)
    app.add_signal("s", on=[entry])
    app.add_arrival_replay("a", [{"time": 0}], spawn="t")
    app.task(name="t", wait={"timeout": 1})(lambda task_id: {"e": "t"})
    with pytest.raises(
        ValueError,
        match=re.escape(f"{place} reads wait.zz, where no wait is in scope")
        + ".*a signal's on entries run with no wait in scope",
    ):
        app.run(until=5)


def test_the_builder_names_a_signal_and_not_a_task_for_a_family_read_in_its_when():
    """A `users.m[0]` of a resource that is no family, in the `when` of a signal's
    `on`, is refused naming the signal's entry once, and calling nothing a task."""
    app = SimulationContext("S", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_signal("s", on=[{"when": "users.m[0] > 1", "fire": {"signal": "s"}}])
    app.add_arrival_replay("a", [{"time": 0}], spawn="t")
    app.task(name="t", wait={"timeout": 1})(lambda task_id: {"e": "t"})
    with pytest.raises(ValueError) as error:
        app.run(until=5)
    assert str(error.value).startswith(
        "signal 's' on entry 0 when reads users.m[...], but 'm' is not a family"
    ), str(error.value)
