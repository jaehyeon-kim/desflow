"""The checks of a model's declarations that `start()` makes.

Each case is a branch of `builder/model_checks.py` that no other test reaches. Most are
refusals that only a builder model gets to, because the blueprint loader refuses
their YAML form first.
"""

import logging

import pytest

from desflow import SimulationContext
from desflow.builder import model_checks
from desflow.builder.model_checks import names_a_member, unmade_member_note
from desflow.core.registry import ParamRejected
from desflow.values import generator


def _job(task_id):
    return {}


def _app():
    return SimulationContext("S", factor=0, random_seed=1)


def _refusal(app, **start):
    with pytest.raises(ValueError) as caught:
        app.start(until=5, **start)
    return str(caught.value)


def test_a_breakdown_of_a_resource_not_added_is_refused():
    app = _app()
    app.add_breakdown(
        "ghost", {"dist": "constant", "value": 1}, {"dist": "constant", "value": 1}
    )
    app.task(name="t", service_time=1)(_job)
    assert _refusal(app) == (
        "a breakdown names resource 'ghost', which is not added; add it with "
        "add_resource()"
    )


@pytest.mark.parametrize(
    ("queue", "task", "message"),
    [
        (
            {"rank": "1"},
            {"seize": {"resource": "r", "priority": 1}},
            "task 't' has a priority on its seize, but resource 'r' ranks its queue "
            "by an expression, which is every request's priority; remove the priority",
        ),
        (
            "fifo",
            {"wait": {"request": {"resource": "r", "priority": 1}}},
            "task 't' has a priority on its request in its wait, but resource 'r' "
            "does not queue by priority; add it with queue='priority'",
        ),
        (
            {"rank": "1"},
            {"wait": {"request": {"resource": "r", "priority": 1}}},
            "task 't' has a priority on its request in its wait, but resource 'r' "
            "ranks its queue by an expression, which is every request's priority; "
            "remove the priority",
        ),
    ],
    ids=["seize-ranked", "request-fifo", "request-ranked"],
)
def test_a_priority_the_queue_cannot_use_is_refused(queue, task, message):
    app = _app()
    app.add_resource("r", current_cap=1, queue=queue)
    app.task(name="t", service_time=1, **task)(_job)
    assert _refusal(app) == message


@pytest.mark.parametrize(
    ("transfer", "message"),
    [
        ({"get": {"store": "s"}}, "task 't' uses store 's', which is not added"),
        (
            {"put": {"container": "c", "amount": 1}},
            "task 't' uses container 'c', which is not added",
        ),
    ],
    ids=["store", "container"],
)
def test_a_transfer_to_a_holder_not_added_is_refused(transfer, message):
    app = _app()
    app.task(name="t", service_time=1, **transfer)(_job)
    assert _refusal(app) == message


@pytest.mark.parametrize(
    ("service_time", "message"),
    [
        (
            "done_count",
            "task 't' reads 'done_count': done_count is read in the when of a wait "
            "group",
        ),
        (
            "foo + 1",
            "task 't' service_time 'foo + 1' uses the name 'foo', which is not one of "
            "the task's vars; write payload.<field>, attr.<name>, level.<store> or "
            "another source for anything else",
        ),
    ],
    ids=["scope", "bare-name"],
)
def test_a_name_a_service_time_cannot_read_is_refused(service_time, message):
    app = _app()
    app.task(name="t", service_time=service_time)(_job)
    assert _refusal(app) == message


def test_an_override_the_registry_refuses_is_left_to_the_run_when_steps_exist():
    """The scenario check skips an override the registry refuses, so the run's own
    refusal of it is the one raised."""
    app = _app()
    app.add_resource("r", current_cap=1, max_cap=2)
    app.add_step(1, "S.resources.r.current_cap", 2)
    app.task(name="t", service_time=1)(_job)
    with pytest.raises(ParamRejected) as caught:
        app.start(until=5, overrides={"S.resources.r.current_cap": "abc"})
    assert caught.value.path == "S.resources.r.current_cap"
    assert str(caught.value) == (
        "resource 'r' needs a whole number of 0 or more for current_cap, got 'abc'"
    )


def test_a_signal_that_fires_from_a_signal_copies_no_task():
    """A signal's `on:` fire from another signal names no task, so no task's
    instances are copied for it."""
    app = _app()
    app.add_signal("other")
    app.add_signal("go", on=[{"fire": {"signal": "other", "from": "go"}}])
    app.task(name="t", service_time=1)(_job)
    app.start(until=5)
    app.close()
    assert app._ends.copied == frozenset()


def test_a_whole_flow_warning_is_logged_once_across_two_starts(tmp_path, caplog):
    path = tmp_path / "model.yaml"
    path.write_text(
        "simulation: {sim_id: W, factor: 0, random_seed: 1}\n"
        "arrivals:\n"
        "  a: {initial: 1, spawn: relay}\n"
        "tasks:\n"
        "  relay: {service_time: 1, next: watch, payload: {e: 1}}\n"
        "  watch: {wait: {running: {task: relay, whole_flow: true}},"
        " payload: {e: 2}}\n"
    )
    app = SimulationContext.from_yaml(path)
    with caplog.at_level(logging.WARNING, logger="desflow.builder.context"):
        for _ in range(2):
            app.start(until=5)
            app.close()
    warned = [r for r in caplog.records if "waits for the whole flow" in r.message]
    assert len(warned) == 1
    assert warned[0].name == "desflow.builder.context"


def test_an_on_action_whose_value_the_path_refuses_is_refused_at_start():
    """The load of a blueprint refuses it at its line; the builder otherwise logged an
    error at each firing and ran on with the old value."""
    app = _app()
    app.add_arrival("a", "exponential", rate=1, spawn="t")
    app.task(name="t", on=[{"path": "S.arrival.a.rate", "value": -1}], service_time=1)(
        _job
    )
    message = _refusal(app)
    assert "the on: action of task 't' sets 'S.arrival.a.rate'" in message
    assert "exponential rate must be greater than 0" in message


def test_an_on_action_with_a_value_the_path_accepts_or_a_generator_starts():
    app = _app()
    app.add_arrival("a", "exponential", rate=1, spawn="t")
    app.task(
        name="t",
        on=[
            {"path": "S.arrival.a.rate", "value": 2},
            {
                "path": "S.arrival.a.rate",
                "value": generator({"gen": "uniform", "low": 1, "high": 2}),
            },
        ],
        service_time=1,
    )(_job)
    app.start(until=5)
    app.close()


def test_a_refused_resource_leaves_its_name_free_for_another_kind():
    app = _app()
    with pytest.raises(ValueError, match="current_cap"):
        app.add_resource("x", current_cap=-1)
    app.add_store("x")


def test_a_refused_signal_leaves_its_name_free_for_another_kind():
    app = _app()
    with pytest.raises(ValueError, match="once"):
        app.add_signal("go", once="yes")
    app.add_resource("go")


def test_a_refused_task_leaves_no_row_target_behind():
    """The `lookup` of a task whose declaration was refused is not a lookup of the
    model."""
    app = _app()
    app.add_store("box")
    with pytest.raises(ValueError, match="declares item"):

        @app.task(
            get={"store": "box", "wait": False},
            vars={"seen": generator({"gen": "lookup", "task": "ghost"})},
        )
        def taker(task_id, item):
            return {}

    assert app._tasks.row_targets == set()


def test_the_check_of_a_member_path_builds_no_name_for_each_member(monkeypatch):
    """A family of 10**7 members would take seconds and over a gigabyte to name."""
    built = []
    real = model_checks.member_name
    monkeypatch.setattr(
        model_checks, "member_name", lambda *a: built.append(a) or real(*a)
    )
    app = _app()
    app.add_resource("desk", family=True, members=1000)
    app.task(name="t", service_time=1)(_job)
    arguments = (app._families, app._holders, "S", app._tasks, app._signals)
    assert names_a_member("S.resources.desk[7].current_cap", *arguments)
    assert "never makes" in unmade_member_note(
        "S.resources.desk[1000].current_cap", *arguments
    )
    assert len(built) <= 2  # the two names of the note's range
