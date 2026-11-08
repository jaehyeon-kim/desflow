"""Every place that holds an expression is checked at its line, and a failure when
one is read names the file, the line and the place (the task in the Builder)."""

import pytest

from desflow.core.context import SimulationContext

BASE = """simulation: {{sim_id: S, factor: 0, random_seed: 1}}
resources: {{desk: {{current_cap: 1}}}}
containers: {{tank: {{current_cap: 5, level: 3}}}}
signals: {{go: {{{sig}}}}}
arrivals: {{a: {{gap: 1, max_events: 2, spawn: serve{att}}}}}
tasks:
  serve: {{service_time: 1, resource: desk, payload: {{e: 1}}{task}}}
{scen}run: {run}
"""


def _model(sig="", att="", task="", scen="", run="{until: 20}"):
    return BASE.format(sig=sig, att=att, task=task, scen=scen, run=run)


def _load(tmp_path, text):
    path = tmp_path / "m.yaml"
    path.write_text(text)
    return SimulationContext.from_yaml(path)


REGISTRY = 'registry["S.resources.dsk.current_cap"] >= 1'
WHERE = "'level.nope > 0'"

REFUSED = {
    "stop": (_model(run=f"{{until: 20, stop: {{condition: '{REGISTRY}'}}}}"), 8),
    "segment stop": (
        _model(run=f"{{until: 20, segments: [{{stop: {{condition: '{REGISTRY}'}}}}]}}"),
        8,
    ),
    "task on interrupt": (
        _model(task=f", on: [{{interrupt: {{task: serve, where: {WHERE}}}}}]"),
        7,
    ),
    "signal on interrupt": (
        _model(sig=f"on: [{{interrupt: {{task: serve, where: {WHERE}}}}}]"),
        4,
    ),
    "signal on resume where": (
        _model(sig=f"on: [{{resume: {{task: serve, where: {WHERE}}}}}]"),
        4,
    ),
    "signal on resume limit": (
        _model(sig="on: [{resume: {task: serve, limit: 'level.nope'}}]"),
        4,
    ),
    "scenario interrupt": (
        _model(
            scen=f"scenario: [{{at: 1, interrupt: {{task: serve, where: {WHERE}}}}}]\n"
        ),
        8,
    ),
    "attribute": (
        _model(
            att=", attributes: {a: 1, b: {gen: attr, name: a}, c: {gen: attr, name: z}}"
        ),
        5,
    ),
}


@pytest.mark.parametrize("place", sorted(REFUSED))
def test_an_unknown_name_is_refused_at_its_place_at_load(tmp_path, place):
    text, line = REFUSED[place]
    with pytest.raises(Exception) as refused:
        _load(tmp_path, text)
    assert f"m.yaml:{line}:" in str(refused.value), str(refused.value)


@pytest.mark.parametrize(
    "value",
    [
        "{gen: when, condition: 'users.desk > 0', then: 1, else: 2}",
        "{gen: expr, expr: '1 if users.desk > 0 else 2'}",
    ],
)
def test_a_signal_value_that_reads_a_family_is_refused_at_the_signal(tmp_path, value):
    text = (
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "resources: {desk: {family: true, members: 2, current_cap: 1}}\n"
        f"signals: {{ping: {{on: [{{path: S.arrival.a.rate, value: {value}}}]}}}}\n"
        "arrivals: {a: {dist: exponential, rate: 1, spawn: serve}}\n"
        "tasks: {serve: {service_time: 1, payload: {e: 1}}}\n"
        "run: {until: 20}\n"
    )
    with pytest.raises(Exception, match=r"m\.yaml:3: .*reads users\.desk of a family"):
        _load(tmp_path, text)


WEIGHTS = """simulation: {{sim_id: Shop, factor: 0, random_seed: 42}}
resources: {{r: {{current_cap: 1}}}}
services: {{s: {{dist: constant, value: 1}}}}
arrivals: {{orders: {{dist: constant, value: 5, spawn: a, attributes: {{w: 0}}}}}}
tasks:
  a: {{service: s, resource: r, payload: {{e: a}}, {next}}}
  b: {{service: s, payload: {{e: b}}}}
  c: {{service: s, payload: {{e: c}}}}
run: {{until: 20}}
"""


@pytest.mark.parametrize(
    "next_, place",
    [
        (
            "next: {when: [{condition: 'attr.nope > 1', task: b}], default: c}",
            "tasks.a.next.when.0.condition",
        ),
        (
            "next: {when: [{condition: '1 / attr.w > 1', task: b}], default: c}",
            "tasks.a.next.when.0.condition",
        ),
        ("next: {weights: {b: 'attr.w', c: 0}}", "tasks.a.next.weights"),
        ("next: {weights: {b: 'attr.w - 1', c: 1}}", "tasks.a.next.weights"),
    ],
)
def test_a_failure_when_an_expression_is_read_names_the_file_line_and_place(
    tmp_path, next_, place
):
    app = _load(tmp_path, WEIGHTS.format(next=next_))
    with pytest.raises(ValueError) as failed:
        app.run(until=12)
    assert str(failed.value).startswith(f"{tmp_path / 'm.yaml'}:6: {place}"), (
        failed.value
    )


@pytest.mark.parametrize(
    "next_",
    [
        "vars: {k: 2}, next: {weights: {b: 'k', c: 1}}",
        "vars: {k: 2}, next: {when: [{condition: 'k > 1', task: b}], default: c}",
    ],
)
def test_a_weight_reads_the_vars_of_its_task_as_a_condition_does(tmp_path, next_):
    _load(tmp_path, WEIGHTS.format(next=next_)).run(until=12)


def test_a_builder_weight_that_fails_names_the_task():
    app = SimulationContext("S", factor=0)
    app.add_arrival("a", "constant", value=1, spawn="a", attributes={"w": 0})
    app.task(service_time=1, name="a", next={"weights": {"b": "attr.w", "c": 0}})(
        lambda task_id: {}
    )
    app.task(service_time=1, name="b")(lambda task_id: {})
    app.task(service_time=1, name="c")(lambda task_id: {})
    with pytest.raises(ValueError, match="^task 'a' next.weights: weights must add"):
        app.run(until=5)
