"""Builder forms that a blueprint already had: the place of a wait's `each` count, and the
arrival `vars` and generator `key` of `add_arrival`."""

import json

import pytest

from desflow import SimulationContext
from desflow.values import generator, Template
from tests.conformance.harness import RecordingEgress, run_builder

YAML = """
simulation: {sim_id: S, factor: 0, random_seed: 7}
run: {until: 30}
arrivals: {a: {dist: constant, value: 1, spawn: t}}
tasks:
  t: TASK
  leaf: {payload: {n: 1}}
"""
COUNT = {"gen": "integer", "bounds": [1, 4]}
EACH = {"each": COUNT, "branch": {"task": "leaf"}}
WAIT_ON_H = {"all": {"h": {"handle": "h"}}}


def _events(records):
    return [
        (r["key"], r.get("task"), r["sim_ts"])
        for r in records
        if r["stream_type"] == "event"
    ]


def _as_builder(value):
    """The Builder form of a YAML value: a mapping with `gen` is a generator."""
    if isinstance(value, dict):
        if "gen" in value:
            return generator(dict(value))
        return {k: _as_builder(v) for k, v in value.items()}
    return value


@pytest.mark.parametrize(
    "task",
    [
        {"wait": EACH},
        {"wait": {"all": {"x": EACH, "y": {"timeout": 0}}}},
        {"wait": {"any": {"x": EACH, "y": {"timeout": 100}}}},
        {"open": {"h": EACH}, "wait": WAIT_ON_H},
        {
            "open": {
                "h": {"timeout": {"after": {"gen": "uniform", "low": 0.1, "high": 0.9}}}
            },
            "wait": WAIT_ON_H,
        },
    ],
    ids=[
        "wait each",
        "wait each under all",
        "wait each under any",
        "open each",
        "open timeout after a generator",
    ],
)
def test_builder_wait_and_open_draw_from_the_stream_of_the_blueprint_place(
    task, tmp_path
):
    (tmp_path / "model.yaml").write_text(
        YAML.replace("TASK", json.dumps({**task, "payload": {}}))
    )
    recorder = RecordingEgress()
    from_yaml = SimulationContext.from_yaml(tmp_path / "model.yaml")
    from_yaml.add_egress(recorder)
    from_yaml.run()
    app = SimulationContext("S", factor=0, random_seed=7)
    app.add_arrival("a", dist="constant", value=1, spawn="t")
    app.task(name="t", **_as_builder(task))(
        lambda task_id: Template({}, name="tasks.t")
    )
    app.task(name="leaf")(lambda task_id: Template({"n": 1}, name="tasks.leaf"))
    built = run_builder(app, 30)
    assert len(_events(recorder.records)) > 20
    assert _events(built) == _events(recorder.records)


CLASH = {
    "any": {
        "e": {"each": 2, "group": "all", "branch": {"timeout": 1}},
        "e[0]": {"timeout": 5},
    }
}


def test_a_wait_branch_named_like_an_element_of_an_each_is_refused_at_load(tmp_path):
    from desflow.blueprint.loader import BlueprintError

    text = (
        "simulation: {sim_id: S, factor: 0, random_seed: 7}\n"
        "arrivals: {a: {initial: 1, spawn: t}}\n"
        "tasks:\n"
        "  t:\n"
        "    wait:\n"
        "      any:\n"
        "        e: {each: 2, group: all, branch: {timeout: 1}}\n"
        "        'e[0]': {timeout: 5}\n"
        "    payload: {}\n"
    )
    path = tmp_path / "m.yaml"
    path.write_text(text)
    line = next(i for i, row in enumerate(text.splitlines(), 1) if "wait:" in row)
    with pytest.raises(BlueprintError) as caught:
        SimulationContext.from_yaml(path)
    message = str(caught.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert (
        "wait branch name 'e[0]' is the name of an element of the each group 'e'"
        in message
    ), message


def test_the_builder_refuses_a_wait_branch_named_like_an_element_of_an_each():
    app = SimulationContext("S", factor=0)
    with pytest.raises(ValueError, match="wait branch name 'e\\[0\\]' is the name"):
        app.task(name="t", wait=CLASH)(lambda task_id: {})
    # A branch with another bracketed name is accepted
    other = {"any": {**CLASH["any"]}}
    other["any"]["f[0]"] = other["any"].pop("e[0]")
    app.task(name="t", wait=other)(lambda task_id: {})


def _players(**extra):
    app = SimulationContext("S", factor=0, random_seed=3)
    app.add_arrival(
        "a",
        gap=1,
        first_at=0,
        spawn="t",
        max_events=6,
        vars={"robot": generator({"gen": "boolean", "p": 0.5})},
        attributes={"robot": generator({"gen": "var", "name": "robot"})},
        key=generator(
            {
                "gen": "format",
                "template": "{p}-{n}",
                "names": {
                    "p": {
                        "gen": "when",
                        "condition": "robot",
                        "then": "R",
                        "else": "U",
                    },
                    "n": {"gen": "sequence", "start": 1},
                },
            }
        ),
        **extra,
    )
    app.task(name="t")(
        lambda task_id: Template(
            {"robot": {"gen": "attr", "name": "robot"}}, name="tasks.t"
        )
    )
    return app


def test_arrival_vars_and_a_generator_key_give_each_entity_its_values():
    records = run_builder(_players(), 50)
    events = [r for r in records if r["stream_type"] == "event"]
    assert [r["key"][1:] for r in events] == ["-1", "-2", "-3", "-4", "-5", "-6"]
    assert {r["key"][0] for r in events} == {"R", "U"}
    assert all((r["key"][0] == "R") == r["value"]["robot"] for r in events)


def _loop_reads(**extra):
    app = SimulationContext("S", factor=0, random_seed=3)
    app.add_arrival(
        "a",
        gap=1,
        first_at=0,
        attributes={"x": generator({"gen": "var", "name": "v"})},
        **extra,
    )
    seen = []

    @app.arrival_loop("a")
    def loop(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("a")
            seen.append((entity.key, entity.attributes["x"]))
        yield context.env.event()

    run_builder(app, 10)
    return seen


def test_arrival_vars_without_a_spawn_are_read_by_wait_for_arrival():
    seen = _loop_reads(vars={"v": generator({"gen": "sequence", "start": 10})})
    assert seen == [("a-1", 10), ("a-2", 11), ("a-3", 12)]


def test_arrival_key_without_a_spawn_is_read_by_wait_for_arrival():
    seen = _loop_reads(
        vars={"v": generator({"gen": "sequence", "start": 10})},
        key=generator(
            {
                "gen": "format",
                "template": "k{n}",
                "names": {"n": {"gen": "var", "name": "v"}},
            }
        ),
    )
    assert seen == [("k10", 10), ("k11", 11), ("k12", 12)]


def test_arrival_vars_refuses_a_plain_mapping_with_gen_and_names_the_arrival():
    app = SimulationContext("S", factor=0)
    with pytest.raises(
        ValueError, match=r"arrival 'a': vars: 'v' is a mapping with a gen"
    ):
        app.add_arrival("a", gap=1, spawn="t", vars={"v": {"gen": "boolean", "p": 0.1}})
