"""A Builder argument of `add_arrival` takes the forms, and refuses the values, of the
blueprint key it mirrors: `gap`, `group`, `key`, `vars` and `max_events`.

Each case is one value written as the YAML key takes it. The Builder gets the same
value, a mapping with `gen` made a `generator(...)`, and both arrivals run. The
records must be equal, or both must refuse, or both must fail at the run with the
same class and the same start of the message. A Builder refusal names the arrival and
says what is refused: the key, or the reason a case gives. Where the YAML and the
Builder check through one function, the YAML's refusal gives the same reason.
"""

import copy
from typing import Any, Dict, List, Tuple

import numpy as np
import pytest
import yaml

from desflow import SimulationContext
from desflow.blueprint.loader import BlueprintError
from desflow.values import Template, generator
from tests.conformance.harness import Outcome, run_builder

_MESSAGE_START = 30
_BASE: Dict[str, Any] = {
    "gap": 1,
    "first_at": 0,
    "spawn": "t",
    "max_events": 4,
}
_PAYLOAD = {"x": {"gen": "attr", "name": "x"}, "e": 1}
_ATTRIBUTES = {"x": 0}


def _as_builder(value: Any) -> Any:
    """The Builder form of a YAML value: a mapping with `gen` is a generator."""
    if isinstance(value, dict):
        if "gen" in value:
            return generator(copy.deepcopy(value))
        return {k: _as_builder(v) for k, v in value.items()}
    return value


def _yaml(arrival: Dict[str, Any], tmp_path) -> Tuple[str, Any]:
    document = {
        "simulation": {"sim_id": "S", "factor": 0, "random_seed": 3},
        "arrivals": {"a": arrival},
        "tasks": {"t": {"payload": _PAYLOAD}},
    }
    path = tmp_path / "model.yaml"
    path.write_text(yaml.safe_dump(document))
    try:
        app = SimulationContext.from_yaml(path)
    except BlueprintError as error:
        return "refused", error
    return _run(app, 30)


def _builder(arrival: Dict[str, Any]) -> Tuple[str, Any]:
    try:
        app = SimulationContext("S", factor=0, random_seed=3)
        app.add_arrival("a", **{k: _as_builder(v) for k, v in arrival.items()})
        app.task(name="t")(lambda task_id: Template(_PAYLOAD, name="tasks.t"))
    except ValueError as error:
        return "refused", error
    return _run(app, 30)


def _run(app: SimulationContext, until: float) -> Tuple[str, Any]:
    outcome = run_builder(app, until, raises=True)
    assert isinstance(outcome, Outcome)
    if outcome.error is not None:
        return "failed", outcome.error
    return "records", [
        (r["key"], r["sim_ts"], r["value"])
        for r in outcome.records
        if r["stream_type"] == "event"
    ]


def _same(arrival: Dict[str, Any], refusal: str, shared: bool, tmp_path) -> None:
    from_yaml = _yaml(arrival, tmp_path)
    built = _builder(arrival)
    assert built[0] == from_yaml[0], (from_yaml, built)
    if from_yaml[0] == "records":
        assert built[1] == from_yaml[1]
        assert built[1], "the case runs no event, so it shows nothing"
    elif from_yaml[0] == "failed":
        assert type(built[1]) is type(from_yaml[1])
        assert str(built[1])[:_MESSAGE_START] == str(from_yaml[1])[:_MESSAGE_START]
    else:
        assert isinstance(built[1], ValueError)
        assert f"arrival 'a': {refusal}" in str(built[1]), built[1]
        if shared:
            assert refusal in str(from_yaml[1]), from_yaml[1]


def _case(
    key: str, value: Any, refusal: str = "", shared: bool = False, **more: Any
) -> Any:
    """One value of `key`; `refusal` is the start of the Builder's refusal after the
    arrival's name, which is the key unless the case gives it, and `shared` says the
    YAML's refusal gives it too."""
    arrival = {**_BASE, "attributes": dict(_ATTRIBUTES), **more}
    arrival[key] = value
    return pytest.param(
        arrival,
        refusal or key,
        shared,
        id=f"{key}={value!r}"[:60] + str(more or ""),
    )


_VARS = {"v": {"gen": "integer", "bounds": [1, 9]}}
_X_FROM_V = {"x": {"gen": "var", "name": "v"}}
_COUNT = {"gen": "integer", "bounds": [1, 3]}

CASES: List[Any] = [
    # gap: a number, a duration, text, a generator, and what the YAML refuses
    _case("gap", 2),
    _case("gap", 0),
    _case("gap", "3 s"),
    _case("gap", "1 + t / 4"),
    _case("gap", {"gen": "uniform", "low": 1, "high": 2}),
    _case("gap", -1),
    _case("gap", True),
    _case("gap", "t +"),
    _case("gap", "attr.x"),
    # group: a number, an expression, a generator
    _case("group", 2),
    _case("group", "int(t / 2) + 1"),
    _case("group", {"gen": "sample", "dist": "poisson", "mean": 2}),
    _case("group", -1, "group must be 0 or more, got -1", shared=True),
    _case("group", 1.5),
    _case("group", True),
    _case("group", "t +"),
    _case("group", "attr.x"),
    # max_events: a number, an expression, a generator
    _case("max_events", 3),
    _case("max_events", "2 + 1"),
    _case("max_events", _COUNT),
    _case("max_events", -1, "max_events must be 0 or more, got -1", shared=True),
    _case("max_events", 1.5),
    _case("max_events", True),
    _case("max_events", "t +"),
    _case("max_events", "attr.x"),
    # key: text is the literal key; a number; an expression generator; other generators
    _case("key", "abc"),
    _case("key", "'k' + str(1)"),
    _case("key", 7),
    _case("key", {"gen": "expr", "expr": "'k' + str(t)"}),
    _case("key", {"gen": "sequence", "start": 1}),
    _case("key", True),
    _case("key", ["a"]),
    _case("key", 1.5),
    # key and vars read the entity's variables; they need no spawn to be accepted
    _case("key", {"gen": "var", "name": "v"}, vars=_VARS),
    _case("vars", _VARS, attributes=_X_FROM_V),
    _case("vars", {"v": 5}, attributes=_X_FROM_V),
    _case("vars", {"v": "abc"}, attributes=_X_FROM_V),
    _case("vars", {"t": 1}, "vars: 't' is a name an expression already", shared=True),
    _case("vars", {"1x": 1}, "vars: '1x' is not a name an", shared=True),
    _case("vars", {"class": 1}, "vars: 'class' is not a name an", shared=True),
    _case("vars", {"v": {"gen": "boolean", "p": 0.5}}, attributes=_X_FROM_V),
    _case("vars", ["v"], "vars is a mapping"),
    _case("vars", "v", "vars is a mapping"),
]


@pytest.mark.parametrize("arrival, refusal, shared", CASES)
def test_add_arrival_takes_what_the_yaml_key_takes(arrival, refusal, shared, tmp_path):
    _same(arrival, refusal, shared, tmp_path)


def test_a_numpy_group_is_taken_as_the_yaml_takes_the_number(tmp_path):
    arrival = {**_BASE, "attributes": dict(_ATTRIBUTES), "group": 2}
    from_yaml = _yaml(arrival, tmp_path)
    built = _builder({**arrival, "group": np.int64(2)})
    assert built == from_yaml
    assert from_yaml[0] == "records" and from_yaml[1]


def _without_spawn(**more: Any) -> Dict[str, Any]:
    return {"gap": 1, "first_at": 0, "attributes": dict(_ATTRIBUTES), **more}


@pytest.mark.parametrize(
    "more",
    [
        {"key": {"gen": "sequence", "start": 1}},
        {"key": "abc"},
        {"vars": _VARS, "attributes": _X_FROM_V},
    ],
    ids=["generator key", "text key", "vars"],
)
def test_an_arrival_without_spawn_takes_key_and_vars_as_the_yaml_does(more, tmp_path):
    arrival = _without_spawn(**more)
    document = {
        "simulation": {"sim_id": "S", "factor": 0},
        "arrivals": {"a": arrival},
    }
    path = tmp_path / "model.yaml"
    path.write_text(yaml.safe_dump(document))
    SimulationContext.from_yaml(path)
    app = SimulationContext("S", factor=0, random_seed=3)
    app.add_arrival("a", **{k: _as_builder(v) for k, v in arrival.items()})


_KEY_FROM_V = {
    "gen": "format",
    "template": "k{n}",
    "names": {"n": {"gen": "var", "name": "v"}},
}
_SEQUENCE_VARS = {"v": {"gen": "sequence", "start": 10}}


def _initial_or_replay(kind: str, more: Dict[str, Any], tmp_path) -> Dict[str, Any]:
    """The YAML arrival of an `initial` alone or of a `replay`, with `more`."""
    if kind == "initial":
        return {"initial": 3, "spawn": "t", "attributes": dict(_ATTRIBUTES), **more}
    (tmp_path / "rows.csv").write_text("time,x\n1,1\n2,2\n")
    return {"replay": {"path": "rows.csv", "time": "time"}, "spawn": "t", **more}


def _builder_initial_or_replay(
    kind: str, arrival: Dict[str, Any], tmp_path
) -> Tuple[str, Any]:
    given = {
        k: _as_builder(v)
        for k, v in arrival.items()
        if k not in ("initial", "replay", "spawn")
    }
    try:
        app = SimulationContext("S", factor=0, random_seed=3)
        if kind == "initial":
            app.add_initial_arrival("a", 3, spawn="t", **given)
        else:
            app.add_arrival_replay(
                "a", tmp_path / "rows.csv", time="time", spawn="t", **given
            )
        app.task(name="t")(lambda task_id: Template(_PAYLOAD, name="tasks.t"))
    except ValueError as error:
        return "refused", error
    return _run(app, 30)


@pytest.mark.parametrize("kind", ["initial", "replay"])
@pytest.mark.parametrize(
    "more, refusal",
    [
        ({"key": "abc"}, ""),
        ({"key": 7}, ""),
        ({"key": {"gen": "sequence", "start": 5}}, ""),
        ({"key": {"gen": "expr", "expr": "'k' + str(t)"}}, ""),
        ({"vars": _SEQUENCE_VARS, "key": _KEY_FROM_V}, ""),
        ({"vars": {"v": 5}, "attributes": _X_FROM_V}, ""),
        ({"vars": {"t": 1}}, "vars: 't' is a name an expression already"),
    ],
    ids=[
        "text key",
        "number key",
        "generator key",
        "expression key",
        "vars and a key that reads them",
        "constant vars",
        "vars name in use",
    ],
)
def test_initial_and_replay_arrivals_take_key_and_vars_as_the_yaml_does(
    kind, more, refusal, tmp_path
):
    arrival = _initial_or_replay(kind, more, tmp_path)
    from_yaml = _yaml(arrival, tmp_path)
    built = _builder_initial_or_replay(kind, arrival, tmp_path)
    assert built[0] == from_yaml[0], (from_yaml, built)
    if refusal:
        assert f"arrival 'a': {refusal}" in str(built[1]), built[1]
        assert refusal in str(from_yaml[1]), from_yaml[1]
    else:
        assert built[1] == from_yaml[1]
        assert built[1], "the case runs no event, so it shows nothing"


def test_an_initial_arrival_with_a_gap_runs_as_the_yaml_does(tmp_path):
    """An `initial` arrival with a `gap` is a source that starts its `initial`
    entities and then one at each gap, in the builder as in YAML."""
    arrival = _initial_or_replay("initial", {"gap": 4}, tmp_path)
    from_yaml = _yaml(arrival, tmp_path)
    built = _builder_initial_or_replay("initial", arrival, tmp_path)
    assert built == from_yaml
    assert built[0] != "refused" and built[1], built
