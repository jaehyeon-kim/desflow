"""The counts of a task are checked at load or at the call, in YAML and the Builder."""

import pytest

from desflow.core.context import SimulationContext
from desflow.core.flow import Router

HEAD = """\
simulation: {sim_id: S, factor: 0, random_seed: 1}
arrivals: {a: {gap: 1, max_events: 3, spawn: sleeper}}
tasks:
"""


def _load(tmp_path, tasks):
    path = tmp_path / "m.yaml"
    path.write_text(HEAD + tasks)
    return SimulationContext.from_yaml(path)


@pytest.mark.parametrize("limit", ["-1", "'0 - 1'"])
def test_a_resume_limit_below_zero_is_refused(tmp_path, limit):
    tasks = (
        "  sleeper: {wait: {suspend: true}, payload: {}}\n"
        f"  waker: {{payload: {{}}, resume: {{task: sleeper, limit: {limit}}}}}\n"
        "run: {until: 10}\n"
    )
    try:
        app = _load(tmp_path, tasks)
    except Exception as error:
        assert "resume limit must be 0 or more" in str(error)
        assert "m.yaml" in str(error)
        return
    # an expression is read when the resume runs
    app.add_arrival("w", "constant", value=1, spawn="waker", max_events=1)
    with pytest.raises(Exception, match="resume limit must be 0 or more"):
        app.run(until=10)


def test_the_builder_refuses_a_resume_limit_below_zero():
    app = SimulationContext("S", factor=0)
    with pytest.raises(ValueError, match="resume limit must be 0 or more"):
        app.task(service_time=0, name="w", resume={"task": "s", "limit": -1})(
            lambda task_id: {}
        )


@pytest.mark.parametrize("size", [".nan", ".inf"])
def test_a_batch_size_that_is_not_finite_is_refused_at_load(tmp_path, size):
    tasks = (
        f"  sleeper: {{service_time: 1, batch: {{size: {size}}}, payload: {{}}}}\n"
        "run: {until: 10}\n"
    )
    with pytest.raises(Exception, match=r"m\.yaml:\d+: .*batch size"):
        _load(tmp_path, tasks)


@pytest.mark.parametrize("weight", [float("inf"), True])
def test_a_route_weight_that_is_infinite_or_true_is_refused(weight):
    with pytest.raises(ValueError, match="weight of 'x' must be"):
        Router({"weights": {"x": weight, "y": 1}}).probabilities


@pytest.mark.parametrize("weight", [".inf", "true"])
def test_yaml_refuses_the_same_weights(tmp_path, weight):
    tasks = (
        f"  sleeper: {{payload: {{}}, next: {{weights: {{x: {weight}, y: 1}}}}}}\n"
        "  x: {payload: {}}\n  y: {payload: {}}\n"
        "run: {until: 10}\n"
    )
    with pytest.raises(Exception, match="weight of 'x' must be"):
        _load(tmp_path, tasks)


def test_a_constant_key_with_a_count_above_one_is_refused(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "arrivals: {a: {gap: 1, spawn: t, key: 'x', count: 2, max_events: 2}}\n"
        "tasks: {t: {payload: {}}}\nrun: {until: 10}\n"
    )
    with pytest.raises(Exception, match=r"m\.yaml:2: .*same key"):
        SimulationContext.from_yaml(path)


BAD_COUNTS = [
    ("-1", -1),
    (".nan", float("nan")),
    (".inf", float("inf")),
    ("true", True),
]
BUILDER_PLACES = {
    "spawn count": lambda v: dict(spawn={"task": "o", "count": v}),
    "on spawn count": lambda v: dict(on=[{"spawn": {"task": "o", "count": v}}]),
    "resume limit": lambda v: dict(resume={"task": "o", "limit": v}),
    "on resume limit": lambda v: dict(on=[{"resume": {"task": "o", "limit": v}}]),
    "batch size": lambda v: dict(batch={"size": v}),
}
YAML_PLACES = {
    "spawn count": "spawn: {{task: o, count: {v}}}",
    "on spawn count": "on: [{{spawn: {{task: o, count: {v}}}}}]",
    "resume limit": "resume: {{task: o, limit: {v}}}",
    "on resume limit": "on: [{{resume: {{task: o, limit: {v}}}}}]",
    "batch size": "batch: {{size: {v}}}",
}


CASES = [
    (place, written, value)
    for place in sorted(BUILDER_PLACES)
    for written, value in BAD_COUNTS
    # a literal batch size below 0 is read when a task ends, not at load
    if not (place == "batch size" and written == "-1")
]


@pytest.mark.parametrize(
    "place, written, value", CASES, ids=[f"{p}:{w}" for p, w, _ in CASES]
)
def test_every_count_place_refuses_a_bad_count_in_yaml_and_the_builder(
    tmp_path, place, written, value
):
    yaml_task = YAML_PLACES[place].format(v=written)
    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "arrivals: {a: {gap: 1, max_events: 3, spawn: t}}\n"
        f"tasks:\n  t: {{service_time: 1, payload: {{}}, {yaml_task}}}\n  o: {{payload: {{}}}}\n"
        "run: {until: 10}\n"
    )
    with pytest.raises(Exception, match=r"m\.yaml:\d+:") as from_yaml:
        SimulationContext.from_yaml(path)
    app = SimulationContext("S", factor=0)
    app.add_arrival("a", "constant", value=1, spawn="t")
    app.task(service_time=1, name="o")(lambda task_id: {})
    with pytest.raises(ValueError) as from_builder:
        app.task(name="t", **BUILDER_PLACES[place](value))(lambda task_id: {})
        app.start(until=10)
    assert str(from_builder.value) and str(from_yaml.value)
