"""The Builder's start() holds the rules the YAML load holds, with the same text.

A refused start() also leaves the run that is open as it was."""

import pytest

from desflow.core.context import SimulationContext

HEAD = (
    "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
    "arrivals: {a: {gap: 1, max_events: 3, spawn: job}}\n"
)


def _app():
    app = SimulationContext("S", factor=0)
    app.add_arrival("a", "constant", value=1, spawn="job")
    return app


def _job(app, **kwargs):
    app.task(service_time=0, name="job", **kwargs)(lambda task_id: {})
    return app


def _yaml_refusal(tmp_path, body, run="run: {until: 10}\n"):
    path = tmp_path / "m.yaml"
    path.write_text(HEAD + body + run)
    with pytest.raises(Exception) as caught:
        SimulationContext.from_yaml(path)
    return str(caught.value)


def _builder_refusal(app, **run):
    with pytest.raises(ValueError) as caught:
        app.start(until=10, **run)
    return str(caught.value)


def test_a_stop_on_a_family_is_refused_in_both(tmp_path):
    yaml = _yaml_refusal(
        tmp_path,
        "signals: {done: {family: true, members: 2}}\ntasks: {job: {payload: {}}}\n",
        "run: {until: 5, stop: {signal: done}}\n",
    )
    app = _job(_app()).add_signal("done", family=True, members=2)
    built = _builder_refusal(app, stop={"signal": "done"})
    assert "m.yaml:" in yaml
    assert "stop names the family 'done'" in yaml
    assert "stop names the family 'done'" in built


@pytest.mark.parametrize("condition", ["attr.x > 1", "payload.n > 1"])
def test_a_stop_condition_that_reads_the_entity_is_refused_in_both(tmp_path, condition):
    yaml = _yaml_refusal(
        tmp_path,
        "tasks: {job: {payload: {}}}\n",
        f"run: {{until: 5, stop: {{condition: '{condition}'}}}}\n",
    )
    built = _builder_refusal(_job(_app()), stop={"condition": condition})
    assert "cannot read" in yaml and "cannot read" in built


@pytest.mark.parametrize("key", ["activate", "deactivate"])
def test_a_switch_of_no_arrival_is_refused_at_start(key):
    built = _builder_refusal(_job(_app(), **{key: "nope"}))
    assert "switches 'nope', which is not an arrival with a spawn" in built


def test_segments_out_of_order_are_refused_at_start():
    built = _builder_refusal(_job(_app()), segments=[{"until": 5}, {"until": 3}])
    assert "must be after the previous end" in built


def test_a_segments_refusal_is_the_first_message_only():
    built = _builder_refusal(_job(_app()), segments={"every": 0})
    assert built == "segments: every must be above 0"


@pytest.mark.parametrize(
    "wait",
    [
        {"request": "nosuch"},
        {"each": 2, "branch": {"request": "nosuch"}},
        {"each": 2, "branch": {"get": {"store": "nosuch"}}},
    ],
)
def test_a_wait_names_no_unknown_holder_inside_each(wait):
    assert "not added" in _builder_refusal(_job(_app(), wait=wait))


@pytest.mark.parametrize("where", ["spawn", "on"])
def test_a_negative_spawn_count_is_refused(tmp_path, where):
    spawn = {"task": "other", "count": -1}
    kwargs = {"spawn": spawn} if where == "spawn" else {"on": [{"spawn": spawn}]}
    app = SimulationContext("S", factor=0)
    with pytest.raises(ValueError, match="a spawn count must be 0 or more, got -1"):
        app.task(service_time=1, name="serve", **kwargs)(lambda task_id: {})
        app.start(until=3)


@pytest.mark.parametrize("value", ["false", 0, None])
def test_a_flag_takes_true_or_false_only(value):
    with pytest.raises(ValueError, match="is true or false"):
        SimulationContext("S").add_store("bin", priority=value)
    with pytest.raises(ValueError, match="is true or false"):
        SimulationContext("S").add_signal("go", once=value)


def test_a_family_named_like_a_member_is_refused_in_both(tmp_path):
    yaml = _yaml_refusal(
        tmp_path,
        "resources: {desk: {family: true, members: 2}, "
        '"desk[1]": {family: true, members: 2}}\n'
        "tasks: {job: {service_time: 1, payload: {}}}\n",
    )
    app = _job(_app())
    app.add_resource("desk", family=True, members=2)
    app.add_resource("desk[1]", family=True, members=2)
    built = _builder_refusal(app)
    assert "starts with the name of the family 'desk' and a bracket" in yaml
    assert "starts with the name of the family 'desk' and a bracket" in built


def test_a_refused_start_leaves_the_open_run_as_it_was():
    app = _job(_app())
    app.start(until=10)
    app.advance(3)
    with pytest.raises(ValueError):
        app.start(until=20, segments=[{"until": 2}], stop={"signal": "nope"})
    app.finish()
    assert app.env.now == 10


@pytest.mark.parametrize("weight, written", [(float("inf"), ".inf"), (True, "true")])
def test_a_route_weight_that_is_infinite_or_true_is_refused_in_both(
    tmp_path, weight, written
):
    path = tmp_path / "m.yaml"
    path.write_text(
        HEAD
        + f"tasks: {{job: {{payload: {{}}, next: {{weights: {{x: {written}, y: 1}}}}}}, "
        "x: {payload: {}}, y: {payload: {}}}\n"
        "run: {until: 3}\n"
    )
    with pytest.raises(Exception, match="the weight of 'x' must be"):
        SimulationContext.from_yaml(path)
    app = _app()
    with pytest.raises(ValueError, match="the weight of 'x' must be"):
        app.task(service_time=0, name="job", next={"weights": {"x": weight, "y": 1}})(
            lambda task_id: {}
        )
        app.start(until=3)
