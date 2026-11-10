"""A name declared twice, or in two sections, or that no expression can read.

A second declaration of one name replaces the first entirely (an arrival, a resource,
a signal); a name in two of resources, stores, containers and signals, or with a dot
or a space, is refused with the same text in the Builder and in YAML."""

import pytest

from desflow.core.context import SimulationContext


def _app():
    return SimulationContext("S", factor=0)


def test_a_second_arrival_replaces_the_first_entirely():
    app = _app()
    app.add_arrival("a", "constant", value=1, spawn="job")
    app.add_arrival("a", "constant", value=2, spawn="job")
    seen = []
    app.task(service_time=0, name="job")(lambda task_id: seen.append(1) or {})
    app.run(until=4.5)
    assert len(seen) == 2


def test_a_second_arrival_without_a_spawn_drops_the_first_loop():
    app = _app()
    app.add_arrival("a", "constant", value=1, spawn="job")
    app.add_arrival("a", "constant", value=1)
    seen = []
    app.task(service_time=0, name="job")(lambda task_id: seen.append(1) or {})
    app.run(until=4.5)
    assert seen == []


def test_a_second_arrival_drops_the_replay_and_the_registry_paths_of_the_first():
    app = _app()
    app.add_arrival_replay("a", [{"time": 1}], spawn="job")
    app.add_arrival("a", "constant", value=1, spawn="job")
    assert "a" not in app._arrivals.replays
    app.add_arrival("a", gap=1, spawn="job")
    assert not [path for path in app.registry_paths() if ".arrival.a." in path]


def test_an_empty_scenario_before_a_step_applies_each_step_once():
    app = _app()
    app.add_resource("desk", current_cap=1, max_cap=9)
    app.add_scenario([])
    app.add_step(at=1, path="S.resources.desk.current_cap", value=3)
    changes = []
    app.on_change.append(changes.append)
    app.run(until=3)
    assert len(changes) == 1


def test_a_signal_with_the_name_of_a_family_is_refused_and_the_family_stays():
    app = _app()
    app.add_resource("desk", current_cap=1, family=True, members=2)
    with pytest.raises(ValueError, match="signal 'desk' has the name of a resource"):
        app.add_signal("desk")
    assert "desk" in app._families.by_name


def test_a_family_with_the_name_of_a_signal_is_refused():
    app = _app()
    app.add_signal("desk")
    with pytest.raises(ValueError, match="resource 'desk' has the name of a signal"):
        app.add_resource("desk", current_cap=1, family=True, members=2)


def test_yaml_refuses_a_name_in_two_sections_at_the_second_place(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0}\n"
        "resources: {desk: {family: true, current_cap: 1, members: 2}}\n"
        "signals: {desk: {}}\n"
        "arrivals: {a: {gap: 1, max_events: 1, spawn: t}}\n"
        "tasks: {t: {service_time: 1, payload: {}}}\n"
        "run: {until: 3}\n"
    )
    with pytest.raises(Exception) as refused:
        SimulationContext.from_yaml(path)
    assert "m.yaml:3" in str(refused.value)
    assert "signal 'desk' has the name of a resource" in str(refused.value)


@pytest.mark.parametrize("name, word", [("a.b", "dot"), ("a b", "space")])
@pytest.mark.parametrize("kind", ["resource", "store", "container", "signal"])
def test_a_name_an_expression_cannot_read_is_refused_in_both(
    tmp_path, kind, name, word
):
    with pytest.raises(ValueError, match=f"has a {word} in its name"):
        _app()._families.claim(kind, name)
    section = {"resource": "resources", "store": "stores"}.get(kind, kind + "s")
    body = "{}" if kind == "signal" else "{current_cap: 5}"
    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0}\n"
        f"{section}: {{'{name}': {body}}}\n"
        "arrivals: {a: {gap: 1, max_events: 1, spawn: t}}\n"
        "tasks: {t: {service_time: 1, payload: {}}}\n"
        "run: {until: 3}\n"
    )
    with pytest.raises(Exception) as refused:
        SimulationContext.from_yaml(path)
    assert "m.yaml:2" in str(refused.value)
    assert f"has a {word} in its name" in str(refused.value)


def test_add_container_refuses_a_dotted_name():
    with pytest.raises(ValueError, match="container 'a.b' has a dot in its name"):
        _app().add_container("a.b", current_cap=5)


def test_a_reading_named_like_a_statistic_or_another_reading_is_refused():
    app = _app()
    app.add_readings(5, {"x": "1"})
    with pytest.raises(ValueError, match="reading 'x' is already a reading"):
        app.add_readings(5, {"x": "2"})
    with pytest.raises(ValueError, match="name of a statistic the run publishes"):
        app.add_readings(5, {"stats.tasks.serve.finished": "100"})


@pytest.mark.parametrize(
    "text, line",
    [
        (
            "simulation: {sim_id: S, factor: 0}\n"
            "arrivals: {a: {gap: 1, max_events: 3, spawn: serve}}\n"
            "tasks:\n"
            "  serve: {service_time: 1, payload: {which: first}}\n"
            "  serve: {service_time: 2, payload: {which: second}}\n"
            "run: {until: 10}\n",
            5,
        ),
        (
            "simulation: {sim_id: S, factor: 0, factor: 1}\n"
            "arrivals: {a: {gap: 1, max_events: 3, spawn: serve}}\n"
            "tasks: {serve: {service_time: 1, payload: {}}}\n"
            "run: {until: 10}\n",
            1,
        ),
    ],
)
def test_a_key_written_twice_in_one_mapping_is_refused_at_its_second_line(
    tmp_path, text, line
):
    from desflow.blueprint.build import check

    path = tmp_path / "m.yaml"
    path.write_text(text)
    with pytest.raises(
        ValueError, match=rf"m\.yaml:{line}: the key '\w+' is written twice"
    ):
        check(str(path))
