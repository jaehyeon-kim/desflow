"""A value set on a live path is checked by the rule its declaration holds.

The registry's `check_value` is the one check: `update` (an ingress message) logs a
refusal and keeps the old value, a scenario step and an `on:` action are refused at
their line at load, and the Builder refuses a step in `start()`."""

import logging

import pytest
import simpy

from desflow.builder.context import SimulationContext
from desflow.core.registry import SimulationRegistry
from desflow.models.params import (
    ContainerConfig,
    ResourceConfig,
    SimParameter,
)

nan = float("nan")
inf = float("inf")

HEAD = (
    "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
    "variables: {flag: true, n: 3}\n"
    "resources: {desk: {current_cap: 2, max_cap: 4}}\n"
    "containers: {tank: {current_cap: 5, max_cap: 10, level: 3}}\n"
    "arrivals: {a: {gap: 1, max_events: 3, spawn: t}}\n"
)
TAIL = "run: {until: 10}\n"


def _registry():
    registry = SimulationRegistry(simpy.Environment())
    registry.register_sim_parameter(
        SimParameter(
            sim_id="S",
            resources={"desk": ResourceConfig(current_cap=2, max_cap=4)},
            containers={"tank": ContainerConfig(current_cap=5, max_cap=10, level=3)},
            stores={},
            variables={"flag": True, "n": 3},
        )
    )
    return registry


REFUSED = [
    ("S.resources.desk.current_cap", 2.5),
    ("S.resources.desk.current_cap", nan),
    ("S.resources.desk.current_cap", True),
    ("S.resources.desk.max_cap", -1),
    ("S.resources.desk.max_cap", 2.5),
    ("S.resources.desk.max_cap", "--5"),
    ("S.resources.desk.queue_limit", True),
    ("S.resources.desk.queue_limit", "--5"),
    ("S.resources.desk.queue_limit", "+-3"),
    ("S.containers.tank.max_cap", nan),
    ("S.containers.tank.max_cap", -1),
    ("S.containers.tank.current_cap", nan),
    ("S.containers.tank.level", nan),
    ("S.containers.tank.level", inf),
    ("S.variables.flag", "maybe"),
    ("S.variables.n", inf),
    ("S.variables.n", nan),
]


@pytest.mark.parametrize("path, value", REFUSED)
def test_check_value_refuses_what_the_declaration_refuses(path, value):
    from desflow.core.registry import ParamRejected

    with pytest.raises(ParamRejected):
        _registry().check_value(path, value)


@pytest.mark.parametrize("path, value", REFUSED)
def test_update_logs_the_refusal_keeps_the_old_value_and_never_raises(
    path, value, caplog
):
    registry = _registry()
    old = registry.get(path).value
    with caplog.at_level(logging.WARNING):
        registry.update(path, value)
    new = registry.get(path).value
    assert new == old or (new != new and old != old)
    assert caplog.records


@pytest.mark.parametrize(
    "value, stored",
    [("false", False), ("true", True), (True, True), (False, False)],
)
def test_a_true_or_false_path_reads_the_text(value, stored):
    registry = _registry()
    registry.update("S.variables.flag", value)
    assert registry.get("S.variables.flag").value is stored


def test_a_negative_current_cap_is_clamped_to_zero_as_documented():
    registry = _registry()
    registry.update("S.resources.desk.current_cap", -1)
    assert registry.get("S.resources.desk.current_cap").value == 0


def test_true_over_a_number_path_is_one():
    registry = _registry()
    registry.update("S.variables.n", True)
    assert registry.get("S.variables.n").value == 1
    assert type(registry.get("S.variables.n").value) is int


def _yaml(tmp_path, where, path, value):
    entry = f"{{path: {path}, value: {value}}}"
    place = (
        f"scenario: [{{at: 5, path: {path}, value: {value}}}]\n"
        if where == "scenario"
        else None
    )
    tasks = (
        "tasks: {t: {service_time: 1, payload: {}"
        + (f", on: [{entry}]" if where == "on" else "")
        + "}}\n"
    )
    file = tmp_path / "m.yaml"
    file.write_text(HEAD + tasks + (place or "") + TAIL)
    return file


YAML_REFUSED = [
    ("S.resources.desk.current_cap", "2.5"),
    ("S.resources.desk.current_cap", ".nan"),
    ("S.resources.desk.max_cap", "-1"),
    ("S.resources.desk.queue_limit", "true"),
    ("S.resources.desk.queue_limit", "'--5'"),
    ("S.containers.tank.max_cap", ".nan"),
    ("S.containers.tank.level", ".nan"),
    ("S.variables.flag", "maybe"),
    ("S.variables.n", ".inf"),
]


@pytest.mark.parametrize("where", ["scenario", "on"])
@pytest.mark.parametrize("path, value", YAML_REFUSED)
def test_yaml_refuses_a_live_value_at_its_line(tmp_path, where, path, value):
    file = _yaml(tmp_path, where, path, value)
    with pytest.raises(Exception) as refused:
        SimulationContext.from_yaml(file)
    text = str(refused.value)
    assert "m.yaml:" in text, text
    assert "Traceback" not in text and "OverflowError" not in text, text


@pytest.mark.parametrize("where", ["scenario", "on"])
def test_yaml_refuses_a_whole_number_path_set_to_infinity(tmp_path, where):
    file = _yaml(tmp_path, where, "S.variables.n", ".inf")
    with pytest.raises(Exception) as refused:
        SimulationContext.from_yaml(file)
    assert "m.yaml:" in str(refused.value)


def test_yaml_refuses_an_on_path_with_no_value(tmp_path):
    file = tmp_path / "m.yaml"
    file.write_text(
        HEAD
        + "tasks: {t: {service_time: 1, payload: {}, "
        + "on: [{path: S.resources.desk.current_cap}]}}\n"
        + TAIL
    )
    with pytest.raises(Exception) as refused:
        SimulationContext.from_yaml(file)
    assert "m.yaml:" in str(refused.value) and "a path needs a value" in str(
        refused.value
    )


def _app():
    app = SimulationContext("S", factor=0)
    app.add_resource("desk", current_cap=1, max_cap=4)
    app.add_arrival("a", "constant", value=1, spawn="job")
    app.task(service_time=0, name="job")(lambda task_id: {})
    return app


@pytest.mark.parametrize("field", ["current_cap", "max_cap"])
def test_the_builder_refuses_a_step_with_a_fraction_in_start(field):
    app = _app()
    app.add_step(at=1, path=f"S.resources.desk.{field}", value=1.5)
    with pytest.raises(ValueError, match="needs a whole number of 0 or more"):
        app.start(until=3)


def test_the_builder_refuses_an_on_path_with_no_value():
    app = SimulationContext("S", factor=0)
    app.add_resource("desk", current_cap=2)
    app.add_arrival("a", "constant", value=1, spawn="job")
    with pytest.raises(ValueError, match="a path needs a value"):
        app.task(
            service_time=0, name="job", on=[{"path": "S.resources.desk.current_cap"}]
        )(lambda task_id: {})
        app.start(until=3)
