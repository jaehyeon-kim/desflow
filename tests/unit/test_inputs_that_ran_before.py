"""Inputs that 0.29.0 ran keep running with the same meaning.

The checks added for 1.0 refuse what could not work, and nothing else: a whole float
seed, infinity as no limit, a flag written as 1 or 0, a live level written as text
and a shared object in an arrival's attributes all behave as before.
"""

import math
import threading
from pathlib import Path

import pytest
import simpy
import yaml
from typer.testing import CliRunner

from desflow.cli import app
from desflow.builder.arrivals import _fresh
from desflow.builder.context import SimulationContext
from desflow.core.registry import SimulationRegistry
from desflow.models.params import ContainerConfig, ResourceConfig, SimParameter

MODEL = {
    "simulation": {"sim_id": "S", "factor": 0, "random_seed": 1},
    "resources": {"desk": {"current_cap": 1}},
    "arrivals": {"orders": {"gap": 1, "max_events": 3, "spawn": "serve"}},
    "tasks": {"serve": {"service_time": 2, "resource": "desk", "payload": {}}},
    "run": {"until": 20},
}


def _write(tmp_path: Path, change) -> Path:
    import copy

    doc = copy.deepcopy(MODEL)
    change(doc)
    path = tmp_path / "m.yaml"
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return path


@pytest.mark.parametrize("seed", [42.0, "42.0", "42"])
def test_a_whole_float_seed_is_the_whole_number(tmp_path, seed):
    path = _write(tmp_path, lambda d: d["simulation"].update(random_seed=seed))
    assert SimulationContext.from_yaml(path).random_seed == 42


@pytest.mark.parametrize(
    "change",
    [
        lambda d: d["tasks"]["serve"].update(patience=math.inf),
        lambda d: d["tasks"]["serve"].update(service_time=math.inf),
        lambda d: d["tasks"]["serve"].update(wait={"timeout": math.inf}),
        lambda d: d["run"].update(drain={"limit": math.inf}),
        lambda d: d["simulation"].update(warmup=math.inf),
    ],
    ids=["patience", "service_time", "wait timeout", "drain limit", "warmup"],
)
def test_infinity_is_no_limit_where_a_time_need_not_end(tmp_path, change):
    path = _write(tmp_path, change)
    result = CliRunner().invoke(app, ["check", str(path)])
    assert result.exit_code == 0, result.output


def test_verify_refuses_a_blueprint_whose_end_is_infinite(tmp_path):
    path = _write(tmp_path, lambda d: d["run"].update(until=math.inf))
    result = CliRunner().invoke(app, ["explain", str(path), "--verify"])
    assert result.exit_code == 2
    assert "run.until gave inf, and it must end" in result.output


@pytest.mark.parametrize("factor, ok", [("0", True), ("2 min", False)])
def test_a_factor_is_a_number_not_a_duration(tmp_path, factor, ok):
    path = _write(tmp_path, lambda d: d["simulation"].update(factor=factor))
    result = CliRunner().invoke(app, ["check", str(path)])
    assert (result.exit_code == 0) is ok, result.output
    if not ok:
        assert "factor is a number, got '2 min'" in result.output


def _registry() -> SimulationRegistry:
    registry = SimulationRegistry(simpy.Environment())
    registry.register_sim_parameter(
        SimParameter(
            sim_id="S",
            resources={"desk": ResourceConfig(current_cap=2, max_cap=4)},
            containers={"tank": ContainerConfig(current_cap=5, max_cap=10, level=3)},
            stores={},
            variables={"flag": True},
        )
    )
    return registry


@pytest.mark.parametrize("value, stored", [(1, True), (0, False), ("false", False)])
def test_a_flag_takes_one_and_zero(value, stored):
    registry = _registry()
    registry.update("S.variables.flag", value)
    assert registry.get("S.variables.flag").value is stored


def test_a_live_capacity_of_minus_infinity_is_refused():
    registry = _registry()
    registry.update("S.resources.desk.current_cap", -math.inf)
    assert registry.get("S.resources.desk.current_cap").value == 2


def test_a_live_level_written_as_text_is_a_float():
    registry = _registry()
    registry.update("S.containers.tank.level", "4")
    value = registry.get("S.containers.tank.level").value
    assert value == 4.0 and isinstance(value, float)


def test_each_entity_gets_its_own_containers_and_shares_other_objects():
    lock = threading.Lock()
    given = {"tags": {"a"}, "pair": ([1], 2), "lock": lock}
    copied = _fresh(given)
    assert copied == given
    assert copied["tags"] is not given["tags"]
    assert copied["pair"][0] is not given["pair"][0]
    assert copied["lock"] is lock
