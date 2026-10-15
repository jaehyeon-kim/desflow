"""YAML blueprints: an invalid distribution parameter is reported with its file and line."""

import textwrap

import pytest

from desflow import SimulationContext
from desflow.blueprint import BlueprintError

BLUEPRINT = """\
simulation:
  sim_id: Shop
  factor: 0
resources:
  packers: {current_cap: 2, max_cap: 5}
services:
  packing: {dist: normal, mean: 3.0, std: -0.5}
run:
  until: 30
"""


def test_invalid_service_parameter_names_the_line(tmp_path):
    path = tmp_path / "blueprint.yaml"
    path.write_text(textwrap.dedent(BLUEPRINT), encoding="utf-8")

    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:7:"), message
    assert "std" in message


ARRIVAL_BLUEPRINT = """\
simulation:
  sim_id: Shop
  factor: 0
resources:
  packers: {current_cap: 2, max_cap: 5}
arrivals:
  parts: {dist: exponential, rate: -1}
run:
  until: 30
"""


def test_invalid_arrival_parameter_names_the_line(tmp_path):
    path = tmp_path / "blueprint.yaml"
    path.write_text(ARRIVAL_BLUEPRINT, encoding="utf-8")

    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:7:"), message
    assert "rate" in message


CONSTANT_BLUEPRINT = """\
simulation:
  sim_id: Shop
  factor: 0
resources:
  packers: {current_cap: 2, max_cap: 5}
services:
  setup: {dist: constant, value: 1.5}
arrivals:
  parts: {dist: constant, value: -1}
run:
  until: 30
"""


def test_constant_in_yaml_is_checked_with_its_line(tmp_path):
    path = tmp_path / "blueprint.yaml"
    path.write_text(CONSTANT_BLUEPRINT, encoding="utf-8")
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    message = str(error.value)
    assert message.startswith(f"{path}:9:"), message
    assert "constant value" in message


def test_constant_service_in_yaml_runs(tmp_path):
    path = tmp_path / "blueprint.yaml"
    path.write_text(
        CONSTANT_BLUEPRINT.replace(
            "{dist: constant, value: -1}", "{dist: constant, value: 2.0}"
        ),
        encoding="utf-8",
    )
    app = SimulationContext.from_yaml(path)
    assert app.compile_parameters().service["setup"].value == 1.5
