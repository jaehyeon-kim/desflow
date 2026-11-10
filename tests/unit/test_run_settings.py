"""A seed that is negative or `true` is refused with the seed's name, on every path.

`random_seed` in YAML, the constructor, `start`, `desflow run --seed`, `desflow explain
--verify --seed`, `verify` and `replicate` each refuse -1 and `true` with `random_seed`
or `seed` (`--seed` for the options), before any run starts and with no NumPy text.
"""

import pytest
from typer.testing import CliRunner

from desflow import SimulationContext
from desflow.analysis.runner import replicate
from desflow.blueprint.loader import BlueprintError
from desflow.cli import app as cli
from desflow.explain import analyze, build_graph, verify

runner = CliRunner()

BLUEPRINT = """\
simulation: {{sim_id: S, factor: 0, random_seed: {seed}}}
resources: {{desk: {{current_cap: 1}}}}
services: {{work: {{dist: constant, value: 1}}}}
arrivals: {{a: {{dist: constant, value: 1, spawn: serve}}}}
tasks: {{serve: {{service: work, resource: desk, payload: {{}}}}}}
run: {{until: 5}}
"""
BAD = ["-1", "true"]


def _file(tmp_path, seed="1"):
    path = tmp_path / "m.yaml"
    path.write_text(BLUEPRINT.format(seed=seed))
    return path


def test_yaml_takes_a_seed_from_a_placeholder(tmp_path):
    path = _file(tmp_path, '"${SEED:-42}"')
    assert SimulationContext.from_yaml(path).random_seed == 42


@pytest.mark.parametrize("seed", BAD)
def test_yaml_refuses_a_bad_random_seed_with_its_line(tmp_path, seed):
    with pytest.raises(BlueprintError, match=r"m\.yaml:1.*random_seed"):
        SimulationContext.from_yaml(_file(tmp_path, seed))


@pytest.mark.parametrize("seed", [-1, True])
def test_the_constructor_refuses_a_bad_random_seed(seed):
    with pytest.raises(ValueError, match="random_seed"):
        SimulationContext("S", random_seed=seed)


@pytest.mark.parametrize("seed", [-1, True])
def test_start_refuses_a_bad_seed(tmp_path, seed):
    context = SimulationContext.from_yaml(_file(tmp_path))
    with pytest.raises(ValueError, match="seed"):
        context.start(until=2, seed=seed)


@pytest.mark.parametrize("seed", [-1, True])
def test_replicate_refuses_a_bad_seed(tmp_path, seed):
    with pytest.raises(ValueError, match="seed"):
        replicate(str(_file(tmp_path)), 2, seed=seed, until=5)


@pytest.mark.parametrize("seed", [-1, True])
def test_verify_refuses_a_bad_seed(tmp_path, seed):
    path = _file(tmp_path)
    explanation = analyze(build_graph(SimulationContext.from_yaml(path)))
    with pytest.raises(ValueError, match="seed"):
        verify(str(path), explanation, until=5, seed=seed, replications=2)


@pytest.mark.parametrize("command", [["run"], ["explain", "--verify"]])
def test_the_command_refuses_a_negative_seed(tmp_path, command):
    result = runner.invoke(
        cli, [command[0], str(_file(tmp_path)), *command[1:], "--seed", "-1"]
    )
    assert result.exit_code != 0, result.output
    assert "whole number of 0 or more, got -1" in result.output
