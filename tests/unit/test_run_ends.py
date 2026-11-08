"""The end of a run, its warm-up and the arguments of a value generator are refused
when they cannot be a time or a number, on every entry that takes them: the
command line, `verify`, `replicate`, an experiment file and a payload generator.

A comparison with a measured NaN is never reported as agreeing.
"""

import math
from pathlib import Path

import click
import pytest
import yaml
from typer.testing import CliRunner

from desflow.analysis.models import seconds
from desflow.analysis.spec import ExperimentSpec
from desflow.blueprint.build import check
from desflow.cli import app
from desflow.explain import verify
from desflow.explain.compare import compare
from desflow.explain.pairs import Pair

ROOT = Path(__file__).resolve().parents[2]
LOCAL = ROOT / "examples" / "yaml" / "local.yaml"


@pytest.mark.parametrize("value", ["nan", "-5"])
def test_the_command_line_refuses_an_end_that_is_not_a_time(value):
    result = CliRunner().invoke(app, ["run", str(LOCAL), "--until", value])
    assert result.exit_code == 2
    assert "--until" in click.unstyle(result.output)


@pytest.mark.parametrize(
    "command",
    [
        ["run", str(LOCAL), "--replications", "2", "--until", "inf"],
        ["explain", str(LOCAL), "--verify", "--until", "inf"],
    ],
)
def test_an_end_that_is_not_paced_refuses_infinity(command):
    result = CliRunner().invoke(app, command)
    assert result.exit_code == 2
    assert "--until gave inf, and it must end" in click.unstyle(result.output)


def test_a_paced_run_takes_an_infinite_end_as_no_end():
    from desflow.cli._common import parse_until

    assert parse_until("inf") == math.inf


def test_the_command_line_takes_a_number_or_a_duration():
    from desflow.cli._common import parse_until

    assert parse_until("30") == 30.0
    assert parse_until("10 min") == 600.0
    assert isinstance(parse_until("30"), float)


@pytest.mark.parametrize("value", [math.nan, math.inf, -1, True])
def test_the_analysis_seconds_refuse_what_is_not_a_time(value):
    with pytest.raises(ValueError, match="warmup"):
        seconds(value, "warmup")


def test_verify_refuses_an_end_of_nan():
    with pytest.raises(ValueError, match="until"):
        verify(str(LOCAL), check(str(LOCAL)).analyze(), until=math.nan)


def _spec(**changes):
    base = dict(
        experiment="e",
        question="q",
        model=str(LOCAL),
        assumptions=["a"],
        inputs={"x": "y"},
        until=100,
        replications=2,
        seed=1,
        measures=["tasks.pack_order.wait_mean"],
        limits=["l"],
        output="out",
    )
    base.update(changes)
    return base


@pytest.mark.parametrize("field", ["until", "warmup"])
@pytest.mark.parametrize("value", [math.nan, math.inf, True])
def test_an_experiment_refuses_an_end_or_warmup_that_is_not_a_time(field, value):
    with pytest.raises(ValueError, match=field):
        ExperimentSpec.model_validate(_spec(**{field: value}))


def test_an_experiment_takes_a_duration():
    assert ExperimentSpec.model_validate(_spec(until="2 min")).until == 120


def test_a_measured_nan_never_agrees():
    from desflow.analysis.intervals import interval

    pair = Pair(
        node="desk",
        measure="utilization",
        statistic="resources.desk.utilization",
        analytical=0.5,
        scale=1.0,
        reason=None,
    )
    assert compare(pair, [math.nan], interval).differs is True
    assert compare(pair, [math.nan, math.nan], interval).differs is True
    assert compare(pair, [0.5], interval).differs is False


@pytest.mark.parametrize(
    "payload, place",
    [
        ({"gen": "uniform", "low": math.nan, "high": 1}, "low"),
        ({"gen": "normal", "mean": 0, "std": math.inf}, "std"),
        ({"gen": "weighted", "options": {"big": math.inf, "small": 1}}, "options.big"),
        ({"gen": "now", "offset": math.nan}, "offset"),
    ],
)
def test_a_generator_refuses_a_literal_nan_or_infinity_at_load(
    tmp_path, payload, place
):
    doc = yaml.safe_load(LOCAL.read_text())
    doc["tasks"]["pack_order"]["payload"] = {"zz": payload}
    path = tmp_path / "m.yaml"
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    result = CliRunner().invoke(app, ["check", str(path)])
    assert result.exit_code == 1
    assert f"{place} must be a finite number" in result.output
    assert "m.yaml:" in result.output
