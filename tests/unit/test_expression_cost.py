"""Expressions that are too deep or too costly to compute are refused at once."""

import time

import pytest

from desflow.blueprint.loader import BlueprintError
from desflow.core.context import SimulationContext
from desflow.core.expressions import Expression, Scope

MODEL = """\
simulation: {{sim_id: S, factor: 0, random_seed: 1}}
arrivals: {{a: {{gap: 1, max_events: 1, spawn: t}}}}
tasks:
  t:
    payload: {{n: {{gen: expr, expr: "{expr}"}}}}
run: {{until: 5}}
"""

TOO_LARGE = ["+".join(["1"] * 1000), "round(5, -10**8)"]


def test_a_deep_sum_is_refused_at_load_with_its_line(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(MODEL.format(expr=TOO_LARGE[0]))
    started = time.monotonic()
    with pytest.raises(BlueprintError, match=r"m\.yaml:5: .*nested too deeply"):
        SimulationContext.from_yaml(path)
    assert time.monotonic() - started < 1.0


def test_round_with_huge_digits_is_refused_with_its_line_at_once(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(MODEL.format(expr=TOO_LARGE[1]))
    started = time.monotonic()
    with pytest.raises(Exception, match=r"m\.yaml:5: .*too large"):
        SimulationContext.from_yaml(path).run(until=5)
    assert time.monotonic() - started < 1.0


def test_a_deep_sum_names_the_expression():
    with pytest.raises(ValueError, match="nested too deeply"):
        Expression("+".join(["1"] * 1000))


def test_round_with_huge_negative_digits_is_refused():
    with pytest.raises(ValueError, match="too large"):
        Expression("round(5, -10**8)").evaluate(Scope())


def test_ordinary_expressions_still_work():
    assert Expression("+".join(["1"] * 150)).evaluate(Scope()) == 150
    assert Expression("round(1234, -2)").evaluate(Scope()) == 1200
