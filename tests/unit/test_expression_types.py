"""An expression gives a real number, a text, a bool or a collection, never a complex
number."""

import pytest

from desflow.builder.context import SimulationContext

HEAD = """\
simulation: {sim_id: P, factor: 0, random_seed: 42}
arrivals: {x: {dist: constant, value: 1, max_events: 1, spawn: t, attributes: {v: -4}}}
run: {until: 10}
"""


def _run(tmp_path, tasks):
    path = tmp_path / "m.yaml"
    path.write_text(HEAD + tasks)
    app = SimulationContext.from_yaml(path)
    app.run(until=10)


@pytest.mark.parametrize(
    "tasks",
    [
        "tasks: {t: {payload: {root: {gen: expr, expr: 'attr.v ** 0.5'}}}}\n",
        "tasks: {t: {payload: {e: 1}, fail: 'attr.v ** 0.5 > 1'}}\n",
    ],
    ids=["payload", "fail condition"],
)
def test_a_negative_base_with_a_fractional_exponent_is_refused(tmp_path, tasks):
    with pytest.raises(Exception, match="complex number"):
        _run(tmp_path, tasks)


def test_a_real_power_still_runs(tmp_path):
    _run(tmp_path, "tasks: {t: {payload: {r: {gen: expr, expr: 'attr.v ** 2'}}}}\n")
