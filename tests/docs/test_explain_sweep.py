"""`--verify` on every blueprint in examples/yaml/ and docs/snippets/yaml/: no row
may say "differs".

Each blueprint in examples/yaml/ (advanced/ included) and docs/snippets/yaml/ is
analysed. A blueprint whose analysis covers no measure has nothing to compare, so it
is not run; every one of its rows must carry the reason. The others run once, with
seed 1, for 100,000 seconds after the warm-up, long enough for every supported
value to settle within the 10 percent one run allows. A "differs" here is a value
the analysis gives where its formulas do not hold, or a defect in the run.
"""

from pathlib import Path

import pytest

from desflow import SimulationContext
from desflow.explain.pairs import pairs

ROOT = Path(__file__).resolve().parents[2]
UNTIL = 100_000


def _blueprints():
    """Every model file: experiment and fit specs have no `simulation` key, and a
    `*_error.yaml` blueprint fails to load on purpose, to show the error. A blueprint
    with `run.before` hooks creates Kafka topics, so it needs a broker; its guide test
    runs it with the hook replaced."""
    files = sorted((ROOT / "examples" / "yaml").rglob("*.yaml"))
    files += sorted((ROOT / "docs" / "snippets" / "yaml").glob("*.yaml"))
    return [
        f
        for f in files
        if any(line.startswith("simulation:") for line in f.read_text().splitlines())
        and not f.name.endswith("_error.yaml")
        and not any(line.startswith("  before:") for line in f.read_text().splitlines())
    ]


@pytest.mark.parametrize("path", _blueprints(), ids=lambda p: str(p.relative_to(ROOT)))
def test_verify_finds_no_difference(path):
    context = SimulationContext.from_yaml(path)
    found = pairs(context.analyze(), 1.0)
    assert all(p.analytical is not None or p.reason for p in found)
    if all(p.analytical is None for p in found):
        return  # nothing the formulas cover, so nothing to run
    start = context.initial_time + (context.warmup or 0.0)
    verification = context.analyze(
        verify=True, until=start + UNTIL, seed=1
    ).verification
    differing = [
        (c.node, c.measure, c.analytical, c.measured)
        for c in verification.comparisons
        if c.differs
    ]
    assert differing == []
    assert all(c.differs is not None or c.reason for c in verification.comparisons)
