"""`explain` returns for every model that builds, and its graph is well formed.

For each model of the capture (the cases of `test_second_run.py`) that builds,
`build_graph`, `analyze`, `render_terminal` and `to_json` return, the node ids are
unique, and every edge ends at a node.
"""

import importlib.util
from pathlib import Path

import pytest
import scipy.optimize  # noqa: F401  # imported before the capture restores sys.modules
import scipy.stats  # noqa: F401

from desflow.explain import analyze, build_graph
from desflow.explain.render import render_terminal, to_json

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "connector_cases", ROOT / "tests" / "unit" / "test_connector_second_run.py"
)
assert _spec is not None and _spec.loader is not None
cases = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cases)
MODELS = cases.second_run.CASES


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=[c[0] for c in MODELS])
def test_explain_returns_and_the_graph_is_well_formed(name, make, until, run):
    with cases._capture_environment():
        try:
            context = make()
        except Exception as exc:
            pytest.skip(f"the model does not build here: {type(exc).__name__}")
        graph = build_graph(context)
        explanation = analyze(graph)
        render_terminal(explanation)
        to_json(explanation)
    ids = [node.id for node in graph.nodes]
    assert len(ids) == len(set(ids)), f"{name}: repeated node ids"
    missing = [
        edge for edge in graph.edges if edge.source not in ids or edge.target not in ids
    ]
    assert not missing, f"{name}: edges that end at no node: {missing[:2]}"
