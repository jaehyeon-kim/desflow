"""A second build of the same code gives the same values for the same seed.

`test_second_run.py` runs one context twice, so it cannot see a name that a build takes
from how many builds the process made before it. Each Builder and low-level model of the
capture (the cases of `test_second_run.py`) is built twice from its factory in one
process, and each build is run with one seed: the records and the statistics must be
equal.
"""

import contextlib
import importlib.util
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import scipy.optimize  # noqa: F401  # imported before the capture restores sys.modules
import scipy.stats  # noqa: F401

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "second_run_cases", ROOT / "tests" / "unit" / "test_second_run.py"
)
assert _spec is not None and _spec.loader is not None
second_run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(second_run)
regression = second_run.regression

MODELS = [
    case
    for case in second_run.CASES
    if (case[0].endswith(":builder") or case[0].endswith(":low_level"))
    and case[0] not in second_run.NOT_COMPARED
]


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=[c[0] for c in MODELS])
def test_two_builds_of_one_factory_give_the_same_records(name, make, until, run):
    logging.disable(logging.CRITICAL)
    try:
        with (
            regression._restored_imports(),
            contextlib.chdir(ROOT),
            patch.dict(os.environ, {"PWD": str(ROOT)}),
            patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
        ):
            first = second_run._outcome(make(), until, run)
            second = second_run._outcome(make(), until, run)
    finally:
        logging.disable(logging.NOTSET)
    assert second == first, f"{name}: {second_run._first_difference(second, first)}"


def test_an_unnamed_template_takes_its_key_from_the_run_not_the_process():
    from desflow.core.environment import DynamicRealtimeEnvironment
    from desflow.values import Template, ValueRuntime

    def build_and_render() -> float:
        template = Template({"amount": {"gen": "uniform", "low": 0, "high": 100}})
        runtime = ValueRuntime(7, DynamicRealtimeEnvironment(factor=0))
        rendered = runtime.render(template)
        assert rendered is not None
        return rendered["amount"]

    assert build_and_render() == build_and_render()
