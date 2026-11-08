"""`replicate` reads `run.until` of a blueprint a factory loads, as it does for a path.

Every YAML blueprint of the capture (the cases of `test_second_run.py`) that has a
`run.until` is replicated with its path and with a factory that loads it, with no
`until`, and both give the same values.
"""

import contextlib
import functools
import importlib.util
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from desflow import SimulationContext, replicate
from desflow.analysis.models import default_until

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "second_run_cases", ROOT / "tests" / "unit" / "test_second_run.py"
)
assert _spec is not None and _spec.loader is not None
second_run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(second_run)
regression = second_run.regression

YAML = [
    (name, make.args[0] if isinstance(make, functools.partial) else None)
    for name, make, *_ in second_run.CASES
    if name.startswith(("example:", "scenario:")) and name.endswith(":yaml")
]


@pytest.mark.parametrize("name,path", YAML, ids=[name for name, _ in YAML])
def test_a_factory_that_loads_a_blueprint_has_its_run_until(name, path):
    logging.disable(logging.CRITICAL)
    try:
        with (
            regression._restored_imports(),
            contextlib.chdir(ROOT),
            patch.dict(os.environ, {"PWD": str(ROOT)}),
            patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
        ):
            factory = functools.partial(SimulationContext.from_yaml, path)
            assert default_until(factory) == default_until(path), name
    finally:
        logging.disable(logging.NOTSET)


def test_replicate_with_a_factory_and_with_a_path_give_the_same_values():
    path = ROOT / "examples" / "yaml" / "local.yaml"
    measures = ["tasks.pack_order.finished"]
    by_path = replicate(str(path), 2, seed=1, measures=measures)
    by_factory = replicate(
        functools.partial(SimulationContext.from_yaml, str(path)),
        2,
        seed=1,
        measures=measures,
    )
    assert by_factory.values == by_path.values
