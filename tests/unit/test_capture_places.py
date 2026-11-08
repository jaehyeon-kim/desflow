"""The places of every YAML model of the capture take only the values they can work
with.

Each model is read once, and its document is changed in memory: a holder's
`current_cap` set to -1, `true`, 2.5 and NaN, and the first parameter of a
distribution set to `true`, NaN and infinity. Loading it must be refused with the
holder's or the parameter's name and the file and line, as `desflow check` would."""

import contextlib
import copy
import dataclasses
import importlib
import importlib.util
import math
import os
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

# Imported here, so the patch in `_load` finds it already in `sys.modules`: imported
# inside `_restored_imports`, it would be dropped and imported again for each case
import pyiceberg.catalog.rest  # noqa: F401
from desflow.models.params import DistributionConfig

build_module = importlib.import_module("desflow.blueprint.build")
ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "second_run_cases", ROOT / "tests" / "unit" / "test_second_run.py"
)
assert _spec is not None and _spec.loader is not None
second_run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(second_run)
regression = second_run.regression

CASES = [case for case in second_run.CASES if not case[0].endswith(":builder")]
PARAMETERS = {
    field.name
    for field in dataclasses.fields(DistributionConfig)
    if field.name not in ("dist", "values", "weights", "edges", "decimals")
}
BAD_CAPACITIES = [-1, True, 2.5, math.nan]
BAD_PARAMETERS = [True, math.nan, math.inf]
HOLDERS = ("resources", "stores", "containers")


def _holders(data: Dict[str, Any]) -> Iterator[Tuple[str, str]]:
    """The first holder of each section that is not a family."""
    for section in HOLDERS:
        entries = data.get(section)
        if not isinstance(entries, dict):
            continue
        for name, entry in entries.items():
            if isinstance(entry, dict) and not entry.get("family"):
                yield section, name
                break


def _parameters(data: Dict[str, Any]) -> Iterator[Tuple[str, str, str]]:
    """The first numeric parameter of the first distribution of services and arrivals."""
    for section in ("services", "arrivals"):
        entries = data.get(section)
        if not isinstance(entries, dict):
            continue
        for name, entry in entries.items():
            if not (isinstance(entry, dict) and "dist" in entry):
                continue
            for key, value in entry.items():
                if (
                    key in PARAMETERS
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                ):
                    yield section, name, key
                    break
            break


def _changing(original: Callable[..., Any], change: Callable[[Dict[str, Any]], None]):
    """`read_document` that gives the document changed by `change`."""

    def read(path, *args, **kwargs):
        data, source = original(path, *args, **kwargs)
        data = copy.deepcopy(data)
        change(data)
        return data, source

    return read


def _load(make: Callable[[], Any], change: Optional[Callable] = None) -> Optional[str]:
    """The refusal text of loading the model with `change` applied, or None."""
    reader = build_module.read_document
    with (
        regression._restored_imports(),
        contextlib.chdir(ROOT),
        patch.dict(os.environ, {"PWD": str(ROOT)}),
        patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
        patch.object(
            build_module,
            "read_document",
            reader if change is None else _changing(reader, change),
        ),
    ):
        try:
            make()
        except Exception as exc:
            return f"{type(exc).__name__}: {exc}"
    return None


def _document(make: Callable[[], Any]) -> Dict[str, Any]:
    captured: List[Dict[str, Any]] = []

    def keep(data: Dict[str, Any]) -> None:
        captured.append(copy.deepcopy(data))

    _load(make, keep)
    return captured[0] if captured else {}


@pytest.mark.parametrize("name,make,until,run", CASES, ids=[case[0] for case in CASES])
def test_a_holder_or_a_parameter_the_model_cannot_work_with_is_refused_at_load(
    name, make, until, run
):
    if _load(make) is not None:
        pytest.skip("the model does not load as it is")
    data = _document(make)
    missed: List[str] = []
    for section, holder in _holders(data):
        for bad in BAD_CAPACITIES:
            if section == "containers" and bad == 2.5:
                continue  # a container holds an amount, not items
            text = _load(make, lambda d: d[section][holder].update(current_cap=bad))
            if text is None or holder not in text or ".yaml:" not in text:
                missed.append(f"{section}.{holder}.current_cap={bad!r}: {text}")
    for section, owner, key in _parameters(data):
        for bad in BAD_PARAMETERS:
            text = _load(make, lambda d: d[section][owner].update({key: bad}))
            if text is None or key not in text or ".yaml:" not in text:
                missed.append(f"{section}.{owner}.{key}={bad!r}: {text}")
    assert not missed, "\n".join(missed)


def _first_task(data: Dict[str, Any]) -> Optional[str]:
    tasks = data.get("tasks")
    return next(iter(tasks), None) if isinstance(tasks, dict) else None


@pytest.mark.parametrize("name,make,until,run", CASES, ids=[case[0] for case in CASES])
def test_a_reading_named_like_a_statistic_or_another_reading_is_refused(
    name, make, until, run
):
    if _load(make) is not None:
        pytest.skip("the model does not load as it is")
    data = _document(make)
    task = _first_task(data)
    if task is None:
        pytest.skip("the model has no task")
    statistic = f"stats.tasks.{task}.finished"
    changes = {
        "a statistic": [{"interval": 5, "readings": {statistic: "100"}}],
        "another reading": [
            {"interval": 5, "readings": {"zz_x": "1"}},
            {"interval": 5, "readings": {"zz_x": "2"}},
        ],
    }
    for what, telemetry in changes.items():
        text = _load(
            make,
            lambda d: d.update(
                telemetry=telemetry,
                stats=d.get("stats") or {"level": "basic", "interval": 5},
            ),
        )
        assert text is not None and ".yaml:" in text, f"{what}: {text}"
