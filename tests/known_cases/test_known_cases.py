"""Every known case of the list runs in all three APIs and gives its original's result.

A case is a folder `tests/known_cases/<case-id>/` (the id in lower case, `-` as `_`) with
`model.yaml`, `builder.py`, `low_level.py` and `original.py` (a plain SimPy program) or
`expected.json`. `index.yaml` has one entry for each case of the list: its id, topic and
title, and either a folder or a `runs_in` that names the case whose document it shares.

For each case the test runs the YAML, the Builder and the Low-level form and checks:

- the YAML and the Builder form publish the same records, with the same `stats`;
- the YAML and the Low-level form publish the same events, as the program logs them;
- the YAML and the Low-level form publish the same `stats`;
- the events equal what `original.py` logs, or `expected.json` states;
- all of them end with an error or all end without one.

A case with an arrival that starts a number of entities at once also runs at sizes 1, 3
and 10, where the order of the events at one time shows a difference.
"""

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from desflow.blueprint.loader import BlueprintError

from tests.known_cases import harness

FOLDER = Path(__file__).parent
INDEX: list[dict[str, Any]] = yaml.safe_load((FOLDER / "index.yaml").read_text("utf-8"))
OWN = {
    harness.folder_of(entry["id"]): entry["id"]
    for entry in INDEX
    if "runs_in" not in entry
}
CASES = sorted(OWN.values())
SIZED = [case for case in CASES if harness.is_sized(case)]


def _paced(cases: list[str]) -> list[Any]:
    """The cases, those that run against the wall clock marked `paced`. A paced case
    runs in a step of its own, without `-n auto`, so another test's CPU time cannot
    make it late."""
    return [
        pytest.param(case, marks=pytest.mark.paced) if harness.is_paced(case) else case
        for case in cases
    ]


def _records(outcome: Any) -> list[dict[str, Any]]:
    """The records of a run without the fields and the readings that tell one run from
    another: the wall-clock time of a record and the lag of a paced run."""
    return [
        {k: v for k, v in record.items() if k not in harness.WALL_FIELDS}
        for record in outcome.records
        if record.get("path_id") not in harness.WALL_PATHS
    ]


def _stats(outcome: Any) -> list[dict[str, Any]]:
    """The statistics a run publishes: the readings at `<sim id>.stats.<path>`."""
    return [r for r in _records(outcome) if ".stats." in str(r.get("path_id"))]


def _check_monitoring(monitored: Any, yaml_run: Any, low_run: Any) -> None:
    """The readings after each request and release, and the trace of each event, that a
    program records with the monitoring patches: the YAML and the Low-level form
    publish the same."""
    tele = list(getattr(monitored, "TELE", []))
    if tele:
        wanted = harness.calls_as_readings(tele)
        assert wanted
        assert harness.reading_series(yaml_run.records) == wanted
        assert harness.reading_series(low_run.records) == wanted
    trace = list(getattr(monitored, "TRACE", []))
    if trace:
        wanted_trace = [(round(t, 3), p, c) for t, p, c in trace if c != "Event"]
        assert wanted_trace
        assert harness.trace_series(yaml_run.trace) == wanted_trace
        assert harness.trace_series(low_run.trace) == wanted_trace


def _check(case: str, tmp_path: Path, size: Any = None) -> None:
    yaml_run = harness.run_yaml(case, tmp_path, size)
    # A case whose program draws random numbers has its draws fixed to values that take
    # every branch for the comparison with the program; the three forms are compared
    # with the draws they make.
    fixed_run = (
        harness.run_yaml(case, tmp_path, size, fixed=True)
        if harness.has_draws(case)
        else yaml_run
    )
    builder_run = harness.run_builder(case, size)
    low_run = harness.run_low_level(case, size)

    if isinstance(yaml_run.error, BlueprintError):
        # the load refuses the document: the Builder and the Low-level form refuse the
        # same input when they run, and the original program fails as well
        assert builder_run.error is not None and low_run.error is not None
        if (FOLDER / harness.folder_of(case) / "original.py").exists():
            assert harness.run_original(case, size)[1] is not None
        return
    assert type(builder_run.error) is type(yaml_run.error), (
        builder_run.error,
        yaml_run.error,
    )
    assert (low_run.error is None) == (yaml_run.error is None), (
        low_run.error,
        yaml_run.error,
    )
    assert _records(builder_run) == _records(yaml_run)
    assert _stats(low_run) == _stats(yaml_run)

    folder = FOLDER / harness.folder_of(case)
    if (folder / "original.py").exists():
        monitored = harness.form(case, "original").HARNESS
        for name in ("TELE", "TRACE"):  # what the program's monitoring patches record
            getattr(monitored, name, []).clear()
        log, error, grants = harness.run_original(case, size)
        assert (error is None) == (fixed_run.error is None), (error, fixed_run.error)
    else:
        expected = json.loads((folder / "expected.json").read_text("utf-8"))
        log = [tuple(row) for row in expected["records"]]
        grants = bool(expected.get("grants", False))
    assert harness.normal(fixed_run.records, grants) == log
    if (folder / "original.py").exists():
        _check_monitoring(monitored, yaml_run, low_run)
    assert harness.normal(low_run.records, grants) == harness.normal(
        yaml_run.records, grants
    )


@pytest.mark.parametrize("case", _paced(CASES))
def test_case_runs_in_three_forms_and_gives_its_original_result(tmp_path, case):
    _check(case, tmp_path)


@pytest.mark.parametrize("size", harness.SIZES)
@pytest.mark.parametrize("case", _paced(SIZED))
def test_case_runs_at_other_sizes(tmp_path, case, size):
    _check(case, tmp_path, size)


def test_every_index_entry_has_a_folder_or_names_the_folder_it_shares():
    ids = [entry["id"] for entry in INDEX]
    assert len(ids) == len(set(ids)), "an id is listed twice"
    for entry in INDEX:
        assert {"id", "topic", "title"} <= set(entry), entry
        assert entry["topic"] in harness.TOPICS, (
            f"{entry['id']}: topic {entry['topic']}"
        )
        folder = FOLDER / harness.folder_of(entry["id"])
        if "runs_in" in entry:
            assert not folder.exists(), f"{entry['id']} has a folder and runs_in"
            home = FOLDER / harness.folder_of(entry["runs_in"])
            assert (home / "model.yaml").exists(), (
                f"{entry['id']} runs_in {entry['runs_in']}, which has no folder"
            )
        else:
            assert (folder / "model.yaml").exists(), f"{entry['id']} has no folder"


def test_every_folder_has_an_index_entry():
    listed = {harness.folder_of(entry["id"]) for entry in INDEX}
    folders = {
        path.name
        for path in FOLDER.iterdir()
        if path.is_dir() and path.name != "__pycache__"
    }
    assert folders - listed == set()


@pytest.mark.parametrize("case", CASES)
def test_case_folder_holds_the_files_of_its_forms(case):
    folder = FOLDER / harness.folder_of(case)
    for name in ("model.yaml", "builder.py", "low_level.py"):
        assert (folder / name).exists(), f"{case}: {name}"
    assert (folder / "original.py").exists() != (folder / "expected.json").exists(), (
        f"{case}: one of original.py and expected.json"
    )


@pytest.mark.parametrize("case", CASES)
def test_case_with_an_arrival_to_size_has_a_size_or_a_reason(case):
    """A document with `initial: <n>` runs at other sizes, or `NOT_SIZED` says why not."""
    if harness.needs_size(case):
        assert harness.is_sized(case) or case in harness.NOT_SIZED, case
    if case in harness.NOT_SIZED:
        assert harness.needs_size(case), f"{case} is not sized and has nothing to size"


def _gives_the_originals_records(case: str, tmp_path: Path, size: int) -> bool:
    """Whether the case's YAML at `size` (draws fixed) gives the records its original
    gives at that size, and both end with an error or both end without one.

    A program that starts a fixed number of entities from a list (ST-RT1) raises
    `StopIteration` at another size, which counts as a difference. An original that
    logs nothing and ends with no error at a size, where it logs records unsized, fails
    the test: the size harness has removed a run the program makes.
    """
    fixed_run = harness.run_yaml(case, tmp_path, size, fixed=True)
    try:
        log, error, grants = harness.run_original(case, size)
    except StopIteration:
        return False
    if not log and error is None:
        assert not harness.run_original(case)[0], (
            f"{case}: the original logs nothing at size {size} and records unsized"
        )
    if (fixed_run.error is None) != (error is None):
        return False
    return harness.normal(fixed_run.records, grants) == log


@pytest.mark.parametrize("case", _paced(sorted(harness.NOT_SIZED)))
def test_not_sized_case_differs_at_other_sizes(tmp_path, case):
    """A `NOT_SIZED` reason says another size is another scenario. A run shows it: the
    YAML does not give the original's records at all of the sizes 1, 3 and 10. A case
    that does gets a `size` in its forms instead."""
    same = []
    for size in harness.SIZES:
        work = tmp_path / str(size)
        work.mkdir()
        if _gives_the_originals_records(case, work, size):
            same.append(size)
    assert same != list(harness.SIZES), (
        f"{case}: the YAML equals its original at {same}"
    )
