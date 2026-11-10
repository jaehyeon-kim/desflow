"""Every known case of the list runs in all three APIs and gives its original's result.

A case is a folder `tests/known_cases/<case-id>/` (the id in lower case, `-` as `_`) with
`model.yaml`, `builder.py`, `low_level.py` and `original.py` or `expected.json`. A case
whose forms are one of SimPy's examples, as the EX- rows are, holds only `original.py`,
whose `EXAMPLE` names the example's files under `examples/`. The
`original.py` is one of four kinds: a plain SimPy program; a function `check` that states
the checks of the row, as GA-, BT-OS, BT-WL and some DX- rows do; a project's own
generator, `generate` with `CONFIGS` and `compare`, as HR-01 and the other BT- rows do;
or `PYTHON_FORM`, the document of a docs snippet or an example that runs its `!python`,
as other DX- rows do. `index.yaml` has one entry for each case of the list: its id, topic and
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

import inspect
import json
from pathlib import Path
from typing import Any, Optional

import pytest
import yaml
from desflow.blueprint.loader import BlueprintError
from desflow.core.signals import SignalFailed
from desflow.core.task import TaskFailed

from tests.known_cases import harness

FOLDER = Path(__file__).parent
INDEX: list[dict[str, Any]] = yaml.safe_load((FOLDER / "index.yaml").read_text("utf-8"))
OWN = {
    harness.folder_of(entry["id"]): entry["id"]
    for entry in INDEX
    if "runs_in" not in entry and "reason" not in entry
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


_MESSAGE_START = 30


def _same_error(one: Optional[BaseException], other: Optional[BaseException]) -> None:
    """Both ended without an error, or with the same class and the same start of the
    message, at every size, so a timeout cannot stand for the error a form is expected
    to raise. The Builder and the YAML are desflow on both sides, so one rule holds."""
    assert (one is None) == (other is None), (one, other)
    if one is None or other is None:
        return
    assert not isinstance(one, TimeoutError) and not isinstance(other, TimeoutError), (
        one,
        other,
    )
    assert type(one) is type(other), (one, other)
    assert str(one)[:_MESSAGE_START] == str(other)[:_MESSAGE_START], (one, other)


def _same_original_error(
    case: str, text: Optional[str], error: Optional[BaseException], size: Any
) -> None:
    """The error of a plain program, as `Class: message`, against the YAML's. It is the
    original's, which `run_original` gives in that form, or the Low-level form's. A task
    or a signal that fails wraps the program's error in `TaskFailed` or `SignalFailed`,
    whose reason is the program's message, so the program's class is the one
    `harness.WRAPPED` names for the case; the check of a rule that desflow words itself
    (`harness.OWN_WORDING`) has the class compared."""
    assert (text is None) == (error is None), (text, error)
    if text is None or error is None:
        return
    assert not text.startswith("TimeoutError"), text
    if size is not None:
        return
    name, _, message = text.partition(": ")
    if isinstance(error, (TaskFailed, SignalFailed)):
        assert name == harness.WRAPPED.get(case), (text, error)
        assert message[:_MESSAGE_START] == str(error.reason)[:_MESSAGE_START], (
            text,
            error,
        )
        return
    assert type(error).__name__ == name, (text, error)
    if case not in harness.OWN_WORDING:
        assert message[:_MESSAGE_START] == str(error)[:_MESSAGE_START], (text, error)


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
    _same_error(builder_run.error, yaml_run.error)
    _same_original_error(
        case,
        None
        if low_run.error is None
        else f"{type(low_run.error).__name__}: {low_run.error}",
        yaml_run.error,
        size,
    )
    assert _records(builder_run) == _records(yaml_run)
    assert _stats(low_run) == _stats(yaml_run)

    folder = FOLDER / harness.folder_of(case)
    if harness.has_a_snippet(case):
        # the original is a snippet that runs its `!python`: the YAML form has its shape
        # (the snippet has one size, so only the forms are compared at another)
        if size is None:
            harness.compare_with_snippet(case, yaml_run.records)
        assert harness.normal(low_run.records, True) == harness.normal(
            yaml_run.records, True
        )
        return
    if harness.has_a_generator(case):
        # the original is a project's generator: the YAML with its draws fixed is
        # compared with it (one size, because the project has one scenario)
        if size is None:
            harness.run_against_generator(case, tmp_path)
        assert harness.normal(low_run.records) == harness.normal(yaml_run.records)
        return
    if harness.states_its_checks(case):
        # the original is a list of stated checks, not a program: they run on the YAML
        if size is None:
            check = harness.form(case, "original").check
            if "seconds" in inspect.signature(check).parameters:  # a timed run
                check(
                    yaml_run.records,
                    seconds=yaml_run.seconds,  # type: ignore[attr-defined]
                )
            else:
                check(yaml_run.records)
        assert harness.normal(low_run.records, True) == harness.normal(
            yaml_run.records, True
        )
        return
    if (folder / "original.py").exists():
        monitored = harness.form(case, "original").HARNESS
        for name in ("TELE", "TRACE"):  # what the program's monitoring patches record
            getattr(monitored, name, []).clear()
        log, error, grants = harness.run_original(case, size)
        _same_original_error(case, error, fixed_run.error, size)
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
    # 569 rows, which the command of known-cases.md, Counts, gives (BT-PR8 moved after
    # 1.0 and is not listed), and the 7 rows it lists under "Left out". A row with no
    # entry fails here.
    assert len(ids) == 569 + len(harness.LEFT_OUT), len(ids)
    for entry in INDEX:
        assert {"id", "topic", "title"} <= set(entry), entry
        assert sum(key in entry for key in ("runs_in", "reason")) <= 1, entry
        assert entry["topic"] in harness.TOPICS, (
            f"{entry['id']}: topic {entry['topic']}"
        )
        folder = FOLDER / harness.folder_of(entry["id"])
        if "reason" in entry:
            assert entry["id"] in harness.LEFT_OUT, f"{entry['id']} is not left out"
            assert not folder.exists(), f"{entry['id']} has a folder and a reason"
        elif entry["id"] in harness.LEFT_OUT:
            raise AssertionError(f"{entry['id']} is left out and has no reason")
        elif "runs_in" in entry:
            assert not folder.exists(), f"{entry['id']} has a folder and runs_in"
            home = harness.form_paths(entry["runs_in"])["model.yaml"]
            assert home.exists(), (
                f"{entry['id']} runs_in {entry['runs_in']}, which has no YAML form"
            )
        else:
            assert folder.is_dir(), f"{entry['id']} has no folder"
            assert harness.form_paths(entry["id"])["model.yaml"].exists(), entry["id"]


def test_every_index_entry_is_a_row_of_known_cases():
    """The ids of the index are the ids of the rows of known-cases.md, which `rows.txt`
    lists (its comment names the command that writes it), and the rows that list leaves
    out (`harness.LEFT_OUT`)."""
    lines = (FOLDER / "rows.txt").read_text("utf-8").splitlines()
    rows = {line for line in lines if line and not line.startswith("#")}
    assert len(rows) == 569, len(rows)
    ids = {entry["id"] for entry in INDEX}
    assert ids - harness.LEFT_OUT == rows, (ids - harness.LEFT_OUT) ^ rows
    assert not harness.LEFT_OUT & rows


def test_no_python_in_known_cases():
    """No YAML form of a case holds `!python`: a case is a model with no Python in it."""
    found = [
        case
        for case in CASES
        if "!python" in harness.python_tags(harness.form_paths(case)["model.yaml"])
    ]
    assert found == []


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
    """A case has its three forms, and an original or the records it expects. The forms
    of a case that runs an example are the example's files, and its folder holds no
    second copy of them."""
    folder = FOLDER / harness.folder_of(case)
    for name, path in harness.form_paths(case).items():
        assert path.exists(), f"{case}: {name}"
        if harness.example_of(case) is not None:
            assert not (folder / name).exists(), f"{case}: a second {name}"
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
    if harness.has_a_generator(case):
        return harness.matches_generator(case, tmp_path, size)
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
