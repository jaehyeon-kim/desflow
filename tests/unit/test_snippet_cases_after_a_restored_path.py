"""The known cases that run a snippet's `!python` form find their modules whatever ran
before them in the process.

The loader puts the folder of a blueprint on `sys.path` and hides that entry from the
next file it loads. pytest's `monkeypatch` restores `sys.path` after a test, which takes
the loader's entry off without the loader knowing. A case then puts the same folder on
`sys.path` itself, and that copy must stay found. On CI the order of the tests in a
worker decided whether a case met this, so this test sets the order itself: a load from
the case's folder inside a `monkeypatch` block, then the case.
"""

import sys
from pathlib import Path

import pytest

from desflow.blueprint.loader import read_document
from tests.known_cases import harness

ROOT = Path(__file__).resolve().parents[2]

CASES = sorted(
    case
    for case in (
        f"DX-{folder.name.split('_')[1]}"
        for folder in (ROOT / "tests" / "known_cases").glob("dx_*")
    )
    if harness.has_a_snippet(case)
)


def _a_blueprint_of(folder: Path) -> Path:
    """A blueprint of `folder` that a case's `!python` form names, to load first."""
    for case in CASES:
        form = ROOT / harness.form(case, "original").PYTHON_FORM
        if form.parent == folder:
            return form
    raise AssertionError(f"no case runs a blueprint of {folder}")


def test_every_snippet_case_is_checked():
    assert {"DX-1", "DX-3", "DX-5", "DX-8"} <= set(CASES)


@pytest.mark.parametrize("case", CASES)
def test_a_snippet_case_finds_its_modules_after_its_folder_was_restored_off_sys_path(
    case, monkeypatch
):
    logic = ROOT / getattr(
        harness.form(case, "original"), "LOGIC", "docs/snippets/yaml"
    )
    with monkeypatch.context() as inner:
        inner.syspath_prepend(str(ROOT / "tests"))
        # The folder is off `sys.path`, so the load puts its own entry there
        sys.path[:] = [entry for entry in sys.path if entry != str(logic)]
        read_document(_a_blueprint_of(logic))
    assert harness.run_snippet(case).error is None
