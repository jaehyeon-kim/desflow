"""Every Python block in the docs is a copy of a file that a test runs.

A block written straight into a page is never run, and the first such
block that was run had a modelling defect. So a page shows Python only as a labelled
copy (`title="examples/..."` or `title="docs/snippets/..."`), which
test_docs_match_examples.py keeps equal to its file, and each snippet folder has a
test that runs its files:

- `docs/snippets/pages/`: test_docs_page_snippets.py runs every file.
- `docs/snippets/tutorials/`: test_docs_tutorials.py.
- `docs/snippets/guides/`: test_docs_guides.py, which needs a test per file, so a new
  guide's script is checked against what its page says.
- `docs/snippets/yaml/` and `yaml_build/`: test_docs_snippets.py runs or builds each
  blueprint.

`docs/snippets/hot_rolling/` is the one exception: its adapter imports modules of the
hot rolling mill project, which is not in this repository, so it cannot run here.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SNIPPETS = ROOT / "docs" / "snippets"

_PYTHON_FENCE = re.compile(r"^ *```(?:python|py|python3)\b(.*)$", re.MULTILINE)


def test_every_python_block_is_a_labelled_copy():
    unlabelled = []
    for page in sorted((ROOT / "docs").rglob("*.md")):
        text = page.read_text(encoding="utf-8")
        for match in _PYTHON_FENCE.finditer(text):
            label = match.group(1)
            if 'title="examples/' not in label and 'title="docs/snippets/' not in label:
                line = text.count("\n", 0, match.start()) + 1
                unlabelled.append(f"{page.relative_to(ROOT)}:{line}")
    assert unlabelled == [], (
        "Put this code in a file under examples/ or docs/snippets/ that a test runs, "
        "and show it as a labelled copy: " + ", ".join(unlabelled)
    )


def test_every_guide_script_has_a_test():
    tests = (ROOT / "tests" / "docs" / "test_docs_guides.py").read_text(
        encoding="utf-8"
    )
    untested = [
        script.name
        for script in sorted((SNIPPETS / "guides").glob("*.py"))
        if f'_load("{script.stem}")' not in tests
    ]
    assert untested == [], (
        "Add a test to tests/docs/test_docs_guides.py that runs and checks: "
        + ", ".join(untested)
    )


def test_snippet_folders_are_all_run():
    """A new snippet folder needs a test that runs it, listed here."""
    run = {"pages", "tutorials", "guides", "yaml", "yaml_build"}
    not_runnable = {"hot_rolling"}  # imports another project's modules
    folders = {
        p.name for p in SNIPPETS.iterdir() if p.is_dir() and p.name != "__pycache__"
    }
    assert folders <= run | not_runnable, (
        f"Add a test that runs: {sorted(folders - run - not_runnable)}"
    )
