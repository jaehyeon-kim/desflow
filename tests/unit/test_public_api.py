"""The public API is pinned by tests/unit/public_api.txt, which scripts/public_api.py writes.

The snapshot holds the signature of every name the docs and examples reach, named at
the first import path 1.x keeps that exports it (`public_api.PROMISED_PACKAGES`), and
the exit codes, arguments and options of the `desflow` command. A change to one of them
fails the first test with the lines that were removed, changed and added, so a
release cannot change the API without a decision. A library that ends the process
cannot be called from a test or a notebook, so the last test checks that every error
raises an exception.
"""

import ast
import importlib
import importlib.util
import re
import textwrap
from pathlib import Path
from typing import Any, Dict, List

import pytest

from desflow import SimulationContext
from desflow.blueprint import BlueprintError, build

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "src" / "desflow"
_SCRIPT = ROOT / "scripts" / "public_api.py"
_spec = importlib.util.spec_from_file_location("public_api", _SCRIPT)
assert _spec is not None and _spec.loader is not None
public_api = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(public_api)

REGENERATE = (
    "If the change is intended, run `uv run --all-extras python scripts/public_api.py` "
    "and add an Upgrading line to the Changelog for each removed or changed name."
)

UNKNOWN_KEY = """\
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 7
  ranom_seed: 7
resources:
  packers: {current_cap: 2, max_cap: 5}
services:
  packing: {dist: normal, mean: 3.0, std: 0.5}
arrivals:
  standard: {dist: exponential, rate: 1.0, spawn: part}
tasks:
  part: {service: packing, resource: packers}
run:
  until: 30
"""


def _by_key(text: str) -> Dict[str, str]:
    return {public_api.key(line): line for line in text.splitlines()}


def difference(expected: str, actual: str) -> str:
    """The removed, changed and added lines, then how to regenerate; empty if equal."""
    if expected == actual:
        return ""
    old, new = _by_key(expected), _by_key(actual)
    removed = [old[k] for k in old if k not in new]
    added = [new[k] for k in new if k not in old]
    changed = [
        f"  was: {old[k]}\n  now: {new[k]}"
        for k in old
        if k in new and old[k] != new[k]
    ]
    parts = [
        "The public API differs from tests/unit/public_api.txt.",
        f"Removed ({len(removed)}):" + "".join(f"\n  {line}" for line in removed),
        f"Changed ({len(changed)}):" + "".join(f"\n{block}" for block in changed),
        f"Added ({len(added)}):" + "".join(f"\n  {line}" for line in added),
        REGENERATE,
    ]
    return "\n".join(parts)


def test_public_api_matches_the_snapshot():
    expected = public_api.SNAPSHOT.read_text(encoding="utf-8")
    message = difference(expected, public_api.snapshot())
    assert message == "", "\n" + message


def test_each_promised_package_exports_a_name_once():
    for package in public_api.PROMISED_PACKAGES:
        names = importlib.import_module(package).__all__
        twice = sorted({name for name in names if names.count(name) > 1})
        assert twice == [], f"{package}.__all__ lists {twice} more than once"


def test_every_documented_name_is_in_the_snapshot():
    paths = public_api.names()
    # The three sources together name far more than `desflow.__all__` does.
    assert len(paths) > 100
    lines = _by_key(public_api.SNAPSHOT.read_text(encoding="utf-8"))
    unresolved: List[str] = []
    for path in paths:
        try:
            public_api.resolve(path)
        except (ImportError, AttributeError):
            unresolved.append(path)
    assert unresolved == [], f"names that do not resolve: {unresolved}"
    missing = [path for path in paths if path not in lines]
    assert missing == [], f"names with no line in public_api.txt: {missing}"


def test_errors_raise_and_never_end_the_process(tmp_path):
    path = tmp_path / "blueprint.yaml"
    path.write_text(UNKNOWN_KEY, encoding="utf-8")
    assert issubclass(BlueprintError, Exception)
    assert not issubclass(BlueprintError, SystemExit)
    for load in (build, SimulationContext.from_yaml):
        with pytest.raises(BlueprintError, match=r"did you mean 'random_seed'"):
            load(path)

    # Only the command line may end the process, with typer.Exit.
    ending: List[str] = []
    for source in sorted(SOURCE.rglob("*.py")):
        relative = source.relative_to(SOURCE)
        in_cli = "cli" in relative.with_suffix("").parts
        tree = ast.parse(source.read_text(encoding="utf-8"))
        # Catching an exit, as a command does for a user's file, ends nothing
        caught = set()
        for handler in ast.walk(tree):
            if isinstance(handler, ast.ExceptHandler) and handler.type is not None:
                caught.update(id(part) for part in ast.walk(handler.type))
        for node in ast.walk(tree):
            if id(node) in caught:
                continue
            name = ast.unparse(node) if isinstance(node, ast.Attribute) else ""
            if isinstance(node, ast.Name):
                name = node.id
            if isinstance(node, ast.ImportFrom) and node.module in ("sys", "os"):
                name = ",".join(f"{node.module}.{a.name}" for a in node.names)
            ends = (
                name in ("sys.exit", "os._exit", "SystemExit", "exit", "quit")
                or "sys.exit" in name.split(",")
                or "os._exit" in name.split(",")
                or (name in ("typer.Exit", "Exit") and not in_cli)
            )
            if ends:
                ending.append(f"{relative}:{node.lineno}: {name}")
    assert ending == [], "; ".join(ending)


def test_the_api_page_names_each_object_at_its_promised_path():
    wrong = []
    for number, line in enumerate(
        public_api.API_PAGE.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.startswith("::: "):
            continue
        written = line[4:].strip()
        promised = public_api.promised_path(public_api.resolve(written), written)
        if not public_api.is_at_promised_path(written, promised):
            wrong.append(f"api.md:{number}: ::: {written} should be ::: {promised}")
    assert wrong == [], "\n" + "\n".join(wrong)


def _outside_message(path: Path, found: Any) -> str:
    package, name = found.use.rsplit(".", 1) if found.use else ("", "")
    if found.text.startswith("import "):
        use = "import the names it uses from a promised path, such as `from desflow import ...`"
    elif found.use:
        use = f"the promised path is {found.use}: `from {package} import {name}`"
    else:
        use = "no path 1.x keeps exports it"
    return (
        f"{path.relative_to(ROOT)}:{found.line}: `{found.text}` is outside"
        f" the import paths 1.x keeps; {use}"
    )


def test_docs_examples_and_the_skill_import_only_from_promised_paths():
    wrong = []
    for folder in ("docs", "examples", "skills/desflow"):
        for path in sorted((ROOT / folder).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            wrong.extend(
                _outside_message(path, found)
                for found in public_api.imports_outside_promise(
                    path.read_text(encoding="utf-8")
                )
            )
    assert wrong == [], "\n" + "\n".join(wrong)


# Imports in a page of docs/ that the guard below finds and 13d1 fixes. Each is the
# page and line, and the row of docs-changes.md (its page and Section cell) that
# names the fix. Remove the entry when the row is handled. The guard fails for a
# line that is not here, and for an entry that no longer matches an import.
KNOWN_OUTSIDE_IN_PAGES: Dict[str, str] = {}

_FENCE = re.compile(r"^\s*```(\w*)")
_IMPORT_TEXT = re.compile(
    r"\bfrom\s+desflow(?:\.\w+)*\s+import\s+\w+(?:\s*,\s*\w+)*|\bimport\s+desflow(?:\.\w+)*"
)


def _imports_in_page(text: str) -> List[Any]:
    """The imports outside the promise in the code blocks and the sentences of a page.

    A `python` block is read as code, and every other line outside a block is read for
    an import written in a sentence, such as `from desflow.analysis import read_column`.
    """
    found: List[Any] = []
    lines = text.splitlines()
    block: List[str] = []
    start = 0
    language = None
    for number, line in enumerate(lines, start=1):
        fence = _FENCE.match(line)
        if fence and language is None:
            language, start, block = fence.group(1), number, []
        elif fence:
            if language in ("python", "py"):
                source = textwrap.dedent("\n".join(block))
                try:
                    inside = public_api.imports_outside_promise(source)
                except SyntaxError:
                    inside = []
                found.extend(
                    public_api.OutsideImport(start + hit.line, hit.text, hit.use)
                    for hit in inside
                )
            language = None
        elif language is not None:
            block.append(line)
        else:
            for match in _IMPORT_TEXT.finditer(line):
                found.extend(
                    public_api.OutsideImport(number, hit.text, hit.use)
                    for hit in public_api.imports_outside_promise(match.group(0))
                )
    return found


def test_pages_import_only_from_promised_paths():
    wrong = []
    seen = set()
    for path in sorted((ROOT / "docs").rglob("*.md")):
        for found in _imports_in_page(path.read_text(encoding="utf-8")):
            where = f"{path.relative_to(ROOT).as_posix()}:{found.line}"
            seen.add(where)
            if where not in KNOWN_OUTSIDE_IN_PAGES:
                wrong.append(_outside_message(path, found))
    assert wrong == [], "\n" + "\n".join(wrong)
    assert sorted(set(KNOWN_OUTSIDE_IN_PAGES) - seen) == [], (
        "These pages no longer have the import: remove them from KNOWN_OUTSIDE_IN_PAGES"
    )


def test_the_guard_names_the_promised_path():
    found = public_api.imports_outside_promise(
        "from desflow.core.registry import SimulationRegistry\n"
        "from desflow.explain.render import render_terminal\n"
        "from desflow.connectors.egress.kafka import KafkaEgress\n"
        "from desflow.blueprint import BlueprintError\n"
        "import desflow.cli.check\n"
    )
    assert [(f.line, f.use) for f in found] == [
        (1, "desflow.SimulationRegistry"),
        (2, "desflow.explain.render_terminal"),
        (5, ""),
    ]
