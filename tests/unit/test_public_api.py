"""The public API is pinned by tests/unit/public_api.txt, which scripts/public_api.py writes.

The snapshot holds the signature of every name the docs and examples reach, and the
exit codes, arguments and options of the `desflow` command. A change to one of them
fails the first test with the lines that were removed, changed and added, so a
release cannot change the API without a decision. A library that ends the process
cannot be called from a test or a notebook, so the last test checks that every error
raises an exception.
"""

import ast
import importlib.util
from pathlib import Path
from typing import Dict, List

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
        for node in ast.walk(tree):
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
