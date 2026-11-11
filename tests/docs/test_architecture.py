"""The text diagrams of `ARCHITECTURE.md` match the layers contract and the packages.

The Packages diagram lists the layers of the contract in `pyproject.toml`, from the top
down, one on each row. Each line marked `✗` under it is an `ignore_imports` entry of the
contract. The Inside Core, Inside Builder and Inside Blueprint diagrams each name every
module of their package. A change to the contract, a package or a text diagram fails
here until the other matches.
"""

import re
import tomllib
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pytest

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = "packages import only the packages below them"


def _contract() -> Dict[str, Any]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    contracts = data["tool"]["importlinter"]["contracts"]
    return next(c for c in contracts if c["name"] == PACKAGES)


def _ignored(contract: Dict[str, Any]) -> Set[Tuple[str, str]]:
    pairs = set()
    for entry in contract.get("ignore_imports", []):
        importer, imported = entry.split("->")
        pairs.add((importer.strip(), imported.strip()))
    return pairs


def _diagram(heading: str) -> List[str]:
    """The lines of the first code block after a heading of `ARCHITECTURE.md`."""
    text = (ROOT / "ARCHITECTURE.md").read_text()
    section = text.split(f"\n## {heading}\n", 1)[1]
    return section.split("```\n", 1)[1].split("\n```", 1)[0].splitlines()


def _packages_drawn(lines: List[str]) -> List[str]:
    """The packages of the Packages diagram, row by row and from left to right in a row."""
    drawn = []
    for line in lines:
        if not line.strip():
            break
        drawn += re.findall(r"[a-z]+", line)
    return drawn


def _crossings(lines: List[str]) -> Set[Tuple[str, str]]:
    found = (re.match(r"\s*✗ (\S+) ──► (\S+)", line) for line in lines)
    return {(m.group(1), m.group(2)) for m in found if m}


def test_the_packages_diagram_draws_the_layers_of_its_contract() -> None:
    index = {}
    for i, text in enumerate(_contract()["layers"]):
        for package in re.split(r"[|:]", text):
            index[package.strip().removeprefix("desflow.")] = i
    drawn = _packages_drawn(_diagram("Packages"))
    # Each package of the contract is drawn once, so none is missing or extra
    assert sorted(drawn) == sorted(index)
    # A row is at or below the rows above it, in the order of the contract's layers
    ranks = [index[name] for name in drawn]
    assert ranks == sorted(ranks), drawn


def test_each_crossing_line_is_an_ignored_import_of_the_contract() -> None:
    assert _crossings(_diagram("Packages")) == _ignored(_contract())


@pytest.mark.parametrize(
    ("heading", "package"),
    [
        ("Inside Core", "core"),
        ("Inside Builder", "builder"),
        ("Inside Blueprint", "blueprint"),
    ],
)
def test_each_package_diagram_names_every_module_of_its_package(
    heading: str, package: str
) -> None:
    modules = {
        path.stem
        for path in (ROOT / "src" / "desflow" / package).glob("*.py")
        if path.stem != "__init__"
    }
    # The notes in brackets are not module names
    text = re.sub(r"\([^)]*\)", "", "\n".join(_diagram(heading)))
    assert modules <= set(re.findall(r"\b[a-z_]+\b", text))
