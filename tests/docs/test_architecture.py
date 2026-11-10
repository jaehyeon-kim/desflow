"""The two diagrams of `ARCHITECTURE.md` match the layers contracts in `pyproject.toml`.

The Packages diagram draws one box per layer of the contract over the packages, and
the Inside Core diagram one box per layer of the contract over `core`. Each box title
names its layer, and each line marked `✗` under a diagram is an `ignore_imports` entry
of its contract. A change to a contract or a diagram fails here until the other
matches.
"""

import re
import tomllib
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Set, Tuple

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = "packages import only the packages below them"
CORE = "the model layer of core imports the run layer, not the other way"
# The box titles of the two layers of core in the Packages diagram
CORE_TITLES = {"core, model layer": 0, "core, run layer": 1}

Layer = Tuple[FrozenSet[str], bool]


def _contracts() -> Dict[str, Dict[str, Any]]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return {c["name"]: c for c in data["tool"]["importlinter"]["contracts"]}


def _layer(text: str, prefix: str = "") -> Layer:
    """A layer as its modules and whether they are independent (`|`) or not (`:`)."""
    sep = "|" if "|" in text else ":"
    return frozenset(prefix + part.strip() for part in text.split(sep)), sep == "|"


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


def _boxes(lines: List[str]) -> List[Tuple[str, List[str]]]:
    """Each box as its title and its body lines, from the top down."""
    boxes: List[Tuple[str, List[str]]] = []
    for line in lines:
        title = re.match(r"\s*┌─ (.+?) ─+┐$", line)
        if title:
            boxes.append((title.group(1), []))
        elif line.strip().startswith("│"):
            boxes[-1][1].append(line.strip().strip("│").strip())
    return boxes


def _crossings(lines: List[str]) -> Set[Tuple[str, str]]:
    found = (re.match(r"\s*✗ (\S+) ──► (\S+)", line) for line in lines)
    return {(m.group(1), m.group(2)) for m in found if m}


def test_the_packages_diagram_draws_the_layers_of_its_contract() -> None:
    contracts = _contracts()
    core = [_layer(text, "desflow.core.") for text in contracts[CORE]["layers"]]
    core_modules = {name.rsplit(".", 1)[1] for names, _ in core for name in names}
    drawn = []
    for title, body in _boxes(_diagram("Packages")):
        if title in CORE_TITLES:
            layer = core[CORE_TITLES[title]]
            # A module of core in the list after the colon must be in that box's layer
            listed = " ".join(body).split(":", 1)[1].split(",")
            names = {re.sub(r"^the ", "", item.strip()) for item in listed}
            placed = {f"desflow.core.{name}" for name in names & core_modules}
            assert placed <= layer[0], f"{title}: {sorted(placed - layer[0])}"
            drawn.append(layer)
        else:
            drawn.append(_layer(title, "desflow."))
    assert drawn == [_layer(text) for text in contracts[PACKAGES]["layers"]]


def test_the_core_diagram_draws_the_layers_of_its_contract() -> None:
    contract = _contracts()[CORE]
    assert contract["containers"] == ["desflow.core"]
    assert contract["exhaustive"] is True
    drawn = [
        (frozenset(re.findall(r"\b[a-z_]+\b", " ".join(body))), False)
        for _, body in _boxes(_diagram("Inside Core"))
    ]
    assert drawn == [_layer(text) for text in contract["layers"]]


def test_each_crossing_line_is_an_ignored_import_of_its_contract() -> None:
    contracts = _contracts()
    assert _crossings(_diagram("Packages")) == _ignored(contracts[PACKAGES])
    assert _crossings(_diagram("Inside Core")) == _ignored(contracts[CORE])
