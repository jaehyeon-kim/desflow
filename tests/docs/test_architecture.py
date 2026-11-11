"""The text diagrams of `ARCHITECTURE.md` match the layers contracts in `pyproject.toml`.

The Packages diagram lists the layers of the contract over the packages, from the top
down, one on each row, and the Inside Core diagram lists the modules of each layer of
the contract over `core`. Each line marked `✗` under a diagram is an `ignore_imports`
entry of its contract. A change to a contract or a text diagram fails here until the
other matches.
"""

import re
import tomllib
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Set, Tuple

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = "packages import only the packages below them"
CORE = "the model layer of core imports the run layer, not the other way"
# The names of the two layers of core in the Packages diagram, by place in the core contract
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


def _packages_drawn(lines: List[str]) -> List[str]:
    """The packages of the Packages diagram, row by row and from left to right in a row."""
    drawn = []
    for line in lines:
        if not line.strip():
            break
        drawn += re.findall(r"core, (?:model|run) layer|[a-z]+", line)
    return drawn


def _core_layers(lines: List[str]) -> List[Layer]:
    """The modules of each layer of the Inside Core diagram, from the top down."""
    modules = {path.stem for path in (ROOT / "src" / "desflow" / "core").glob("*.py")}
    layers: List[List[str]] = []
    for line in lines:
        if re.match(r" (model|run) layer\b", line):
            layers.append([])
            line = line[len(" model layer") :]
        if layers:
            # The notes in brackets are not module names
            words = re.findall(r"\b[a-z_]+\b", re.sub(r"\([^)]*\)", "", line))
            layers[-1] += [word for word in words if word in modules]
    return [(frozenset(found), False) for found in layers]


def _crossings(lines: List[str]) -> Set[Tuple[str, str]]:
    found = (re.match(r"\s*✗ (\S+) ──► (\S+)", line) for line in lines)
    return {(m.group(1), m.group(2)) for m in found if m}


def test_the_packages_diagram_draws_the_layers_of_its_contract() -> None:
    contracts = _contracts()
    layers = contracts[PACKAGES]["layers"]
    core = [i for i, text in enumerate(layers) if "desflow.core." in text]
    index = {}
    for i, text in enumerate(layers):
        for package in _layer(text)[0]:
            if not package.startswith("desflow.core."):
                index[package.removeprefix("desflow.")] = i
    for title, place in CORE_TITLES.items():
        index[title] = core[place]
    drawn = _packages_drawn(_diagram("Packages"))
    # Each package of the contract is drawn once, so none is missing or extra
    assert sorted(drawn) == sorted(index)
    # A row is at or below the rows above it, in the order of the contract's layers
    ranks = [index[name] for name in drawn]
    assert ranks == sorted(ranks), drawn
    # The two layers of core are the two layers of the core contract
    contract = _contracts()[CORE]["layers"]
    assert [_layer(layers[i]) for i in core] == [
        _layer(text, "desflow.core.") for text in contract
    ]


def test_the_core_diagram_lists_the_modules_of_its_layers() -> None:
    contract = _contracts()[CORE]
    assert contract["containers"] == ["desflow.core"]
    assert contract["exhaustive"] is True
    drawn = _core_layers(_diagram("Inside Core"))
    assert drawn == [_layer(text) for text in contract["layers"]]


def test_each_crossing_line_is_an_ignored_import_of_its_contract() -> None:
    contracts = _contracts()
    assert _crossings(_diagram("Packages")) == _ignored(contracts[PACKAGES])
    assert _crossings(_diagram("Inside Core")) == _ignored(contracts[CORE])
