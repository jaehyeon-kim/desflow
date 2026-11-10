"""Shared by the BT cases (the benchtop projects): the project folders, the builder
helpers, and the stand-ins the originals need.

The projects are the copy in `tests/copied_projects/benchtop`, which the tests read and never
change.
"""

import contextlib
import functools
import importlib
import sys
import types
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from desflow.values import generator

PROJECTS = Path(__file__).resolve().parents[1] / "copied_projects" / "benchtop"


@contextlib.contextmanager
def importable(path: Path) -> Iterator[None]:
    """Makes the modules of a folder importable, with no bytecode written into it, and
    restores both afterwards (the modules it imported stay in `sys.modules`).

    Args:
        path: The folder.

    Yields:
        None: Inside the block the folder's modules import.
    """
    before = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    added = str(path) not in sys.path
    if added:
        sys.path.insert(0, str(path))
    try:
        yield
    finally:
        sys.dont_write_bytecode = before
        if added and str(path) in sys.path:
            sys.path.remove(str(path))


@contextlib.contextmanager
def project_path(name: str) -> Iterator[Path]:
    """The folder of a benchtop project, made importable for the block.

    Args:
        name: The project's folder, such as `live-dashboard`.

    Yields:
        Path: The folder.
    """
    path = PROJECTS / name
    if not path.is_dir():
        raise FileNotFoundError(f"the benchtop project {name} is not at {path}")
    with importable(path):
        yield path


def in_project(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """A decorator that runs a function inside `project_path(name)`.

    Args:
        name: The project's folder.

    Returns:
        Callable: The decorator, which returns the function wrapped.
    """

    def wrap(function: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(function)
        def run(*args: Any, **kwargs: Any) -> Any:
            with project_path(name):
                return function(*args, **kwargs)

        return run

    return wrap


Path_ = tuple[Any, ...]


def sequence_of(start: int = 0) -> dict[str, Any]:
    """The YAML form of a count of calls of the node it replaces.

    Args:
        start: The value of the first call.

    Returns:
        dict[str, Any]: A `sequence` generator.
    """
    return {"gen": "sequence", "start": start}


def cycle(table: list[Any]) -> dict[str, Any]:
    """The YAML form of a fixed list: its next value, in order, one for each call of
    the node it replaces.

    Args:
        table: The values, which repeat after the last.

    Returns:
        dict[str, Any]: An `expr` generator that reads a count of its calls.
    """
    return {
        "gen": "expr",
        "expr": f"{table!r}[fix_n % {len(table)}]",
        "names": {"fix_n": sequence_of()},
    }


def fix_draws(
    node: Any,
    *,
    fraction: Callable[[Path_], Any],
    fake: Callable[[dict[str, Any], Path_], Any] | None = None,
    path: Path_ = (),
) -> Any:
    """The tree with each draw node replaced by a value fixed from the node's own
    parameters, and every other structure of the tree kept. A model's own logic, such as
    which choices it has, their weights, its bounds and its chances, therefore still
    decides the result, and a change to any of them changes the records.

    - `uniform`: `low + (high - low) * f`, `f` the fixed fraction;
    - `integer`: `low + int(f * (high + 1 - low))`;
    - `boolean`: `f < p`, `p` the node's chance;
    - `weighted`: the option whose share of the total weight holds `f` (the node's own
      weights);
    - `one_of`: the choice at the next place of a count, `n % number of choices`, and
      only that choice is evaluated, as the generator does;
    - `fake`: what `fake` gives for the node.

    Args:
        node: A loaded document or a part of it.
        fraction: Gives the number in [0, 1) a node at `path` takes, as a value.
        fake: Gives the value of a `fake` node (the node and its path).
        path: The keys and places that lead to `node`.

    Returns:
        Any: The tree with its draws fixed.
    """
    if isinstance(node, list):
        return [
            fix_draws(v, fraction=fraction, fake=fake, path=(*path, i))
            for i, v in enumerate(node)
        ]
    if not isinstance(node, dict):
        return node
    node = {
        k: fix_draws(v, fraction=fraction, fake=fake, path=(*path, k))
        for k, v in node.items()
    }
    kind = node.get("gen")
    if kind == "fake" and fake is not None:
        return fake(node, path)
    if kind == "one_of":
        number = len(node["choices"])
        return {
            "gen": "switch",
            "on": {
                "gen": "expr",
                "expr": f"fix_n % {number}",
                "names": {"fix_n": sequence_of()},
            },
            "cases": dict(enumerate(node["choices"])),
        }
    if kind == "uniform":
        fixed = {
            "gen": "expr",
            "expr": "fix_lo + (fix_hi - fix_lo) * fix_f",
            "names": {
                "fix_lo": node["low"],
                "fix_hi": node["high"],
                "fix_f": fraction(path),
            },
        }
        if "decimals" in node:
            fixed["decimals"] = node["decimals"]
        return fixed
    if kind == "integer":
        low, high = node["bounds"]
        return {
            "gen": "expr",
            "expr": "fix_lo + int(fix_f * (fix_hi + 1 - fix_lo))",
            "names": {"fix_lo": low, "fix_hi": high, "fix_f": fraction(path)},
        }
    if kind == "boolean":
        return {
            "gen": "expr",
            "expr": "fix_f < fix_p",
            "names": {"fix_f": fraction(path), "fix_p": node.get("p", 0.5)},
        }
    if kind == "weighted":
        if "options" in node:
            values, weights = list(node["options"]), list(node["options"].values())
        else:
            values = [c["value"] for c in node["choices"]]
            weights = [c["weight"] for c in node["choices"]]
        total, before, cases = float(sum(weights)), 0.0, []
        for place, (value, weight) in enumerate(zip(values, weights)):
            case: dict[str, Any] = {"value": value}
            if before > 0:
                case["min"] = before
            before += weight / total
            if place < len(values) - 1:
                case["max"] = before
            cases.append(case)
        return {"gen": "switch", "on": fraction(path), "cases": cases}
    return node


def deep(node: Any) -> Any:
    """The value with each generator written as a mapping made a typed generator, which
    a builder argument other than a `Template` needs.

    Args:
        node: A value of a document, with generators as mappings that have `gen`.

    Returns:
        Any: The same value with each generator typed.
    """
    if isinstance(node, dict):
        return (
            generator(node) if "gen" in node else {k: deep(v) for k, v in node.items()}
        )
    if isinstance(node, list):
        return [deep(v) for v in node]
    return node


class Context:
    """What `Template.render` needs of a run: its value runtime."""

    def __init__(self, values: Any) -> None:
        self.values = values


class Run:
    """What the value runtime asks of a running model: its id and no signals, for a
    low-level form whose generators read the registry and draw from a service."""

    def __init__(self, sim_id: str) -> None:
        self.sim_id = sim_id
        self.values: Any = None

    def _stop_run(self) -> None:
        pass


class Entity:
    """What a low-level form hands `Template.render` for an entity: its key and the
    attributes `attr` reads."""

    def __init__(self, key: str, **attributes: Any) -> None:
        self.key = key
        self.attributes = dict(attributes)


@contextlib.contextmanager
def stand_in_modules(*names: str) -> Iterator[None]:
    """Answers the import of modules that the test environment does not have, for code
    that imports them and a test that never calls them (a project imports `sklearn` for a
    function the test does not run). A module that is installed is left as it is.

    Args:
        *names: Dotted module names, such as `sklearn.preprocessing`.

    Yields:
        None: Inside the block the modules import.
    """
    made: list[str] = []
    for name in names:
        try:
            importlib.import_module(name)
        except ImportError:
            module = types.ModuleType(name)

            def attribute(name: str) -> Any:
                if name.startswith("_"):
                    raise AttributeError(name)
                return type(name, (), {})

            module.__getattr__ = attribute  # type: ignore[attr-defined,method-assign]
            sys.modules[name] = module
            made.append(name)
    try:
        yield
    finally:
        for name in made:
            sys.modules.pop(name, None)
