"""Every public function and class has a docstring, and every exported name is on
the API page.

The API page renders docstrings with mkdocstrings, which hides names that start
with an underscore. So a public function or method in `src/desflow` with
parameters needs a docstring with an `Args:` section, a public class needs a
docstring, and every name a package of `PROMISED_PACKAGES` exports needs a
`::: <path>` line in `docs/reference/api.md`, at the path 1.x keeps for it
(`scripts/public_api.py`).
"""

import ast
import importlib
import importlib.util
import re
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "src" / "desflow"
_spec = importlib.util.spec_from_file_location(
    "public_api", ROOT / "scripts" / "public_api.py"
)
assert _spec is not None and _spec.loader is not None
public_api = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(public_api)


def _public_without_docs() -> List[str]:
    """`file:line name` for each public definition that lacks what it needs."""
    found: List[str] = []
    for path in sorted(SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        public_file = not path.name.startswith("_") or path.name == "__init__.py"
        _visit(tree, "", public_file, path.relative_to(SOURCE), found)
    return found


def _visit(node, prefix: str, public: bool, path: Path, found: List[str]) -> None:
    for child in ast.iter_child_nodes(node):
        if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        name = f"{prefix}{child.name}"
        is_public = public and not child.name.startswith("_")
        if is_public:
            doc = ast.get_docstring(child)
            where = f"{path}:{child.lineno} {name}"
            if not doc:
                found.append(f"{where}: no docstring")
            elif not isinstance(child, ast.ClassDef):
                params = [
                    a.arg
                    for a in child.args.args + child.args.kwonlyargs
                    if a.arg not in ("self", "cls")
                ]
                is_property = any(
                    isinstance(d, ast.Name) and d.id == "property"
                    for d in child.decorator_list
                )
                if params and not is_property and "Args:" not in doc:
                    found.append(f"{where}: no Args section")
        if isinstance(child, ast.ClassDef):
            _visit(child, f"{name}.", is_public, path, found)


def test_every_public_definition_has_a_docstring_with_its_args():
    assert _public_without_docs() == []


def test_every_exported_name_is_on_the_api_page():
    """Every name of every promised package has a `:::` line at its promised path."""
    text = (ROOT / "docs" / "reference/api.md").read_text(encoding="utf-8")
    entries = set(re.findall(r"^::: (\S+)$", text, re.M))
    missing = []
    for package in public_api.PROMISED_PACKAGES:
        module = importlib.import_module(package)
        for name in module.__all__:
            path = public_api.promised_path(getattr(module, name), f"{package}.{name}")
            module_form = f"{path}.{path.rsplit('.', 1)[-1]}"
            if path not in entries and module_form not in entries:
                missing.append(path)
    assert sorted(set(missing)) == []
