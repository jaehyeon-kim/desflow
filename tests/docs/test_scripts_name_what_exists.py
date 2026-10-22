"""Every script under examples/ and docs/snippets/ names only what desflow has.

The tests run these scripts, but not every line of them: an example's `run()` wires
real connectors and is never called, and docs/snippets/hot_rolling/ imports another
project's modules, so it is never imported at all. A rename in desflow would leave
those lines stale without any test failing. So each file is parsed, every name it
imports from desflow must exist, and every method it calls on the builder (`app`,
`context`), the environment (`env`, `context.env`) or the registry must exist on that
class.
"""

import ast
import importlib
import importlib.util
from pathlib import Path

from desflow import (
    DynamicRealtimeEnvironment,
    SimulationContext,
    SimulationRegistry,
)

ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = ROOT / "scripts" / "public_api.py"
_spec = importlib.util.spec_from_file_location("public_api", _SCRIPT)
assert _spec is not None and _spec.loader is not None
public_api = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(public_api)
OWNERS = {
    "app": SimulationContext,
    "context": SimulationContext,
    "env": DynamicRealtimeEnvironment,
    "registry": SimulationRegistry,
}


def _imported_elsewhere(tree):
    """Names the file imports from another package, such as nicegui's `app`."""
    return {
        alias.asname or alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        or (
            isinstance(node, ast.ImportFrom)
            and not (node.module or "").startswith("desflow")
        )
        for alias in node.names
    }


def _missing_names(path):
    """Every desflow name or method the file uses that does not exist."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    owners = {
        name: cls
        for name, cls in OWNERS.items()
        if name not in _imported_elsewhere(tree)
    }
    missing = []
    for node in ast.walk(tree):
        where = f"{path.relative_to(ROOT)}:{getattr(node, 'lineno', 0)}"
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("desflow")
        ):
            module = importlib.import_module(node.module)
            missing += [
                f"{where}: {node.module} has no {alias.name}"
                for alias in node.names
                if not hasattr(module, alias.name)
            ]
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            owner = node.func.value
            # app.x(...), env.x(...), context.env.x(...), env.registry.x(...)
            if isinstance(owner, ast.Name) and owner.id in owners:
                cls = owners[owner.id]
            elif isinstance(owner, ast.Attribute) and owner.attr in ("env", "registry"):
                cls = OWNERS[owner.attr]
            else:
                continue
            if not hasattr(cls, node.func.attr):
                missing.append(f"{where}: {cls.__name__} has no {node.func.attr}")
    return missing


def test_scripts_name_only_what_exists():
    scripts = list(public_api.scripts())
    # A path that matched nothing would make the check pass without reading a file.
    assert any("hot_rolling" in p.parts for p in scripts)
    assert len(scripts) > 30
    missing = [name for path in scripts for name in _missing_names(path)]
    assert missing == [], "; ".join(missing)
