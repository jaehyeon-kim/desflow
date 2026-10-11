"""Every Python block in the docs is valid, and names only what exists.

Labelled blocks are copies of tested files. The unlabelled ones, short fragments in
the guides, are not run, so this checks what goes stale first when the code changes:
each block must parse, every name it imports from dynamic_des must exist, and every
method it calls on the builder (`app`), the environment (`env`, `context.env`) or the
registry must exist on that class.
"""

import ast
import importlib
import re
from pathlib import Path

import pytest

from dynamic_des import (
    DynamicRealtimeEnvironment,
    SimulationContext,
    SimulationRegistry,
)

ROOT = Path(__file__).resolve().parents[2]
BLOCK = re.compile(r"^( *)```python([^\n]*)\n(.*?)\n\1```", re.S | re.M)
OWNERS = {
    "app": SimulationContext,
    "context": SimulationContext,
    "env": DynamicRealtimeEnvironment,
    "registry": SimulationRegistry,
}


def _blocks():
    for page in sorted((ROOT / "docs").rglob("*.md")):
        text = page.read_text(encoding="utf-8")
        for m in BLOCK.finditer(text):
            indent = m.group(1)
            code = "\n".join(
                row[len(indent) :] if row.startswith(indent) else row
                for row in m.group(3).split("\n")
            )
            line = text[: m.start()].count("\n") + 1
            yield f"{page.relative_to(ROOT)}:{line}", code


BLOCKS = list(_blocks())


def test_blocks_are_found():
    assert len(BLOCKS) > 50


@pytest.mark.parametrize("where,code", BLOCKS, ids=[w for w, _ in BLOCKS])
def test_block_is_valid_and_names_what_exists(where, code):
    tree = ast.parse(code)  # a SyntaxError fails the test with its line
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("dynamic_des")
        ):
            module = importlib.import_module(node.module)
            for alias in node.names:
                assert hasattr(module, alias.name), (
                    f"{where}: {node.module} has no {alias.name}"
                )
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            owner = node.func.value
            # app.x(...), env.x(...), context.env.x(...), env.registry.x(...)
            if isinstance(owner, ast.Name) and owner.id in OWNERS:
                cls = OWNERS[owner.id]
            elif isinstance(owner, ast.Attribute) and owner.attr in ("env", "registry"):
                cls = OWNERS[owner.attr]
            else:
                continue
            assert hasattr(cls, node.func.attr), (
                f"{where}: {cls.__name__} has no {node.func.attr}"
            )
