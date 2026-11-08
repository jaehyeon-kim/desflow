"""A refusal built from parts has a separator at each join.

`_not_added` begins with `which`, so a text it follows must end in a space or a comma
and a space. The source scan finds every join of it in `core/context.py`; the run
checks the `running(<name>)` refusal that lacked one.
"""

import ast
from pathlib import Path

import pytest

from desflow.core.context import SimulationContext

SOURCE = Path(__file__).resolve().parents[2] / "src" / "desflow" / "core" / "context.py"


def _last_text(node):
    if isinstance(node, ast.JoinedStr):
        parts = [v for v in node.values if isinstance(v, ast.Constant)]
        return parts[-1].value if node.values and node.values[-1] is parts[-1] else ""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return ""


def test_every_join_with_not_added_has_a_separator():
    tree = ast.parse(SOURCE.read_text())
    bad = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.BinOp)
            and isinstance(node.op, ast.Add)
            and isinstance(node.right, ast.Call)
            and getattr(node.right.func, "id", "") == "_not_added"
        ):
            text = _last_text(node.left)
            if not text.endswith((", ", " ")):
                bad.append(node.lineno)
    assert not bad, f"_not_added joins a text with no separator at lines {bad}"


def test_the_running_refusal_is_one_sentence():
    app = SimulationContext("S", factor=0)
    app.task(service_time=0, name="t", wait={"condition": "running(pak) == 0"})(
        lambda task_id: {}
    )
    with pytest.raises(ValueError) as caught:
        app.start(until=5)
    assert str(caught.value).endswith(
        "task 't' reads running(pak), but no task is called 'pak'; add it with "
        "@app.task()"
    ) or "'pak'; " in str(caught.value)
    assert "'pak'which" not in str(caught.value)
