"""Finds the asserts of the agent's tests that read a wall clock, for the reviewer's K8.

A clock reading is a call of `time.time`, `time.monotonic`, `time.perf_counter` or their
`_ns` forms, or of a function of the test file that returns one. An assert counts when it
reads a reading or a name assigned one, at module level or in a function that holds the
assert (`clocks.json`).
"""

import ast
from typing import Any, Dict, Iterator, List, Set, Tuple, Union


TIME_CALLS = {
    "time",
    "monotonic",
    "perf_counter",
    "monotonic_ns",
    "perf_counter_ns",
    "time_ns",
}


# Whether a function returns a clock value: True, False, or one flag per tuple element.
Shape = Union[bool, Tuple[bool, ...]]


class Clock:
    """Finds the names and functions of a test file that hold a wall-clock reading."""

    def __init__(self, tree: ast.Module):
        self.tree = tree
        self.calls = set(TIME_CALLS)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "time":
                for alias in node.names:
                    if alias.name in TIME_CALLS:
                        self.calls.add(alias.asname or alias.name)
        self.functions: Dict[str, Shape] = {}
        defs = []
        for n in tree.body:
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                defs.append(n)
        changed = True
        while changed:
            changed = False
            for fn in defs:
                shape = self._returns(fn)
                if shape and self.functions.get(fn.name) != shape:
                    self.functions[fn.name] = shape
                    changed = True

    def _is_call(self, node: ast.AST) -> bool:
        """True for a call of a clock, or of a function that returns a clock value alone."""
        if not isinstance(node, ast.Call):
            return False
        if isinstance(node.func, ast.Attribute):
            return node.func.attr in TIME_CALLS
        name = getattr(node.func, "id", "")
        return name in self.calls or self.functions.get(name) is True

    def expr(self, node: ast.AST, names: Set[str]) -> bool:
        """True when an expression reads a clock or a name that holds a clock value."""
        return any(
            self._is_call(n) or (isinstance(n, ast.Name) and n.id in names)
            for n in ast.walk(node)
        )

    def _targets(self, target: ast.AST, value: ast.AST, names: Set[str]) -> Set[str]:
        """Returns the names an assignment gives a clock value, element by element."""
        if isinstance(target, ast.Name):
            return {target.id} if self.expr(value, names) else set()
        if not isinstance(target, ast.Tuple):
            return set()
        shape = (
            self.functions.get(value.func.id, False)
            if isinstance(value, ast.Call) and isinstance(value.func, ast.Name)
            else False
        )
        if isinstance(value, ast.Tuple) and len(value.elts) == len(target.elts):
            flags = [self.expr(v, names) for v in value.elts]
        elif isinstance(shape, tuple):
            flags = list(shape)
        else:
            flags = [self.expr(value, names)] * len(target.elts)
        found = set()
        for element, flag in zip(target.elts, flags):
            if flag and isinstance(element, ast.Name):
                found.add(element.id)
        return found

    def names(self, scope: ast.AST) -> Set[str]:
        """Returns every name in `scope` assigned a clock value, followed to a fixed point."""
        found: Set[str] = set()
        changed = True
        while changed:
            changed = False
            for node in ast.walk(scope):
                if isinstance(node, ast.Assign):
                    new = set().union(
                        *(self._targets(t, node.value, found) for t in node.targets)
                    )
                    if new - found:
                        found |= new
                        changed = True
        return found

    def _returns(self, fn: ast.AST) -> Shape:
        """Returns the shape of what a function returns, or False."""
        local = self.names(fn)
        for node in ast.walk(fn):
            if isinstance(node, ast.Return) and node.value is not None:
                if isinstance(node.value, ast.Tuple):
                    flags = tuple(self.expr(v, local) for v in node.value.elts)
                    if any(flags):
                        return flags
                elif self.expr(node.value, local):
                    return True
        return False


DEFS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def _asserts_in_scope(
    clock: Clock, tree: ast.Module
) -> Iterator[Tuple[ast.Assert, Set[str]]]:
    """Yields each assert with the clock names it can read: those the module assigns
    outside its functions and classes, and those of each function that holds the assert.

    A name assigned a clock reading in one test therefore does not make an assert on the
    same name in another test a clock assert."""
    top = []
    for statement in tree.body:
        if not isinstance(statement, DEFS):
            top.append(statement)
    module_names = clock.names(ast.Module(body=top, type_ignores=[]))

    def visit(node: ast.AST, names: Set[str]) -> Iterator[Tuple[ast.Assert, Set[str]]]:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                yield from visit(child, names | clock.names(child))
            else:
                if isinstance(child, ast.Assert):
                    yield child, names
                yield from visit(child, names)

    yield from visit(tree, module_names)


def clock_asserts(spec: dict, files: dict) -> dict:
    """Returns each assert of the tests that reads a wall clock, and the task's paced time.

    The reviewer judges K8 from these facts: each assert's line and text, and `paced_wall`,
    the wall time the request's pacing gives, which is null for an unpaced task.
    """
    found: List[Dict[str, Any]] = []
    if "test_model.py" in files:
        test_file = files["test_model.py"]
        lines = test_file.text.splitlines()
        clock = Clock(test_file.tree)
        for node, names in _asserts_in_scope(clock, test_file.tree):
            if clock.expr(node.test, names):
                end = getattr(node, "end_lineno", node.lineno) or node.lineno
                text = "\n".join(lines[node.lineno - 1 : end]).strip()
                found.append(
                    {"file": "test_model.py", "line": node.lineno, "text": text}
                )
    found.sort(key=lambda entry: entry["line"])
    paced = spec.get("paced_wall") if spec.get("wall_min") else None
    return {"paced_wall": paced, "asserts": found}
