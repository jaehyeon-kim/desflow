"""Conditions and amounts written as expressions, checked against a whitelist.

An expression is parsed with Python's `ast` and every node is checked before it is
ever evaluated; nothing goes through `eval`. It allows numbers, arithmetic (`+`, `-`,
`*`, `/`, `//`, `%`), comparisons (`<`, `<=`, `>`, `>=`, `==`, `!=`), `and`, `or`,
`not`, the conditional form `a if c else b`, `True` and `False`, and string constants
compared with `==` or `!=`. Each name carries its source as a prefix:

| Name | Value |
|---|---|
| `payload.total` | the field `total` of the upstream payload |
| `attr.priority` | the entity's attribute `priority` |
| `level.stock` | the level of the store or container `stock` |
| `item.size` | the field `size` of the item a store `get` took, or in a get's `filter` the item it is offered |
| `wait.restock.done`, `.ok`, `.value`, `.value.qty` | whether the task's `wait` branch `restock` happened before the wait ended, whether it succeeded, and its value or a field of it |
| `signal.restocked.count`, `.ok`, `.value` | how many times the signal `restocked` has fired, whether its last firing succeeded, and its value |
| `process.name`, `.instance`, `.target` | the running task's name, its instance id, and what it waits on now (`timeout`, `signal.<n>`, `task.<n>`, `condition`, `resource.<n>`, `get.<n>`, `put.<n>`, `service`), None once ended |
| `registry["Shop.arrival.orders.rate"]` | a registry value, by path |
| `t` | the simulation time in seconds |
"""

import ast
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple

_PREFIXES = ("payload", "attr", "level", "item")
# A two-part source with a fixed set of attributes, unlike the open `_PREFIXES`.
_PROCESS = ("name", "instance", "target")
_ARITHMETIC: Dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
    ast.Mult: lambda a, b: a * b,
    ast.Div: lambda a, b: a / b,
    ast.FloorDiv: lambda a, b: a // b,
    ast.Mod: lambda a, b: a % b,
}
_COMPARISONS: Dict[type, Callable[[Any, Any], bool]] = {
    ast.Lt: lambda a, b: a < b,
    ast.LtE: lambda a, b: a <= b,
    ast.Gt: lambda a, b: a > b,
    ast.GtE: lambda a, b: a >= b,
    ast.Eq: lambda a, b: a == b,
    ast.NotEq: lambda a, b: a != b,
}
_EQUALITY = (ast.Eq, ast.NotEq)
# Names of more than one part, by their first word, with the parts each takes
_PARTS = {"wait": ("done", "ok", "value"), "signal": ("count", "ok", "value")}


def _nested(node: ast.Attribute) -> Optional[Tuple[str, List[str]]]:
    """`wait.<branch>.<part>` or `signal.<name>.<part>` as its word and parts."""
    parts: List[str] = []
    current: ast.AST = node
    while isinstance(current, ast.Attribute):
        parts.insert(0, current.attr)
        current = current.value
    if isinstance(current, ast.Name) and current.id in _PARTS:
        return current.id, parts
    return None


def _read_nested(scope: "Scope", source: str, parts: List[str]) -> Any:
    """The value of a `wait.` or `signal.` name in a scope."""
    name, part = parts[0], parts[1]
    if source == "wait":
        if name not in scope.waits:
            raise KeyError(f"the task has no wait branch '{name}'")
        value = scope.waits[name][part]
    else:
        if name not in scope.signals:
            raise KeyError(f"no signal is called '{name}'")
        value = getattr(scope.signals[name], part)
    if len(parts) == 3:
        if not isinstance(value, Mapping) or parts[2] not in value:
            raise KeyError(f"the value of {source}.{name} has no field '{parts[2]}'")
        value = value[parts[2]]
    return value


@dataclass
class Scope:
    """
    What the names in an expression read.

    Attributes:
        payload: The upstream payload, read as `payload.<field>`.
        attributes: The entity's attributes, read as `attr.<name>`.
        levels: Stores and containers by name, read as `level.<name>`.
        registry: The simulation registry, read as `registry["<path>"]`.
        now: The simulation time, read as `t`.
        item: The item a store get took, or a filter's candidate, read as
            `item.<field>`.
        waits: The task's `wait` branches by name, each a mapping of `done`, `ok`
            and `value`, read as `wait.<branch>.<part>`.
        signals: Signals by name, read as `signal.<name>.<part>`.
        process: The running task's `name`, `instance` and `target`, read as
            `process.<part>`.
    """

    payload: Mapping[str, Any] = field(default_factory=dict)
    attributes: Mapping[str, Any] = field(default_factory=dict)
    levels: Mapping[str, Any] = field(default_factory=dict)
    registry: Optional[Any] = None
    now: float = 0.0
    item: Optional[Mapping[str, Any]] = None
    waits: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    signals: Mapping[str, Any] = field(default_factory=dict)
    process: Mapping[str, Any] = field(default_factory=dict)


class Expression:
    """
    A parsed and checked expression, ready to evaluate against a `Scope`.

    Attributes:
        text (str): The expression as written.
        references (Dict[str, Set[str]]): The names it reads, by source: `payload`,
            `attr`, `level`, `item`, `wait`, `signal` and `registry`, so a loader
            can check them in advance.
    """

    def __init__(self, text: str):
        """
        Parses and checks an expression.

        Args:
            text: The expression, such as `level.stock == 0 or payload.total > 100`.

        Raises:
            ValueError: If it is not valid Python or uses anything outside the
                whitelist. The message names what was refused.
        """
        if not isinstance(text, str):
            raise ValueError(f"an expression must be text, got {text!r}")
        self.text = text
        try:
            tree = ast.parse(text.strip(), mode="eval")
        except SyntaxError as exc:
            raise ValueError(f"'{text}' is not a valid expression: {exc.msg}") from None
        self.references: Dict[str, Set[str]] = {
            "payload": set(),
            "attr": set(),
            "level": set(),
            "item": set(),
            "wait": set(),
            "signal": set(),
            "process": set(),
            "registry": set(),
        }
        self._check(tree.body, strings=False)
        self._tree = tree.body

    def evaluate(self, scope: Scope) -> Any:
        """
        Evaluates the expression.

        Args:
            scope: What its names read.

        Returns:
            Any: The value, such as a bool for a condition or a number for an amount.

        Raises:
            ValueError: If a name it reads is missing, or the arithmetic fails, such
                as a division by zero. The message names the expression.
        """
        try:
            return self._value(self._tree, scope)
        except (KeyError, ArithmeticError, TypeError) as exc:
            reason = exc.args[0] if isinstance(exc, KeyError) else exc
            raise ValueError(f"cannot evaluate '{self.text}': {reason}") from None

    def _refuse(self, what: str) -> ValueError:
        return ValueError(f"'{self.text}' uses {what}, which an expression cannot")

    def _check(self, node: ast.AST, strings: bool) -> None:
        """Walks the tree once, refusing every node the whitelist does not name."""
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or (isinstance(node.value, (int, float))):
                return
            if isinstance(node.value, str):
                if strings:
                    return
                raise self._refuse("a string outside an == or != comparison")
            raise self._refuse(f"the constant {node.value!r}")
        if isinstance(node, ast.Name):
            if node.id == "t":
                return
            raise self._refuse(
                f"the name '{node.id}' (write payload.<field>, attr.<name>, "
                f"level.<name>, item.<field>, wait.<branch>.<part>, "
                f'signal.<name>.<part>, process.<name>, registry["<path>"] or t)'
            )
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "process":
                if node.attr not in _PROCESS:
                    raise self._refuse(
                        f"'process.{node.attr}' (write process."
                        f"{', process.'.join(_PROCESS)})"
                    )
                self.references["process"].add(node.attr)
                return
            nested = _nested(node)
            if nested is not None:
                source, parts = nested
                parts_allowed = _PARTS[source]
                if (
                    len(parts) < 2
                    or parts[1] not in parts_allowed
                    or len(parts) > (3 if parts[1] == "value" else 2)
                ):
                    raise self._refuse(
                        f"'{source}.{'.'.join(parts)}' (write {source}.<name>."
                        f"{', .'.join(parts_allowed)}, or .value.<field>)"
                    )
                self.references[source].add(parts[0])
                return
            if isinstance(node.value, ast.Name) and node.value.id in _PREFIXES:
                if node.attr.startswith("__"):
                    raise self._refuse(f"the name '{node.attr}'")
                self.references[node.value.id].add(node.attr)
                return
            raise self._refuse(
                "an attribute that is not payload., attr., level., item. or process."
            )
        if isinstance(node, ast.Subscript):
            index = node.slice
            if (
                isinstance(node.value, ast.Name)
                and node.value.id == "registry"
                and isinstance(index, ast.Constant)
                and isinstance(index.value, str)
            ):
                self.references["registry"].add(index.value)
                return
            raise self._refuse('an index other than registry["<path>"]')
        if isinstance(node, ast.BinOp):
            if type(node.op) not in _ARITHMETIC:
                raise self._refuse(f"the operator {type(node.op).__name__}")
            self._check(node.left, False)
            self._check(node.right, False)
            return
        if isinstance(node, ast.UnaryOp):
            if not isinstance(node.op, (ast.USub, ast.UAdd, ast.Not)):
                raise self._refuse(f"the operator {type(node.op).__name__}")
            self._check(node.operand, False)
            return
        if isinstance(node, ast.BoolOp):
            for value in node.values:
                self._check(value, False)
            return
        if isinstance(node, ast.Compare):
            for op in node.ops:
                if type(op) not in _COMPARISONS:
                    raise self._refuse(f"the comparison {type(op).__name__}")
            equality = all(isinstance(op, _EQUALITY) for op in node.ops)
            for operand in (node.left, *node.comparators):
                self._check(operand, equality)
            return
        if isinstance(node, ast.IfExp):
            self._check(node.test, False)
            self._check(node.body, strings)
            self._check(node.orelse, strings)
            return
        raise self._refuse(f"a {type(node).__name__}")

    def _value(self, node: ast.AST, scope: Scope) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return scope.now
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "process":
                if node.attr not in scope.process:
                    raise KeyError(f"the process has no '{node.attr}'")
                return scope.process[node.attr]
            nested = _nested(node)
            if nested is not None:
                return _read_nested(scope, *nested)
            source = node.value.id  # type: ignore[attr-defined]
            if source == "payload":
                if node.attr not in scope.payload:
                    raise KeyError(f"the payload has no field '{node.attr}'")
                return scope.payload[node.attr]
            if source == "attr":
                if node.attr not in scope.attributes:
                    raise KeyError(f"the entity has no attribute '{node.attr}'")
                return scope.attributes[node.attr]
            if source == "item":
                if not isinstance(scope.item, Mapping) or node.attr not in scope.item:
                    raise KeyError(f"the item has no field '{node.attr}'")
                return scope.item[node.attr]
            if node.attr not in scope.levels:
                raise KeyError(f"no store or container is called '{node.attr}'")
            return scope.levels[node.attr].level
        if isinstance(node, ast.Subscript):
            if scope.registry is None:
                raise KeyError("there is no registry to read")
            return scope.registry.get(node.slice.value).value  # type: ignore[attr-defined]
        if isinstance(node, ast.BinOp):
            return _ARITHMETIC[type(node.op)](
                self._value(node.left, scope), self._value(node.right, scope)
            )
        if isinstance(node, ast.UnaryOp):
            operand = self._value(node.operand, scope)
            if isinstance(node.op, ast.Not):
                return not operand
            return -operand if isinstance(node.op, ast.USub) else +operand
        if isinstance(node, ast.BoolOp):
            if isinstance(node.op, ast.And):
                result: Any = True
                for value in node.values:
                    result = self._value(value, scope)
                    if not result:
                        return result
                return result
            result = False
            for value in node.values:
                result = self._value(value, scope)
                if result:
                    return result
            return result
        if isinstance(node, ast.Compare):
            left = self._value(node.left, scope)
            for op, comparator in zip(node.ops, node.comparators):
                right = self._value(comparator, scope)
                if not _COMPARISONS[type(op)](left, right):
                    return False
                left = right
            return True
        assert isinstance(node, ast.IfExp)
        if self._value(node.test, scope):
            return self._value(node.body, scope)
        return self._value(node.orelse, scope)
