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
| `registry["Shop.arrival.orders.rate"]` | a registry value, by path |
| `t` | the simulation time in seconds |
"""

import ast
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Set

_PREFIXES = ("payload", "attr", "level")
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
    """

    payload: Mapping[str, Any] = field(default_factory=dict)
    attributes: Mapping[str, Any] = field(default_factory=dict)
    levels: Mapping[str, Any] = field(default_factory=dict)
    registry: Optional[Any] = None
    now: float = 0.0


class Expression:
    """
    A parsed and checked expression, ready to evaluate against a `Scope`.

    Attributes:
        text (str): The expression as written.
        references (Dict[str, Set[str]]): The names it reads, by source: `payload`,
            `attr`, `level` and `registry`, so a loader can check them in advance.
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
                f'level.<name>, registry["<path>"] or t)'
            )
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id in _PREFIXES:
                if node.attr.startswith("__"):
                    raise self._refuse(f"the name '{node.attr}'")
                self.references[node.value.id].add(node.attr)
                return
            raise self._refuse("an attribute that is not payload., attr. or level.")
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
            source = node.value.id  # type: ignore[attr-defined]
            if source == "payload":
                if node.attr not in scope.payload:
                    raise KeyError(f"the payload has no field '{node.attr}'")
                return scope.payload[node.attr]
            if source == "attr":
                if node.attr not in scope.attributes:
                    raise KeyError(f"the entity has no attribute '{node.attr}'")
                return scope.attributes[node.attr]
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
