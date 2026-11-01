"""Conditions and amounts written as expressions, checked against a whitelist.

An expression is parsed with Python's `ast` and every node is checked before it is
ever evaluated; nothing goes through `eval`. It allows numbers, arithmetic (`+`, `-`,
`*`, `/`, `//`, `%`, `**`), the calls `abs`, `min`, `max`, `round`, `floor`, `ceil`,
`sqrt`, `exp`, `log`, `log10`, `sin`, `cos` and `tan`, comparisons (`<`, `<=`, `>`, `>=`, `==`, `!=`), `and`, `or`,
`not`, the conditional form `a if c else b`, `True` and `False`, and string constants
compared with `==`, `!=`, `in` or `not in`. The arithmetic is Python's: `/` gives a
float, and `//` rounds down and gives an integer for two integers. `and` and `or`
stop at the first operand that decides the result, and do not absorb an error: an
operand that cannot be evaluated fails the expression. Each name carries its source as a prefix:

| Name | Value |
|---|---|
| `payload.total` | the field `total` of the upstream payload |
| `attr.priority` | the entity's attribute `priority` |
| `level.stock` | the level of the store or container `stock` |
| `users.packers` | how many units of the resource `packers` are held |
| `queue.packers` | how many requests wait for the resource `packers` |
| `putters.stock`, `getters.stock` | how many puts, and how many gets, wait on the store or container `stock`; one granted at once is not counted |
| `count(parcels)`, `count(parcels, item.area == "north")` | how many items the store `parcels` holds, and how many of them meet the condition on `item.<field>` |
| `items(shelf)`, `items(shelf, item.sku)` | the items the store `shelf` holds as a list, and the field `sku` of each; read with `in` and `not in`, as in `payload.sku in items(shelf, item.sku)` |
| `item.size` | the field `size` of the item a store `get` took, or in a get's `filter` the item it is offered |
| `wait.restock.done`, `.ok`, `.value`, `.value.qty` | whether the task's `wait` branch `restock` happened before the wait ended, whether it succeeded, and its value or a field of it |
| `signal.restocked.count`, `.ok`, `.value` | how many times the signal `restocked` has fired, whether its last firing succeeded, and its value |
| `process.name`, `.instance`, `.target` | the running task's name, its instance id, and what it waits on now (`timeout`, `signal.<n>`, `task.<n>`, `condition`, `resource.<n>`, `get.<n>`, `put.<n>`, `service`), None once ended |
| `registry["Shop.arrival.orders.rate"]` | a registry value, by path |
| `t` | the simulation time in seconds |
| `has(attr.risk)` | whether the entity has the attribute `risk` |
| `total`, `price` | a var of the task, or a name of an `expr` generator, read bare; only in a place evaluated after the payload is built (`put`, `fail`, `next.when`) and in `expr` |
| `record.stream_type`, `record.key`, `record.task`, `record.value.status` | in an egress `when` only: a field of the record the sink is offered, which is None when the record has none (a telemetry record has no key and no task); `record.value.<field>` is None when the value is not a mapping or lacks the field |
"""

import ast
import math
from dataclasses import dataclass, field, replace
from typing import (
    Any,
    Callable,
    Container,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)

_PREFIXES = (
    "payload",
    "attr",
    "level",
    "item",
    "users",
    "queue",
    "putters",
    "getters",
)
# The two functions of a store, and what each takes after the store's name
_CALLS = ("count", "items")
# A two-part source with a fixed set of attributes, unlike the open `_PREFIXES`.
_PROCESS = ("name", "instance", "target")
# An exponent above this with a base other than -1, 0 or 1 is refused, because
# `9 ** 9 ** 9` would hold the run for hours
_LARGEST_EXPONENT = 10_000
# The most bits a whole-number power may have, estimated as the bits of the base times
# the exponent, so a large base with a small exponent is refused too
_LARGEST_POWER_BITS = 1_000_000


def _power(base: Any, exponent: Any) -> Any:
    """`base ** exponent`, refusing a whole-number power too large to compute."""
    if (
        isinstance(exponent, int)
        and isinstance(base, int)
        and exponent > _LARGEST_EXPONENT
        and abs(base) > 1
    ):
        raise OverflowError(f"the power {base} ** {exponent} is too large")
    if (
        isinstance(exponent, int)
        and isinstance(base, int)
        and exponent > 0
        and abs(base).bit_length() * exponent > _LARGEST_POWER_BITS
    ):
        raise OverflowError("the power is too large to compute")
    return base**exponent


_ARITHMETIC: Dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
    ast.Mult: lambda a, b: a * b,
    ast.Div: lambda a, b: a / b,
    ast.FloorDiv: lambda a, b: a // b,
    ast.Mod: lambda a, b: a % b,
    ast.Pow: _power,
}
# The calls on numbers, each with the fewest and most arguments it takes
_FUNCTIONS: Dict[str, Tuple[Callable[..., Any], int, Optional[int]]] = {
    "abs": (abs, 1, 1),
    "min": (min, 2, None),
    "max": (max, 2, None),
    "round": (round, 1, 2),
    "floor": (math.floor, 1, 1),
    "ceil": (math.ceil, 1, 1),
    "sqrt": (math.sqrt, 1, 1),
    "exp": (math.exp, 1, 1),
    "log": (math.log, 1, 2),
    "log10": (math.log10, 1, 1),
    "sin": (math.sin, 1, 1),
    "cos": (math.cos, 1, 1),
    "tan": (math.tan, 1, 1),
}
# Names an expression gives a meaning, so a var cannot take them. The words from `all`
# to `waiting` are names that later batches give a meaning (the Y5 functions, the
# `cause`, `upstream` and `arg` names and the resource names of the Facts table), so a
# var that took one would break when they ship
RESERVED_NAMES = frozenset(
    _PREFIXES
    + _CALLS
    + ("wait", "signal", "process", "record", "registry", "t", "has")
    + tuple(_FUNCTIONS)
    + (
        "all",
        "any",
        "arg",
        "batch",
        "bool",
        "cause",
        "done_count",
        "drop_key",
        "each",
        "float",
        "get_key",
        "getting",
        "handle",
        "held",
        "holder",
        "int",
        "item_priority",
        "len",
        "member",
        "members",
        "peek",
        "putting",
        "range",
        "request",
        "running",
        "set_key",
        "sorted",
        "str",
        "sum",
        "upstream",
        "waiting",
    )  # fmt: skip
)
_COMPARISONS: Dict[type, Callable[[Any, Any], bool]] = {
    ast.Lt: lambda a, b: a < b,
    ast.LtE: lambda a, b: a <= b,
    ast.Gt: lambda a, b: a > b,
    ast.GtE: lambda a, b: a >= b,
    ast.Eq: lambda a, b: a == b,
    ast.NotEq: lambda a, b: a != b,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
}
# The comparisons that take a string on either side
_EQUALITY = (ast.Eq, ast.NotEq, ast.In, ast.NotIn)
_MEMBERSHIP = (ast.In, ast.NotIn)
# The names of a record in an egress `when`: the record's own fields, then `value.<field>`
_RECORD = ("stream_type", "key", "task")
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


def _record_parts(node: ast.Attribute) -> Optional[List[str]]:
    """`record.<name>` or `record.value.<field>` as its parts, or None for any
    other name."""
    parts: List[str] = []
    current: ast.AST = node
    while isinstance(current, ast.Attribute):
        parts.insert(0, current.attr)
        current = current.value
    if isinstance(current, ast.Name) and current.id == "record":
        return parts
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
        levels: Stores and containers by name, read as `level.<name>`, `putters.<name>`
            and `getters.<name>` (the puts and gets waiting), and, for a store,
            `count(<name>)` and `items(<name>)`.
        resources: Resources by name, read as `users.<name>` (units held) and
            `queue.<name>` (requests waiting).
        registry: The simulation registry, read as `registry["<path>"]`.
        now: The simulation time, read as `t`.
        item: The item a store get took, or a filter's candidate, read as
            `item.<field>`.
        waits: The task's `wait` branches by name, each a mapping of `done`, `ok`
            and `value`, read as `wait.<branch>.<part>`.
        signals: Signals by name, read as `signal.<name>.<part>`.
        process: The running task's `name`, `instance` and `target`, read as
            `process.<part>`.
        record: The record an egress `when` is offered, read as `record.stream_type`,
            `record.key`, `record.task` and `record.value.<field>`.
        names: The bare names: a task's vars, and the names of an `expr` generator.
        entity: The key of the entity the task runs for, which `gen: entity` gives;
            None where no task runs.
    """

    payload: Mapping[str, Any] = field(default_factory=dict)
    attributes: Mapping[str, Any] = field(default_factory=dict)
    levels: Mapping[str, Any] = field(default_factory=dict)
    resources: Mapping[str, Any] = field(default_factory=dict)
    registry: Optional[Any] = None
    now: float = 0.0
    item: Optional[Mapping[str, Any]] = None
    waits: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    signals: Mapping[str, Any] = field(default_factory=dict)
    process: Mapping[str, Any] = field(default_factory=dict)
    record: Optional[Mapping[str, Any]] = None
    names: Mapping[str, Any] = field(default_factory=dict)
    entity: Optional[str] = None


class AnyName:
    """The `names` of an `Expression` that accepts every bare name, so a loader can
    check them later against the vars it finds (`Expression.references["name"]`)."""

    def __contains__(self, name: object) -> bool:
        return True


ANY_NAME = AnyName()


class Expression:
    """
    A parsed and checked expression, ready to evaluate against a `Scope`.

    Attributes:
        text (str): The expression as written.
        references (Dict[str, Set[str]]): The names it reads, by source: `payload`,
            `attr`, `level`, `users`, `queue`, `putters`, `getters`, `item`, `wait`,
            `signal`, `process`, `registry` and `name` (the bare names), so a loader
            can check them in advance. `count` and `items` hold the store names the
            two functions read, and `store_item` the `item.<field>` names inside
            them, which read each item the store holds and not the item of a get.
            `has` holds the attribute names `has(attr.<name>)` tests, which need not
            exist.
    """

    def __init__(
        self,
        text: str,
        records: bool = False,
        names: Optional[Container[str]] = None,
    ):
        """
        Parses and checks an expression.

        Args:
            text: The expression, such as `level.stock == 0 or payload.total > 100`.
            records: True for an egress `when`, whose expression reads the record
                (`record.stream_type`, `record.key`, `record.task` and
                `record.value.<field>`) and nothing else.
            names: The bare names it may read, such as a task's vars. None refuses
                every bare name but `t`; `ANY_NAME` accepts any, for a loader that
                checks them against the vars afterwards.

        Raises:
            ValueError: If it is not valid Python or uses anything outside the
                whitelist. The message names what was refused.
        """
        if not isinstance(text, str):
            raise ValueError(f"an expression must be text, got {text!r}")
        self.text = text
        self._records = records
        self._names = names
        try:
            tree = ast.parse(text.strip(), mode="eval")
        except SyntaxError as exc:
            raise ValueError(f"'{text}' is not a valid expression: {exc.msg}") from None
        self.references: Dict[str, Set[str]] = {
            "payload": set(),
            "attr": set(),
            "level": set(),
            "users": set(),
            "queue": set(),
            "putters": set(),
            "getters": set(),
            "count": set(),
            "items": set(),
            "store_item": set(),
            "item": set(),
            "wait": set(),
            "signal": set(),
            "process": set(),
            "registry": set(),
            "name": set(),
            "has": set(),
        }
        # The record names an egress `when` reads, such as `value.event_type`
        self.record_names: Set[str] = set()
        # True inside the arguments of count() and items()
        self._in_call = False
        self._check(tree.body, strings=False)
        self._tree = tree.body

    @property
    def reads_queues(self) -> bool:
        """bool: Whether the expression reads a queue length or what a store holds, so
        a wait on it is checked again after a change of a queue length."""
        return any(
            self.references[key]
            for key in ("putters", "getters", "queue", "users", "count", "items")
        )

    def outside_run(self, names: bool = True) -> Optional[str]:
        """Names the sources the expression reads that a place reading the run only
        cannot: the entity's `attr` and `has`, `payload`, `item`, a `wait` branch,
        `process` and, unless `names` is False, a bare name.

        Args:
            names: Whether a bare name counts, as it does where no var is readable.

        Returns:
            Optional[str]: The sources, such as `attr, payload`; None when it reads
            none of them.
        """
        found = [
            kind
            for kind in ("attr", "payload", "item", "wait", "process")
            if self.references[kind] or (kind == "attr" and self.references["has"])
        ]
        if names and self.references["name"]:
            found.append("the name " + ", ".join(sorted(self.references["name"])))
        return ", ".join(found) if found else None

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
        except (KeyError, ArithmeticError, TypeError, ValueError) as exc:
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
            if node.id == "t" and not self._records:
                return
            if not self._records and self._names is not None and node.id in self._names:
                self.references["name"].add(node.id)
                return
            if self._records and node.id == "t":
                raise self._refuse(
                    "the name 't': an egress when reads only the record, whose "
                    "time is not a name (write record.stream_type, record.key, "
                    "record.task or record.value.<field>)"
                )
            if self._records and node.id == "record":
                raise self._refuse(
                    "the name 'record' alone (write record.stream_type, "
                    "record.key, record.task or record.value.<field>)"
                )
            raise self._refuse(
                f"the name '{node.id}' (write payload.<field>, attr.<name>, "
                f"level.<name>, users.<name>, queue.<name>, putters.<name>, "
                f"getters.<name>, item.<field>, count(<store>), items(<store>), "
                f"wait.<branch>.<part>, "
                f'signal.<name>.<part>, process.<name>, registry["<path>"] or t)'
            )
        if isinstance(node, ast.Attribute):
            parts = _record_parts(node)
            if parts is None and self._records:
                raise self._refuse(
                    "a name that is not record.stream_type, record.key, "
                    "record.task or record.value.<field>"
                )
            if parts is not None:
                if not self._records:
                    raise self._refuse(
                        "the name 'record', which only an egress when reads"
                    )
                known = (len(parts) == 1 and parts[0] in _RECORD) or (
                    len(parts) == 2
                    and parts[0] == "value"
                    and not parts[1].startswith("__")
                )
                if not known:
                    raise self._refuse(
                        f"'record.{'.'.join(parts)}' (write record."
                        f"{', record.'.join(_RECORD)} or record.value.<field>)"
                    )
                self.record_names.add(".".join(parts))
                return
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
                source = node.value.id
                if source == "item" and self._in_call:
                    source = "store_item"
                self.references[source].add(node.attr)
                return
            raise self._refuse(
                "an attribute that is not payload., attr., level., users., queue., "
                "putters., getters., item. or process."
            )
        if isinstance(node, (ast.Subscript, ast.Call)) and self._records:
            raise self._refuse("an index or a call, which an egress when cannot")
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
        if isinstance(node, ast.Call):
            self._check_call(node)
            return
        if isinstance(node, ast.Compare):
            for op in node.ops:
                if type(op) not in _COMPARISONS:
                    raise self._refuse(f"the comparison {type(op).__name__}")
            if any(isinstance(op, _MEMBERSHIP) for op in node.ops) and not (
                len(node.ops) == 1 and self._is_items(node.comparators[0])
            ):
                raise self._refuse(
                    "in or not in with anything but items(<store>) on the right "
                    "(write x in items(<store>) or x in items(<store>, item.<field>))"
                )
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

    @staticmethod
    def _is_items(node: ast.AST) -> bool:
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "items"
        )

    def _check_call(self, node: ast.Call) -> None:
        """Checks a call: `count(<store>)` or `items(<store>)`, `has(attr.<name>)`, or
        one of the functions on numbers."""
        func = node.func
        if isinstance(func, ast.Name) and func.id == "has":
            self._check_has(node)
            return
        if isinstance(func, ast.Name) and func.id in _FUNCTIONS:
            self._check_function(node, func.id)
            return
        self._check_store_call(node)

    def _check_has(self, node: ast.Call) -> None:
        """Checks `has(attr.<name>)`."""
        argument = node.args[0] if len(node.args) == 1 else None
        if node.keywords or not (
            isinstance(argument, ast.Attribute)
            and isinstance(argument.value, ast.Name)
            and argument.value.id == "attr"
            and not argument.attr.startswith("__")
        ):
            raise self._refuse("has() with anything but one attr.<name> (has(attr.x))")
        self.references["has"].add(argument.attr)

    def _check_function(self, node: ast.Call, name: str) -> None:
        """Checks a call of a function on numbers: its arguments are expressions."""
        _function, fewest, most = _FUNCTIONS[name]
        count = len(node.args)
        if node.keywords or count < fewest or (most is not None and count > most):
            wanted = (
                f"{fewest}"
                if fewest == most
                else f"{fewest} or more"
                if most is None
                else f"{fewest} to {most}"
            )
            raise self._refuse(f"{name}() with other arguments (it takes {wanted})")
        for argument in node.args:
            self._check(argument, False)

    def _check_store_call(self, node: ast.Call) -> None:
        """Checks a `count(...)` or `items(...)` call: a bare store name, then an
        optional condition (`count`) or `item.<field>` (`items`)."""
        func = node.func
        if not (isinstance(func, ast.Name) and func.id in _CALLS):
            raise self._refuse(
                "a Call other than count(<store>), items(<store>), has(attr.<name>) "
                f"or {', '.join(_FUNCTIONS)}"
            )
        name = func.id
        if self._in_call:
            raise self._refuse(f"{name}() inside count() or items()")
        if node.keywords or not 1 <= len(node.args) <= 2:
            raise self._refuse(
                f"{name}() with other arguments (write {name}(<store>) or "
                f"{name}(<store>, {'<condition>' if name == 'count' else 'item.<field>'}))"
            )
        store = node.args[0]
        if not isinstance(store, ast.Name):
            raise self._refuse(f"{name}() of anything but a bare store name")
        self.references[name].add(store.id)
        if len(node.args) == 1:
            return
        second = node.args[1]
        self._in_call = True
        try:
            if name == "items":
                if not (
                    isinstance(second, ast.Attribute)
                    and isinstance(second.value, ast.Name)
                    and second.value.id == "item"
                ):
                    raise self._refuse(
                        "items(<store>, ...) with anything but item.<field>"
                    )
            self._check(second, False)
        finally:
            self._in_call = False

    def _call(self, node: ast.Call, scope: Scope) -> Any:
        """The value of a call for a scope."""
        name = node.func.id  # type: ignore[attr-defined]
        if name == "has":
            return node.args[0].attr in scope.attributes  # type: ignore[attr-defined]
        if name in _FUNCTIONS:
            function = _FUNCTIONS[name][0]
            return function(*(self._value(a, scope) for a in node.args))
        store = node.args[0].id  # type: ignore[attr-defined]
        holder = scope.levels.get(store)
        if holder is None or not hasattr(holder, "putters_waiting"):
            raise KeyError(f"no store is called '{store}'")
        if not hasattr(holder, "items"):
            raise KeyError(f"'{store}' is a container, and {name}() reads a store")
        held = [
            entry.item if getattr(holder, "priority", False) else entry
            for entry in holder.items
        ]
        if len(node.args) == 1:
            return len(held) if name == "count" else held
        values = [
            self._value(node.args[1], replace(scope, item=entry)) for entry in held
        ]
        return sum(1 for value in values if value) if name == "count" else values

    def _value(self, node: ast.AST, scope: Scope) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Call):
            return self._call(node, scope)
        if isinstance(node, ast.Name):
            if node.id == "t":
                return scope.now
            if node.id not in scope.names:
                raise KeyError(f"'{node.id}' is not a var that has a value yet")
            return scope.names[node.id]
        if isinstance(node, ast.Attribute):
            parts = _record_parts(node)
            if parts is not None:
                record = scope.record or {}
                if len(parts) == 1:
                    return record.get(parts[0])
                value = record.get("value")
                return value.get(parts[1]) if isinstance(value, Mapping) else None
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
            if source in ("putters", "getters"):
                if node.attr not in scope.levels:
                    raise KeyError(f"no store or container is called '{node.attr}'")
                holder = scope.levels[node.attr]
                return (
                    holder.putters_waiting
                    if source == "putters"
                    else holder.getters_waiting
                )
            if source in ("users", "queue"):
                if node.attr not in scope.resources:
                    raise KeyError(f"no resource is called '{node.attr}'")
                resource = scope.resources[node.attr]
                return len(resource.users if source == "users" else resource.queue)
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
