"""Conditions and amounts written as expressions, checked against a whitelist.

An expression is parsed with Python's `ast` and every node is checked before it is
ever evaluated; nothing goes through `eval`. It allows numbers, arithmetic (`+`, `-`,
`*`, `/`, `//`, `%`, `**`), the calls `abs`, `min`, `max`, `round`, `floor`, `ceil`,
`sqrt`, `exp`, `log`, `log10`, `sin`, `cos` and `tan`, comparisons (`<`, `<=`, `>`, `>=`, `==`, `!=`), `and`, `or`,
`not`, the conditional form `a if c else b`, `True` and `False`, and string constants
compared with `==`, `!=`, `in` or `not in`, passed to `get_key`, `set_key` or `drop_key`, used as an index or a list or mapping entry, or added with `+`. Lists and mappings read with Python's results: an index `attr.l[0]`, `attr.l[-1]`, `attr.d['k']`, a slice `attr.l[1:]` and `attr.l[::-1]`, and a dotted read of a field of a mapping at any depth; `len`, `sum`, `sorted`, `range` (a list), `any`, `all`, `str`, `int`, `float` and `bool`; list and mapping literals; `+` on two lists or two texts; one-clause comprehensions such as `[x.v for x in items(q) if x.v > 0]`; `None`, with `is` and `is not` on `None` only; `in` and `not in` over any list, mapping or text; and `get_key(d, k, default)`, `set_key(d, k, v)` and `drop_key(d, k)`, where the last two return a new mapping. A call takes positional arguments only. A write stores a whole value, so no form changes a list or mapping in place. An index out of range, a missing key and text plus a number are run failures. The arithmetic is Python's: `/` gives a
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
| `process.key`, `process.flow_name` | the entity's key, and the first task of the flow this process runs (one name for the process's life; a child flow has its own) |
| `arg.gap` | an argument the parent passed to this flow with `args`, or a name `set_args` wrote in this process; a name not written or passed is a run failure |
| `cause.by`, `.usage_since`, `.resource`, `.value`, `.left` | why the task was interrupted, read in the task an `on_interrupt_next` edge reaches; a name the cause does not have is a run failure |
| `upstream.total` | a field of the payload of the task that routed here; `payload.` in `next` is the task's own |
| `item`, `item.req.id`, `item_priority` | the whole item a store get took (it lasts through the `next` chain), a field of it at any depth, and the priority of the item a priority store gave |
| `held(m)`, `waiting(m)` | the resource `m`'s holders in grant order, and its waiting requests in queue order; each entry is a mapping of `entity`, `rank`, `time`, `preempt` and `key`, and for a holder `since` |
| `putting(s)`, `getting(s)` | the puts and the gets waiting on a store or container; each entry has `entity` and, for a put, `item` or `amount` |
| `running(work)`, `running(work, attr.id == 3)` | how many instances of the task `work` are running, optionally those for which the condition holds, read in each instance's scope |
| `peek()` | the time of the next scheduled event, infinity when none is left |
| `done_count` | in the `when` of a `wait`, how many branches of the group are done |
| `signal.restocked.processed` | whether the signal's firing has been processed |
| `registry["Shop.arrival.orders.rate"]` | a registry value, by path |
| `t` | the simulation time in seconds |
| `has(attr.risk)` | whether the entity has the attribute `risk` |
| `total`, `price` | a var of the task, or a name of an `expr` generator, read bare; only in a place evaluated after the payload is built (`put`, `fail`, `next.when`) and in `expr` |
| `record.stream_type`, `record.key`, `record.task`, `record.value.status` | in an egress `when` only: a field of the record the sink is offered, which is None when the record has none (a telemetry record has no key and no task); `record.value.<field>` is None when the value is not a mapping or lacks the field |
"""

import ast
import math
from numbers import Integral as _Integral
from dataclasses import dataclass, field, replace
from simpy import PriorityItem
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
    "arg",
    "cause",
    "upstream",
)
# The two functions of a store, and what each takes after the store's name
_CALLS = ("count", "items")
# A two-part source with a fixed set of attributes, unlike the open `_PREFIXES`.
_PROCESS = ("name", "instance", "target", "key", "flow_name")
# The sources that read a number, so no field or index follows them
_NUMBERS = ("level", "users", "queue", "putters", "getters")
# Names with no prefix that read the state of the task
_FLAGS = ("item", "item_priority", "done_count")
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


class _Work:
    """The items one evaluation has made, so nested loops, ranges and repetitions
    cannot add up to more than `_LARGEST_RANGE` between them."""

    depth = 0
    made = 0


_WORK = _Work()


def _spend(items: int) -> None:
    """Counts `items` made by the evaluation under way, and refuses more than
    1,000,000 in all."""
    _WORK.made += items
    if _WORK.made > _LARGEST_RANGE:
        raise ValueError(
            f"an expression makes more than {_LARGEST_RANGE:,} items in one reading"
        )


def _multiply(a: Any, b: Any) -> Any:
    """`a * b`, refusing a list, tuple or text repeated into more than 1,000,000
    items."""
    for sequence, times in ((a, b), (b, a)):
        if isinstance(sequence, (list, tuple, str)) and isinstance(times, _Integral):
            made = len(sequence) * max(int(times), 0)
            if made > _LARGEST_RANGE:
                raise ValueError(
                    f"a list or text repeated into more than {_LARGEST_RANGE:,} items"
                )
            _spend(made)
    return a * b


def _modulo(a: Any, b: Any) -> Any:
    """`a % b` for numbers; `%` on text formats it, whose width sets the size of the
    result, so it is refused."""
    if isinstance(a, str):
        raise ValueError("% on text formats it, which an expression cannot do")
    return a % b


def _sum(values: Any, start: Any = 0) -> Any:
    """`sum(values, start)`; a start that is a list, tuple or text would join them,
    which takes time that grows with the square of their size, so it is refused."""
    if isinstance(start, (list, tuple, str)):
        raise ValueError("sum starts from a number, not a list or text")
    return sum(values, start)


_ARITHMETIC: Dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
    ast.Mult: lambda a, b: _multiply(a, b),
    ast.Div: lambda a, b: a / b,
    ast.FloorDiv: lambda a, b: a // b,
    ast.Mod: _modulo,
    ast.Pow: _power,
}
# The most items `range` may give, because it gives a list
_LARGEST_RANGE = 1_000_000


def _range(*arguments: Any) -> List[Any]:
    """`range(...)` as a list, the only sequence the language has."""
    made = range(*arguments)
    if len(made) > _LARGEST_RANGE:
        raise OverflowError(f"range gives {len(made)} items, above {_LARGEST_RANGE}")
    _spend(len(made))
    return list(made)


def _set_key(mapping: Any, key: Any, value: Any) -> Dict[Any, Any]:
    """A copy of `mapping` with `key` set."""
    if not isinstance(mapping, Mapping):
        raise TypeError(f"set_key needs a mapping, got {mapping!r}")
    return {**mapping, key: value}


def _drop_key(mapping: Any, key: Any) -> Dict[Any, Any]:
    """A copy of `mapping` without `key`, which need not be there."""
    if not isinstance(mapping, Mapping):
        raise TypeError(f"drop_key needs a mapping, got {mapping!r}")
    return {k: v for k, v in mapping.items() if k != key}


def _get_key(mapping: Any, key: Any, default: Any) -> Any:
    """`mapping[key]`, or `default` when the key is missing."""
    if not isinstance(mapping, Mapping):
        raise TypeError(f"get_key needs a mapping, got {mapping!r}")
    return mapping.get(key, default)


# The calls on values, each with the fewest and most arguments it takes
_FUNCTIONS: Dict[str, Tuple[Callable[..., Any], int, Optional[int]]] = {
    "abs": (abs, 1, 1),
    "min": (min, 1, None),
    "max": (max, 1, None),
    "len": (len, 1, 1),
    "sum": (_sum, 1, 2),
    "sorted": (sorted, 1, 1),
    "range": (_range, 1, 3),
    "any": (any, 1, 1),
    "all": (all, 1, 1),
    "str": (str, 1, 1),
    "int": (int, 1, 1),
    "float": (float, 1, 1),
    "bool": (bool, 1, 1),
    "get_key": (_get_key, 3, 3),
    "set_key": (_set_key, 3, 3),
    "drop_key": (_drop_key, 2, 2),
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
# to `waiting` are the functions and names of Batch 6, and the words that the Facts
# table keeps for the handles and resource names of later batches (`batch`, `each`,
# `handle`, `holder`, `member`, `members`, `request`)
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
    ast.Is: lambda a, b: a is b,
    ast.IsNot: lambda a, b: a is not b,
}
# The comparisons that take a string on either side
_EQUALITY = (ast.Eq, ast.NotEq, ast.In, ast.NotIn, ast.Is, ast.IsNot)
_MEMBERSHIP = (ast.In, ast.NotIn)
# The names of a record in an egress `when`: the record's own fields, then `value.<field>`
_RECORD = ("stream_type", "key", "task")
# Names of more than one part, by their first word, with the parts each takes
_PARTS = {
    "wait": ("done", "ok", "value"),
    "signal": ("count", "ok", "value", "processed"),
}
# The calls that read the state of the run by a bare name, with the fewest and most
# arguments each takes
_STATE_CALLS = {
    "held": (1, 1),
    "waiting": (1, 1),
    "putting": (1, 1),
    "getting": (1, 1),
    "peek": (0, 0),
    "running": (1, 2),
}


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
        signal = scope.signals[name]
        value = getattr(signal, part)
    for field_name in parts[2:]:
        if not isinstance(value, Mapping) or field_name not in value:
            raise KeyError(f"the value of {source}.{name} has no field '{field_name}'")
        value = value[field_name]
    return value


def _chain(node: ast.AST) -> Tuple[Optional[ast.AST], List[str]]:
    """The base of a chain of dotted reads, and the names read on it, in order."""
    parts: List[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.insert(0, current.attr)
        current = current.value
    return current, parts


def _field(value: Any, name: str, what: str = "the value") -> Any:
    """The field `name` of a mapping, for a dotted read."""
    if not isinstance(value, Mapping) or name not in value:
        raise KeyError(f"{what} has no field '{name}'")
    return value[name]


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
        args: The process's local names, read as `arg.<name>`: what the parent passed
            and what `set_args` wrote; None where no task runs.
        cause: The cause of the interrupt the task was reached by, read as
            `cause.<name>`; None where none reached it.
        upstream: The payload of the task that routed here, read as `upstream.<field>`.
        item_priority: The priority of the item a priority store gave, or None.
        done_count: How many branches of a `when` group are done, or None.
        loops: The names of the comprehensions being evaluated.
        peek: Gives the time of the next scheduled event.
        instances: Gives the scopes of the running instances of a task.
        has_item: Whether a get has given the task an item (it may be None).
        stream: The stream `previous` follows for the entity, or None where no task
            runs.
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
    args: Optional[Mapping[str, Any]] = None
    cause: Optional[Mapping[str, Any]] = None
    upstream: Optional[Mapping[str, Any]] = None
    item_priority: Any = None
    done_count: Optional[int] = None
    loops: Mapping[str, Any] = field(default_factory=dict)
    peek: Optional[Callable[[], Any]] = None
    instances: Optional[Callable[[str], List["Scope"]]] = None
    has_item: bool = False
    stream: Any = None


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
            "arg": set(),
            "cause": set(),
            "upstream": set(),
            "held": set(),
            "waiting": set(),
            "putting": set(),
            "getting": set(),
            "running": set(),
            "flag": set(),
            "loop": set(),
        }
        # The names of the comprehensions being checked
        self._local: Set[str] = set()
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
            for key in (
                "putters",
                "getters",
                "queue",
                "users",
                "count",
                "items",
                "held",
                "waiting",
                "putting",
                "getting",
                "running",
            )
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
            for kind in (
                "attr",
                "payload",
                "item",
                "wait",
                "process",
                "arg",
                "cause",
                "upstream",
            )
            if self.references[kind] or (kind == "attr" and self.references["has"])
        ]
        found.extend(sorted(self.references["flag"]))
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
        if _WORK.depth == 0:
            _WORK.made = 0
        _WORK.depth += 1
        try:
            return self._value(self._tree, scope)
        except MemoryError:
            raise ValueError(
                f"cannot evaluate '{self.text}': it needs more memory than there is"
            ) from None
        except (KeyError, IndexError, ArithmeticError, TypeError, ValueError) as exc:
            reason = exc.args[0] if isinstance(exc, KeyError) else exc
            raise ValueError(f"cannot evaluate '{self.text}': {reason}") from None
        finally:
            _WORK.depth -= 1

    def _refuse(self, what: str) -> ValueError:
        return ValueError(f"'{self.text}' uses {what}, which an expression cannot")

    def _check(self, node: ast.AST, strings: bool) -> None:
        """Walks the tree once, refusing every node the whitelist does not name."""
        if isinstance(node, ast.Constant):
            if (
                node.value is None
                or isinstance(node.value, bool)
                or isinstance(node.value, (int, float))
            ):
                return
            if isinstance(node.value, str):
                if strings:
                    return
                raise self._refuse(
                    "a string outside a comparison, an index, a list or mapping "
                    "entry, a get_key, set_key or drop_key call or a +"
                )
            raise self._refuse(f"the constant {node.value!r}")
        if isinstance(node, ast.Name):
            if node.id in self._local:
                return
            if node.id == "t" and not self._records:
                return
            if node.id in _FLAGS and not self._records:
                self.references["flag"].add(node.id)
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
                f"getters.<name>, item.<field>, item, item_priority, arg.<name>, "
                f"cause.<name>, upstream.<field>, count(<store>), items(<store>), "
                f"held(<resource>), waiting(<resource>), putting(<store>), "
                f"getting(<store>), running(<task>), peek(), wait.<branch>.<part>, "
                f'signal.<name>.<part>, process.<name>, registry["<path>"] or t)'
            )
        if isinstance(node, ast.Attribute):
            self._check_attribute(node)
            return
        if (
            isinstance(
                node, (ast.Subscript, ast.Call, ast.List, ast.Dict, ast.ListComp)
            )
            and self._records
        ):
            raise self._refuse(
                "an index, a call or a list, which an egress when cannot"
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
            if isinstance(node.value, ast.Name) and node.value.id == "registry":
                raise self._refuse('an index other than registry["<path>"]')
            held_number = node.value
            base, parts = _chain(held_number)
            if (
                isinstance(base, ast.Name)
                and parts
                and base.id in _NUMBERS
                and len(parts) == 1
            ):
                raise self._refuse(
                    f"an index on {base.id}.{parts[0]}, which is a number"
                )
            self._check(node.value, False)
            if isinstance(index, ast.Slice):
                for part in (index.lower, index.upper, index.step):
                    if part is not None:
                        self._check(part, False)
            else:
                self._check(index, True)
            return
        if isinstance(node, ast.BinOp):
            if type(node.op) not in _ARITHMETIC:
                raise self._refuse(f"the operator {type(node.op).__name__}")
            both = isinstance(node.op, ast.Add)
            self._check(node.left, both)
            self._check(node.right, both)
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
            for op, comparator in zip(node.ops, node.comparators):
                if isinstance(op, (ast.Is, ast.IsNot)) and not (
                    isinstance(comparator, ast.Constant) and comparator.value is None
                ):
                    raise self._refuse(
                        "is or is not with anything but None on the right (write "
                        "x is None, or use == for a value)"
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
        if isinstance(node, ast.List):
            for element in node.elts:
                self._check(element, True)
            return
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if key is None:
                    raise self._refuse("a ** inside a mapping")
                self._check(key, True)
                self._check(value, True)
            return
        if isinstance(node, ast.ListComp):
            self._check_comprehension(node)
            return
        raise self._refuse(f"a {type(node).__name__}")

    def _check_comprehension(self, node: ast.ListComp) -> None:
        """Checks a one-clause comprehension: its loop name is not a name of the
        language or a var, and is visible in the condition and the item only."""
        generators = node.generators
        if len(generators) != 1 or generators[0].is_async:
            raise self._refuse("a comprehension with more than one clause")
        clause = generators[0]
        if not isinstance(clause.target, ast.Name):
            raise self._refuse("a comprehension over anything but one name")
        name = clause.target.id
        if (
            name in RESERVED_NAMES
            or name in self._local
            or (
                self._names is not None
                and self._names is not ANY_NAME
                and name in self._names
            )
        ):
            raise self._refuse(
                f"the loop name '{name}', which is a name of the language or a var"
            )
        self.references["loop"].add(name)
        self._check(clause.iter, False)
        self._local.add(name)
        try:
            for condition in clause.ifs:
                self._check(condition, False)
            self._check(node.elt, True)
        finally:
            self._local.discard(name)

    def _check_attribute(self, node: ast.Attribute) -> None:
        """Checks a dotted read: a name with its parts, or a field of a mapping that a
        call, an index or a loop name gives."""
        base, parts = _chain(node)
        if not isinstance(base, ast.Name):
            if any(part.startswith("__") for part in parts):
                raise self._refuse("a name that starts with two underscores")
            if self._records:
                raise self._refuse(
                    "a name that is not record.stream_type, record.key, "
                    "record.task or record.value.<field>"
                )
            self._check(base, False)  # type: ignore[arg-type]
            return
        record = _record_parts(node)
        if record is None and self._records:
            raise self._refuse(
                "a name that is not record.stream_type, record.key, "
                "record.task or record.value.<field>"
            )
        if record is not None:
            if not self._records:
                raise self._refuse("the name 'record', which only an egress when reads")
            known = (len(record) == 1 and record[0] in _RECORD) or (
                len(record) == 2
                and record[0] == "value"
                and not record[1].startswith("__")
            )
            if not known:
                raise self._refuse(
                    f"'record.{'.'.join(record)}' (write record."
                    f"{', record.'.join(_RECORD)} or record.value.<field>)"
                )
            self.record_names.add(".".join(record))
            return
        if base.id in self._local:
            if any(part.startswith("__") for part in parts):
                raise self._refuse("a name that starts with two underscores")
            return
        if base.id == "process":
            if len(parts) != 1 or parts[0] not in _PROCESS:
                raise self._refuse(
                    f"'process.{'.'.join(parts)}' (write process."
                    f"{', process.'.join(_PROCESS)})"
                )
            self.references["process"].add(parts[0])
            return
        nested = _nested(node)
        if nested is not None:
            source, nested_parts = nested
            parts_allowed = _PARTS[source]
            if (
                len(nested_parts) < 2
                or nested_parts[1] not in parts_allowed
                or len(nested_parts) > (99 if nested_parts[1] == "value" else 2)
                or any(part.startswith("__") for part in nested_parts)
            ):
                raise self._refuse(
                    f"'{source}.{'.'.join(nested_parts)}' (write {source}.<name>."
                    f"{', .'.join(parts_allowed)}, or .value.<field>)"
                )
            self.references[source].add(nested_parts[0])
            return
        if base.id in _PREFIXES:
            if any(part.startswith("__") for part in parts):
                raise self._refuse(f"the name '{parts[0]}'")
            if base.id in _NUMBERS and len(parts) > 1:
                raise self._refuse(
                    f"'{base.id}.{'.'.join(parts)}' (a {base.id} read is a number)"
                )
            source = base.id
            if source == "item" and self._in_call:
                source = "store_item"
            self.references[source].add(parts[0])
            return
        raise self._refuse(
            "an attribute that is not payload., attr., level., users., queue., "
            "putters., getters., item., arg., cause., upstream. or process."
        )

    def _check_call(self, node: ast.Call) -> None:
        """Checks a call: `count(<store>)` or `items(<store>)`, `has(attr.<name>)`, a
        call that reads the state of the run by a bare name, or one of the functions
        on values."""
        func = node.func
        if isinstance(func, ast.Name) and func.id == "has":
            self._check_has(node)
            return
        if isinstance(func, ast.Name) and func.id in _STATE_CALLS:
            self._check_state_call(node, func.id)
            return
        if isinstance(func, ast.Name) and func.id in _FUNCTIONS:
            self._check_function(node, func.id)
            return
        self._check_store_call(node)

    def _check_state_call(self, node: ast.Call, name: str) -> None:
        """Checks `held(r)`, `waiting(r)`, `putting(x)`, `getting(x)`, `peek()` and
        `running(task[, condition])`: the first argument is a bare name."""
        fewest, most = _STATE_CALLS[name]
        count = len(node.args)
        if node.keywords or not fewest <= count <= most:
            wanted = (
                "no argument"
                if most == 0
                else "a bare name"
                if fewest == most
                else "a bare task name and an optional condition"
            )
            raise self._refuse(f"{name}() with other arguments (it takes {wanted})")
        if name == "peek":
            return
        first = node.args[0]
        if not isinstance(first, ast.Name) or first.id in self._local:
            raise self._refuse(f"{name}() of anything but a bare name")
        self.references[name].add(first.id)
        if count == 2:
            # The condition is read in each instance's scope, so what it reads of the
            # instance does not make the place one that needs an entity
            outer, self.references = (
                self.references,
                {key: set() for key in self.references},
            )
            try:
                self._check(node.args[1], False)
            finally:
                inner, self.references = self.references, outer
            for key in (
                "level",
                "users",
                "queue",
                "putters",
                "getters",
                "count",
                "items",
                "signal",
                "registry",
                "held",
                "waiting",
                "putting",
                "getting",
                "running",
            ):
                self.references[key] |= inner[key]

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
        keyed = name in ("get_key", "set_key", "drop_key")
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
            self._check(argument, keyed)

    def _check_store_call(self, node: ast.Call) -> None:
        """Checks a `count(...)` or `items(...)` call: a bare store name, then an
        optional condition (`count`) or `item.<field>` (`items`)."""
        func = node.func
        if not (isinstance(func, ast.Name) and func.id in _CALLS):
            raise self._refuse(
                "a Call other than count(<store>), items(<store>), has(attr.<name>), "
                f"{', '.join(_STATE_CALLS)} or {', '.join(_FUNCTIONS)}"
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
                chain_base = (
                    _chain(second)[0] if isinstance(second, ast.Attribute) else None
                )
                if not (isinstance(chain_base, ast.Name) and chain_base.id == "item"):
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
        if name in _STATE_CALLS:
            return self._state_call(name, node, scope)
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
            self._value(node.args[1], replace(scope, item=entry, has_item=True))
            for entry in held
        ]
        return sum(1 for value in values if value) if name == "count" else values

    def _state_call(self, name: str, node: ast.Call, scope: Scope) -> Any:
        """The value of `held`, `waiting`, `putting`, `getting`, `peek` or `running`."""
        if name == "peek":
            if scope.peek is None:
                raise KeyError("peek() needs a running simulation")
            return scope.peek()
        target = node.args[0].id  # type: ignore[attr-defined]
        if name == "running":
            if scope.instances is None:
                raise KeyError("running() needs a running simulation")
            scopes = scope.instances(target)
            if len(node.args) == 1:
                return len(scopes)
            return sum(
                1
                for each in scopes
                if self._value(node.args[1], replace(each, loops=scope.loops))
            )
        if name in ("held", "waiting"):
            if target not in scope.resources:
                raise KeyError(f"no resource is called '{target}'")
            resource = scope.resources[target]
            if name == "held":
                return [_request_entry(r, True) for r in resource.users]
            return [_request_entry(r, False) for r in resource.queue]
        holder = scope.levels.get(target)
        if holder is None or not hasattr(holder, "putters_waiting"):
            raise KeyError(f"no store or container is called '{target}'")
        events = holder.put_queue if name == "putting" else holder.get_queue
        return [_transfer_entry(e, name == "putting", holder) for e in events]

    def _index(self, node: ast.AST, scope: Scope) -> Any:
        """The index of a subscript: a value, or a slice."""
        if isinstance(node, ast.Slice):
            return slice(
                *(
                    None if part is None else self._value(part, scope)
                    for part in (node.lower, node.upper, node.step)
                )
            )
        return self._value(node, scope)

    def _value(self, node: ast.AST, scope: Scope) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Call):
            return self._call(node, scope)
        if isinstance(node, ast.Name):
            if node.id in scope.loops:
                return scope.loops[node.id]
            if node.id == "t":
                return scope.now
            if node.id == "item":
                if scope.item is None and not scope.has_item:
                    raise KeyError("no store get has given the task an item")
                return scope.item
            if node.id == "item_priority":
                if scope.item_priority is None:
                    raise KeyError("no get from a priority store has given an item")
                return scope.item_priority
            if node.id == "done_count":
                if scope.done_count is None:
                    raise KeyError("done_count is read in the when of a wait")
                return scope.done_count
            if node.id not in scope.names:
                raise KeyError(f"'{node.id}' is not a var that has a value yet")
            return scope.names[node.id]
        if isinstance(node, ast.Attribute):
            return self._attribute(node, scope)
        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Name) and node.value.id == "registry":
                if scope.registry is None:
                    raise KeyError("there is no registry to read")
                return scope.registry.get(node.slice.value).value  # type: ignore[attr-defined]
            base = self._value(node.value, scope)
            index = self._index(node.slice, scope)
            if isinstance(base, Mapping) and not isinstance(index, slice):
                if index not in base:
                    raise KeyError(f"no key {index!r}")
            return base[index]
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
        if isinstance(node, ast.List):
            return [self._value(element, scope) for element in node.elts]
        if isinstance(node, ast.Dict):
            return {
                self._value(key, scope): self._value(value, scope)  # type: ignore[arg-type]
                for key, value in zip(node.keys, node.values)
            }
        if isinstance(node, ast.ListComp):
            clause = node.generators[0]
            name = clause.target.id  # type: ignore[attr-defined]
            made = []
            for entry in self._value(clause.iter, scope):
                _spend(1)
                inner = replace(scope, loops={**scope.loops, name: entry})
                if all(self._value(test, inner) for test in clause.ifs):
                    made.append(self._value(node.elt, inner))
            return made
        assert isinstance(node, ast.IfExp)
        if self._value(node.test, scope):
            return self._value(node.body, scope)
        return self._value(node.orelse, scope)

    def _attribute(self, node: ast.Attribute, scope: Scope) -> Any:
        """The value of a dotted read."""
        base, parts = _chain(node)
        if not isinstance(base, ast.Name):
            return _field(self._value(node.value, scope), node.attr)
        record = _record_parts(node)
        if record is not None:
            row = scope.record or {}
            if len(record) == 1:
                return row.get(record[0])
            value = row.get("value")
            return value.get(record[1]) if isinstance(value, Mapping) else None
        source, first = base.id, parts[0]
        if source in scope.loops:
            value = scope.loops[source]
            for part in parts:
                value = _field(value, part, f"'{source}'")
            return value
        if source == "process":
            if first not in scope.process:
                raise KeyError(f"the process has no '{first}'")
            return scope.process[first]
        nested = _nested(node)
        if nested is not None:
            return _read_nested(scope, *nested)
        if source in ("putters", "getters"):
            if first not in scope.levels:
                raise KeyError(f"no store or container is called '{first}'")
            holder = scope.levels[first]
            return (
                holder.putters_waiting
                if source == "putters"
                else holder.getters_waiting
            )
        if source in ("users", "queue"):
            if first not in scope.resources:
                raise KeyError(f"no resource is called '{first}'")
            resource = scope.resources[first]
            return len(resource.users if source == "users" else resource.queue)
        if source == "level":
            if first not in scope.levels:
                raise KeyError(f"no store or container is called '{first}'")
            return scope.levels[first].level
        if source == "item":
            if not isinstance(scope.item, Mapping):
                raise KeyError(f"the item has no field '{first}'")
            value = scope.item
            for part in parts:
                value = _field(value, part, "the item")
            return value
        sources = {
            "payload": (scope.payload, "the payload has no field '{}'"),
            "attr": (scope.attributes, "the entity has no attribute '{}'"),
            "upstream": (scope.upstream, "the upstream payload has no field '{}'"),
        }
        if source == "arg":
            if scope.args is None or first not in scope.args:
                raise KeyError(
                    f"the process has no argument '{first}': an argument is passed "
                    f"with args or written with set_args"
                )
            container: Any = scope.args
        elif source == "cause":
            if scope.cause is None:
                raise KeyError(
                    "no interrupt reached this task: cause is read in a task an "
                    "on_interrupt_next edge reaches"
                )
            if first not in scope.cause:
                raise KeyError(f"the cause has no '{first}'")
            container = scope.cause
        else:
            container, message = sources[source]
            if container is None or first not in container:
                raise KeyError(message.format(first))
        value = container[first]
        for part in parts[1:]:
            value = _field(value, part, f"'{source}.{first}'")
        return value


def _request_entry(request: Any, holding: bool) -> Dict[str, Any]:
    """The entry `held` and `waiting` give for a request."""
    entry: Dict[str, Any] = {
        "entity": getattr(request, "entity_key", None),
        "rank": getattr(request, "priority", 0),
        "time": getattr(request, "time", None),
        "preempt": getattr(request, "preempt", None),
        "key": getattr(request, "key", None),
    }
    if holding:
        entry["since"] = getattr(request, "usage_since", None)
    return entry


def _transfer_entry(event: Any, putting: bool, holder: Any) -> Dict[str, Any]:
    """The entry `putting` and `getting` give for a waiting put or get."""
    entry: Dict[str, Any] = {"entity": getattr(event, "entity_key", None)}
    if hasattr(holder, "items"):
        if putting:
            item = getattr(event, "item", None)
            entry["item"] = item.item if isinstance(item, PriorityItem) else item
    else:
        entry["amount"] = getattr(event, "amount", None)
    return entry
