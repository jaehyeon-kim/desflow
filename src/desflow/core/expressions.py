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
| `attr.priority`, `attr["class"]` | the entity's attribute `priority`; the index form reads a name that is not a Python word, such as `class` |
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
| `cause.by`, `.usage_since`, `.resource`, `.value`, `.left` | why the task was interrupted, read in the task an `on_interrupt_next` or `on_interrupt_run` edge reaches; a name the cause does not have is a run failure |
| `upstream.total` | a field of the payload of the task that routed here, or its `status` and `reason` when `on_fail`, `on_renege` or `on_balk` routed here; `payload.` in `next` is the task's own |
| `item`, `item.req.id`, `item_priority` | the whole item a store get took (it lasts through the `next` chain), a field of it at any depth, and the priority of the item a priority store gave |
| `held(m)`, `waiting(m)` | the resource `m`'s holders in grant order, and its waiting requests in queue order; each entry is a mapping of `entity`, `rank`, `time`, `preempt` and `key`, and for a holder `since` |
| `putting(s)`, `getting(s)` | the puts and the gets waiting on a store or container; each entry has `entity` and, for a put, `item` or `amount` |
| `running(work)`, `running(work, attr.id == 3)` | how many instances of the task `work` are running, optionally those for which the condition holds, read in each instance's scope |
| `peek()` | the time of the next scheduled event, infinity when none is left |
| `done_count` | in the `when` of a `wait`, how many branches of the group are done |
| `signal.restocked.processed` | whether the signal's firing has been processed |
| `handle.slot.triggered`, `.processed`, `.ok`, `.value`, `.alive` | the event the task, or an earlier task of its flow, opened under the name `slot`: whether it has happened, whether it has been processed, whether it succeeded, its value (reading it before it happened is a run failure), and, for a child flow, whether the flow still runs |
| `registry["Shop.arrival.orders.rate"]` | a registry value, by path |
| `t` | the simulation time in seconds |
| `has(attr.risk)` | whether the entity has the attribute `risk` |
| `total`, `price` | a var of the task, or a name of an `expr` generator, read bare; only in a place evaluated after the payload is built (`put`, `fail`, `next.when`) and in `expr` |
| `record.stream_type`, `record.key`, `record.task`, `record.value.status` | in an egress `when` only: a field of the record the sink is offered, which is None when the record has none (a telemetry record has no key and no task); `record.value.<field>` is None when the value is not a mapping or lacks the field |
"""

import ast
import math
import unicodedata
from numbers import Integral as _Integral
from dataclasses import dataclass, field, replace
from pydantic import TypeAdapter, ValidationError
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
    Union,
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


# The deepest parse tree an expression may have. Python's own parser and the passes
# here recurse, so a sum of 500 terms ran past the recursion limit
_LARGEST_DEPTH = 200


def _depth_of(tree: ast.AST) -> int:
    """The depth of a parse tree, counted without recursion."""
    deepest = 0
    stack: List[Tuple[ast.AST, int]] = [(tree, 1)]
    while stack:
        node, depth = stack.pop()
        deepest = max(deepest, depth)
        stack.extend((child, depth + 1) for child in ast.iter_child_nodes(node))
    return deepest


def _round(number: Any, digits: Any = None) -> Any:
    """`round(number, digits)`, refusing negative digits whose power of ten is too
    large to compute, as `**` does."""
    if (
        isinstance(digits, int)
        and not isinstance(digits, bool)
        and digits < -_LARGEST_POWER_BITS // 4
    ):
        raise OverflowError(f"round to {digits} digits is too large to compute")
    return round(number) if digits is None else round(number, digits)


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
    result = base**exponent
    if isinstance(result, complex):
        raise ValueError(f"{base!r} ** {exponent!r} gives a complex number")
    return result


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
    "round": (_round, 1, 2),
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


def folded_name(name: str) -> Optional[str]:
    """The name an expression reads instead of `name`, or None when it reads `name`.

    Python's parser reads an identifier in its NFKC form, so a micro sign (`µ`), a
    ligature (`ﬁ`) or a full-width letter in a name makes the expression read a
    different name, which looks the same.

    Args:
        name: A name as the model writes it.

    Returns:
        Optional[str]: The NFKC form of `name`, or None when it is the same.
    """
    folded = unicodedata.normalize("NFKC", name)
    return None if folded == name else folded


# Names an expression gives a meaning, so a var cannot take them. The words from `all`
# to `waiting` are the functions and names of Batch 6, and the words that the Facts
# table keeps for the handles and resource names of later batches (`batch`, `each`,
# `handle`, `holder`, `member`, `members`, `request`). `prod` and `sumprod` are the
# list functions a later 1.x release adds, so no var may take them now
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
        "prod",
        "putting",
        "range",
        "request",
        "running",
        "set_key",
        "sorted",
        "str",
        "sum",
        "sumprod",
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
MEMBER_FIELDS = ("key", "users", "queue", "level", "count", "putters", "getters")
_PARTS = {
    "wait": ("done", "ok", "value", "values", "flags"),
    "signal": ("count", "ok", "value", "processed"),
    "handle": ("triggered", "processed", "ok", "value", "alive", "values", "flags"),
}
# The names of the element an `each` branch is made for, read as `each.<name>`
_EACH_FIELDS = ("index", "value")
# The fields of the request and of the holder a preempt rule reads, `holder.attr.<name>`
# being the holder's entity attributes
_REQUEST_FIELDS = ("rank", "time", "preempt", "key")
_HOLDER_FIELDS = (*_REQUEST_FIELDS, "since")
# The fields of the batch a `close_when` reads
_BATCH_FIELDS = ("size", "age")
# The calls that read the state of the run by a bare name, with the fewest and most
# arguments each takes
_STATE_CALLS = {
    "held": (1, 1),
    "waiting": (1, 1),
    "putting": (1, 1),
    "getting": (1, 1),
    "peek": (0, 0),
    "running": (1, 2),
    "members": (1, 1),
}


_BOOLEAN = TypeAdapter(bool)


def boolean_word(value: Any) -> Any:
    """A `preempt` as written: text that blueprint validation reads as a boolean
    (`t`, `yes`, `off`, `1` and the like) is that boolean, and any other text stays
    text, an expression read when the request is made.

    Args:
        value: The `preempt` as given.

    Returns:
        Any: A bool for a boolean word, else `value` unchanged.
    """
    if isinstance(value, str):
        try:
            return _BOOLEAN.validate_python(value)
        except ValidationError:
            return value
    return value


def normal_key(key: Any) -> Union[int, str]:
    """The key a family member is made for.

    `true` and `false` are 1 and 0, because a Python `dict` holds `True` and `1` as one
    key. A NumPy integer is an integer.

    Args:
        key: The value a reference computed.

    Returns:
        Union[int, str]: The key.

    Raises:
        TypeError: If the key is neither an integer, a boolean nor text.
    """
    if isinstance(key, bool):
        return int(key)
    if isinstance(key, _Integral):
        return int(key)
    if isinstance(key, str):
        return key
    raise TypeError(f"a family key is an integer or text, got {key!r}")


def member_name(family: str, key: Any) -> str:
    """The name of a family's member: the family's name and Python's `repr` of the key
    in brackets, so `3` and `'3'` are two members, and every key has one name.

    Args:
        family: The family's name.
        key: The member's key.

    Returns:
        str: The name, such as `desk[3]` or `desk['a.b']`.
    """
    return f"{family}[{normal_key(key)!r}]"


def rank_problem(expression: "Expression") -> Optional[str]:
    """What a queue `rank` reads that no request site can give, or None.

    A rank is read in the scope of whatever makes the request or the get or put,
    before it waits, so it cannot read a `wait` branch, a handle or the item of a get.

    Args:
        expression: The parsed rank.

    Returns:
        Optional[str]: The reason it is refused, or None.
    """
    found = [
        kind for kind in ("wait", "handle") if expression.references[kind]
    ] + sorted(expression.references["flag"])
    if found:
        return (
            f"'{expression.text}' reads {', '.join(found)}, which a request has not "
            f"got when its rank is read"
        )
    return None


def _nested(node: ast.Attribute) -> Optional[Tuple[str, List[str]]]:
    """`wait.<branch>.<part>`, `signal.<name>.<part>` or `handle.<name>.<part>` as its
    word and parts."""
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
    """The value of a `wait.`, `signal.` or `handle.` name in a scope."""
    name, part = parts[0], parts[1]
    if source == "handle":
        return _read_handle(scope, name, part, parts[2:])
    if source == "wait":
        if name not in scope.waits:
            raise KeyError(f"the task has no wait branch '{name}'")
        if part not in scope.waits[name]:
            raise KeyError(
                f"wait.{name}.{part} is read on a branch made with each, and "
                f"'{name}' is not one"
            )
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


def _read_handle(scope: "Scope", name: str, part: str, fields: List[str]) -> Any:
    """`handle.<name>.<part>`, and the fields of a `.value` that is a mapping.

    Raises:
        KeyError: If no event is open under the name, the part does not suit what it
            was opened as (`alive` is for a task), or `.value` is read before the
            event is triggered, as SimPy's `Event.value` raises.
    """
    handle = scope.handles(name) if scope.handles is not None else None
    if handle is None:
        raise KeyError(f"no event is open under the name '{name}'")
    value = handle.read(name, part)
    for field_name in fields:
        if not isinstance(value, Mapping) or field_name not in value:
            raise KeyError(f"the value of handle.{name} has no field '{field_name}'")
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


class _AttrIndexes(ast.NodeTransformer):
    """Rewrites `attr["<name>"]` as the read `attr.<name>`, so an attribute whose name
    Python's parser refuses as a word, such as `class`, can be read."""

    def visit_Subscript(self, node: ast.Subscript) -> ast.AST:  # noqa: N802
        self.generic_visit(node)
        if (
            isinstance(node.value, ast.Name)
            and node.value.id == "attr"
            and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)
        ):
            return ast.copy_location(
                ast.Attribute(node.value, node.slice.value, ast.Load()), node
            )
        return node


def _attr_indexes_as_reads(tree: ast.Expression) -> ast.Expression:
    """The tree with each `attr["<name>"]` written as `attr.<name>`; a tree that
    binds the name `attr` in a comprehension is left as it is."""
    bound = {
        node.id
        for clause in ast.walk(tree)
        if isinstance(clause, ast.comprehension)
        for node in ast.walk(clause.target)
        if isinstance(node, ast.Name)
    }
    if "attr" in bound:
        return tree
    return ast.fix_missing_locations(_AttrIndexes().visit(tree))


def _key_read_as(container: Mapping[str, Any], name: str) -> Optional[str]:
    """The key of `container` that a dotted read of `name` means, or None.

    The parser gives a name in its NFKC form, so a key written with a micro sign
    (`µs`) is read as `name` when `name` is `μs`.
    """
    if name in container:
        return name
    for key in container:
        if isinstance(key, str) and unicodedata.normalize("NFKC", key) == name:
            return key
    return None


def _field(value: Any, name: str, what: str = "the value") -> Any:
    """The field `name` of a mapping, for a dotted read."""
    key = _key_read_as(value, name) if isinstance(value, Mapping) else None
    if key is None:
        raise KeyError(f"{what} has no field '{name}'")
    return value[key]


@dataclass
class Scope:
    """
    What the names in an expression read.

    Attributes:
        payload: The upstream payload, read as `payload.<field>`; None when it is
            null.
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
        handles: Gives the event opened under a name, read as `handle.<name>.<part>`;
            None where no task runs.
        request: The request a preempt rule tests, read as `request.rank`,
            `request.time`, `request.preempt` and `request.key`; None elsewhere.
        holder: The user whose unit it would take, read as `holder.rank`, `.time`,
            `.preempt`, `.key`, `.since` and `holder.attr.<name>`; None elsewhere.
        batch: The open batch a `close_when` tests; None elsewhere.
        each: The element an `each` branch is made for, read as `each.index` and
            `each.value`; None elsewhere.
        member: The member a `lowest` is read for, read as `member.<name>`; None
            elsewhere.
        families: The declared families by name, which `members()` and a read of a
            member that does not exist yet use.
        family_keys: The keys of the members the run has made, by family name,
            which `members()` reads.
    """

    payload: Optional[Mapping[str, Any]] = field(default_factory=dict)
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
    handles: Optional[Callable[[str], Any]] = None
    request: Optional[Mapping[str, Any]] = None
    holder: Optional[Mapping[str, Any]] = None
    batch: Optional[Mapping[str, Any]] = None
    each: Optional[Mapping[str, Any]] = None
    member: Optional[Mapping[str, Any]] = None
    families: Mapping[str, Any] = field(default_factory=dict)
    family_keys: Mapping[str, List[Any]] = field(default_factory=dict)


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
            `signal`, `handle`, `process`, `registry` and `name` (the bare names), so a loader
            can check them in advance. `count` and `items` hold the store names the
            two functions read, and `store_item` the `item.<field>` names inside
            them, which read each item the store holds and not the item of a get.
            `has` holds the attribute names `has(attr.<name>)` tests, which need not
            exist.
        where (str): The place of the expression in a blueprint, such as
            `model.yaml:12: tasks.t.fail`, which an evaluation error names first;
            empty when it has none.
    """

    def __init__(
        self,
        text: str,
        records: bool = False,
        names: Optional[Container[str]] = None,
        rule: bool = False,
        batch: bool = False,
        member: bool = False,
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
            rule: True for the `when` and `victim` of a preempt rule, which also read
                `request.<field>` and `holder.<field>` and none of the entity's names.
            batch: True for a batch's `close_when`, which also reads `batch.size` (how
                many members the batch holds) and `batch.age` (the seconds since its
                first member joined).
            member: True for the `lowest` of a reference, which also reads
                `member.key`, `member.users`, `member.queue`, `member.level`,
                `member.count`, `member.putters` and `member.getters`.

        Raises:
            ValueError: If it is not valid Python or uses anything outside the
                whitelist. The message names what was refused.
        """
        if not isinstance(text, str):
            raise ValueError(f"an expression must be text, got {text!r}")
        self.text = text
        self.where = ""
        self._records = records
        self._names = names
        self._rule = rule
        self._batch = batch
        self._member = member
        # The reads of a family by index, such as `users.desk[attr.k]`, and the reads
        # with no index, as (source, name), so a loader can match them to the families
        self.indexed: Set[Tuple[str, str]] = set()
        self.plain: Set[Tuple[str, str]] = set()
        nested = f"'{text}' is nested too deeply"
        try:
            tree = ast.parse(text.strip(), mode="eval")
        except SyntaxError as exc:
            raise ValueError(f"'{text}' is not a valid expression: {exc.msg}") from None
        except (RecursionError, MemoryError):
            raise ValueError(nested) from None
        if _depth_of(tree) > _LARGEST_DEPTH:
            raise ValueError(nested)
        tree = _attr_indexes_as_reads(tree)
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
            "handle": set(),
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
            "members": set(),
            "each": set(),
            "member": set(),
            "flag": set(),
            "loop": set(),
        }
        # The `request.` and `holder.` reads of a preempt rule, such as `holder.rank`
        self.rule_reads: Set[str] = set()
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
                "handle",
                "process",
                "arg",
                "cause",
                "upstream",
                "each",
                "member",
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
                as a division by zero. The message names the expression, after its
                place when it has one.
        """
        if _WORK.depth == 0:
            _WORK.made = 0
        _WORK.depth += 1
        place = f"{self.where}: " if self.where else ""
        try:
            return self._value(self._tree, scope)
        except RecursionError:
            raise ValueError(f"{place}'{self.text}' is nested too deeply") from None
        except MemoryError:
            raise ValueError(
                f"{place}cannot evaluate '{self.text}': it needs more memory than "
                "there is"
            ) from None
        except (KeyError, IndexError, ArithmeticError, TypeError, ValueError) as exc:
            reason = exc.args[0] if isinstance(exc, KeyError) else exc
            raise ValueError(
                f"{place}cannot evaluate '{self.text}': {reason}"
            ) from None
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
            self._check_name(node)
            return
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
            self._check_subscript(node)
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

    def _check_name(self, node: ast.Name) -> None:
        """Checks a bare name: a loop name, `t`, a flag or a var, and nothing else."""
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

    def _check_subscript(self, node: ast.Subscript) -> None:
        """Checks an index: `registry["<path>"]`, a member of a family such as
        `users.desk[attr.k]`, or an index or a slice of a value."""
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
            and not isinstance(index, ast.Slice)
        ):
            # `users.desk[attr.k]`: a member of a family
            self.references[base.id].add(parts[0])
            self.indexed.add((base.id, parts[0]))
            self._check(index, True)
            return
        if (
            isinstance(base, ast.Name)
            and parts
            and base.id in _NUMBERS
            and len(parts) == 1
        ):
            raise self._refuse(f"an index on {base.id}.{parts[0]}, which is a number")
        self._check(node.value, False)
        if isinstance(index, ast.Slice):
            for part in (index.lower, index.upper, index.step):
                if part is not None:
                    self._check(part, False)
        else:
            self._check(index, True)

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
        if isinstance(base, ast.Subscript) and self._indexed_signal(base, parts):
            return
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
        if base.id in ("request", "holder"):
            self._check_rule_read(base.id, parts)
            return
        if base.id == "each":
            if len(parts) != 1 or parts[0] not in _EACH_FIELDS:
                raise self._refuse(
                    f"'each.{'.'.join(parts)}' (write each.{', each.'.join(_EACH_FIELDS)})"
                )
            self.references["each"].add(parts[0])
            return
        if base.id == "member":
            if not self._member:
                raise self._refuse(
                    f"the name 'member.{'.'.join(parts)}', which only the lowest of a "
                    f"reference reads"
                )
            if len(parts) != 1 or parts[0] not in MEMBER_FIELDS:
                raise self._refuse(
                    f"'member.{'.'.join(parts)}' (write member."
                    f"{', member.'.join(MEMBER_FIELDS)})"
                )
            self.references["member"].add(parts[0])
            return
        if base.id == "batch":
            self._check_batch_read(parts)
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
            if source == "signal":
                self.plain.add(("signal", nested_parts[0]))
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
            if source in _NUMBERS:
                self.plain.add((source, parts[0]))
            self.references[source].add(parts[0])
            return
        raise self._refuse(
            "an attribute that is not payload., attr., level., users., queue., "
            "putters., getters., item., arg., cause., upstream. or process."
        )

    def _indexed_signal(self, base: ast.Subscript, parts: List[str]) -> bool:
        """Checks `signal.<family>[<key>].<part>`, a read of a member of a family of
        signals. Returns False when the subscript is not on a `signal.<name>`."""
        inner, names = _chain(base.value)
        if not (
            isinstance(inner, ast.Name) and inner.id == "signal" and len(names) == 1
        ):
            return False
        if (
            not parts
            or parts[0] not in _PARTS["signal"]
            or len(parts) > (99 if parts[0] == "value" else 1)
            or any(part.startswith("__") for part in parts)
            or isinstance(base.slice, ast.Slice)
        ):
            raise self._refuse(
                f"'signal.{names[0]}[...].{'.'.join(parts)}' (write signal.<name>[<key>]."
                f"{', .'.join(_PARTS['signal'])}, or .value.<field>)"
            )
        self._check(base.slice, True)
        self.references["signal"].add(names[0])
        self.indexed.add(("signal", names[0]))
        return True

    def _check_rule_read(self, source: str, parts: List[str]) -> None:
        """Checks `request.<field>`, `holder.<field>` or `holder.attr.<name>`, which only
        the `when` and `victim` of a preempt rule read."""
        if not self._rule:
            raise self._refuse(
                f"the name '{source}.{'.'.join(parts)}', which only the when and "
                f"victim of a preempt rule read"
            )
        fields = _REQUEST_FIELDS if source == "request" else _HOLDER_FIELDS
        if source == "holder" and parts[0] == "attr":
            if len(parts) != 2 or parts[1].startswith("__"):
                raise self._refuse(
                    "a read of holder.attr other than holder.attr.<name>"
                )
        elif len(parts) != 1 or parts[0] not in fields:
            raise self._refuse(
                f"'{source}.{'.'.join(parts)}' (write {source}."
                f"{', '.join(fields)}"
                + (" or holder.attr.<name>" if source == "holder" else "")
                + ")"
            )
        self.rule_reads.add(f"{source}.{'.'.join(parts)}")

    def _check_batch_read(self, parts: List[str]) -> None:
        """Checks `batch.size` or `batch.age`, which only a batch's `close_when` reads."""
        if not self._batch:
            raise self._refuse(
                f"the name 'batch.{'.'.join(parts)}', which only the close_when of a "
                f"batch reads"
            )
        if len(parts) != 1 or parts[0] not in _BATCH_FIELDS:
            raise self._refuse(
                f"'batch.{'.'.join(parts)}' (write batch.{', batch.'.join(_BATCH_FIELDS)})"
            )
        self.rule_reads.add(f"batch.{parts[0]}")

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
        if (
            name != "running"
            and name != "members"
            and isinstance(first, ast.Subscript)
            and isinstance(first.value, ast.Name)
            and first.value.id not in self._local
            and not isinstance(first.slice, ast.Slice)
        ):
            self._check(first.slice, True)
            self.references[name].add(first.value.id)
            self.indexed.add((name, first.value.id))
            return
        if not isinstance(first, ast.Name) or first.id in self._local:
            raise self._refuse(f"{name}() of anything but a bare name")
        self.references[name].add(first.id)
        if name != "running":
            self.plain.add((name, first.id))
        if count == 2:
            # The condition is read in each instance's scope, so what it reads of the
            # instance does not make the place one that needs an entity
            outer, self.references = (
                self.references,
                {key: set() for key in self.references},
            )
            before = (self.indexed, self.plain)
            self.indexed, self.plain = set(), set()
            try:
                self._check(node.args[1], False)
            finally:
                inner, self.references = self.references, outer
                inner_indexed, inner_plain = self.indexed, self.plain
                self.indexed, self.plain = before
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
                "members",
            ):
                self.references[key] |= inner[key]
            self.indexed |= inner_indexed
            self.plain |= inner_plain

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
        if (
            isinstance(store, ast.Subscript)
            and isinstance(store.value, ast.Name)
            and store.value.id not in self._local
            and not isinstance(store.slice, ast.Slice)
        ):
            self._check(store.slice, True)
            self.references[name].add(store.value.id)
            self.indexed.add((name, store.value.id))
        elif not isinstance(store, ast.Name):
            raise self._refuse(f"{name}() of anything but a bare store name")
        else:
            self.references[name].add(store.id)
            self.plain.add((name, store.id))
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
            attribute = node.args[0].attr  # type: ignore[attr-defined]
            return _key_read_as(scope.attributes, attribute) is not None
        if name in _STATE_CALLS:
            return self._state_call(name, node, scope)
        if name in _FUNCTIONS:
            function = _FUNCTIONS[name][0]
            return function(*(self._value(a, scope) for a in node.args))
        store = self._target(node.args[0], scope)
        holder = scope.levels.get(store)
        if holder is None and self._blank_of(node.args[0], scope) is not None:
            held: List[Any] = []
            if len(node.args) == 1:
                return 0 if name == "count" else held
            return 0 if name == "count" else held
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
        if name == "members":
            family = scope.families.get(node.args[0].id)  # type: ignore[attr-defined]
            if family is None:
                raise KeyError(f"'{node.args[0].id}' is not a family")  # type: ignore[attr-defined]
            return list(scope.family_keys.get(family.name, []))
        target = self._target(node.args[0], scope)
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
                if self._blank_of(node.args[0], scope) is not None:
                    return []
                raise KeyError(f"no resource is called '{target}'")
            resource = scope.resources[target]
            if name == "held":
                return [_request_entry(r, True) for r in resource.users]
            return [_request_entry(r, False) for r in resource.queue]
        holder = scope.levels.get(target)
        if holder is None and self._blank_of(node.args[0], scope) is not None:
            return []
        if holder is None or not hasattr(holder, "putters_waiting"):
            raise KeyError(f"no store or container is called '{target}'")
        events = holder.put_queue if name == "putting" else holder.get_queue
        return [_transfer_entry(e, name == "putting", holder) for e in events]

    def _read_signal_member(
        self, name: str, key: Any, parts: List[str], scope: Scope
    ) -> Any:
        """`signal.<family>[<key>].<part>`: the member's value, or what a signal that
        has not fired gives when the member is not made yet."""
        full = member_name(name, key)
        signal = scope.signals.get(full)
        if signal is None:
            family = scope.families.get(name)
            if family is None:
                raise KeyError(f"no signal is called '{full}'")
            value = family.blank(parts[0], scope)
        else:
            value = getattr(signal, parts[0])
        for field_name in parts[1:]:
            if not isinstance(value, Mapping) or field_name not in value:
                raise KeyError(
                    f"the value of signal.{full} has no field '{field_name}'"
                )
            value = value[field_name]
        return value

    def _target(self, node: ast.AST, scope: Scope) -> str:
        """The name a bare or indexed argument of a call reads: `q`, or the member
        `q['k']` of a family `q[<key>]` makes."""
        if isinstance(node, ast.Subscript):
            return member_name(
                node.value.id,  # type: ignore[attr-defined]
                self._value(node.slice, scope),  # type: ignore[attr-defined]
            )
        return node.id  # type: ignore[attr-defined]

    def _blank_of(self, node: ast.AST, scope: Scope) -> Optional[Any]:
        """The family a member read names, when the member does not exist yet, so the
        read gives what a new member would; None for a plain name or a name that is not
        a family."""
        if not isinstance(node, ast.Subscript):
            return None
        return scope.families.get(node.value.id)  # type: ignore[attr-defined]

    def _read_member(self, source: str, name: str, key: Any, scope: Scope) -> Any:
        """`users.<name>[<key>]` and the other reads of one member of a family: the
        member's value, or what a new member gives when it is not made yet."""
        full = member_name(name, key)
        if source in ("users", "queue"):
            where, what = scope.resources, "resource"
        else:
            where, what = scope.levels, "store or container"
        holder = where.get(full)
        if holder is None:
            family = scope.families.get(name)
            if family is None:
                raise KeyError(f"no {what} is called '{full}'")
            return family.blank(source, scope)
        if source == "users":
            return len(holder.users)
        if source == "queue":
            return len(holder.queue)
        if source == "level":
            return holder.level
        return holder.putters_waiting if source == "putters" else holder.getters_waiting

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
            held_base, held_parts = _chain(node.value)
            if (
                isinstance(held_base, ast.Name)
                and held_base.id in _NUMBERS
                and len(held_parts) == 1
                and not isinstance(node.slice, ast.Slice)
                and held_base.id not in scope.loops
            ):
                return self._read_member(
                    held_base.id, held_parts[0], self._value(node.slice, scope), scope
                )
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

    def _read_rule(self, source: str, parts: List[str], scope: Scope) -> Any:
        """The value of `request.<field>`, `holder.<field>` or `holder.attr.<name>`."""
        row = scope.request if source == "request" else scope.holder
        if row is None:
            raise KeyError(f"no request is being tested, so {source} has no value")
        if parts[0] == "attr":
            attributes = row.get("attr")
            if not isinstance(attributes, Mapping):
                raise KeyError(f"the holder has no attribute '{parts[1]}'")
            key = _key_read_as(attributes, parts[1])
            if key is None:
                raise KeyError(f"the holder has no attribute '{parts[1]}'")
            return attributes[key]
        return row[parts[0]]

    def _attribute(self, node: ast.Attribute, scope: Scope) -> Any:
        """The value of a dotted read."""
        base, parts = _chain(node)
        if isinstance(base, ast.Subscript):
            inner, names = _chain(base.value)
            if isinstance(inner, ast.Name) and inner.id == "signal" and len(names) == 1:
                return self._read_signal_member(
                    names[0], self._value(base.slice, scope), parts, scope
                )
        if not isinstance(base, ast.Name):
            return _field(self._value(node.value, scope), node.attr)
        if base.id == "each" and base.id not in scope.loops:
            if scope.each is None:
                raise KeyError("each is read in the branch of an each wait or open")
            return scope.each[parts[0]]
        if base.id == "member" and base.id not in scope.loops:
            if scope.member is None:
                raise KeyError("member is read in the lowest of a reference")
            return scope.member[parts[0]]
        record = _record_parts(node)
        if record is not None:
            row = scope.record or {}
            if len(record) == 1:
                return row.get(record[0])
            value = row.get("value")
            return value.get(record[1]) if isinstance(value, Mapping) else None
        source, first = base.id, parts[0]
        if source in ("request", "holder"):
            return self._read_rule(source, parts, scope)
        if source == "batch":
            if scope.batch is None:
                raise KeyError("no batch is being tested, so batch has no value")
            return scope.batch[parts[0]]
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
            found = None if scope.args is None else _key_read_as(scope.args, first)
            if found is None:
                raise KeyError(
                    f"the process has no argument '{first}': an argument is passed "
                    f"with args or written with set_args"
                )
            first = found
            container: Any = scope.args
        elif source == "cause":
            if scope.cause is None:
                raise KeyError(
                    "no interrupt reached this task: cause is read in a task an "
                    "on_interrupt_next edge or an on_interrupt_run edge reaches"
                )
            found = _key_read_as(scope.cause, first)
            if found is None:
                raise KeyError(f"the cause has no '{first}'")
            first = found
            container = scope.cause
        else:
            container, message = sources[source]
            if source == "payload" and container is None:
                raise KeyError("the payload is null")
            found = None if container is None else _key_read_as(container, first)
            if found is None:
                raise KeyError(message.format(first))
            first = found
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
