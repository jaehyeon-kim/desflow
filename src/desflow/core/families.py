"""Families: resources, stores, containers and signals made from a computed key.

A family is one entry of `resources`, `stores`, `containers` or `signals` written with
`family: true`. Its members are made from keys: `members: n` makes the keys 0 to n - 1
when the run starts, and a reference `{of: <family>, key: <expression>}` makes the
member of its key at its first use. A member is an ordinary resource, store, container
or signal named after the family and the key: `desk[3]`, `desk['north']`. A reference
`{of: [a, b], key: ...}` picks one of the declared names, and `{of: <family>, lowest:
<expression>}` picks the member with the lowest value.

`Reference` is how a task names a member. It is a `str` whose text is the first name of
`of`, so a check that reads names finds the declared entry, and code that opens the
member calls `resolve`.
"""

from numbers import Integral
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from desflow.core.expressions import Expression

# The kinds a family can be, and the section of a blueprint each one is written in
KINDS = ("resource", "store", "container", "signal")
SECTIONS = {
    "resource": "resources",
    "store": "stores",
    "container": "containers",
    "signal": "signals",
}
# The capacities of a family that may be written as an expression of the creating step
EXPRESSION_FIELDS = ("current_cap", "max_cap", "level")


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
    if isinstance(key, Integral):
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


def family_part(name: str) -> str:
    """The text before the first bracket of a name: the family a member name belongs to,
    or the name itself when it has no bracket.

    Args:
        name: A resource, store, container or signal name.

    Returns:
        str: The part before the first `[`.
    """
    return name.split("[", 1)[0]


def split_path(path: str) -> List[str]:
    """Splits a registry path at its dots, except those inside brackets, so a member of
    a family is one part: `S.resources.desk['a.b'].current_cap` has four parts.

    Args:
        path: A registry path.

    Returns:
        List[str]: The parts, in order.
    """
    parts: List[str] = []
    current = ""
    depth = 0
    quote = ""
    escaped = False
    for char in path:
        if quote:
            current += char
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if depth and char in "'\"":
            quote = char
            current += char
        elif char == "[":
            depth += 1
            current += char
        elif char == "]" and depth:
            depth -= 1
            current += char
        elif char == "." and not depth:
            parts.append(current)
            current = ""
        else:
            current += char
    parts.append(current)
    return parts


def member_path(path: str, families: Mapping[str, Any]) -> Optional[Tuple[str, str]]:
    """The family and the attribute a registry path names, when it names a member of a
    declared family, such as `S.resources.desk[3].current_cap`.

    Args:
        path: A registry path.
        families: The declared families, by name.

    Returns:
        Optional[Tuple[str, str]]: The family and the last part, or None when the path
            is not a member path of a family.
    """
    parts = split_path(path)
    if len(parts) < 4 or "[" not in parts[2]:
        return None
    family = family_part(parts[2])
    if family not in families or parts[1] != SECTIONS[families[family].kind]:
        return None
    return family, parts[-1]


class Reference(str):
    """A name a task computes: `{of, key}` or `{of, lowest}`.

    Its text is the first name of `of`, so a check that looks a name up finds the
    declared entry. Opening the member is `resolve`.

    Attributes:
        of (Union[str, Tuple[str, ...]]): A family's name, or the declared names to
            pick from.
        key (Optional[str]): The expression that gives the key (or the name, for a
            list), as written.
        lowest (Optional[str]): The expression that gives each member a value, as
            written.
        key_expression (Optional[Expression]): The parsed `key`.
        lowest_expression (Optional[Expression]): The parsed `lowest`.
    """

    of: Union[str, Tuple[str, ...]]
    key: Optional[str]
    lowest: Optional[str]
    key_expression: Optional[Expression]
    lowest_expression: Optional[Expression]

    def __new__(
        cls,
        of: Union[str, List[str], Tuple[str, ...]],
        key: Union[None, int, str] = None,
        lowest: Optional[str] = None,
    ) -> "Reference":
        """
        Checks and parses a reference.

        Args:
            of: A family's name, or a list of declared names.
            key: The expression that gives the key, or an integer.
            lowest: The expression that gives each member a value, which reads
                `member.<name>`.

        Raises:
            ValueError: If the form is malformed, or an expression does not parse.
        """
        if isinstance(of, str):
            if not of:
                raise ValueError("a reference names a family or a list of names")
            names: Tuple[str, ...] = (of,)
        elif (
            isinstance(of, (list, tuple))
            and of
            and all(isinstance(n, str) and n for n in of)
        ):
            names = tuple(of)
            if len(set(names)) != len(names):
                raise ValueError(f"a reference lists a name twice: {list(names)}")
        else:
            raise ValueError(
                f"a reference's of is a family's name or a list of names, got {of!r}"
            )
        if (key is None) == (lowest is None):
            raise ValueError("a reference takes key or lowest, exactly one of them")
        if isinstance(key, bool) or not (key is None or isinstance(key, (str, int))):
            raise ValueError(
                f"a reference's key is an expression, got {key!r}; write text such as "
                f"'attr.city'"
            )
        if lowest is not None and not isinstance(lowest, str):
            raise ValueError(f"a reference's lowest is an expression, got {lowest!r}")
        self = super().__new__(cls, names[0])
        self.of = of if isinstance(of, str) else names
        self.key = None if key is None else str(key)
        self.lowest = lowest
        self.key_expression = None if key is None else _parsed(str(key), "key")
        self.lowest_expression = (
            None if lowest is None else _parsed(lowest, "lowest", member=True)
        )
        return self

    def __getnewargs__(self) -> Tuple[Any, ...]:
        return (self.of, self.key, self.lowest)

    def __repr__(self) -> str:
        found = {"of": self.of}
        if self.key is not None:
            found["key"] = self.key
        if self.lowest is not None:
            found["lowest"] = self.lowest
        return f"Reference({found})"

    @property
    def names(self) -> Tuple[str, ...]:
        """Tuple[str, ...]: The declared names `of` names, one for a family."""
        return (self.of,) if isinstance(self.of, str) else self.of

    @property
    def listed(self) -> bool:
        """bool: Whether `of` is a list of declared names and not one family."""
        return not isinstance(self.of, str)

    @property
    def expressions(self) -> List[Expression]:
        """List[Expression]: The parsed `key` or `lowest`."""
        return [
            e for e in (self.key_expression, self.lowest_expression) if e is not None
        ]

    def mapping(self) -> Dict[str, Any]:
        """The reference as the mapping it is written as."""
        found: Dict[str, Any] = {
            "of": self.of if isinstance(self.of, str) else list(self.of)
        }
        if self.key is not None:
            found["key"] = self.key
        else:
            found["lowest"] = self.lowest
        return found


def _parsed(text: str, what: str, member: bool = False) -> Expression:
    """A `key` or `lowest` parsed, or a ValueError that names it."""
    try:
        return Expression(text, member=member)
    except ValueError as error:
        raise ValueError(f"a reference's {what}: {error}") from None


def name_of(value: Any, what: str = "name") -> Union[str, Reference]:
    """A name as a task takes it: text, a `Reference`, or the mapping `{of, key}` or
    `{of, lowest}` that is one.

    Args:
        value: The name as written.
        what: The key it is written under, for the message.

    Returns:
        Union[str, Reference]: The name.

    Raises:
        ValueError: If it is none of these, or a mapping is not a reference.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        unknown = set(value) - {"of", "key", "lowest"}
        if unknown or "of" not in value:
            raise ValueError(
                f"{what} is a name or a reference {{of: <family>, key: <expression>}} "
                f"or {{of: <family>, lowest: <expression>}}, got {dict(value)!r}"
            )
        return Reference(value["of"], value.get("key"), value.get("lowest"))
    raise ValueError(f"{what} is a name or a reference, got {value!r}")


def references(*places: Any) -> List[Reference]:
    """The references among some names, lists of names, or nodes' targets, in order.

    Args:
        *places: Names, `Reference`s, or lists of them.

    Returns:
        List[Reference]: Each `Reference` found.
    """
    found: List[Reference] = []
    for place in places:
        if isinstance(place, Reference):
            found.append(place)
        elif isinstance(place, (list, tuple)):
            found.extend(references(*place))
    return found


class Family:
    """One declared family.

    Attributes:
        name (str): The entry's name.
        kind (str): `resource`, `store`, `container` or `signal`.
        members: A count place (a number, text, or a generator) read once when the run
            starts, or None for a family that starts empty.
        expressions (Dict[str, Expression]): The capacities written as text, by field
            (`current_cap`, `max_cap`, `level`), read once at each member's creation.
        keys (List[Union[int, str]]): The keys made so far in this run, in creation
            order.
    """

    def __init__(
        self,
        name: str,
        kind: str,
        members: Any = None,
        expressions: Optional[Mapping[str, str]] = None,
    ):
        """
        Checks and keeps a family.

        Args:
            name: The entry's name.
            kind: `resource`, `store`, `container` or `signal`.
            members: A count place, or None.
            expressions: Capacity fields written as expression text.

        Raises:
            ValueError: If an expression does not parse.
        """
        self.name = name
        self.kind = kind
        self.members = members
        self.expressions: Dict[str, Expression] = {}
        for field_name, text in (expressions or {}).items():
            try:
                parsed = Expression(text)
            except ValueError as error:
                raise ValueError(
                    f"{SECTIONS[kind]}.{name}.{field_name}: {error}"
                ) from None
            outside = parsed.outside_run() if members is not None else None
            if outside:
                raise ValueError(
                    f"{SECTIONS[kind]}.{name}.{field_name} '{text}' reads {outside}, "
                    f"but a family with members reads the run for every member, so it "
                    f"reads level, users, queue, signal, registry and t, not an entity"
                )
            self.expressions[field_name] = parsed
        self.keys: List[Union[int, str]] = []

    def blank(self, source: str, scope: Any) -> Any:
        """What a member that does not exist yet would give a read.

        Args:
            source: `users`, `queue`, `level`, `putters`, `getters`, `count`, `ok`,
                `value` or `processed`.
            scope: The scope the read is made in, for a level written as an
                expression.

        Returns:
            Any: The value a new member gives: 0 for a count, the starting level for a
                container, and the value of a signal that has not fired.
        """
        if source in ("ok", "value"):
            return None
        if source == "processed":
            return False
        if source == "level" and self.kind == "container":
            level = self.expressions.get("level")
            if level is not None:
                return level.evaluate(scope)
            return self.starting_level
        return 0

    # The starting level a container family declares as a number; set by the context
    starting_level: Any = 0
