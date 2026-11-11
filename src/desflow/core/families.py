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

The module also holds `read_place`, which reads a place of a run, and `whole`, which
takes a count as `range` does: an integer, or a boolean as 1 or 0, and refuses any
other value, even `2.0`. A family reads its `members` with them, and `builder` imports them
from here, below it.
"""

from dataclasses import dataclass, field, replace
from numbers import Integral
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from desflow.core.expressions import Expression, Scope, member_name, normal_key

if TYPE_CHECKING:
    from desflow.builder.holders import HolderRun
    from desflow.builder.signals import SignalRun
    from desflow.values.runtime import ValueRuntime

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


def whole(value: Any, what: str) -> int:
    """A count place's result: a whole number, a boolean counting as 1, as `range`
    takes it.

    Args:
        value: The result of the place.
        what: Where it was written, for the message.

    Raises:
        TypeError: If it is not a whole number, as `range(2.0)` raises.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Integral):
        return int(value)
    raise TypeError(f"{what} gave {value!r}, which is not a whole number")


def read_place(
    values: "ValueRuntime", place: Any, names: Optional[Dict[str, Any]] = None
) -> Any:
    """Reads a place over the run: a number as it is, an expression or a generator,
    with `names` as its bare names.

    Args:
        values: The run's values, which give the run scope and evaluate a generator.
        place: The place: a number, an expression or a generator.
        names: The bare names the place reads; None for none.
    """
    scope = values.scope_of(names=names)
    if isinstance(place, Expression):
        return place.evaluate(scope)
    return values.evaluate(place, scope)


def family_of(name: str, families: Iterable[str]) -> Optional[str]:
    """The declared family a member name belongs to, found by the family's whole name:
    the family `f` for which the name is `f[<key>]`, so a family whose name holds a
    dot or a bracket reads as any other.

    Args:
        name: A resource, store, container or signal name.
        families: The names of the declared families.

    Returns:
        Optional[str]: The family, or None when the name is no member of one.
    """
    for family in families:
        if name.startswith(f"{family}[") and name.endswith("]"):
            return family
    return None


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


def member_path(
    path: str, families: Mapping[str, Any], sim_id: str
) -> Optional[Tuple[str, str]]:
    """The family and the attribute a registry path names, when it names a member of a
    declared family, such as `S.resources.desk[3].current_cap`.

    The model's sim id is taken off the front of the path before the section is read,
    so a sim id with a dot, such as `Shop.v2`, reads as any other.

    Args:
        path: A registry path.
        families: The declared families, by name.
        sim_id: The model's sim id.

    Returns:
        Optional[Tuple[str, str]]: The family and the last part, or None when the path
            is not a member path of a family.
    """
    if not path.startswith(sim_id + "."):
        return None
    rest = path[len(sim_id) + 1 :]
    for family, declared in families.items():
        head = f"{SECTIONS[declared.kind]}.{family}"
        if rest.startswith(head + "["):
            parts = split_path(rest[len(head) :])
            if len(parts) >= 2:
                return family, parts[-1]
    return None


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

    # The starting level a container family declares as a number; set by
    # `Families.declare`
    starting_level: Any = 0


def name_problem(kind: str, name: str, held_by: Optional[str]) -> Optional[str]:
    """Why a resource, store, container or signal cannot have this name, or None.

    One check for the Builder and the YAML load, so each refuses with the same text.

    Args:
        kind: `resource`, `store`, `container` or `signal`.
        name: The name.
        held_by: The kind that already has the name, or None.
    """
    if held_by is not None and held_by != kind:
        return (
            f"{kind} '{name}' has the name of a {held_by}; the names of resources, "
            f"stores, containers and signals are one set, so use another name"
        )
    for word, mark in (("dot", "."), ("space", " ")):
        if mark in name:
            return (
                f"{kind} '{name}' has a {word} in its name, which an expression "
                f"cannot name; use a name without one"
            )
    return None


class Families:
    """The families a model declares, by name.

    Attributes:
        by_name: Each family, by the name of its entry.
    """

    def __init__(self) -> None:
        self.by_name: Dict[str, Family] = {}
        # The kind each declared name has, family or not
        self.kinds: Dict[str, str] = {}

    def new_run(
        self,
        values: "ValueRuntime",
        holder_run: "HolderRun",
        signal_run: "SignalRun",
    ) -> "FamilyRun":
        """The part of one run that makes the members, from the run's parts.

        Args:
            values: The run's values, for the run scope and `members`.
            holder_run: The run's resources, stores and containers.
            signal_run: The run's signals.
        """
        return FamilyRun(self, values, holder_run, signal_run)

    def claim(self, kind: str, name: str) -> None:
        """Records that a resource, store, container or signal has this name, and
        refuses a name another kind holds or one an expression cannot read. A name
        declared again in its own kind replaces the first.

        Args:
            kind: `resource`, `store`, `container` or `signal`.
            name: The entry's name.

        Raises:
            ValueError: If another kind has the name, or it holds a dot or a space.
        """
        problem = name_problem(kind, name, self.kinds.get(name))
        if problem is not None:
            raise ValueError(problem)
        self.kinds[name] = kind

    def declare(
        self,
        kind: str,
        name: str,
        family: bool,
        members: Any,
        texts: Mapping[str, str],
        start: Any = 0,
    ) -> None:
        """Keeps or drops the family of an entry that `add_resource`, `add_store`,
        `add_container` or `add_signal` registered, as it was written.

        Args:
            kind: `resource`, `store`, `container` or `signal`.
            name: The entry's name.
            family: Whether the entry was written as a family.
            members: The family's count place, as `Family.members` holds it; text is read
                as an expression.
            texts: The capacities written as text, by field.
            start: The starting level of a container member.
        """
        if not family:
            self.by_name.pop(name, None)
            return
        if isinstance(members, str):
            members = Expression(members)
        made = Family(name, kind, members, texts)
        made.starting_level = start
        self.by_name[name] = made

    def origin(self, name: str) -> str:
        """The entry whose settings a store or container takes: the family of a member,
        or the name itself.

        Args:
            name: The store's or container's name.
        """
        return family_of(name, self.by_name) or name

    def without(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """The entries of a section that are not families: a family's entry is the
        template of its members and has no paths of its own.

        Args:
            config: The section's entries, by name.
        """
        if not self.by_name:
            return config
        return {k: v for k, v in config.items() if k not in self.by_name}


@dataclass
class FamilyRun:
    """Makes and picks the members of the families in one run. The members are
    kept with the run's other resources, stores, containers and signals.

    Attributes:
        families: The declarations.
        values: The run's values, for the run scope and `members`.
        holder_run: The run's resources, stores and containers.
        signal_run: The run's signals.
        keys: The keys of the members made so far in this run, by family name, in
            creation order.
    """

    families: Families
    values: "ValueRuntime"
    holder_run: "HolderRun"
    signal_run: "SignalRun"
    keys: Dict[str, List[Union[int, str]]] = field(default_factory=dict)

    def start_members(self) -> None:
        """Makes the members `members` asks for, in family order, keys 0 to n - 1, in
        run scope."""
        for family in self.families.by_name.values():
            if family.members is None:
                continue
            count = whole(
                read_place(self.values, family.members),
                f"{SECTIONS[family.kind]}.{family.name}.members",
            )
            scope = self.values.scope_of()
            for key in range(max(count, 0)):
                self.open_member(family, key, scope)

    def open_member(self, family: Family, key: Any, scope: Optional[Scope]) -> str:
        """Gives the name of a family's member, making the member when this is the
        first use of its key. The member lasts to the end of the run.

        Args:
            family: The family.
            key: The member's key, an integer or text; a boolean is 1 or 0.
            scope: What a capacity written as text reads. A family with `members`
                reads the run scope for every member.

        Returns:
            str: The member's name, such as `desk[3]`.

        Raises:
            TypeError: If `key` is not an integer or text.
            ValueError: If a capacity written as text gives an unusable value.
        """
        key = normal_key(key)
        name = member_name(family.name, key)
        holders = self.holders_of(family)
        if name in holders:
            return name
        base = family.name
        if family.kind == "signal":
            # A member takes the family's settings, read from the family's entry,
            # so the run adds no member to the declarations
            self.signal_run.make(name, base)
            self.keys.setdefault(family.name, []).append(key)
            return name
        if family.members is not None or scope is None:
            scope = self.values.scope_of()
        self.holder_run.make_member(family, name, scope)
        self.keys.setdefault(family.name, []).append(key)
        return name

    def open_reference(self, reference: Reference, scope: Scope) -> str:
        """The name a reference gives now: the member of its `key`, made when it is
        new, or the member with the lowest `lowest`. A key and a lowest are read once.

        Args:
            reference: The reference.
            scope: What its expression reads: the step of the reference.

        Returns:
            str: The name of the resource, store, container or signal.

        Raises:
            KeyError: If `key` gives a name that `of` does not list.
            ValueError: If the family has no member to pick from.
            TypeError: If `key` is not an integer or text.
        """
        if reference.listed:
            chosen = reference.key_expression.evaluate(scope)  # type: ignore[union-attr]
            if chosen not in reference.names:
                raise KeyError(
                    f"the key of {reference.mapping()} gave {chosen!r}, which is not "
                    f"one of {list(reference.names)}"
                )
            return str(chosen)
        family = self.families.by_name[str(reference.of)]
        if reference.key_expression is not None:
            return self.open_member(
                family, reference.key_expression.evaluate(scope), scope
            )
        keys = self.keys.get(family.name, [])
        if not keys:
            raise ValueError(
                f"{reference.mapping()} has no member to pick from: the family is empty"
            )
        best: Any = None
        found: Any = None
        for key in list(keys):
            value = reference.lowest_expression.evaluate(  # type: ignore[union-attr]
                replace(scope, member=self.member_row(family, key))
            )
            if best is None or value < best:
                best, found = value, key
        return self.open_member(family, found, scope)

    def member_row(self, family: Family, key: Any) -> Dict[str, Any]:
        """What `member.<name>` reads for one member.

        Args:
            family: The family.
            key: The member's key.
        """
        name = member_name(family.name, key)
        row: Dict[str, Any] = {"key": key}
        if family.kind == "resource":
            resource = self.holder_run.resources[name]
            row.update(users=len(resource.users), queue=len(resource.queue))
        elif family.kind == "signal":
            signal = self.signal_run.made[name]
            row.update(count=signal.count)
        else:
            holder = self.holder_run.levels()[name]
            row.update(
                level=getattr(holder, "level", None),
                count=len(getattr(holder, "items", [])),
                putters=len(holder.put_queue),
                getters=len(holder.get_queue),
            )
        return row

    def holders_of(self, family: Family) -> Dict[str, Any]:
        """The map a family's members are kept in.

        Args:
            family: The family.
        """
        holders: Dict[str, Dict[str, Any]] = {
            "resource": self.holder_run.resources,  # type: ignore[dict-item]
            "store": self.holder_run.stores,  # type: ignore[dict-item]
            "container": self.holder_run.containers,  # type: ignore[dict-item]
            "signal": self.signal_run.made,  # type: ignore[dict-item]
        }
        return holders[family.kind]
