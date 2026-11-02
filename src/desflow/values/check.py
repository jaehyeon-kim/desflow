"""Checks of the generators in a blueprint or a builder task, made before the run.

Each function returns the problems it finds as `(location, message)` pairs, where the
location is a path of keys inside the thing checked. A blueprint loader turns them
into errors with a file and line, and `SimulationContext.run` into `ValueError`s.
"""

import string
from pathlib import Path
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)

from pydantic import BaseModel

from desflow.core.expressions import Scope
from desflow.utils import suggestion
from desflow.values.models import (
    Aggregate,
    Attr,
    Entity,
    Expr,
    Fake,
    Format,
    Lookup,
    Object,
    Param,
    Previous,
    Repeat,
    Row,
    Sample,
    Upstream,
    Var,
    When,
    _Gen,
)

Problem = Tuple[Tuple[Any, ...], str]
_FIELDS = ("null_rate", "unique", "omit_rate", "omit_when", "gen")


# Every key of a task that holds a value (a generator, or a mapping of them), with
# whether the value is evaluated after the task's own waiting get, so that it may
# read `item.<field>`. The blueprint check and the Builder's `run()` check both read
# this one list, so a place added here is checked in both.
TASK_VALUE_PLACES: Tuple[Tuple[str, bool], ...] = (
    ("payload", True),
    ("set", True),
    ("set_args", True),
    ("vars_once", False),
    ("vars", True),
    ("finish_vars", True),
    ("on_start", True),
    ("spawn", True),
    ("interrupt", True),
    ("resume", True),
    ("on", True),
    ("wait", False),
    ("open", False),
    ("service_time", True),
    ("patience", False),
    ("key", False),
    ("batch.size", False),
    ("batch.timeout", False),
    ("batch.close_when", False),
    ("put.item", True),
)


def task_value_places(
    read: Callable[[str], Any],
) -> Iterator[Tuple[str, Tuple[str, ...], bool, Any]]:
    """Every value place of a task: its key, its location below the task, whether it
    is evaluated after the task's own get, and its value.

    Args:
        read: Gives the value of a place by its key (`put.item` for the item of a
            `put`), or None when the task has none.
    """
    for key, after in TASK_VALUE_PLACES:
        yield key, tuple(key.split(".")), after, read(key)


def children(
    node: Any, path: Tuple[Any, ...]
) -> Iterator[Tuple[Any, Tuple[Any, ...], bool]]:
    """Every value directly under `node`, with its path and whether it is the value of
    a mapping field (where `omit_rate` and `omit_when` apply).

    Args:
        node: The generator, or the value, to act on.
        path: Where the value is written, as a path of keys.
    """
    if isinstance(node, BaseModel):
        for name, field in type(node).model_fields.items():
            if name in _FIELDS:
                continue
            value = getattr(node, name)
            here = path + (field.alias or name,)
            if isinstance(value, Mapping) and not isinstance(node, Object):
                # The entries of `names`, `cases`, `options` and `kwargs` are not
                # fields of a mapping that is published, so nothing is left out of one
                for key, item in value.items():
                    yield item, here + (key,), False
            else:
                yield value, here, False
    elif isinstance(node, Mapping):
        for key, item in node.items():
            yield item, path + (key,), True
    elif isinstance(node, (list, tuple)):
        for index, item in enumerate(node):
            yield item, path + (index,), False


def generators_in(
    node: Any, kinds: Tuple[type, ...], path: Tuple[Any, ...] = ()
) -> Iterator[Tuple[Tuple[Any, ...], Any]]:
    """Every generator of the given kinds under a value, with where it is written.

    Args:
        node: The value: a generator, a mapping, a list or a constant.
        kinds: The generator classes to find.
        path: Where `node` is written.

    Yields:
        Tuple[Tuple[Any, ...], Any]: The place of the generator and the generator.
    """
    if isinstance(node, kinds):
        yield path, node
    for item, item_path, _ in children(node, path):
        yield from generators_in(item, kinds, item_path)


def _expand_places(places: Tuple[Any, ...]) -> Iterator[Any]:
    """The values to search in some places: a `Template` gives its `payload`, `vars`
    and `vars_once`, a `BoundPayload` gives its template and its fields.

    Raises:
        TypeError: If a place is a set or an iterator, which are not searched.
    """
    from desflow.values.template import BoundPayload, Template

    for place in places:
        if isinstance(place, Template):
            yield place.payload
            yield place.vars
            yield place.vars_once
        elif isinstance(place, BoundPayload):
            yield from _expand_places((place.template, place.fields))
        elif isinstance(place, (set, frozenset, Iterator)):
            # Neither is searched by `generators_in`, so a lookup inside would be
            # missed with no message
            raise TypeError(
                f"a place for note_values is a generator, a mapping, a list, a "
                f"constant or a Template, not {type(place).__name__}"
            )
        else:
            yield place


def looked_up(*places: Any) -> Tuple[Set[str], Set[str], bool]:
    """What the values of some places read from other records.

    Args:
        places: Values: generators, mappings, lists, constants, or a `Template` (its
            `payload`, `vars` and `vars_once` are searched).

    Returns:
        Tuple[Set[str], Set[str], bool]: The tasks a `lookup` names, the tasks a
            `lookup` or `aggregate` names, and whether a `previous` is there.

    Raises:
        TypeError: If a place is a set or an iterator, which are not searched.
    """
    gates: Set[str] = set()
    rows: Set[str] = set()
    previous = False
    for place in _expand_places(places):
        for _, node in generators_in(place, (Lookup, Aggregate, Previous)):
            if isinstance(node, Previous):
                previous = True
                continue
            rows.add(node.task)
            if isinstance(node, Lookup):
                gates.add(node.task)
    return gates, rows, previous


def lookup_cycle(gates: Mapping[str, Collection[str]]) -> Optional[List[str]]:
    """A cycle of tasks that look each other up, so none could start, or None.

    Args:
        gates: For each task, the tasks its `lookup` generators name.

    Returns:
        Optional[List[str]]: The tasks of the cycle, the first one repeated at the end.
    """
    done: Set[str] = set()

    def walk(name: str, trail: List[str]) -> Optional[List[str]]:
        if name in trail:
            return trail[trail.index(name) :] + [name]
        if name in done:
            return None
        for target in sorted(gates.get(name, ())):
            found = walk(target, trail + [name])
            if found:
                return found
        done.add(name)
        return None

    for task in sorted(gates):
        found = walk(task, [])
        if found:
            return found
    return None


def check_value(
    node: Any,
    path: Tuple[Any, ...],
    visible: Collection[str],
    *,
    in_task: bool,
    services: Optional[Collection[str]] = None,
    base_dir: Optional[Path] = None,
    registry: Any = None,
    local: Collection[str] = (),
    mapping_field: bool = False,
    in_arrival: bool = False,
    run_scope: bool = False,
    in_repeat: bool = False,
) -> List[Problem]:
    """The problems of the generators under a value.

    Args:
        node: The value: a generator, a mapping, a list or a constant.
        path: Where it is written.
        visible: The var names a bare name or `var` may read here.
        in_task: Whether the place belongs to a task; `var` is refused elsewhere.
        services: The names of the model's services, or None to skip that check.
        base_dir: What a `row` file is relative to; None leaves the file unread.
        registry: A registry to check `param` paths against, or None to skip.
        local: Names an enclosing `expr` or `format` defines for its own arguments.
        mapping_field: Whether `node` is the value of a mapping field.
        in_arrival: Whether the place is a `vars`, `key` or `attributes` of an
            arrival: `var` reads the arrival's vars there, and `entity` is refused.
        run_scope: Whether the place reads the run only, as an arrival's gap, count,
            stagger, `max_events` and `vars` do: the entity has no attributes yet,
            so `attr` is refused.
        in_repeat: Whether the value is inside the `value` of a `repeat`, which a
            `lookup` with `distinct` needs.
    """
    found: List[Problem] = []
    names = set(visible) | set(local)
    if isinstance(node, _Gen):
        if not mapping_field and (
            node.omit_rate is not None or node.omit_when is not None
        ):
            found.append(
                (
                    path,
                    "omit_rate and omit_when drop a field of a mapping, such as a "
                    "payload, so they belong on a generator that is the value of a "
                    "key there; a var, a list item or an argument cannot be left out",
                )
            )
        if node.omit_when is not None:
            found += _expression_names(node.omit_when, path + ("omit_when",), names)
        inner = set(local)
        if isinstance(node, (Expr, Format)):
            inner |= set(node.names)
        found += _kind_problems(
            node,
            path,
            names,
            in_task,
            services,
            base_dir,
            registry,
            inner,
            in_arrival,
            run_scope,
            in_repeat,
        )
    for item, item_path, is_field in children(node, path):
        found += check_value(
            item,
            item_path,
            visible,
            in_task=in_task,
            services=services,
            base_dir=base_dir,
            registry=registry,
            local=local,
            mapping_field=is_field,
            in_arrival=in_arrival,
            run_scope=run_scope,
            in_repeat=in_repeat
            or (isinstance(node, Repeat) and item_path[-1] == "value"),
        )
    return found


def names_read(place: Any) -> Set[str]:
    """The bare names a value reads: those of an `Expression`, and every `var`, `expr`,
    `format` and `when` argument of a generator that is not defined by the generator
    itself.

    Args:
        place: An `Expression`, a generator, a mapping or list that holds one, or a
            constant.
    """
    from desflow.core.expressions import ANY_NAME, Expression

    if isinstance(place, Expression):
        return set(place.references["name"])
    found: Set[str] = set()
    if isinstance(place, _Gen):
        own: Set[str] = set()
        texts: List[str] = []
        if place.omit_when is not None:
            texts.append(place.omit_when)
        if isinstance(place, Var):
            found.add(place.name)
        elif isinstance(place, Expr):
            texts.append(place.expr)
            own = set(place.names)
        elif isinstance(place, When):
            texts.append(place.condition)
        elif isinstance(place, Format):
            own = set(place.names)
            try:
                for _, field, _, _ in string.Formatter().parse(place.template):
                    if field is not None:
                        found.add(field.split(".")[0].split("[")[0])
            except ValueError:
                pass
        for text in texts:
            try:
                found |= Expression(text, names=ANY_NAME).references["name"]
            except ValueError:
                pass
        found -= own
    for item, _, _ in children(place, ()):
        found |= names_read(item)
    return found


def _expression_names(
    text: str,
    path: Tuple[Any, ...],
    known: Collection[str],
    run_scope: bool = False,
) -> List[Problem]:
    from desflow.core.expressions import ANY_NAME, Expression

    try:
        parsed = Expression(text, names=ANY_NAME)
    except ValueError as exc:
        return [(path, str(exc))]
    found = [
        (path, _unknown_name(name, known))
        for name in sorted(parsed.references["name"] - set(known))
    ]
    outside = parsed.outside_run(names=False) if run_scope else None
    if outside:
        found.append(
            (
                path,
                f"'{text}' reads {outside}, but this place reads the run (level, "
                "users, queue, signal, registry, vars and t), not the entity or a "
                "task",
            )
        )
    return found


def _unknown_name(name: str, known: Collection[str]) -> str:
    where = (
        "no var of this task has a value here yet"
        if not known
        else f"the names here are {', '.join(sorted(known))}"
    )
    return f"'{name}' is not a var that has a value here; {where}" + suggestion(
        name, known
    )


def _kind_problems(
    node: _Gen,
    path: Tuple[Any, ...],
    names: Collection[str],
    in_task: bool,
    services: Optional[Collection[str]],
    base_dir: Optional[Path],
    registry: Any,
    local: Collection[str],
    in_arrival: bool = False,
    run_scope: bool = False,
    in_repeat: bool = False,
) -> List[Problem]:
    found: List[Problem] = []
    known = set(names) | set(local)
    if isinstance(node, Attr) and run_scope:
        found.append(
            (
                path,
                f"attr '{node.name}': this place reads the run, and the entity has "
                "no attributes yet",
            )
        )
    elif isinstance(node, Previous):
        if not in_task:
            found.append(
                (
                    path,
                    "previous reads the last finished payload of the task it is "
                    "written in, so it stands in a task; there is none here",
                )
            )
    elif isinstance(node, Upstream):
        if not in_task:
            found.append(
                (
                    path,
                    "upstream reads the payload of the task that routed to this one, "
                    "so it stands in a task; there is none here",
                )
            )
    elif isinstance(node, Lookup):
        if node.distinct and not in_repeat:
            found.append(
                (
                    path + ("distinct",),
                    "distinct draws without replacement within one repeat, so the "
                    "lookup stands inside the value of a repeat",
                )
            )
    elif isinstance(node, Entity):
        if not in_task:
            found.append(
                (
                    path,
                    "entity is the key of the entity a task runs for; there is no "
                    "task here"
                    + (", and the key is made in this place" if in_arrival else ""),
                )
            )
    elif isinstance(node, Var):
        if not (in_task or in_arrival):
            found.append(
                (path, f"var '{node.name}': vars belong to a task; there is none here")
            )
        elif in_arrival and not in_task and not names:
            found.append(
                (
                    path,
                    f"var '{node.name}': vars belong to a task or to the vars of "
                    f"an arrival; this arrival has none",
                )
            )
        elif node.name not in names:
            found.append((path + ("name",), _unknown_name(node.name, names)))
    elif isinstance(node, Expr):
        for name in sorted(set(node.names) & set(names)):
            found.append(
                (
                    path + ("names", name),
                    f"'{name}' is also a var of this task; a name here may not "
                    "take the name of a var",
                )
            )
        found += _expression_names(node.expr, path + ("expr",), known, run_scope)
    elif isinstance(node, When):
        found += _expression_names(
            node.condition, path + ("condition",), known, run_scope
        )
    elif isinstance(node, Format):
        try:
            fields = [
                field
                for _, field, _, _ in string.Formatter().parse(node.template)
                if field is not None
            ]
        except ValueError as exc:
            return [(path + ("template",), f"template {node.template!r}: {exc}")]
        heads = {field.split(".")[0].split("[")[0] for field in fields}
        for head in sorted(heads):
            if head == "" or head.isdigit():
                found.append(
                    (path + ("template",), "write {name}, not {} or {0}, in a template")
                )
            elif head not in known:
                found.append((path + ("template",), _unknown_name(head, known)))
    elif isinstance(node, Fake):
        found += _fake_problems(node, path)
    elif isinstance(node, Sample):
        if node.service is not None and services is not None:
            if node.service not in services:
                found.append(
                    (
                        path + ("service",),
                        f"sample names service '{node.service}', "
                        + (
                            "which is not under services"
                            + suggestion(node.service, services)
                        ),
                    )
                )
    elif isinstance(node, Param):
        if registry is not None:
            try:
                registry.get(node.path)
            except KeyError:
                found.append(
                    (
                        path + ("path",),
                        f"'{node.path}' is not a registry path. Paths look like "
                        "Shop.resources.<name>.current_cap or Shop.arrival.<name>.rate",
                    )
                )
    elif isinstance(node, Row) and base_dir is not None:
        problem = load_rows(node, base_dir)
        if problem is not None:
            found.append((path + ("file",), problem))
    return found


def _fake_problems(node: Fake, path: Tuple[Any, ...]) -> List[Problem]:
    try:
        from faker import Faker
    except ImportError:
        return [
            (
                path,
                "gen fake needs the Faker package; install it with "
                "pip install 'desflow[fake]'",
            )
        ]
    try:
        faker = Faker(node.locale)
    except (AttributeError, ValueError, ImportError) as exc:
        return [
            (path + ("locale",), f"locale {node.locale!r} is not one Faker has: {exc}")
        ]
    method = getattr(faker, node.method, None)
    if node.method.startswith("_") or method is None or not callable(method):
        providers = sorted(name for name in dir(faker) if not name.startswith("_"))
        return [
            (
                path + ("method",),
                f"Faker has no method '{node.method}'"
                + suggestion(node.method, providers),
            )
        ]
    return []


def load_rows(node: Row, base_dir: Path) -> Optional[str]:
    """Reads the file of a `row` generator into the node.

    Args:
        node: The generator, or the value, to act on.
        base_dir: The folder a relative file name is read from.

    Returns:
        Optional[str]: The problem, or None when the rows were read.
    """
    from desflow.core.expressions import ANY_NAME, Expression
    from desflow.core.replay import _records, _value

    path = Path(node.file)
    if not path.is_absolute():
        path = base_dir / path
    if node._rows is not None:
        return None
    if path.suffix.lower() not in (".csv", ".parquet"):
        return f"{node.file} must be a .csv or .parquet file"
    if not path.is_file():
        return f"{node.file} does not exist (looked for {path})"
    try:
        records = _records(path)
    except ImportError:
        return (
            f"{node.file} is a Parquet file, which needs the parquet extra: "
            "pip install 'desflow[parquet]'"
        )
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        return f"{node.file} cannot be read: {exc}"
    rows = [{key: _value(value) for key, value in record.items()} for record in records]
    if node.where is not None:
        try:
            expression = Expression(node.where, names=ANY_NAME)
            rows = [row for row in rows if expression.evaluate(Scope(names=row))]
        except ValueError as exc:
            return f"where cannot be read over the columns of {node.file}: {exc}"
    if not rows:
        return f"{node.file} has no rows" + (
            f" where {node.where}" if node.where is not None else ""
        )
    if node.weight is not None and node.weight not in rows[0]:
        return f"{node.file} has no column '{node.weight}' for the weights"
    node._rows = rows
    return None


def task_value_problems(
    *,
    vars_once: Mapping[str, Any],
    vars: Mapping[str, Any],
    on_start: Mapping[str, Any],
    finish_vars: Mapping[str, Any],
    others: Mapping[str, Any],
    wait_values: Mapping[Tuple[Any, ...], Any],
    services: Optional[Collection[str]] = None,
    base_dir: Optional[Path] = None,
    registry: Any = None,
    early_end: bool = False,
    service_time: Any = None,
) -> List[Problem]:
    """The problems of every value a task holds.

    Args:
        vars_once, vars, on_start, finish_vars: The task's keys of those names.
        others: The values read after the vars, by the key they are written under:
            `payload`, `set`, `interrupt`, `on`.
        wait_values: The values read before the vars have a value, by their path
            inside the task: a wait timeout's delay and value, a patience and a key.
        service_time: The task's `service_time` when it is a generator. It may read
            every var, which get their value when it is read.
        services: As for `check_value`.
        base_dir: As for `check_value`.
        registry: As for `check_value`.
        early_end: Whether the task can end before its `vars` have a value: it has a
            `patience`, a `balk`, a `wait`, `on_interrupt: abandon` or a resource with
            a `queue_limit`. Its `on:` actions then run with the `vars_once` names
            only, because they run on those paths too.
    """
    common: Dict[str, Any] = dict(
        in_task=True, services=services, base_dir=base_dir, registry=registry
    )
    found: List[Problem] = []
    once: List[str] = []
    for name, value in vars_once.items():
        found += check_value(value, ("vars_once", name), once, **common)
        once.append(name)
    seen = list(once)
    for name, value in vars.items():
        found += check_value(value, ("vars", name), seen, **common)
        seen.append(name)
    for name, value in on_start.items():
        found += check_value(
            value, ("on_start", name), seen, mapping_field=True, **common
        )
    if service_time is not None:
        found += check_value(service_time, ("service_time",), seen, **common)
    after = list(seen)
    for name, value in finish_vars.items():
        found += check_value(value, ("finish_vars", name), after, **common)
        after.append(name)
    for key, value in others.items():
        # An `on:` action runs when the task ends early too, and no var has a value
        # then; the other keys are read only when the task finishes
        visible = once if (key == "on" and early_end) else after
        found += check_value(value, (key,), visible, **common)
    for where, value in wait_values.items():
        found += check_value(value, where, once, **common)
    return found
