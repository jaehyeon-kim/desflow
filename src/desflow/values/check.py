"""Checks of the generators in a blueprint or a builder task, made before the run.

Each function returns the problems it finds as `(location, message)` pairs, where the
location is a path of keys inside the thing checked. A blueprint loader turns them
into errors with a file and line, and `SimulationContext.run` into `ValueError`s.
"""

import string
from pathlib import Path
from typing import Any, Collection, Dict, Iterator, List, Mapping, Optional, Tuple

from desflow.core.expressions import Scope
from desflow.utils import suggestion
from desflow.values.models import (
    Expr,
    Fake,
    Format,
    Object,
    Param,
    Row,
    Sample,
    Var,
    When,
    _Gen,
)

Problem = Tuple[Tuple[Any, ...], str]
_FIELDS = ("null_rate", "unique", "omit_rate", "omit_when", "gen")


def children(
    node: Any, path: Tuple[Any, ...]
) -> Iterator[Tuple[Any, Tuple[Any, ...], bool]]:
    """Every value directly under `node`, with its path and whether it is the value of
    a mapping field (where `omit_rate` and `omit_when` apply).

    Args:
        node: The generator, or the value, to act on.
        path: Where the value is written, as a path of keys.
    """
    from pydantic import BaseModel

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
            node, path, names, in_task, services, base_dir, registry, inner
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
        )
    return found


def _expression_names(
    text: str, path: Tuple[Any, ...], known: Collection[str]
) -> List[Problem]:
    from desflow.core.expressions import ANY_NAME, Expression

    try:
        wanted = Expression(text, names=ANY_NAME).references["name"]
    except ValueError as exc:
        return [(path, str(exc))]
    return [(path, _unknown_name(name, known)) for name in sorted(wanted - set(known))]


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
) -> List[Problem]:
    found: List[Problem] = []
    known = set(names) | set(local)
    if isinstance(node, Var):
        if not in_task:
            found.append(
                (path, f"var '{node.name}': vars belong to a task; there is none here")
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
        found += _expression_names(node.expr, path + ("expr",), known)
    elif isinstance(node, When):
        found += _expression_names(node.condition, path + ("condition",), known)
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
) -> List[Problem]:
    """The problems of every value a task holds.

    Args:
        vars_once, vars, on_start, finish_vars: The task's keys of those names.
        others: The values read after the vars, by the key they are written under:
            `payload`, `set`, `interrupt`, `on`.
        wait_values: The values of the wait's timeouts, by their path inside the task.
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
