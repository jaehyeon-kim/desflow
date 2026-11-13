"""Generated values for the builder and low-level APIs: `Template` and `BoundPayload`.

A `Template` holds the same mapping of generators a blueprint's `payload` does, and
validates it with the same models, so a mistake raises Pydantic's `ValidationError`
(without a file or a line).
`looked_up` finds what some values, templates among them, read from other records.
"""

import re
import weakref
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)

from pydantic import BaseModel

from desflow.values.check import Place, check_value, generators_in
from desflow.values.models import (
    Aggregate,
    Lookup,
    Previous,
    Gen,
    generator_adapter,
    payload_adapter,
    values_adapter,
)

_PATH = Tuple[Any, ...]

_WHOLE_NUMBER = re.compile(r"-?\d+")
# The keys of unnamed templates, and the names that a prefix keeps away from them
_UNNAMED_LOOKALIKE = re.compile(r"(named:)*template\d+")


def _escaped(text: str) -> str:
    return text.replace("\\", "\\\\").replace(".", "\\.")


def _segment(part: Any) -> str:
    """One step of a generator's dotted path, written so that no two different steps
    give the same text and no two different paths give the same dotted text.

    A step that is a whole number or a text without a dot, a backslash or a number
    shape is written as it is, so the sequence of a common path does not change. A dot
    or a backslash in a text is escaped. A text that looks like a whole number starts
    with a backslash, which tells it from the number key, and a key that is neither a
    text nor a whole number starts with its type name, which tells it from the text.
    """
    if isinstance(part, str):
        return (
            "\\" + _escaped(part) if _WHOLE_NUMBER.fullmatch(part) else _escaped(part)
        )
    if isinstance(part, int) and not isinstance(part, bool):
        return str(part)
    return f"\\{type(part).__name__}:{_escaped(str(part))}"


def bind(
    node: Any,
    path: _PATH,
    locate: Optional[Callable[[_PATH], str]] = None,
    *,
    overwrite: bool = True,
    text: Optional[str] = None,
) -> None:
    """Gives every generator under `node` its place in the file.

    The place is a dotted path, which names the generator's random sequence, and a
    "file:line: path" text for the messages of a failure during the run. The parts of
    `path` are joined as written, because they are the names of the model's elements.
    A key of a mapping under `node` is escaped where it holds a dot, so that a key
    `a.b` and the keys `a` then `b` have different paths. An element name is not
    escaped, so a name with a dot and a section word, such as a task `a.payload.b`,
    can give a generator the path of a key nested in another element, and the two
    then share a random sequence and a counter.

    Args:
        node: A value: a generator, a mapping, a list, or a constant.
        path: Where `node` is written, from the top of the file.
        locate: Gives the "file:line: path" text for a path; without it the dotted
            path alone is used.
        overwrite: Whether a generator that already has a place gets a new one.
        text: The dotted path of `node`; the parts of `path` joined, when left out.
    """
    if text is None:
        text = ".".join(str(part) for part in path)
    if isinstance(node, BaseModel):
        if isinstance(node, Gen) and (overwrite or not node._path):
            node._path = text
            node._where = locate(path) if locate else node._path
        for name, field in type(node).model_fields.items():
            if name in ("null_rate", "unique", "omit_rate", "omit_when", "gen"):
                continue
            part = field.alias or name
            bind(
                getattr(node, name),
                path + (part,),
                locate,
                overwrite=overwrite,
                text=f"{text}.{part}",
            )
    elif isinstance(node, Mapping):
        for key, item in node.items():
            bind(
                item,
                path + (key,),
                locate,
                overwrite=overwrite,
                text=f"{text}.{_segment(key)}",
            )
    elif isinstance(node, (list, tuple)):
        for index, item in enumerate(node):
            bind(
                item,
                path + (index,),
                locate,
                overwrite=overwrite,
                text=f"{text}.{index}",
            )


def generator(spec: Any) -> Any:
    """Validates one generator written as a mapping, such as `{"gen": "uniform",
    "low": 2, "high": 200}`, and gives the typed object.

    Pass the result where a builder method takes a value, such as `attributes` of
    `add_arrival`, `items` of `add_store`, `set` of a scenario step, or the `value`
    of an `on` action. The place gives it its random sequence when it is added.
    A `row` is read from the working directory when the place is added, and a missing
    file raises `ValueError` there.

    Args:
        spec: The generator as a mapping.

    Raises:
        pydantic.ValidationError: If the generator is not valid.
    """
    return generator_adapter().validate_python(spec)


def _runtime(context: Any) -> Any:
    """The value runtime of a context, or a `ValueError` that says there is none."""
    values = getattr(context, "values", None)
    if values is None:
        raise ValueError(
            "a Template renders in a run: pass the SimulationContext of a started run"
        )
    return values


# How many unnamed templates the process has made: each takes a first key from it
_made = 0


def _next_serial() -> int:
    """The creation number of a new unnamed template, counted from 1 by the process."""
    global _made
    _made += 1
    return _made


class TemplateNumbers:
    """The keys of the unnamed templates that one model's runs have rendered.

    The first run that renders an unnamed template gives it the key
    `template<n>`, where `n` counts the unnamed templates rendered so far. For one
    model and seed the order of first renders is the same in every process, so the
    keys, and with them the random sequences, do not depend on what else the
    process built or ran before. Two unnamed templates of one model never share a
    key. A template that two models share keeps the key the first gave it, and can
    then share it with an unnamed template of the second model: give it a `name`.
    """

    def __init__(self) -> None:
        self._seen: "weakref.WeakSet[Template]" = weakref.WeakSet()
        self._taken: Set[str] = set()

    def number(self, template: "Template") -> None:
        """Gives an unnamed template its key, once, and keeps it from the next one.

        A template that an earlier run or another model numbered keeps its key.

        Args:
            template: The template being rendered.
        """
        if template.name or template in self._seen:
            return
        self._seen.add(template)
        if not template._numbered:
            n = len(self._taken) + 1
            while f"template{n}" in self._taken:
                n += 1
            template._take_key(f"template{n}")
        self._taken.add(template.key)


_NUMBERS_OF: "weakref.WeakKeyDictionary[Any, TemplateNumbers]" = (
    weakref.WeakKeyDictionary()
)


def numbers_of(context: Any) -> TemplateNumbers:
    """The numbering that all runs of a context share; a run with no context has its own.

    Args:
        context: The context the run belongs to, or None.

    Returns:
        TemplateNumbers: The context's numbering, or a new one for a run with no
            context or a context that cannot be a mapping key.
    """
    try:
        return _NUMBERS_OF.setdefault(context, TemplateNumbers())
    except TypeError:
        return TemplateNumbers()


def _key_of(name: Optional[str], serial: Optional[int]) -> str:
    """The key a template's random sequences are named by: its name, or `template<n>`
    for the `n`-th unnamed template. A name that looks like an unnamed key gets
    `named:` in front, so no name takes the key of an unnamed template.
    """
    if not name:
        return f"template{serial}"
    return f"named:{name}" if _UNNAMED_LOOKALIKE.fullmatch(name) else name


class Template:
    """A payload of generated values, for a builder or low-level script.

    Return it from a task function and the task renders it when the finished event is
    published, or call `render` in your own code. It is the same mapping a blueprint's
    `payload` takes: a mapping whose values may be generators (`{"gen": "uniform",
    "low": 2, "high": 200}`), or one generator that gives a mapping or null. A null
    gives an event whose value is null, which a Kafka sink with `envelope` off writes
    as a tombstone.

    Example:
        ```python
        order = Template(
            {"order_id": {"gen": "uuid"}, "amount": {"gen": "uniform", "low": 2, "high": 200}},
            name="place_order",
        )

        @app.task(service_id="packing", resource_id="packers")
        def place_order(task_id):
            return order
        ```

    Attributes:
        name (Optional[str]): What the template is called; it names the random
            sequences of its generators, so give two templates different names. A name
            with dots is a path: `tasks.wash` gives its generators the random
            sequences of a blueprint's `tasks.wash.payload`. A name with a dot and a
            section word can share a sequence and a counter with a key nested in
            another element.
        payload: The validated mapping.
        vars: The named values a `vars` key would hold, evaluated before the payload.
        vars_once: The same, evaluated once for each entity.
        uses_previous (bool): Whether a generator in it is a `previous`, so the task
            that renders it keeps its last payload.
        looks_up (Set[str]): The tasks a `lookup` in it names. A task that renders it
            does not go on before each of them has a row.
    """

    def __init__(
        self,
        payload: Any,
        name: Optional[str] = None,
        vars: Optional[Mapping[str, Any]] = None,
        vars_once: Optional[Mapping[str, Any]] = None,
        check: bool = True,
    ):
        """
        Args:
            payload: A mapping, or one generator that gives a mapping or null.
            name: What the template is called.
            vars: Named values, each seeing the ones before it by name.
            vars_once: Named values made once for each entity.
            check: Whether to check the generators now, such as that a `row` file
                exists; a blueprint has checked them when it was loaded.

        Raises:
            pydantic.ValidationError: If a generator or a mapping is not valid.
            ValueError: If a check finds a problem, such as a `row` file that does not
                exist (a relative path is read from the working directory).
        """
        # An unnamed template starts with a key from the process's creation order, so
        # two never share a random sequence or a counter before a run renders them.
        # The first run that renders it
        # numbers it by the order of renders (`TemplateNumbers`), so two builds of one
        # code give one key.
        self.name = name
        self._numbered = bool(name)
        self.key: str = _key_of(name, None if name else _next_serial())
        # A name is a path: "tasks.wash" gives the generators the places a blueprint's
        # `tasks.wash.payload` has, and so the same random sequences
        base = tuple(self.key.split("."))
        self.payload: Any = payload_adapter().validate_python(payload)
        self.vars: Dict[str, Any] = values_adapter().validate_python(dict(vars or {}))
        self.vars_once: Dict[str, Any] = values_adapter().validate_python(
            dict(vars_once or {})
        )
        bind(self.payload, base + ("payload",), overwrite=False)
        bind(self.vars, base + ("vars",), overwrite=False)
        bind(self.vars_once, base + ("vars_once",), overwrite=False)
        # Whether a task that renders this template keeps its last payload for it
        self.uses_previous: bool = any(
            True
            for place in (self.payload, self.vars, self.vars_once)
            for _ in generators_in(place, (Previous,))
        )
        self.looks_up: Set[str] = {
            node.task
            for place in (self.payload, self.vars, self.vars_once)
            for _, node in generators_in(place, (Lookup,))
        }
        if check:
            self._check(base)

    def _take_key(self, key: str) -> None:
        """Gives an unnamed template its key and binds its generators to it, so their
        random sequences and counters do not depend on how many templates the process
        made before.

        Args:
            key: The key, `template<n>`.
        """
        self._numbered = True
        if key == self.key:
            return
        self.key = key
        base = (key,)
        bind(self.payload, base + ("payload",), overwrite=True)
        bind(self.vars, base + ("vars",), overwrite=True)
        bind(self.vars_once, base + ("vars_once",), overwrite=True)

    def _check(self, base: Tuple[str, ...]) -> None:
        problems: List[str] = []
        visible = set(self.vars) | set(self.vars_once)
        # The vars a task gives a template are known only when it runs
        place = Place(in_task=True, base_dir=Path.cwd(), vars_known=False)
        for label, node in (
            ("payload", self.payload),
            ("vars", self.vars),
            ("vars_once", self.vars_once),
        ):
            for path, message in check_value(
                node,
                base + (label,),
                visible,
                place,
                mapping_field=False,
            ):
                problems.append(f"{'.'.join(str(p) for p in path)}: {message}")
        if problems:
            raise ValueError("; ".join(problems))

    def render(self, context: Any, entity: Any = None) -> Optional[Dict[str, Any]]:
        """Evaluates the template at the simulation time now.

        Args:
            context: The `SimulationContext` of the run.
            entity: The entity whose attributes `attr` reads; none by default.

        Returns:
            Optional[Dict[str, Any]]: The mapping, as plain Python data, or None when
            the payload is one generator that gave null.
        """
        return _runtime(context).render(self, entity)

    def bind(self, entity: Any = None, **fields: Any) -> "BoundPayload":
        """The template with an entity and fixed fields, rendered by the task that
        publishes it. `fields` are written over the generated ones.

        Args:
            entity: The entity whose attributes are read, or None.
            **fields: Fields written over the generated ones.
        """
        return BoundPayload(self, entity, fields)


class BoundPayload:
    """A `Template` bound to an entity and to fields that overwrite generated ones,
    such as the `id_field` of a task. A task renders it where it would publish a
    plain payload.

    Attributes:
        template (Template): The template.
        entity: The entity whose attributes `attr` reads, or None for the task's own.
        fields (Dict[str, Any]): Fields written over the generated mapping.
        attributes (List[str]): Names of entity attributes written over the mapping,
            each under its own name, read when the payload is built.
    """

    def __init__(
        self,
        template: Template,
        entity: Any = None,
        fields: Optional[Mapping[str, Any]] = None,
        attributes: Optional[List[str]] = None,
    ):
        self.template = template
        self.entity = entity
        self.fields: Dict[str, Any] = dict(fields or {})
        self.attributes: List[str] = list(attributes or [])

    def render(self, context: Any) -> Optional[Dict[str, Any]]:
        """Evaluates the template, then writes `fields` over it.

        Args:
            context: The simulation context that holds the value runtime.

        Returns:
            Optional[Dict[str, Any]]: The mapping, or None when the template gave
            null and there are no fields or attributes to write over it.

        Raises:
            ValueGenerationError: If the template gives something other than a
                mapping or null, or gives null while there are fields or attributes
                to write over it.
        """
        values = _runtime(context)
        return values.render(
            self.template, self.entity, extras=self.fields, attributes=self.attributes
        )


def _expand_values(values: Tuple[Any, ...]) -> Iterator[Any]:
    """The values to search, with each template opened: a `Template` gives its
    `payload`, `vars` and `vars_once`, a `BoundPayload` its template and its fields.

    Raises:
        TypeError: If a value is a set or an iterator, which are not searched.
    """
    for value in values:
        if isinstance(value, Template):
            yield value.payload
            yield value.vars
            yield value.vars_once
        elif isinstance(value, BoundPayload):
            yield from _expand_values((value.template, value.fields))
        elif isinstance(value, (set, frozenset, Iterator)):
            # Neither is searched by `generators_in`, so a lookup inside would be
            # missed with no message
            raise TypeError(
                f"a place for note_values is a generator, a mapping, a list, a "
                f"constant or a Template, not {type(value).__name__}"
            )
        else:
            yield value


def looked_up(*values: Any) -> Tuple[Set[str], Set[str], bool]:
    """What some values read from other records.

    Args:
        values: Generators, mappings, lists, constants, or a `Template` (its
            `payload`, `vars` and `vars_once` are searched).

    Returns:
        Tuple[Set[str], Set[str], bool]: The tasks a `lookup` names, the tasks a
            `lookup` or `aggregate` names, and whether a `previous` is there.

    Raises:
        TypeError: If a value is a set or an iterator, which are not searched.
    """
    gates: Set[str] = set()
    rows: Set[str] = set()
    previous = False
    for value in _expand_values(values):
        for _, node in generators_in(value, (Lookup, Aggregate, Previous)):
            if isinstance(node, Previous):
                previous = True
                continue
            rows.add(node.task)
            if isinstance(node, Lookup):
                gates.add(node.task)
    return gates, rows, previous


__all__: List[str] = ["Template", "BoundPayload", "bind", "generator"]
