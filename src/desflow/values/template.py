"""Generated values for the builder and low-level APIs: `Template` and `BoundPayload`.

A `Template` holds the same mapping of generators a blueprint's `payload` does, and
validates it with the same models, so a mistake raises Pydantic's `ValidationError`
(without a file or a line).
"""

import itertools
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple

from pydantic import BaseModel

from desflow.values.check import generators_in
from desflow.values.models import (
    Lookup,
    Previous,
    _Gen,
    generator_adapter,
    payload_adapter,
    values_adapter,
)

_PATH = Tuple[Any, ...]


def bind(
    node: Any,
    path: _PATH,
    locate: Optional[Callable[[_PATH], str]] = None,
    *,
    overwrite: bool = True,
) -> None:
    """Gives every generator under `node` its place in the file.

    The place is a dotted path, which names the generator's random sequence, and a
    "file:line: path" text for the messages of a failure during the run.

    Args:
        node: A value: a generator, a mapping, a list, or a constant.
        path: Where `node` is written, from the top of the file.
        locate: Gives the "file:line: path" text for a path; without it the dotted
            path alone is used.
        overwrite: Whether a generator that already has a place gets a new one.
    """
    if isinstance(node, BaseModel):
        if isinstance(node, _Gen) and (overwrite or not node._path):
            node._path = ".".join(str(part) for part in path)
            node._where = locate(path) if locate else node._path
        for name, field in type(node).model_fields.items():
            if name in ("null_rate", "unique", "omit_rate", "omit_when", "gen"):
                continue
            bind(
                getattr(node, name),
                path + (field.alias or name,),
                locate,
                overwrite=overwrite,
            )
    elif isinstance(node, Mapping):
        for key, item in node.items():
            bind(item, path + (key,), locate, overwrite=overwrite)
    elif isinstance(node, (list, tuple)):
        for index, item in enumerate(node):
            bind(item, path + (index,), locate, overwrite=overwrite)


def generator(spec: Any) -> Any:
    """Validates one generator written as a mapping, such as `{"gen": "uniform",
    "low": 2, "high": 200}`, and gives the typed object.

    Pass the result where a builder method takes a value, such as `attributes` of
    `add_arrival`, `items` of `add_store`, `set` of a scenario step, or the `value`
    of an `on` action. The place gives it its random sequence when it is added.

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


_UNNAMED = itertools.count(1)


class Template:
    """A payload of generated values, for a builder or low-level script.

    Return it from a task function and the task renders it when the finished event is
    published, or call `render` in your own code. It is the same mapping a blueprint's
    `payload` takes: a mapping whose values may be generators (`{"gen": "uniform",
    "low": 2, "high": 200}`), or one generator that gives a mapping.

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
            sequences of its generators, so give two templates different names.
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
            payload: A mapping, or one generator that gives a mapping.
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
        # An unnamed template is numbered in creation order, so two never share a
        # random sequence or a counter
        self.name = name
        self.key: str = name or f"template{next(_UNNAMED)}"
        base = (self.key,)
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

    def _check(self, base: Tuple[str, ...]) -> None:
        from desflow.values.check import check_value

        problems: List[str] = []
        visible = set(self.vars) | set(self.vars_once)
        for label, node in (
            ("payload", self.payload),
            ("vars", self.vars),
            ("vars_once", self.vars_once),
        ):
            for path, message in check_value(
                node,
                base + (label,),
                visible,
                in_task=True,
                base_dir=Path.cwd(),
                mapping_field=False,
            ):
                if "is not a var that has a value here" in message:
                    # The vars a task gives a template are known only when it runs
                    continue
                problems.append(f"{'.'.join(str(p) for p in path)}: {message}")
        if problems:
            raise ValueError("; ".join(problems))

    def render(self, context: Any, entity: Any = None) -> Dict[str, Any]:
        """Evaluates the template at the simulation time now.

        Args:
            context: The `SimulationContext` of the run.
            entity: The entity whose attributes `attr` reads; none by default.

        Returns:
            Dict[str, Any]: The mapping, as plain Python data.
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

    def render(self, context: Any) -> Dict[str, Any]:
        """Evaluates the template, then writes `fields` over it.

        Args:
            context: The simulation context that holds the value runtime.
        """
        values = _runtime(context)
        return values.render(
            self.template, self.entity, extras=self.fields, attributes=self.attributes
        )


__all__: List[str] = ["Template", "BoundPayload", "bind", "generator"]
