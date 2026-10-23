"""Write the blueprint JSON Schema that ships in the package.

The schema describes a blueprint file in the form it is written in YAML, so an editor
can complete and check keys as you type. It is made from the Pydantic `Blueprint` model
in `src/desflow/blueprint/models.py` and committed as
`src/desflow/blueprint/blueprint.schema.json`. The package only reads that file; it
never calls `model_json_schema`, so it does not depend on the Pydantic version that
wrote it.

Plain `Blueprint.model_json_schema()` cannot be used as it is, for four reasons:

- It raises on the callable fields (`Task.payload`, `Process.function`,
  `Telemetry.function`, `Run.before`). Here a callable, and a `Type[...]` field, is a
  `!python` reference: a string such as `module.attribute`.
- A `before` validator widens what YAML may write beyond the field's type (a duration
  such as `10 min` for a number of seconds), and the schema does not see it. The table
  `INPUT_FORMS` says, for each validator, what it adds. A test fails when a model gets
  a `before` or `wrap` validator that is not in the table.
- `${VAR}` and `${VAR:-default}` are replaced before YAML reads the value, so an editor
  that reads the file as written must accept the placeholder where a number, a boolean
  or a fixed name is expected.
- `after` of a spawn or fire action, `Stats.level` and `on_interrupt` are plain strings
  in the model and checked by a validator, so `_tighten` gives them the duration form
  and the enums the validators use.
- `Connector.type` accepts any string in the model, and `build.py` refuses an unknown
  name. The schema lists the names `build.py` knows.

Usage: uv run python scripts/gen_schema.py
"""

import json
import tomllib
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple, Type

from pydantic import BaseModel
from pydantic.json_schema import GenerateJsonSchema

from desflow.blueprint.build import EGRESS_TYPES, INGRESS_TYPES
from desflow.blueprint.models import Blueprint, _Model
from desflow.config import INTERRUPT_POLICIES, STATS_LEVELS
from desflow.utils import MULTIPLIERS

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = ROOT / "src" / "desflow" / "blueprint" / "blueprint.schema.json"
SCHEMA_ID = "https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json"
DIALECT = "https://json-schema.org/draft/2020-12/schema"


def version() -> str:
    """The package version, read from pyproject.toml so it is the source's version."""
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    return str(tomllib.loads(text)["project"]["version"])


def _letters(text: str) -> str:
    """`min` as `[mM][iI][nN]`: a unit is read in lower case, so any case is valid."""
    return "".join(f"[{c}{c.upper()}]" if c.isalpha() else c for c in text)


_UNITS = "|".join(_letters(unit) for unit in MULTIPLIERS)
_DURATION_BODY = rf"\d+(?:\.\d+)?\s*(?:{_UNITS})"

# The shared forms are written once, in `$defs`, and every field points at them
PYTHON_REF: Dict[str, Any] = {"$ref": "#/$defs/PythonReference"}
PLACEHOLDER: Dict[str, Any] = {"$ref": "#/$defs/Placeholder"}
DURATION: Dict[str, Any] = {"$ref": "#/$defs/Duration"}
INSTANT: Dict[str, Any] = {"$ref": "#/$defs/Time"}

# An ISO date, or a date and a time with an optional zone, as the models read it
_ISO_DATETIME = (
    r"^\s*\d{4}-\d{2}-\d{2}(?:[Tt ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?"
    r"(?:[Zz]|[+-]\d{2}(?::?\d{2})?)?)?\s*$"
)

SHARED_DEFS: Dict[str, Any] = {
    # What the loader turns into a function, a class or an object
    "PythonReference": {
        "type": "string",
        "pattern": r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$",
        "title": "!python reference",
        "description": "A !python reference such as module.attribute",
    },
    # `${VAR}` or `${VAR:-default}` in a string: replaced before YAML reads the value
    "Placeholder": {
        "type": "string",
        "pattern": r"^.*\$\{[A-Za-z_][A-Za-z0-9_]*(:-[^}]*)?\}.*$",
        "title": "Placeholder",
        "description": (
            "A ${VAR} or ${VAR:-default} reference, replaced when the file is loaded"
        ),
    },
    "Duration": {
        "type": "string",
        "pattern": rf"^\s*{_DURATION_BODY}\s*$",
        "title": "Duration",
        "description": "A number and a unit, such as 10 min or 2.5 h",
    },
    "Time": {
        "title": "Time",
        "description": "now, a signed duration from now such as -1d, or an ISO datetime",
        "anyOf": [
            {"type": "string", "pattern": rf"^\s*{_letters('now')}\s*$"},
            {"type": "string", "pattern": rf"^\s*[+-]\s*{_DURATION_BODY}\s*$"},
            {"type": "string", "pattern": _ISO_DATETIME},
            PLACEHOLDER,
        ],
    },
}

PATIENCE: Dict[str, Any] = {
    "type": "string",
    "title": "Duration or expression",
    "description": "A duration such as 30 s, or an expression such as attr.limit",
}

SIGNAL_NAMES: Dict[str, Any] = {
    "type": "array",
    "items": {"type": "string"},
    "title": "list of names",
    "description": "The names of signals that need no options",
}

STATS_LEVEL: Dict[str, Any] = {
    "enum": list(STATS_LEVELS),
    "title": "level name",
    "description": "A statistics level alone, with the default interval",
}

DURATION_ITEMS = "duration items"
PROCESS_REFS = "process references"

# What each `before` or `wrap` validator adds to the schema, by the class that defines
# it and the function's name. A form is the schema added to the field with `anyOf`,
# `DURATION_ITEMS` and `PROCESS_REFS` which add to the items of a list, or None for a
# validator that narrows or only checks, so the field's type already says what it takes.
INPUT_FORMS: Dict[Tuple[str, str], Any] = {
    ("Simulation", "_parse_instant"): INSTANT,
    ("Simulation", "_parse_duration"): DURATION,
    ("Distribution", "_duration"): DURATION,
    ("Distribution", "_durations"): DURATION_ITEMS,
    ("SpawnAction", "_after_duration"): DURATION,
    ("FireAction", "_after_duration"): DURATION,
    ("Task", "_patience_duration"): PATIENCE,
    ("Stats", "_parse_interval"): DURATION,
    ("ScenarioStep", "_parse_at"): DURATION,
    ("Drain", "_parse_limit"): DURATION,
    ("Run", "_parse_until"): DURATION,
    ("Blueprint", "_signal_mapping"): SIGNAL_NAMES,
    ("Blueprint", "_bare_level"): STATS_LEVEL,
    ("Blueprint", "_bare_functions"): PROCESS_REFS,
    # Narrowing validators and the wait parser, which returns the value unchanged
    ("TransferStep", "_priority_is_a_literal"): None,
    ("Task", "_wait_parses"): None,
    ("EgressConnector", "_known_name"): None,
}

_OUTSIDE = ("default", "description", "title")


class _Schema(GenerateJsonSchema):
    """Writes a callable, a class or an instance type as a !python reference, and
    leaves out the title Pydantic makes from every field name."""

    def callable_schema(self, schema: Any) -> Dict[str, Any]:
        return dict(SHARED_DEFS["PythonReference"])

    def is_subclass_schema(self, schema: Any) -> Dict[str, Any]:
        return dict(SHARED_DEFS["PythonReference"])

    def is_instance_schema(self, schema: Any) -> Dict[str, Any]:
        return dict(SHARED_DEFS["PythonReference"])

    def field_title_should_be_set(self, schema: Any) -> bool:
        return False


def model_classes() -> Dict[str, Type[BaseModel]]:
    """Every blueprint model, by class name: the subclasses of `_Model` and `_Model`'s
    own subclasses in turn, with `Blueprint`."""
    found: Dict[str, Type[BaseModel]] = {}
    pending: List[Type[BaseModel]] = [_Model]
    while pending:
        for sub in pending.pop().__subclasses__():
            if sub.__module__ == Blueprint.__module__ and sub.__name__ not in found:
                found[sub.__name__] = sub
                pending.append(sub)
    return found


def validators(model: Type[BaseModel]) -> Iterator[Tuple[str, str, Any]]:
    """The `before` and `wrap` field validators a model has, as the class that defines
    each, its function name and the validator's info."""
    for name, decorator in model.__pydantic_decorators__.field_validators.items():
        if decorator.info.mode in ("before", "wrap"):
            owner = decorator.func.__qualname__.split(".")[0]
            yield owner, name, decorator.info


def _widen(prop: Dict[str, Any], form: Dict[str, Any]) -> None:
    """Adds `form` as one more thing the property accepts."""
    if "anyOf" in prop:
        prop["anyOf"].append(dict(form))
        return
    inner = {k: v for k, v in prop.items() if k not in _OUTSIDE}
    for key in inner:
        del prop[key]
    prop["anyOf"] = [inner, dict(form)]


def _apply_input_forms(schema: Dict[str, Any]) -> None:
    classes = model_classes()
    defs = schema["$defs"]
    for name, model in classes.items():
        node = schema if name == "Blueprint" else defs.get(name)
        if node is None:
            continue
        for owner, function, info in validators(model):
            form = INPUT_FORMS[(owner, function)]
            if form is None:
                continue
            for field in info.fields:
                alias = model.model_fields[field].alias or field
                prop = node["properties"][alias]
                if form == DURATION_ITEMS:
                    lists = [alt for alt in prop.get("anyOf", [prop]) if "items" in alt]
                    assert lists, f"{name}.{field} has no list to widen"
                    for alt in lists:
                        _widen(alt["items"], DURATION)
                elif form == PROCESS_REFS:
                    _widen(prop["items"], PYTHON_REF)
                else:
                    _widen(prop, form)
                    if form == INSTANT:
                        # The Time form reads an ISO datetime more exactly than the
                        # field's own `format: date-time`, which editors do not check
                        prop["anyOf"] = [
                            alt for alt in prop["anyOf"] if "format" not in alt
                        ]


def _connector_names(schema: Dict[str, Any]) -> None:
    """`Connector.type` and `EgressConnector.type` list the names `build.py` knows."""
    for name, types in (
        ("Connector", INGRESS_TYPES),
        ("EgressConnector", EGRESS_TYPES),
    ):
        prop = schema["$defs"][name]["properties"]["type"]
        prop["anyOf"] = [
            {"enum": list(types), "title": "short name"},
            dict(PYTHON_REF),
        ]


def _tighten(schema: Dict[str, Any]) -> None:
    """Narrows three fields whose model type is a plain string but whose validator
    refuses most strings: the `after` of a spawn or fire action must be a duration,
    `Stats.level` one of the levels, and `on_interrupt` one of the policies."""
    defs = schema["$defs"]
    for name in ("SpawnAction", "FireAction"):
        prop = defs[name]["properties"]["after"]
        prop["anyOf"] = [alt for alt in prop["anyOf"] if alt != {"type": "string"}]
        for alt in prop["anyOf"]:
            if alt == {"type": "number"}:
                alt["exclusiveMinimum"] = 0
    defs["Stats"]["properties"]["level"] = {"enum": list(STATS_LEVELS)}
    defs["Task"]["properties"]["on_interrupt"] = {
        "enum": list(INTERRUPT_POLICIES),
        "default": "resume",
    }


def _share(node: Any) -> Any:
    """Points every written-out !python reference at the shared definition."""
    if isinstance(node, list):
        return [_share(item) for item in node]
    if not isinstance(node, dict):
        return node
    if node == SHARED_DEFS["PythonReference"]:
        return dict(PYTHON_REF)
    return {
        key: value if key in ("default", "enum", "const") else _share(value)
        for key, value in node.items()
    }


def _is_leaf(node: Any) -> bool:
    if not isinstance(node, dict) or "anyOf" in node:
        return False
    kind = node.get("type")
    return (isinstance(kind, str) and kind in ("number", "integer", "boolean")) or (
        "enum" in node or "const" in node
    )


def _placeholders(node: Any) -> Any:
    """Lets every number, integer, boolean and fixed-name leaf take a placeholder."""
    if isinstance(node, list):
        return [_placeholders(item) for item in node]
    if not isinstance(node, dict):
        return node
    if _is_leaf(node):
        inner = {k: v for k, v in node.items() if k not in _OUTSIDE}
        wrapped = {k: v for k, v in node.items() if k in _OUTSIDE}
        wrapped["anyOf"] = [inner, dict(PLACEHOLDER)]
        return wrapped
    out: Dict[str, Any] = {}
    for key, value in node.items():
        if key == "properties":
            # The keys here are field names, such as `default`, not schema keywords
            out[key] = {name: _placeholders(sub) for name, sub in value.items()}
        elif key == "anyOf":
            # A leaf among the alternatives is one more alternative, plus one placeholder
            alternatives = [
                alt if _is_leaf(alt) else _placeholders(alt) for alt in value
            ]
            if any(_is_leaf(alt) for alt in value):
                alternatives.append(dict(PLACEHOLDER))
            out[key] = alternatives
        elif key in ("default", "enum", "const", "examples", "required"):
            out[key] = value
        else:
            out[key] = _placeholders(value)
    return out


def schema() -> Dict[str, Any]:
    """The blueprint JSON Schema, as a dict."""
    raw = Blueprint.model_json_schema(schema_generator=_Schema, mode="validation")
    _apply_input_forms(raw)
    _connector_names(raw)
    _tighten(raw)
    body = _placeholders(_share(raw))
    body.pop("title", None)
    body.pop("description", None)
    body["$defs"] = {**body["$defs"], **SHARED_DEFS}
    # Reads from the top: what the file is, then its keys, then the shared parts
    body = {
        key: body[key]
        for key in ("type", "additionalProperties", "required", "properties", "$defs")
        if key in body
    }
    header: Dict[str, Any] = {
        "$schema": DIALECT,
        "$id": SCHEMA_ID,
        "title": "desflow blueprint",
        "description": (
            f"The keys a desflow blueprint file takes, for desflow {version()}."
        ),
    }
    return {**header, **body}


def text() -> str:
    """The schema as the committed file holds it."""
    return json.dumps(schema(), indent=2, sort_keys=False, allow_nan=False) + "\n"


def main() -> None:
    """Writes the schema into the package."""
    SCHEMA_FILE.write_text(text(), encoding="utf-8")
    print(f"Wrote {SCHEMA_FILE.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
