"""The blueprint JSON Schema agrees with the models and with every blueprint file.

`scripts/gen_schema.py` writes `src/desflow/blueprint/blueprint.schema.json` from the
Pydantic models. These tests check that the committed file is current, that every
blueprint in the repository passes it, that a misspelt key is refused at every level,
and that the forms the models accept before validating are all in the schema.
"""

import datetime
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import yaml
from jsonschema import Draft202012Validator

from desflow.blueprint import models
from desflow.blueprint.loader import BlueprintLoader, _keep_on_keys, substitute
from desflow.blueprint.schema import schema_text

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = json.loads(schema_text())

# Files in the blueprint folders that are not blueprints: experiment files, fitted
# settings that a blueprint includes, and the settings `desflow fit` writes.
NOT_BLUEPRINTS = {
    "examples/yaml/experiment.yaml",
    "examples/yaml/packing_fit.yaml",
    "docs/snippets/yaml/parameter_sweeps_experiment.yaml",
    "docs/snippets/yaml/comparing_designs_experiment.yaml",
    "docs/snippets/yaml/checking_a_model_experiment.yaml",
    "docs/snippets/yaml/complete_model_experiment.yaml",
    "docs/snippets/yaml/fitted/orders.yaml",
    "docs/snippets/yaml/fitted/packing.yaml",
    "docs/snippets/yaml/fitted/packing_gamma.yaml",
    "docs/snippets/yaml/fitted/packing_times.yaml",
    "docs/snippets/yaml/fitted/complete_orders.yaml",
    "docs/snippets/yaml/fitted/complete_picking.yaml",
    "docs/snippets/yaml/fitted/complete_packing.yaml",
}
FOLDERS = ("examples/yaml", "tests/conformance/blueprints", "docs/snippets/yaml")


def _generator():
    spec = importlib.util.spec_from_file_location(
        "gen_schema", ROOT / "scripts" / "gen_schema.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["gen_schema"] = module
    spec.loader.exec_module(module)
    return module


class _TextLoader(BlueprintLoader):
    """Reads a file as it is written: `!python` stays its reference text and `${VAR}`
    stays in place, so the schema sees what the author typed."""


def _python_text(loader, node):
    return loader.construct_scalar(node)


def _plain_str(loader, node):
    return loader.construct_scalar(node)


def _include(loader, node):
    extra: Dict[str, Any] = {}
    if isinstance(node, yaml.MappingNode):
        extra = dict(loader.construct_mapping(node, deep=True))
        name = extra.pop("file")
    else:
        # The path is substituted, as the loader does, so the file can be read
        name = substitute(loader.construct_scalar(node))
    return {
        **_read(
            Path(loader.name).parent / name, getattr(loader, "includes", {}).get(node)
        ),
        **extra,
    }


_TextLoader.add_constructor("!python", _python_text)
_TextLoader.add_constructor("tag:yaml.org,2002:str", _plain_str)
_TextLoader.add_constructor("!include", _include)


def _read(path: Path, at: Optional[str] = None) -> Dict[str, Any]:
    loader = _TextLoader(path.read_text(encoding="utf-8"))
    loader.name = str(path)
    try:
        node = loader.get_single_node()
        assert node is not None
        includes: Dict[Any, str] = {}
        _keep_on_keys(node, includes, at)
        setattr(loader, "includes", includes)
        return loader.construct_document(node)
    finally:
        loader.dispose()


def _plain(value: Any) -> Any:
    """JSON-compatible: an unquoted YAML timestamp becomes the ISO text it was."""
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    return value


def _files() -> Iterator[Path]:
    for folder in FOLDERS:
        yield from sorted((ROOT / folder).rglob("*.yaml"))


def _relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def test_committed_schema_is_current():
    assert _generator().text() == schema_text(), (
        "The committed schema is out of date; run `uv run python scripts/gen_schema.py`"
    )


def test_every_blueprint_matches_the_schema():
    validator = Draft202012Validator(SCHEMA)
    blueprints = 0
    for path in _files():
        data = _plain(_read(path))
        listed = _relative(path) in NOT_BLUEPRINTS
        assert ("simulation" in data) != listed, (
            f"{_relative(path)}: a file with a simulation key is a blueprint, and "
            f"one without is listed in NOT_BLUEPRINTS"
        )
        if listed:
            continue
        # A fitted file is read as part of the blueprint that includes it
        errors = sorted(validator.iter_errors(data), key=lambda e: list(e.path))
        assert not errors, (
            f"{_relative(path)}: {'/'.join(map(str, errors[0].path))}: "
            f"{errors[0].message[:200]}"
        )
        blueprints += 1
    assert blueprints > 100


def _nodes(value: Any, path: tuple = ()) -> Iterator[tuple]:
    """Every mapping in a document, with its path."""
    if isinstance(value, dict):
        yield path, value
        for key, item in value.items():
            yield from _nodes(item, (*path, key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _nodes(item, (*path, index))


def _walk(error) -> Iterator[Any]:
    yield error
    for inner in error.context or ():
        yield from _walk(inner)


def _at(data: Any, path: tuple) -> Any:
    for step in path:
        data = data[step]
    return data


def _model_errors(data: Any) -> set:
    """What the models report for `data`, as (type, location) pairs."""
    try:
        models.Blueprint.model_validate(data)
    except ValueError as exc:
        return {(item["type"], tuple(item["loc"])) for item in exc.errors()}  # type: ignore[attr-defined]
    return set()


def _same_place(loc: tuple, here: tuple) -> bool:
    """A model error is at the place of a new key when it is at or inside the place, or
    at the `wait` or `subscribe` node above it, which the models parse as a whole."""
    if loc[: len(here)] == here:
        return True
    return here[: len(loc)] == loc and loc[-1:] in (("wait",), ("subscribe",))


def test_unknown_key_is_refused_at_every_level():
    validator = Draft202012Validator(SCHEMA)
    defs = SCHEMA["$defs"]
    modelled = {name for name, node in defs.items() if "properties" in node}
    reached = set()
    for path in _files():
        if _relative(path) in NOT_BLUEPRINTS:
            continue
        data = _plain(_read(path))
        before = _model_errors(data)
        for where, node in list(_nodes(data)):
            if "not_a_key" in node:
                continue
            node["not_a_key"] = 1
            try:
                refusals = [
                    inner
                    for error in validator.iter_errors(data)
                    for inner in _walk(error)
                    if inner.validator == "additionalProperties"
                    and tuple(inner.absolute_path) == where
                    and "not_a_key" in inner.message
                ]
                # The model reports a new error at that place or inside it: at the key
                # for most nodes, and at the node itself for a `wait` or `subscribe`
                # (`next: {when: [...]}` is read as `next.rules.when` by the model)
                here = tuple(step for step in where if step != "rules")
                models_refuse = any(
                    _same_place(tuple(step for step in loc if step != "rules"), here)
                    for _, loc in _model_errors(data) - before
                )
            finally:
                del node["not_a_key"]
            for refusal in refusals:
                for name, definition in defs.items():
                    if refusal.schema == definition and "properties" in definition:
                        reached.add(name)
                if refusal.schema == SCHEMA:
                    reached.add("<top>")
            # The models read the text as typed, so a `${VAR}` where they parse an
            # expression (a wait, a patience, an amount) is an error of its own;
            # that place is checked by the schema only
            unreadable = any(where[: len(loc)] == loc for _, loc in before if loc)
            if refusals and not unreadable:
                assert models_refuse, (
                    f"{_relative(path)} at {where}: the schema refuses an unknown "
                    f"key but the model does not"
                )
    assert reached - {"<top>"} == modelled, sorted(modelled - reached)
    assert "<top>" in reached


def _model_accepts(data: Dict[str, Any]) -> bool:
    try:
        models.Blueprint.model_validate(data)
    except ValueError:
        return False
    return True


def _with(section: str, value: Any) -> Dict[str, Any]:
    """A minimal blueprint whose `section` holds `value`."""
    base: Dict[str, Any] = {"simulation": {"sim_id": "test"}}
    if section == "simulation":
        base["simulation"].update(value)
    elif section.startswith("services."):
        base["services"] = {"work": value}
    elif section == "*":  # several sections at once
        base.update(value)
    elif section == "egress":
        base["egress"] = [value]
    elif section == "ingress":
        base["ingress"] = [value]
    else:
        base[section] = value
    return base


# Each form the models read before they validate: (section, value, accepted). The
# schema and the models must give the same answer for each.
FORMS = [
    ("run", {"until": "10 min"}, True),
    ("run", {"until": 5}, True),
    ("run", {"until": "soon"}, False),
    ("simulation", {"warmup": "2 h"}, True),
    ("simulation", {"warmup": 30}, True),
    ("simulation", {"warmup": "2 parsecs"}, False),
    ("simulation", {"logical_start_time": "now"}, True),
    ("simulation", {"logical_start_time": "-1d"}, True),
    ("simulation", {"logical_start_time": "2026-01-01T00:00:00"}, True),
    ("simulation", {"logical_start_time": "yesterday"}, False),
    ("stats", "basic", True),
    ("stats", "loud", False),
    ("stats", {"level": "all", "interval": "5 min"}, True),
    ("stats", {"level": "all", "interval": "often"}, False),
    ("signals", ["go", "stop"], True),
    ("signals", [1], False),
    ("services.work", {"dist": "uniform", "low": "1 s", "high": "2 s"}, True),
    ("services.work", {"dist": "uniform", "low": "3 days", "high": "5 days"}, True),
    ("services.work", {"dist": "uniform", "low": "soon", "high": 2}, False),
    ("services.work", {"dist": "empirical", "values": ["1 s", 2.5]}, True),
    ("services.work", {"dist": "empirical", "values": ["1 s", "later"]}, False),
    ("egress", {"type": "Console"}, True),
    ("ingress", {"type": "Local"}, True),
    # The model and the schema both read a placeholder where a connector option goes
    ("egress", {"type": "Postgres", "config": {"port": "${PG_PORT:-5432}"}}, True),
    ("simulation", {"factor": [1]}, False),
    (
        "*",
        {
            "resources": {"r": {}},
            "tasks": {
                "t": {
                    "payload": {},
                    "service": "w",
                    "patience": "attr.limit",
                    "resource": "r",
                }
            },
        },
        True,
    ),
    (
        "*",
        {
            "resources": {"r": {}},
            "tasks": {
                "t": {
                    "payload": {},
                    "service": "w",
                    "resource": "r",
                    "on_interrupt": "abandon",
                }
            },
        },
        True,
    ),
    (
        "*",
        {
            "resources": {"r": {}},
            "tasks": {
                "t": {
                    "payload": {},
                    "service": "w",
                    "resource": "r",
                    "on_interrupt": "nope",
                }
            },
        },
        False,
    ),
    (
        "*",
        {
            "tasks": {
                "u": {"payload": {}},
                "t": {
                    "payload": {},
                    "on": [{"spawn": {"task": "u", "after": "5 min"}}],
                },
            }
        },
        True,
    ),
    (
        "*",
        {
            "tasks": {
                "u": {"payload": {}},
                "t": {"payload": {}, "on": [{"spawn": {"task": "u", "after": "soon"}}]},
            }
        },
        False,
    ),
    (
        "*",
        {
            "signals": {"s": {}},
            "tasks": {
                "t": {
                    "payload": {},
                    "on": [{"fire": {"signal": "s", "after": "5 min"}}],
                }
            },
        },
        True,
    ),
    (
        "*",
        {
            "signals": {"s": {}},
            "tasks": {
                "t": {"payload": {}, "on": [{"fire": {"signal": "s", "after": "soon"}}]}
            },
        },
        False,
    ),
    (
        "*",
        {
            "tasks": {
                "u": {"payload": {}},
                "t": {"payload": {}, "on": [{"spawn": {"task": "u", "after": 0}}]},
            }
        },
        False,
    ),
    (
        "*",
        {
            "signals": {"s": {}},
            "tasks": {
                "t": {"payload": {}, "on": [{"fire": {"signal": "s", "after": 0}}]}
            },
        },
        False,
    ),
    ("stats", {"level": "loud"}, False),
    ("processes", ["a.b"], True),
]


def _noop_process(context):
    yield context.env.timeout(1)


def test_schema_and_model_agree_on_forms():
    validator = Draft202012Validator(SCHEMA)
    for section, value, accepted in FORMS:
        data = _with(section, value)
        assert validator.is_valid(data) is accepted, f"schema: {section} {value}"
        if section == "processes":
            # The loader turns a `!python` reference into the function before the
            # model reads it; a bare one is the function alone
            data = _with(section, [_noop_process])
        assert _model_accepts(data) is accepted, f"model: {section} {value}"


def test_connector_names_are_those_build_knows():
    # `build.py` refuses an unknown short name (test_unknown_connector_type_names_the_line),
    # where the model accepts any string, because a `!python` class is also allowed
    validator = Draft202012Validator(SCHEMA)
    assert not validator.is_valid(_with("egress", {"type": "Nowhere"}))
    assert not validator.is_valid(_with("ingress", {"type": "Nowhere"}))
    assert not validator.is_valid(_with("egress", {"type": "kafka"}))  # wrong case
    assert validator.is_valid(_with("egress", {"type": "Kafka"}))
    assert validator.is_valid(_with("egress", {"type": "my_module.MyEgress"}))


def test_a_placeholder_passes_where_a_number_is_expected():
    validator = Draft202012Validator(SCHEMA)
    for section, value in [
        ("run", {"until": "${HORIZON:-5}"}),
        ("simulation", {"warmup": "${WARMUP:-30}"}),
        ("services.work", {"dist": "uniform", "low": "${LOW:-1}", "high": 2}),
        ("resources", {"packers": {"current_cap": "${CAP:-2}", "max_cap": 3}}),
        ("egress", {"type": "${SINK:-Console}"}),
        (
            "*",
            {
                "arrivals": {
                    "a": {
                        "replay": {"path": "x.csv", "fields": "${FIELDS:-all}"},
                        "spawn": "t",
                    }
                },
                "tasks": {"t": {"payload": {}}},
            },
        ),
    ]:
        assert validator.is_valid(_with(section, value)), f"{section} {value}"


def test_every_before_validator_is_listed():
    generator = _generator()
    seen = set()
    for model in generator.model_classes().values():
        for owner, name, _ in generator.validators(model):
            seen.add((owner, name))
    assert seen == set(generator.INPUT_FORMS), (
        sorted(seen - set(generator.INPUT_FORMS)),
        sorted(set(generator.INPUT_FORMS) - seen),
    )
