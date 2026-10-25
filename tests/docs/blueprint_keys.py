"""Which blueprint keys the labelled YAML blocks on the pages set, by model.

Many key names belong to more than one model, such as `weights` (an empirical
distribution, and a route's shares) or `after` (a timeout, a `fire` action and a
`spawn` action). So a key is read as a pair, the model that declares it and its
name, and a block counts for a pair only when the key is set where the schema puts
that model. Each block is parsed, so a key named only in a comment does not count.

A block can be a whole blueprint or a part of one, such as one task or the tasks
section. A part is matched to the models whose keys hold all of its keys, or to a
mapping of names to such a model.
"""

import json
import re
import textwrap
from typing import Any, Dict, List, Set, Tuple

import yaml
from feature_lists import LABELLED, PAGES

Pair = Tuple[str, str]
ROOT_MODEL = "Blueprint"


def _schema() -> Dict[str, Any]:
    from desflow.blueprint.schema import schema_text

    return json.loads(schema_text())


SCHEMA = _schema()
DEFS: Dict[str, Any] = SCHEMA["$defs"]


def pairs() -> List[Pair]:
    """Every (model, key) pair the blueprint JSON Schema declares."""
    found = {(ROOT_MODEL, key) for key in SCHEMA["properties"]}
    for model, node in DEFS.items():
        found |= {(model, key) for key in node.get("properties", {})}
    return sorted(found)


class _Loader(yaml.SafeLoader):
    """Reads `!python`, `!include` and any other tag as an empty value, and only
    `true` and `false` as booleans, so the key `on:` stays the text `on`."""


_Loader.yaml_implicit_resolvers = {}
for _first, _resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items():
    _Loader.yaml_implicit_resolvers[_first] = [
        (tag, rx) for tag, rx in _resolvers if tag != "tag:yaml.org,2002:bool"
    ]
_Loader.add_implicit_resolver(
    "tag:yaml.org,2002:bool",
    re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"),
    list("tTfF"),
)
_Loader.add_multi_constructor("!", lambda loader, suffix, node: None)


def _resolve(node: Any) -> Tuple[str, Any]:
    """The model a `$ref` names, and its schema; other nodes are returned as given."""
    name = ""
    while isinstance(node, dict) and "$ref" in node:
        name = node["$ref"].rsplit("/", 1)[1]
        node = DEFS[name]
    return name, node


def _fits(value: Any, node: Dict[str, Any]) -> bool:
    """Whether `value` can be an instance of this schema alternative."""
    choices = node.get("anyOf", []) + node.get("oneOf", [])
    if choices:
        return any(_fits(value, _resolve(choice)[1]) for choice in choices)
    if isinstance(value, dict):
        props = node.get("properties")
        if props is not None:
            return set(value) <= set(props)
        return node.get("type") == "object" or "additionalProperties" in node
    if isinstance(value, list):
        return node.get("type") == "array" or "items" in node
    return "properties" not in node


def _walk(value: Any, node: Any, model: str, used: Set[Pair]) -> None:
    """Adds each (model, key) pair `value` sets, read against the schema `node`."""
    name, node = _resolve(node)
    model = name or model
    if not isinstance(node, dict):
        return
    for alternative in node.get("anyOf", []) + node.get("oneOf", []):
        if _fits(value, _resolve(alternative)[1]):
            _walk(value, alternative, model, used)
    for part in node.get("allOf", []):
        _walk(value, part, model, used)
    if isinstance(value, dict):
        props = node.get("properties", {})
        for key, inner in value.items():
            if key in props:
                used.add((model, key))
                _walk(inner, props[key], model, used)
        extra = node.get("additionalProperties")
        if isinstance(extra, dict) and not props:
            for inner in value.values():
                _walk(inner, extra, model, used)
    elif isinstance(value, list) and isinstance(node.get("items"), dict):
        for inner in value:
            _walk(inner, node["items"], model, used)


def _contexts(block: Dict[str, Any]) -> List[Tuple[Any, str]]:
    """The schemas a block's top level can be: the blueprint, a model whose keys
    hold every key of the block, or a mapping of names to such a model."""
    found: List[Tuple[Any, str]] = []
    if set(block) <= set(SCHEMA["properties"]):
        found.append((SCHEMA, ROOT_MODEL))
    for model, node in DEFS.items():
        props = node.get("properties", {})
        if props and set(block) <= set(props):
            found.append((node, model))
        values = list(block.values())
        if props and all(
            isinstance(v, dict) and v and set(v) <= set(props) for v in values
        ):
            found.append(({"additionalProperties": {"$ref": f"#/$defs/{model}"}}, ""))
    return found


def used_pairs() -> Set[Pair]:
    """Every (model, key) pair a labelled YAML block on the pages sets."""
    used: Set[Pair] = set()
    for page in PAGES:
        for _, language, code in LABELLED.findall(page.read_text(encoding="utf-8")):
            if language != "yaml":
                continue
            block = yaml.load(textwrap.dedent(code), Loader=_Loader)
            if not isinstance(block, dict):
                continue
            for node, model in _contexts(block):
                _walk(block, node, model, used)
    return used
