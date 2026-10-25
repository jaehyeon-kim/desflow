"""What a property of the blueprint JSON Schema takes, in words."""

import json
from typing import Any, Dict, List, Optional


def blueprint_schema() -> Dict[str, Any]:
    """The blueprint JSON Schema that ships with the package."""
    from desflow.blueprint.schema import schema_text

    return json.loads(schema_text())


def ref_name(node: Dict[str, Any]) -> Optional[str]:
    """The model a schema node refers to.

    Args:
        node: A node of the JSON Schema.

    Returns:
        Optional[str]: The model's name from its `$ref`, or None when it has none.
    """
    ref = node.get("$ref")
    return ref.rsplit("/", 1)[1] if isinstance(ref, str) else None


def models_in(node: Any, defs: Dict[str, Any]) -> List[str]:
    """The names of the models a property refers to, in order.

    Args:
        node: The property's schema node.
        defs: The schema's `$defs`.

    Returns:
        List[str]: The model names, each once.
    """
    found: List[str] = []
    if isinstance(node, dict):
        name = ref_name(node)
        if name is not None and "properties" in defs[name]:
            found.append(name)
        for key in ("anyOf", "oneOf", "items", "additionalProperties"):
            inner = node.get(key)
            for item in inner if isinstance(inner, list) else [inner]:
                found += [m for m in models_in(item, defs) if m not in found]
    return found


def type_text(node: Any, defs: Dict[str, Any], anchors: Dict[str, str]) -> str:
    """What a property takes, in a few words. A model links to its section.

    Args:
        node: The property's schema node.
        defs: The schema's `$defs`.
        anchors: The page anchor of each model's section.

    Returns:
        str: The text for the table's type column.
    """
    if node is True or node == {}:
        return "any value"
    name = ref_name(node)
    if name is not None:
        if name == "Placeholder":
            return ""
        target = defs[name]
        if "properties" in target:
            return f"[{name}](#{anchors.get(name, name.lower())})"
        return str(target.get("title", name))
    if "title" in node:
        return str(node["title"])
    if "enum" in node:
        return " or ".join(f"`{value}`" for value in node["enum"])
    if "anyOf" in node or "oneOf" in node:
        parts: List[str] = []
        for alt in node.get("anyOf") or node["oneOf"]:
            if alt.get("type") == "null":
                continue
            text = type_text(alt, defs, anchors)
            if text and text not in parts:
                parts.append(text)
        return " or ".join(parts)
    kind = node.get("type")
    if kind == "array":
        return f"list of {type_text(node.get('items', {}), defs, anchors)}".rstrip()
    if kind == "object":
        extra = node.get("additionalProperties")
        if extra in (None, True):
            return "mapping"
        return f"mapping of names to {type_text(extra, defs, anchors)}"
    if "title" in node:
        return str(node["title"])
    return {"string": "text"}.get(str(kind), str(kind))


def default_text(prop: Dict[str, Any], required: bool) -> str:
    """The default of a property, as the table shows it.

    Args:
        prop: The property's schema node.
        required: Whether the property must be set.

    Returns:
        str: `required`, `none`, the default as JSON in backticks, or empty.
    """
    if required:
        return "required"
    if "default" not in prop:
        return ""
    value = prop["default"]
    if value is None:
        return "none"
    return f"`{json.dumps(value)}`"
