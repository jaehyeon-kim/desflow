"""The YAML Keys page, and its sections the skill also uses."""

from typing import Dict, List, Optional, Set

from .schema import blueprint_schema, default_text, models_in, ref_name, type_text


def paragraphs(text: str) -> List[str]:
    """A docstring as Markdown paragraphs of one line each.

    Args:
        text: The docstring.

    Returns:
        List[str]: One line per paragraph.
    """
    return [" ".join(part.split()) for part in text.strip().split("\n\n") if part]


def yaml_keys_body(level: int = 2) -> List[str]:
    """One section per top-level key of a blueprint, as lines of Markdown.

    A key whose entries are a model shows that model's keys in a table, and each model
    nested in it follows as a section of its own, in the order it is first used. The
    YAML Keys page and the skill's `references/blueprint-keys.md` both use these lines.

    Args:
        level: The heading level of the top-level keys.

    Returns:
        List[str]: The lines of Markdown.
    """
    schema = blueprint_schema()
    defs = schema["$defs"]
    top = schema["properties"]
    hashes = "#" * level
    sub = "#" * (level + 1)

    # A model that is the entry of a top-level key is shown under that key
    primary: Dict[str, str] = {}
    for key, prop in top.items():
        models = models_in(prop, defs)
        if models and models[0] not in primary.values():
            primary[key] = models[0]
    anchors = {model: key for key, model in primary.items()}
    taken: Set[str] = set()

    def table(model: str) -> List[str]:
        required = set(defs[model].get("required", []))
        rows = ["| Key | Type | Default |", "|---|---|---|"]
        for name, prop in defs[model]["properties"].items():
            rows.append(
                f"| `{name}` | {type_text(prop, defs, anchors)} | "
                f"{default_text(prop, name in required)} |"
            )
        return rows

    def section(model: str, heading: Optional[str]) -> List[str]:
        """The description and table of a model, then the models nested in it."""
        taken.add(model)
        lines: List[str] = []
        if heading:
            lines += [heading, ""]
        for paragraph in paragraphs(defs[model].get("description", "")):
            lines += [paragraph, ""]
        lines += table(model) + [""]
        for prop in defs[model]["properties"].values():
            for nested in models_in(prop, defs):
                if nested not in taken and nested not in primary.values():
                    lines += section(nested, f"{sub} {nested}")
        return lines

    lines: List[str] = []
    for key, prop in top.items():
        required = key in schema.get("required", [])
        lines += [f"{hashes} {key}", ""]
        # A key that is one model needs no link to the table that follows
        kind = "mapping" if ref_name(prop) else type_text(prop, defs, anchors)
        lines.append(f"Type: {kind}. " + ("Required." if required else "Optional."))
        lines.append("")
        if key in primary:
            lines += section(primary[key], None)
        else:
            for nested in models_in(prop, defs):
                if nested not in taken:
                    lines += section(nested, f"{sub} {nested}")
    return lines


def yaml_keys_page() -> str:
    """The YAML Keys reference page, as Markdown."""
    lines = [
        "# YAML Keys",
        "",
        "Every key a blueprint file takes, generated from the JSON Schema that ships "
        "with desflow. [YAML Blueprints](../user-guide/writing-a-model/blueprints.md) explains how a "
        "file is read. "
        "[Set Up Your Editor](../how-to/tools/set-up-your-editor.md) shows how to have "
        "your editor complete and check these keys as you type.",
        "",
        "Each section below is a top-level key. A table lists the keys that an entry "
        "of it takes, and a section follows for each entry nested in it. Where a "
        "value is a number, a whole number, `true` or `false`, or one of a fixed "
        "set of names, a `${VAR}` or `${VAR:-default}` reference is also "
        "accepted, and a duration such as `5 min` is accepted where the type says "
        "Duration.",
        "",
    ]
    return "\n".join(lines + yaml_keys_body(2)).rstrip("\n") + "\n"
