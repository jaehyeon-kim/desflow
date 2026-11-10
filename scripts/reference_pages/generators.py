"""The Generators section: an overview page and one page for each `gen` kind.

The kinds, their keys and their JSON Schema come from the blueprint JSON Schema. The
groups, the sentences that say when to use a kind and what its example shows are
written here, because the schema does not know them. Each kind has three example files
in `examples/generators/` and the records they print in `<kind>_output.txt`;
`tests/docs/test_generator_examples.py` runs the files and compares what they print
with that text.
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from .schema import blueprint_schema, default_text, models_in, type_text
from .yaml_keys import paragraphs

ROOT = Path(__file__).resolve().parents[2]
GENERATOR_EXAMPLES = ROOT / "examples" / "generators"
# The tab titles of every code tab set, with the suffix of each form's file
FORMS = (
    ("YAML", ".yaml", "yaml"),
    ("Builder", "_builder.py", "python"),
    ("Low-level", "_low_level.py", "python"),
)
# The keys every kind accepts
MODIFIERS = ("null_rate", "unique", "omit_rate", "omit_when")


@dataclass(frozen=True)
class Group:
    """A group of kinds that give the same sort of value.

    Attributes:
        title: The group's name in the sidebar.
        use: What the group's kinds are for.
        kinds: The kinds, in the order the sidebar lists them.
    """

    title: str
    use: str
    kinds: Tuple[str, ...]


@dataclass(frozen=True)
class Kind:
    """The sentences a kind's page needs that the schema does not hold.

    Attributes:
        use: When to use the kind.
        shows: What the kind's example shows.
    """

    use: str
    shows: str


# The groups of the Generators section of the sidebar, in its order. A kind added to the
# schema joins the group whose kinds give the same sort of value.
GROUPS: Dict[str, Group] = {
    "random-numbers": Group(
        "Random Numbers",
        "Numbers and flags drawn from a range or a distribution.",
        ("integer", "uniform", "normal", "sample", "boolean"),
    ),
    "choices": Group(
        "Choices",
        "A value picked from a list you write.",
        ("one_of", "weighted", "pick"),
    ),
    "text-and-ids": Group(
        "Text and IDs",
        "Names, identifiers and text.",
        ("fake", "uuid", "sequence", "format", "lower", "upper"),
    ),
    "time": Group(
        "Time",
        "The simulation clock as text or numbers.",
        ("now", "format_time"),
    ),
    "maths": Group(
        "Maths",
        "Arithmetic on other values.",
        ("add", "subtract", "multiply", "divide", "min", "max"),
    ),
    "conditions": Group(
        "Conditions",
        "A value that depends on a condition or a case, or that is kept as written.",
        ("when", "switch", "expr", "literal"),
    ),
    "structure": Group(
        "Structure",
        "Mappings and lists built from other generators.",
        ("object", "repeat"),
    ),
    "model-state": Group(
        "Model State",
        "Values the model already holds: the entity, the task's vars and the registry.",
        ("attr", "var", "param", "entity"),
    ),
    "related-records": Group(
        "Related Records",
        "Values from the records other tasks or earlier orders published, and from files.",
        ("lookup", "previous", "upstream", "aggregate", "row"),
    ),
    "python": Group(
        "Python",
        "A value from your own Python function, the one kind that runs `!python`.",
        ("call",),
    ),
}

KINDS: Dict[str, Kind] = {
    "integer": Kind(
        "A count or a quantity in which every whole number in the range is as likely as any other.",
        "`quantity` is a whole number from 1 to 50, and 1 and 50 are both possible.",
    ),
    "uniform": Kind(
        "A price or a size in which every value in the range is as likely as any other.",
        "`price` is a number from 1 up to, but not including, 10, rounded to 2 decimals.",
    ),
    "normal": Kind(
        "A measurement that clusters around a mean, such as a weight.",
        "`weight_g` has a mean of 500 and a standard deviation of 20, rounded to 1 decimal.",
    ),
    "sample": Kind(
        "A duration or a size whose shape you know, such as exponential gaps, or one that "
        "follows the live distribution of a service.",
        "`wait_s` is a draw from an exponential distribution with a rate of 0.5, so its "
        "mean is 2 seconds, rounded to 2 decimals.",
    ),
    "boolean": Kind(
        "A flag that is true for a share of the orders, such as a gift.",
        "`gift` is true for about half of the orders.",
    ),
    "one_of": Kind(
        "One value out of a list, each equally likely, such as a colour.",
        "`colour` is red, green or blue, each with a chance of one in three.",
    ),
    "weighted": Kind(
        "One value out of a list in which some values are more likely, such as a region.",
        "`region` is north with weight 3, south with weight 1 or east with weight 2, so "
        "north has a chance of one in two.",
    ),
    "pick": Kind(
        "Several different values out of a list, such as optional extras.",
        "`extras` is a list of 2 different extras out of 4, in the order drawn.",
    ),
    "fake": Kind(
        "Names, addresses and other realistic text, when invented values are enough.",
        "`customer` is a made-up person's name.",
    ),
    "uuid": Kind(
        "A unique identifier, such as an order id.",
        "`order_id` is a version 4 UUID, and the same seed gives the same UUIDs.",
    ),
    "sequence": Kind(
        "An order number or a label that counts up.",
        "`order_id` counts up from 1000 and is written as `ord-1000`, `ord-1001` and so on.",
    ),
    "format": Kind(
        "Text built from parts, such as a label made of a region and a number.",
        "`label` joins a region and a number, each from its own generator, with a dash.",
    ),
    "lower": Kind(
        "A code or a label that must be in one standard case, whatever case it was drawn in.",
        "`code` is a code from a list, written in lower case.",
    ),
    "upper": Kind(
        "A code or a label that must be in one standard case, whatever case it was drawn in.",
        "`code` is a code from a list, written in upper case.",
    ),
    "now": Kind(
        "A time field of a record, with an offset when it is dated before or after the clock.",
        "`placed_at` is the time of the order, and `due_at` is one hour later.",
    ),
    "format_time": Kind(
        "A time in the form another system expects, such as epoch milliseconds or a local time zone.",
        "`day` is the time of the order as year, month, day, hour and minute. `epoch_s` "
        "is the same time as seconds since 1 January 1970.",
    ),
    "add": Kind(
        "A total, or a time that falls some seconds after another time.",
        "`total` is 10 plus a whole number from 1 to 5.",
    ),
    "subtract": Kind(
        "A difference, or a time that falls some seconds before another time.",
        "`left` is 100 less a whole number from 1 to 20.",
    ),
    "multiply": Kind(
        "A cost, such as a quantity times a price.",
        "`area` is a whole number from 1 to 9 times 2.5, rounded to 1 decimal.",
    ),
    "divide": Kind(
        "A ratio, such as a total divided by a count.",
        "`share` is a whole number from 1 to 9 divided by 4, rounded to 2 decimals.",
    ),
    "min": Kind(
        "A cap: the smaller of a drawn value and a limit.",
        "`lowest` is the smallest of two whole numbers from 1 to 9 and the number 5.",
    ),
    "max": Kind(
        "A floor: the larger of a drawn value and a minimum.",
        "`highest` is the largest of two whole numbers from 1 to 9 and the number 5.",
    ),
    "when": Kind(
        "A value that changes with one condition, such as a discount for a large order.",
        "`quantity` is a var, a whole number from 1 to 6. `size` is large when "
        "`quantity` is above 3, and small otherwise.",
    ),
    "switch": Kind(
        "One value out of several, chosen by an exact match or by a range.",
        "`band` is small below 3, medium from 3 up to, but not including, 5, and large "
        "otherwise.",
    ),
    "expr": Kind(
        "A value computed from vars and numbers by an expression.",
        "`total` is the var `quantity` times a `price` of 2.5, rounded to 2 decimals.",
    ),
    "literal": Kind(
        "A value that must be kept as written, even when it looks like a generator.",
        "`template` is the mapping `{gen: not_evaluated}`, published as it is.",
    ),
    "object": Kind(
        "A nested record that is sometimes null or left out as a whole.",
        "`address` is a mapping of a city and a postcode. With `null_rate: 0.5`, about "
        "half of the orders have no address (`null`).",
    ),
    "repeat": Kind(
        "The items of an order: a list in which each item is drawn on its own.",
        "`scores` is a list of 3 whole numbers from 1 to 5.",
    ),
    "attr": Kind(
        "A value the entity has carried since it started, such as a size the arrival gave it.",
        "Each order starts with a `size` attribute from its arrival, and the payload "
        "publishes it.",
    ),
    "var": Kind(
        "A value drawn once for the task and used in several places.",
        "`quantity` is drawn once for each order, so `first` and `second` are equal.",
    ),
    "param": Kind(
        "A live setting of the simulation, which a scenario step or an ingress update can "
        "change.",
        "`tax_rate` is the variable `tax_rate` of the simulation `Gen`, read when each "
        "order is published.",
    ),
    "entity": Kind(
        "A payload field that names the entity the record belongs to.",
        "`order_key` is the key of the order, such as `orders-1`.",
    ),
    "lookup": Kind(
        "A value from a record that another task already published, such as an existing "
        "customer.",
        "Three customers start at time 0. Each order picks one of them at random and "
        "publishes its `customer_id`.",
    ),
    "previous": Kind(
        "A value from the last finished record of the same task, such as the previous "
        "reading.",
        "`before` is the `reading` of the order before, and 0 for the first order.",
    ),
    "upstream": Kind(
        "A value from the record of the task that routed to this one.",
        "The task `order` routes to the task `invoice`, which publishes the amount of the "
        "order.",
    ),
    "aggregate": Kind(
        "A mean, sum, minimum, maximum or count over earlier records of another task.",
        "Three readings start at time 0. Each order publishes the mean of the last 3 "
        "levels.",
    ),
    "row": Kind(
        "A whole row of a CSV or Parquet file, such as a product from a price list.",
        "`product` is one row of `products.csv`, and `shuffle` gives every row once, in a "
        "random order.",
    ),
    "call": Kind(
        "A value that your own Python function computes.",
        "`total` is a price from 10 to 50 plus a tax of 10 per cent, added by `with_tax` "
        "in `call_helpers.py`.",
    ),
}

# What each key every kind accepts does
_MODIFIER_TEXT = {
    "null_rate": "The chance, from 0 to 1, that the value is `null` instead of a draw.",
    "unique": "When true, a draw that repeats an earlier value of this generator is drawn "
    "again, up to 1000 times, and the run stops with an error if no new value is found.",
    "omit_rate": "The chance, from 0 to 1, that the field of a mapping is left out.",
    "omit_when": "An expression. The field of a mapping is left out when it holds.",
}


def generator_models() -> Dict[str, str]:
    """Every `gen` kind of the JSON Schema, with the name of its model.

    Returns:
        Dict[str, str]: The kind, such as `one_of`, mapped to its model, such as
            `OneOf`, in the order of the schema.
    """
    found: Dict[str, str] = {}
    for model, node in blueprint_schema()["$defs"].items():
        gen = node.get("properties", {}).get("gen")
        if gen is None:
            continue
        kind = next(alt["const"] for alt in gen["anyOf"] if "const" in alt)
        found[kind] = model
    return found


def _reached(nodes: List[Any], defs: Dict[str, Any]) -> Set[str]:
    """The models that the schema nodes reach, through any depth of nesting."""
    found: Set[str] = set()
    queue = [name for node in nodes for name in models_in(node, defs)]
    while queue:
        name = queue.pop()
        if name not in found:
            found.add(name)
            for prop in defs[name]["properties"].values():
                queue += models_in(prop, defs)
    return found


def generator_only_models() -> Set[str]:
    """The models whose keys are on the Generators pages and not on YAML Keys.

    These are the model of every kind and the models only the kinds reach, such as
    `HistogramBin`. A model that a kind and another part of a blueprint both reach is on
    YAML Keys.

    Returns:
        Set[str]: The model names.
    """
    schema = blueprint_schema()
    defs = schema["$defs"]
    kinds = set(generator_models().values())
    elsewhere = _reached(list(schema["properties"].values()), defs)
    own = _reached([p for k in kinds for p in defs[k]["properties"].values()], defs)
    return kinds | (own - elsewhere)


def key_pages(model: str) -> List[str]:
    """The pages of the Generators section that must list the keys of a model.

    Args:
        model: A model of the generator kinds, such as `OneOf`, `HistogramBin`, or
            `Generator` for the keys every kind accepts.

    Returns:
        List[str]: The paths relative to the docs folder: the page of the kind for a
            kind's model, the page of every kind that reaches it for a model only the
            kinds reach, and the index page for `Generator`.

    Raises:
        KeyError: When the keys of the model are not on the Generators pages.
    """
    if model == "Generator":
        return ["generators/index.md"]
    defs = blueprint_schema()["$defs"]
    models = generator_models()
    pages = [
        _page_name(kind)
        for kind, kind_model in models.items()
        if model == kind_model
        or model in _reached(list(defs[kind_model]["properties"].values()), defs)
    ]
    if not pages or model not in generator_only_models():
        raise KeyError(model)
    return pages


def _group_of(kind: str) -> str:
    """The folder of the group that lists `kind`.

    Args:
        kind: The kind, such as `one_of`.

    Returns:
        str: The group's folder name, such as `choices`.
    """
    return next(slug for slug, group in GROUPS.items() if kind in group.kinds)


def _page_name(kind: str) -> str:
    """The path of a kind's page, relative to the docs folder.

    Args:
        kind: The kind, such as `format_time`.

    Returns:
        str: The path, such as `generators/time/format-time.md`.
    """
    return f"generators/{_group_of(kind)}/{kind.replace('_', '-')}.md"


def _example_file(kind: str, form: str) -> Path:
    """One form of a kind's example.

    Args:
        kind: The kind.
        form: The tab title: `YAML`, `Builder` or `Low-level`.

    Returns:
        Path: The file in `examples/generators/`.
    """
    suffix = next(suffix for title, suffix, _ in FORMS if title == form)
    return GENERATOR_EXAMPLES / f"{kind}{suffix}"


def output_file(kind: str) -> Path:
    """The records a kind's example prints, as the page shows them.

    Args:
        kind: The kind.

    Returns:
        Path: The text file in `examples/generators/`.
    """
    return GENERATOR_EXAMPLES / f"{kind}_output.txt"


def _tab(title: str, kind: str, language: str, form: str) -> List[str]:
    """One tab of the example: the file as a labelled block, indented under the tab."""
    path = _example_file(kind, form)
    label = f"examples/generators/{path.name}"
    lines = [f'=== "{title}"', "", f'    ```{language} title="{label}"']
    for line in path.read_text(encoding="utf-8").rstrip("\n").split("\n"):
        lines.append(f"    {line}" if line else "")
    return lines + ["    ```", ""]


def _keys_table(model: str, defs: Dict[str, Any], skip: Tuple[str, ...]) -> List[str]:
    """The keys of a model in a table, without the keys in `skip`."""
    required = set(defs[model].get("required", []))
    rows = ["| Key | Type | Default |", "|---|---|---|"]
    for name, prop in defs[model]["properties"].items():
        if name in skip:
            continue
        if name == "gen":
            kind = defs[model]["properties"]["gen"]["anyOf"][0]["const"]
            rows.append(f"| `gen` | `{kind}` | required |")
            continue
        rows.append(
            f"| `{name}` | {type_text(prop, defs, {})} | "
            f"{default_text(prop, name in required)} |"
        )
    return rows


def generator_page(kind: str) -> str:
    """The page of one kind, as Markdown.

    Args:
        kind: The kind, such as `one_of`.

    Returns:
        str: The page: what the kind does, its keys, its example in three tabs with the
            records it prints, and its JSON Schema.
    """
    defs = blueprint_schema()["$defs"]
    model = generator_models()[kind]
    lines = [f"# {kind}", ""]
    for paragraph in paragraphs(defs[model]["description"]):
        lines += [paragraph, ""]
    lines += [f"Use it for {KINDS[kind].use[0].lower()}{KINDS[kind].use[1:]}", ""]
    lines += ["## Keys", ""]
    table = _keys_table(model, defs, MODIFIERS) + [""]
    nested_lines: List[str] = []
    for key in defs[model]["properties"]:
        for nested in models_in(defs[model]["properties"][key], defs):
            if "gen" in defs[nested]["properties"]:
                continue
            nested_lines += [f"### {nested}", ""]
            for paragraph in paragraphs(defs[nested].get("description", "")):
                nested_lines += [paragraph, ""]
            nested_lines += _keys_table(nested, defs, ()) + [""]
    accepts = (
        "Every kind also accepts `null_rate`, `unique`, `omit_rate` and `omit_when`, "
        "which [Generators](../index.md#keys-every-kind-accepts) describes."
    )
    # A page explains `Value` only when one of its tables uses the word
    if any(re.search(r"\bValue\b", row) for row in table + nested_lines):
        accepts = (
            "A `Value` is a constant or a generator, as [What a Generator Is]"
            f"(../index.md#what-a-generator-is) explains. {accepts}"
        )
    lines += table + [accepts, ""] + nested_lines
    lines += ["## Example", "", f"{KINDS[kind].shows} {_RUN_LINE}", ""]
    for title, _, language in FORMS:
        lines += _tab(title, kind, language, title)
    output = output_file(kind).read_text(encoding="utf-8").rstrip("\n")
    lines += [
        "All three print the same records:",
        "",
        "```text",
        output,
        "```",
        "",
        "## JSON Schema",
        "",
        f"The schema of `{model}`, from `desflow schema`.",
        "",
        "```json",
        json.dumps(defs[model], indent=2),
        "```",
        "",
    ]
    return "\n".join(lines)


_RUN_LINE = (
    "An order starts every 60 seconds for 200 seconds, with the seed 42, so three "
    "orders start."
)


def generators_index() -> str:
    """The section page: what a generator is, which kind to use, the keys every kind
    accepts and how the examples run.

    Returns:
        str: The page, as Markdown.
    """
    defs = blueprint_schema()["$defs"]
    lines = [
        "# Generators",
        "",
        "A generator draws a value each time a blueprint makes a record. This section "
        "has one page for each kind, with its keys, an example in YAML, Builder and "
        "Low-level, the records the example prints, and its JSON Schema.",
        "",
        "## What a Generator Is",
        "",
        "A place that takes a value, such as a field of a task's `payload` or of an "
        "arrival's `attributes`, takes a constant or a generator. A generator is a "
        "mapping with a `gen` key that names its kind, such as "
        "`{gen: integer, bounds: [1, 6]}`. The tables of this section call such a place "
        "`Value`. [YAML Keys](../reference/yaml-keys.md) calls the generator part of such a "
        "place `Generator`: a key that takes a generator, an integer or text is written "
        "`Generator or integer or text`.",
        "",
        "Each generator draws from a random sequence of its own, named for the place "
        "it is written at. The same seed gives the same values. Adding a field to a "
        "mapping moves no other draw. Inserting an item into a list moves the draws of "
        "the items after it. The keys of every other part of a blueprint are on "
        "[YAML Keys](../reference/yaml-keys.md).",
        "",
        "## Choose a Kind",
        "",
        "The sidebar groups the kinds by the sort of value they give:",
        "",
    ]
    for group in GROUPS.values():
        lines.append(f"- **{group.title}:** {group.use}")
    lines += ["", "| Group | Kind | Use it for |", "|---|---|---|"]
    for group in GROUPS.values():
        for kind in group.kinds:
            link = f"[`{kind}`]({_page_name(kind).removeprefix('generators/')})"
            lines.append(f"| {group.title} | {link} | {KINDS[kind].use} |")
    lines += [
        "",
        "## Keys Every Kind Accepts",
        "",
        "These four keys go on any kind.",
        "",
        "| Key | Type | Default | What it does |",
        "|---|---|---|---|",
    ]
    first = next(m for m in defs.values() if "gen" in m.get("properties", {}))
    for key in MODIFIERS:
        prop = first["properties"][key]
        lines.append(
            f"| `{key}` | {type_text(prop, defs, {})} | "
            f"{default_text(prop, False)} | {_MODIFIER_TEXT[key]} |"
        )
    lines += [
        "",
        "`omit_rate` and `omit_when` apply to a field of a mapping, such as a field of "
        "a `payload`. A blueprint that writes them on any other value is refused when "
        "it loads.",
        "",
        "## Run the Examples",
        "",
        "Every example is a model in which an order starts every 60 seconds and the "
        "run lasts 200 seconds, so three orders start. The seed is 42 and `factor` is "
        "0, so a run takes no wall-clock time and prints the same records each time. "
        "No example needs a service.",
        "",
        "Each kind has three files in `examples/generators/` of the repository: "
        "`<kind>.yaml`, `<kind>_builder.py` and `<kind>_low_level.py`. Run them from "
        "the repository's root folder:",
        "",
        "```bash",
        "desflow run examples/generators/integer.yaml",
        "python examples/generators/integer_builder.py",
        "python examples/generators/integer_low_level.py",
        "```",
        "",
        '`fake` needs the `fake` extra, which `pip install "desflow[fake]"` adds. '
        "`row` reads `products.csv` and `call` imports `call_helpers.py`, both in the "
        "same folder.",
        "",
    ]
    return "\n".join(lines)


def generator_pages() -> Dict[str, str]:
    """The index page and the page of every kind.

    Returns:
        Dict[str, str]: Each page's path relative to the docs folder, in the order of
            the sidebar, with its Markdown.
    """
    pages = {"generators/index.md": generators_index()}
    for group in GROUPS.values():
        for kind in group.kinds:
            pages[_page_name(kind)] = generator_page(kind)
    return pages


def record_outputs() -> List[str]:
    """Writes `<kind>_output.txt` for each kind from a run of its YAML example.

    Returns:
        List[str]: The kinds whose file changed.
    """
    from blocks.capture import yaml_records

    changed: List[str] = []
    for kind in generator_models():
        text = "\n".join(yaml_records(f"examples/generators/{kind}.yaml")) + "\n"
        path = output_file(kind)
        if not path.exists() or path.read_text(encoding="utf-8") != text:
            path.write_text(text, encoding="utf-8")
            changed.append(kind)
    return changed
