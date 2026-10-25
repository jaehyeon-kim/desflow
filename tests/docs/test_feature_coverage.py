"""Every blueprint key, connector, extra and distribution family has a tested example,
a reference entry and a place in the skill, and every connector and extra a README
line. feature_lists.py says how the features are read and what a tested example is;
test_feature_coverage_commands.py and test_feature_coverage_api.py check the commands
and the public API."""

import json
import re

import pytest
from blueprint_keys import pairs, used_pairs
from feature_lists import (
    API,
    COMMANDS,
    DOCS,
    PAGES,
    PYTHON_CODE,
    README,
    SKILL,
    SKILL_TEXT,
    YAML_CODE,
    connectors,
    extras,
    families,
    gen_pages,
)

# --- Blueprint keys -------------------------------------------------------------

USED = used_pairs()


@pytest.mark.parametrize(
    "model,key", pairs(), ids=[f"{model}.{key}" for model, key in pairs()]
)
def test_every_blueprint_key_has_an_example_a_reference_and_a_skill_entry(model, key):
    assert (model, key) in USED, (
        f"{model}.{key}: no labelled YAML block on a Learn, How-To, User Guide or "
        "Examples page sets it"
    )
    page = gen_pages().yaml_keys_page()
    assert f"`{key}`" in page or f"## {key}\n" in page, f"{key}: not on YAML Keys"
    keys = (SKILL / "references" / "blueprint-keys.md").read_text(encoding="utf-8")
    assert f"`{key}`" in keys or f"## {key}\n" in keys, f"{key}: not in the skill"


# --- Connectors -----------------------------------------------------------------


# The name a reader knows each connector by, as the README writes it
_README_NAMES = {"Postgres": "PostgreSQL", "Jsonl": "JSONL", "Local": "local schedule"}


@pytest.mark.parametrize("kind,name", connectors())
def test_every_connector_has_an_example_a_reference_a_skill_and_a_readme_line(
    kind, name
):
    assert re.search(rf"type: {name}\b", YAML_CODE), f"{kind} {name}: no example"
    yaml_format = (DOCS / "reference" / "yaml-format.md").read_text(encoding="utf-8")
    assert f"`{name}`" in yaml_format, f"{kind} {name}: not on YAML Format"
    connectors = (SKILL / "references" / "connectors.md").read_text(encoding="utf-8")
    assert f"| `{name}` |" in connectors
    assert _README_NAMES.get(name, name) in README, f"{name}: not in the README"


# --- Extras ---------------------------------------------------------------------


@pytest.mark.parametrize("extra", extras())
def test_every_extra_has_an_example_a_reference_a_skill_and_a_readme_line(extra):
    from desflow.blueprint.examples import EXAMPLES

    pages = "\n".join(page.read_text(encoding="utf-8") for page in PAGES)
    installed = re.search(rf"desflow\[[a-z,]*\b{extra}\b[a-z,]*\]", pages)
    assert installed or any(extra in e.extras for e in EXAMPLES.values()), (
        f"{extra}: no page installs it and no example needs it"
    )
    getting_started = (DOCS / "getting-started.md").read_text(encoding="utf-8")
    assert f"| `{extra}` |" in getting_started, f"{extra}: not on Installation"
    assert f"| `{extra}` |" in README, f"{extra}: not in the README"
    if extra != "all":  # all installs every other extra, so it has no feature
        assert f"`{extra}`" in SKILL_TEXT, f"{extra}: not in the skill"


# --- Distribution families ------------------------------------------------------


@pytest.mark.parametrize("family", families())
def test_every_family_has_an_example_a_reference_and_a_skill_entry(family):
    assert re.search(rf"dist: {family}\b", YAML_CODE) or (
        f'dist="{family}"' in PYTHON_CODE
    ), f"{family}: no labelled block draws from it"
    assert f"## {family}" in gen_pages().distributions_page()
    distributions = (SKILL / "references" / "distributions.md").read_text("utf-8")
    assert f"## {family}" in distributions


def test_the_lists_are_not_empty():
    """A reader that silently finds nothing would make every case above vacuous."""
    assert len(COMMANDS) >= 9 and len(API) >= 50 and len(pairs()) >= 150
    assert len(USED) >= 150
    assert len(connectors()) >= 10 and len(extras()) >= 9 and len(families()) >= 10
    assert json.loads((SKILL / "assets" / "blueprint.schema.json").read_text("utf-8"))
