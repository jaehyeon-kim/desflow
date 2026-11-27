"""The Generators section: every kind is in a group, and its examples print what its
page shows.

Each `gen` kind of the JSON Schema has a page, written by `scripts/gen_pages.py`, and
an example blueprint in `examples/generators/`, `<kind>.yaml`. Generators are written in
blueprints, so there is no Builder or Low-level example. Each blueprint is run unpaced,
and the records it prints are compared with `<kind>_output.txt`, which the page shows.
`python scripts/gen_pages.py` writes the output files again from the examples.
"""

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from blocks.capture import yaml_records  # noqa: E402

_SCRIPT = ROOT / "scripts" / "gen_pages.py"
_spec = importlib.util.spec_from_file_location("gen_pages", _SCRIPT)
assert _spec is not None and _spec.loader is not None
gen_pages = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_pages)

from reference_pages import generators  # noqa: E402

FOLDER = ROOT / "examples" / "generators"
KINDS = list(generators.generator_models())
# The files of the folder that are not an example of a kind
SUPPORT = {"products.csv", "call_helpers.py"}
HEADING = re.compile(r"^(#{1,6}) ", re.M)
FENCE = re.compile(r"^ *```.*?^ *```", re.S | re.M)


def test_every_kind_a_group_lists_is_in_the_schema_once():
    listed = [kind for group in generators.GROUPS.values() for kind in group.kinds]
    assert sorted(listed) == sorted(KINDS), (
        "Add each kind to the group of GROUPS in scripts/reference_pages/generators.py "
        "whose kinds give the same sort of value, and to the Generators block of the "
        "Sidebar in planning docs.md"
    )
    assert generators.KINDS.keys() == set(KINDS)


def test_the_examples_folder_holds_a_blueprint_and_an_output_for_each_kind():
    expected = set(SUPPORT)
    for kind in KINDS:
        expected |= {f"{kind}.yaml", f"{kind}_output.txt"}
    assert {p.name for p in FOLDER.iterdir() if p.name != "__pycache__"} == expected


@pytest.mark.parametrize("kind", KINDS)
def test_an_example_prints_the_records_its_page_shows(kind):
    shown = generators.output_file(kind).read_text(encoding="utf-8").rstrip("\n")
    records = yaml_records(f"examples/generators/{kind}.yaml")
    assert "\n".join(records) == shown, (
        f"{kind}: run `python scripts/gen_pages.py` if the example changed on purpose"
    )


def test_every_example_runs_without_a_pause_and_with_a_fixed_seed():
    for kind in KINDS:
        text = (FOLDER / f"{kind}.yaml").read_text(encoding="utf-8")
        assert re.search(r"^\s+factor: 0$", text, re.M), kind
        assert re.search(r"^\s+random_seed: 42$", text, re.M), kind


@pytest.mark.parametrize("kind", KINDS)
def test_a_page_shows_the_example_and_its_records_with_no_tabs(kind):
    page = generators.generator_page(kind)
    text = (FOLDER / f"{kind}.yaml").read_text(encoding="utf-8").rstrip("\n")
    assert f'```yaml title="examples/generators/{kind}.yaml"\n{text}\n```' in page
    assert "=== " not in page, f"{kind}: a page with one form has no tabs"
    output = (FOLDER / f"{kind}_output.txt").read_text(encoding="utf-8").rstrip("\n")
    assert f"```text\n{output}\n```" in page


def test_every_generator_page_is_within_the_size_limits():
    for name, page in gen_pages.generator_pages().items():
        headings = HEADING.findall(FENCE.sub("", page))
        assert headings.count("##") <= 10 and len(headings) <= 15, name
