"""The Generators section: every kind is in a group, and its examples print what its
page shows.

Each `gen` kind of the JSON Schema has a page, written by `scripts/gen_pages.py`, and
three example files in `examples/generators/`: `<kind>.yaml`, `<kind>_builder.py` and
`<kind>_low_level.py`. Each file is run unpaced, and the records it prints are compared
with `<kind>_output.txt`, which the page shows. `python scripts/gen_pages.py` writes the
output files again from the YAML examples.
"""

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from blocks.capture import capture, cwd, load_script, yaml_records  # noqa: E402

_SCRIPT = ROOT / "scripts" / "gen_pages.py"
_spec = importlib.util.spec_from_file_location("gen_pages", _SCRIPT)
assert _spec is not None and _spec.loader is not None
gen_pages = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_pages)

from reference_pages import generators  # noqa: E402

FOLDER = ROOT / "examples" / "generators"
KINDS = list(generators.generator_models())
FORMS = ["YAML", "Builder", "Low-level"]
# The files of the folder that are not an example of a kind
SUPPORT = {"products.csv", "call_helpers.py"}
HEADING = re.compile(r"^(#{1,6}) ", re.M)
FENCE = re.compile(r"^ *```.*?^ *```", re.S | re.M)


def _lines(kind, form):
    """The records one form of a kind's example prints."""
    if form == "YAML":
        return yaml_records(f"examples/generators/{kind}.yaml")
    suffix = "builder" if form == "Builder" else "low_level"
    module = load_script(f"examples/generators/{kind}_{suffix}.py")
    with cwd(ROOT):
        if form == "Builder":
            return capture(lambda: module.build().run(until=module.UNTIL))
        return capture(module.run)


def test_every_kind_a_group_lists_is_in_the_schema_once():
    listed = [kind for group in generators.GROUPS.values() for kind in group.kinds]
    assert sorted(listed) == sorted(KINDS), (
        "Add each kind to the group of GROUPS in scripts/reference_pages/generators.py "
        "whose kinds give the same sort of value, and to the Generators block of the "
        "Sidebar in planning docs.md"
    )
    assert generators.KINDS.keys() == set(KINDS)


def test_the_examples_folder_holds_three_files_and_an_output_for_each_kind():
    expected = set(SUPPORT)
    for kind in KINDS:
        expected |= {
            f"{kind}.yaml",
            f"{kind}_builder.py",
            f"{kind}_low_level.py",
            f"{kind}_output.txt",
        }
    assert {p.name for p in FOLDER.iterdir() if p.name != "__pycache__"} == expected


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("kind", KINDS)
def test_an_example_prints_the_records_its_page_shows(kind, form):
    shown = generators.output_file(kind).read_text(encoding="utf-8").rstrip("\n")
    assert "\n".join(_lines(kind, form)) == shown, (
        f"{kind} {form}: run `python scripts/gen_pages.py` if the YAML example "
        "changed on purpose"
    )


def test_no_example_imports_a_name_outside_the_public_api():
    """`ValueRuntime` is not in the public API, and a name an example uses joins it."""
    for path in FOLDER.glob("*.py"):
        assert "ValueRuntime" not in path.read_text(encoding="utf-8"), path.name


def test_every_example_runs_without_a_pause_and_with_a_fixed_seed():
    for kind in KINDS:
        text = (FOLDER / f"{kind}.yaml").read_text(encoding="utf-8")
        assert re.search(r"^\s+factor: 0$", text, re.M), kind
        assert re.search(r"^\s+random_seed: 42$", text, re.M), kind
        for suffix in ("builder", "low_level"):
            code = (FOLDER / f"{kind}_{suffix}.py").read_text(encoding="utf-8")
            assert "factor=0" in code, f"{kind} {suffix}"
            # A low-level file draws only through a `Sampler`; a builder file always
            # sets the seed of its simulation
            draws = suffix == "builder" or "Sampler" in code
            if draws:
                assert "random_seed=42" in code or "Sampler(seed=42)" in code, (
                    f"{kind} {suffix}: no seed 42"
                )


@pytest.mark.parametrize("kind", KINDS)
def test_a_page_shows_the_example_files_and_their_records(kind):
    page = generators.generator_page(kind)
    for title, suffix, _ in generators.FORMS:
        text = (FOLDER / f"{kind}{suffix}").read_text(encoding="utf-8").rstrip("\n")
        indented = "\n".join(f"    {line}" if line else "" for line in text.split("\n"))
        assert f'=== "{title}"' in page
        assert f"    ```{'yaml' if title == 'YAML' else 'python'} title=" in page
        assert indented in page, f"{kind} {title}: the block differs from the file"
    output = (FOLDER / f"{kind}_output.txt").read_text(encoding="utf-8").rstrip("\n")
    assert f"```text\n{output}\n```" in page
    assert [t for t, _, _ in generators.FORMS] == FORMS


def test_every_generator_page_is_within_the_size_limits():
    for name, page in gen_pages.generator_pages().items():
        headings = HEADING.findall(FENCE.sub("", page))
        assert headings.count("##") <= 10 and len(headings) <= 15, name
