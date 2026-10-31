"""The generated Reference pages follow the code: every family, every statistic and
every conformance check.

scripts/gen_pages.py builds the Distributions, Statistic Names and SimPy
Compatibility pages during the docs build. These tests check that each page lists
everything the code has, so a new family, statistic or conformance test cannot be
left off, or published without a meaning or a page.
"""

import ast
import importlib.util
import re
from pathlib import Path

from desflow.config import STATS_LEVELS
from desflow.core.distributions import DISTRIBUTIONS

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "gen_pages.py"
_spec = importlib.util.spec_from_file_location("gen_pages", _SCRIPT)
assert _spec is not None and _spec.loader is not None
gen_pages = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_pages)


def test_every_family_has_an_example_and_a_section():
    assert set(gen_pages.EXAMPLES) == set(DISTRIBUTIONS)
    page = gen_pages.distributions_page()
    for name, family in DISTRIBUTIONS.items():
        assert f"\n## {name}\n" in page
        assert family.rules in page


def test_every_statistic_has_a_meaning_and_each_level_adds_to_the_last():
    names = gen_pages.statistic_names()
    assert list(names) == list(STATS_LEVELS)
    assert names["none"] == []
    levels = list(names.values())
    for before, after in zip(levels, levels[1:]):
        assert set(before) < set(after)
    page = gen_pages.statistics_page()
    for name in names["all"]:
        assert f"`{name}`" in page  # _meaning raises KeyError for a new statistic


def test_yaml_keys_page_lists_every_key():
    import json

    from desflow.blueprint.schema import schema_text

    schema = json.loads(schema_text())
    page = gen_pages.yaml_keys_page()
    for key in schema["properties"]:
        assert f"\n## {key}\n" in page
    for name, node in schema["$defs"].items():
        for key in node.get("properties", {}):
            assert f"| `{key}` |" in page, f"{name}.{key}"
    # one line per paragraph, as the writing rules ask
    assert "\n\n".join(gen_pages.paragraphs("a\nb\n\nc")) == "a b\n\nc"


def test_command_line_page_holds_the_current_command_list():
    page = (ROOT / "docs" / "cli" / "index.md").read_text(encoding="utf-8")
    assert gen_pages.cli_overview() in page, "copy cli_overview() into What It Is"


def test_every_command_has_a_cli_page_and_every_page_names_a_command():
    from typer.main import get_command

    from desflow.cli import app

    def paths(command, path):
        yield path
        for name, child in getattr(command, "commands", {}).items():
            yield from paths(child, f"{path} {name}")

    commands = set(paths(get_command(app), "desflow"))
    pages = gen_pages.cli_pages()
    named = {page.split("\n")[0].removeprefix("# ") for page in pages.values()}
    assert named | {"desflow"} == commands
    assert len(pages) == len(commands) - 1
    for path, page in pages.items():
        command = page.split("\n")[0].removeprefix("# ")
        assert f"Usage: {command} [OPTIONS]" in page, path
        assert "object at 0x" not in page, path
    overview = gen_pages.cli_overview()
    assert "Usage: desflow [OPTIONS]" in overview
    for command in commands - {"desflow"}:
        assert f"[`{command}`]" in overview
    # Every CLI page links the section of the Command Line page that uses it
    # The strict build checks each anchor, so only the link is checked here
    for path, page in pages.items():
        assert re.search(r"\]\(index\.md(?:#[a-z-]+)?\)", page), path


ROOT = _SCRIPT.parents[1]


def _conformance_tests():
    """Every test function in tests/conformance/, as a pytest node id."""
    found = set()
    for path in sorted((ROOT / "tests" / "conformance").glob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                found.add(f"{path.relative_to(ROOT)}::{node.name}")
    return found


def test_every_register_check_is_a_test_and_every_test_is_listed():
    listed = [check for entry in gen_pages.register() for check in entry["checks"]]
    assert len(listed) == len(set(listed)), "a check is listed twice"
    assert set(listed) == _conformance_tests()


def test_every_register_page_exists_and_every_feature_has_a_row():
    page = gen_pages.compatibility_page()
    for entry in gen_pages.register():
        assert entry["id"] in gen_pages.GROUPS
        if entry.get("status", "native") == "native":
            assert (ROOT / "docs" / entry["page"].split("#")[0]).is_file(), entry[
                "page"
            ]
            if entry["inventory"]:
                assert f"[{entry['operation']}](../{entry['page']})" in page
        else:
            # A gap replaces the reason of a difference in its row
            rows = [
                c
                for c in gen_pages.constructs()["constructs"]
                if c["id"] in entry["inventory"] and not c.get("gaps")
            ]
            if rows:
                assert entry["differences"].replace("|", "\\|") in page


# The 98 modelling features of SimPy 4.1.2, by inventory id; the SP- prefix keeps
# them apart from the register's own group ids
INVENTORY = [
    f"SP-{i}"
    for i in (
        [f"E{n}" for n in range(1, 18) if n != 11]
        + ["E11"]
        + [f"R{n}" for n in range(1, 5)]
        + [f"V{n}" for n in range(1, 12)]
        + [f"P{n}" for n in range(1, 13)]
        + [f"C{n}" for n in range(1, 9)]
        + [f"B{n}" for n in range(1, 9)]
        + [f"S{n}" for n in range(1, 15)]
        + [f"Q{n}" for n in range(1, 9)]
        + [f"K{n}" for n in range(1, 6)]
        + [f"T{n}" for n in range(1, 10)]
        + ["U1", "U2"]
    )
]


def test_the_register_is_the_full_simpy_matrix():
    """Every SimPy feature is in exactly one row, native or a difference; a native
    row has checks and its three forms, and a difference gives its reason."""
    assert len(INVENTORY) == len(set(INVENTORY)) == 98
    seen = [i for entry in gen_pages.register() for i in entry["inventory"]]
    assert sorted(seen) == sorted(INVENTORY)
    for entry in gen_pages.register():
        status = entry.get("status", "native")
        assert status in ("native", "difference"), entry["operation"]
        if status == "native":
            assert entry["checks"], entry["operation"]
            assert all(entry[api] for api in ("yaml", "builder", "low_level"))
        else:
            assert not entry["checks"] and entry["differences"], entry["operation"]


def test_the_simpy_version_is_read_from_the_lock():
    """The page names the SimPy version in uv.lock, the one the tests run on."""
    import tomllib

    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    version = next(p["version"] for p in lock["package"] if p["name"] == "simpy")
    assert gen_pages.simpy_version() == version
    assert f"a plain SimPy {version} model" in gen_pages.compatibility_page()


def test_every_native_feature_says_what_it_is():
    """Each native feature has a sentence saying what it does, in the register."""
    rows = [e for e in gen_pages.register() if e.get("status", "native") == "native"]
    assert all(row.get("description") for row in rows)


# SimPy's areas, in the order of SimPy's documentation
AREAS = [
    "Environment",
    "Events and Timeout",
    "Processes",
    "Conditions",
    "Interrupts",
    "Resources",
    "Containers",
    "Stores",
    "Real-Time",
]


def test_the_page_has_one_section_for_each_area_and_one_row_for_each_feature():
    """One table for each SimPy area, in SimPy's order, with a row for each of the
    98 features and the columns SimPy, YAML, Builder, Low-level and Status."""
    page = gen_pages.compatibility_page()
    data = gen_pages.constructs()
    assert [line[3:] for line in page.splitlines() if line.startswith("## ")] == AREAS
    assert sorted(c["id"] for c in data["constructs"]) == sorted(INVENTORY)
    assert page.count("| SimPy | YAML | Builder | Low-level | Status |") == len(AREAS)
    rows = [line for line in page.splitlines() if line.startswith("| ")]
    assert len(rows) == len(INVENTORY) + len(AREAS) * 1  # a row each, and a header
    for row in rows:
        assert row.count(" | ") == 4 or "\\|" in row, row


def test_every_gap_is_used_defined_and_linked_to_a_form():
    """A gap names a construct and has a sentence; a form it links is a heading on
    Call Python from YAML; a gap left in no row is deleted."""
    data = gen_pages.constructs()
    used = {g for c in data["constructs"] for g in c.get("gaps", [])}
    assert used == set(data["gaps"])
    page = (ROOT / "docs" / "how-to" / "tools" / "yaml-advanced.md").read_text(
        encoding="utf-8"
    )
    for gap, info in data["gaps"].items():
        assert info["text"], gap
        if "form" in info:
            heading = gen_pages.PYTHON_FORMS[info["form"]]
            titles = [
                "-".join(re.sub(r"[^a-z0-9 -]", "", line[4:].lower()).split())
                for line in page.splitlines()
                if line.startswith("### ")
            ]
            assert heading in titles, gap
    # The gaps that 0.29.0 removes are on the page until their batch deletes them
    assert "S4" not in data["gaps"]
    for gap in ("S3", "S5"):
        assert data["gaps"][gap]["fixed"] == "0.29.0"
    # T4, T13 and T14 are fixed in 0.30.0 and link no form
    for gap in ("T4", "T13", "T14"):
        assert data["gaps"][gap]["fixed"] == "0.30.0"
        assert "form" not in data["gaps"][gap]
    # The run and time rules (T1 to T8) ship whole in 0.30.0; the resource, wait and
    # interrupt rows in 0.31.0; the additive Low-level and filter rows in 0.32.0
    for number in range(1, 9):
        assert data["gaps"][f"T{number}"]["fixed"] == "0.30.0"
    for gap in ("T9", "T10", "T11", "I5", "I8", "I9", "H3", "V3"):
        assert data["gaps"][gap]["fixed"] == "0.31.0"
    for gap in ("L1", "L2", "V6"):
        assert data["gaps"][gap]["fixed"] == "0.32.0"
    # A gap with only part of its row planned names no release
    for gap in ("V1", "I6", "M3"):
        assert "fixed" not in data["gaps"][gap]
    # No release plans H9 and M6, so the page keeps their process forms
    for gap in ("H9", "M6"):
        assert "fixed" not in data["gaps"][gap]
    for gap, info in data["unattached"].items():
        assert "fixed" not in info, gap
    page = gen_pages.compatibility_page()
    assert "Planned for 0.30.0" in page and "Fixed in" not in page


def test_gaps_that_belong_to_no_feature_are_named_on_the_page():
    """V4, V10, B1 and B3 are written in plain Python by a SimPy program, so no
    feature row holds them; a paragraph names each with its `!python` form."""
    data = gen_pages.constructs()
    page = gen_pages.compatibility_page()
    assert list(data["unattached"]) == ["V4", "V10", "B1", "B3"]
    used = {g for c in data["constructs"] for g in c.get("gaps", [])}
    for gap, info in data["unattached"].items():
        assert gap not in used and gap not in data["gaps"]
        assert f"**{gap}**, {info['text']}" in page
        assert info["python"] in page


def test_a_row_shown_native_has_no_gap_that_says_otherwise():
    """The constructs that the gap texts name carry those gaps, so they are not
    shown native: a peek, a step and a sync (T8), a cause and a user list (V7),
    a balk (I6), a container's amounts (T7), a held unit after
    `cancel()` (H9) and a process name across `next` (M6)."""
    rows = {c["id"]: c.get("gaps", []) for c in gen_pages.constructs()["constructs"]}
    for construct, gap in (
        ("SP-E8", "T8"),
        ("SP-E9", "T8"),
        ("SP-R4", "T8"),
        ("SP-P10", "V7"),
        ("SP-P10", "V3"),
        ("SP-S6", "V7"),
        ("SP-S11", "V7"),
        ("SP-S14", "I6"),
        ("SP-V4", "S3"),
        ("SP-P3", "M3"),
        ("SP-K1", "T7"),
        ("SP-K2", "T7"),
        ("SP-K4", "T7"),
        ("SP-E5", "S3"),
        ("SP-E6", "S3"),
        ("SP-P11", "S5"),
        ("SP-E3", "M6"),
        ("SP-P6", "M6"),
        ("SP-E17", "T8"),
        ("SP-S9", "H9"),
    ):
        assert gap in rows[construct], (construct, gap)
    assert "V9" not in rows["SP-P2"]
    # A container's `level: 5` reads as an int, and the Low-level queue types and
    # event classes give SimPy's result, so these rows stay native
    for construct in ("SP-K3", "SP-B6", "SP-B7"):
        assert rows[construct] == [], construct
    assert rows["SP-B2"] == ["L1"]
