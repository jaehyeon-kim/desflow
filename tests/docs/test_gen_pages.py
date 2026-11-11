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
from desflow.models.distributions import DISTRIBUTIONS

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "gen_pages.py"
_spec = importlib.util.spec_from_file_location("gen_pages", _SCRIPT)
assert _spec is not None and _spec.loader is not None
gen_pages = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_pages)

# gen_pages puts scripts/ on the path, where the page modules are
from reference_pages import generators  # noqa: E402


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
    # The keys of a generator kind are on the page of that kind, the four keys every kind
    # accepts are on the Generators index, and a model only the kinds reach is on the page
    # of each kind that reaches it. Every other key is on YAML Keys.
    only = generators.generator_only_models()
    pages = gen_pages.generator_pages()
    for key in schema["properties"]:
        assert f"\n## {key}\n" in page
    assert "(../generators/index.md)" in page
    for name, node in schema["$defs"].items():
        for key in node.get("properties", {}):
            if name not in only:
                assert f"| `{key}` |" in page, f"{name}.{key}"
                continue
            owner = "Generator" if key in generators.MODIFIERS else name
            for path in generators.key_pages(owner):
                assert f"| `{key}` |" in pages[path], f"{name}.{key} on {path}"
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
                if entry.get("differences"):
                    assert entry["differences"] in page, entry["id"]
        else:
            # A gap replaces the reason of a difference in its row
            rows = [
                c
                for c in gen_pages.constructs()["constructs"]
                if c["id"] in entry["inventory"] and not c.get("gaps")
            ]
            if rows:
                assert entry["differences"] in page, entry["id"]


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


def test_the_page_has_one_section_for_each_area_and_one_entry_for_each_feature():
    """One section for each SimPy area, in SimPy's order, then a section for the
    features that are not native. Each of the 98 features has a sentence in bold saying
    what it does in SimPy, then its SimPy, YAML, Builder and Low-level forms as a list."""
    page = gen_pages.compatibility_page()
    data = gen_pages.constructs()
    headings = [line[3:] for line in page.splitlines() if line.startswith("## ")]
    assert headings == [*AREAS, "Where desflow Differs"]
    assert sorted(c["id"] for c in data["constructs"]) == sorted(INVENTORY)
    for construct in data["constructs"]:
        assert construct["description"].endswith("."), construct["id"]
        lead = f"\n**{construct['description']}**"
        form = f"\n\n- SimPy: {construct['simpy']}\n- YAML: "
        assert lead in page and form in page, construct["id"]
        assert page.index(form, page.index(lead)) > page.index(lead), construct["id"]
    areas = page[: page.index("\n## Where desflow Differs\n")]
    for label in ("SimPy", "YAML", "Builder", "Low-level"):
        assert areas.count(f"\n- {label}: ") == len(INVENTORY), label


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
    # 0.29.0 removes S1 to S5 from the page
    for gap in ("S1", "S2", "S3", "S4", "S5"):
        assert gap not in data["gaps"]
    # The run and time rules (T1 to T8, T13 and T14) are removed by Batch 1 of 1.0.0;
    # the T10 row is removed by Batch 8
    for number in (*range(1, 9), 13, 14):
        assert f"T{number}" not in data["gaps"]
    # Batch 4 of 1.0.0 removes V3 and V4
    for gap in ("V3", "V4"):
        assert gap not in data["gaps"] and gap not in data["unattached"]
    # Batch 2 of 1.0.0 removes T9, T11, I5, I8, I9 and H3
    for gap in ("T9", "T11", "I5", "I8", "I9", "H3"):
        assert gap not in data["gaps"]
    # Batch 3 of 1.0.0 removes L1, L2 and V6
    for gap in ("L1", "L2", "V6"):
        assert gap not in data["gaps"]
    # Batch 5 of 1.0.0 removes V1, V2, A1 and A4, and the plain-Python entry V10
    for gap in ("V1", "V2", "A1", "A4", "V10"):
        assert gap not in data["gaps"] and gap not in data["unattached"]
    # Batch 6 of 1.0.0 removes V7, V9, V11, V12, A3, A5, A7, M4 and M6
    for gap in ("V7", "V9", "V11", "V12", "A3", "A5", "A7", "M4", "M6"):
        assert gap not in data["gaps"]
    # Batch 8 of 1.0.0 removes H1, H5, H6, H8, H9, I10, T10, M5 and M7
    for gap in ("H1", "H5", "H6", "H8", "H9", "I10", "T10", "M5", "M7"):
        assert gap not in data["gaps"]
    # Batch 9 of 1.0.0 removes I3, I4, I6 and M3
    for gap in ("I3", "I4", "I6", "M3"):
        assert gap not in data["gaps"]
    # Batch 10 of 1.0.0 removes M1, and the plain-Python entries B1 and B3
    for gap in ("M1", "B1", "B3"):
        assert gap not in data["gaps"] and gap not in data["unattached"]
    for gap, info in data["unattached"].items():
        assert "fixed" not in info, gap
    page = gen_pages.compatibility_page()
    assert "Planned for" not in page and "Fixed in" not in page


def test_no_gap_is_left_that_belongs_to_no_feature():
    """Batch 10 of 1.0.0 removes B1 and B3, the last two gaps a SimPy program writes in
    plain Python, so `unattached` is empty and the page has no paragraph for it. A gap
    added there needs the paragraph back in `compatibility.py`."""
    data = gen_pages.constructs()
    assert list(data["unattached"]) == []
    page = gen_pages.compatibility_page()
    assert "belong to no SimPy feature" not in page


def test_a_row_shown_native_has_no_gap_that_says_otherwise():
    """The constructs that a gap text names carry that gap, so they are not shown
    native."""
    rows = {c["id"]: c.get("gaps", []) for c in gen_pages.constructs()["constructs"]}
    # A gap that remains shows on its rows
    assert rows["SP-V5"] == ["M2"]
    # Batch 10 gives the row of M1 its native status
    assert rows["SP-Q8"] == []
    # Batch 9 gives the rows of I3, I4, I6 and M3 their native status
    for construct in ("SP-S14", "SP-S13", "SP-P3", "SP-P9", "SP-V1", "SP-V6"):
        assert rows[construct] == [], construct
    # Batch 8 gives the rows of H1, H5, H6, H9, I10, T10 and M7 their native status
    for construct in ("SP-S9", "SP-P12", "SP-P7", "SP-S1", "SP-S12", "SP-Q4", "SP-V9"):
        assert rows[construct] == [], construct
    assert "V9" not in rows["SP-P2"]
    # A container's `level: 5` reads as an int, and the Low-level queue types and
    # event classes give SimPy's result, so these rows stay native
    for construct in (
        "SP-K1",
        "SP-K2",
        "SP-K3",
        "SP-K4",
        "SP-E8",
        "SP-E9",
        "SP-E17",
        "SP-R4",
        "SP-B6",
        "SP-B7",
    ):
        assert rows[construct] == [], construct
    assert rows["SP-B2"] == []
