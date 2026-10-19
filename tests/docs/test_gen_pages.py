"""The generated Reference pages follow the code: every family, every statistic and
every conformance check.

scripts/gen_pages.py builds the Distributions, Statistic Names and SimPy
Compatibility pages during the docs build. These tests check that each page lists
everything the code has, so a new family, statistic or conformance test cannot be
left off, or published without a meaning or a page.
"""

import ast
import importlib.util
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


def test_every_register_page_exists_and_every_group_has_a_heading():
    page = gen_pages.compatibility_page()
    for entry in gen_pages.register():
        assert entry["id"] in gen_pages.GROUPS
        if entry.get("status", "native") == "native":
            assert (ROOT / "docs" / entry["page"].split("#")[0]).is_file(), entry[
                "page"
            ]
            assert f"[{entry['operation']}](../{entry['page']})" in page
        else:
            assert entry["operation"] in page


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
    """Every SimPy feature is in exactly one row; a native row has checks and its
    three forms, a row not yet native names its release and has checks only for
    the APIs in its checked list, and a difference gives its reason."""
    assert len(INVENTORY) == len(set(INVENTORY)) == 98
    seen = [i for entry in gen_pages.register() for i in entry["inventory"]]
    assert sorted(seen) == sorted(INVENTORY)
    for entry in gen_pages.register():
        status = entry.get("status", "native")
        assert status in ("native", "not yet", "difference"), entry["operation"]
        if status == "native":
            assert entry["checks"], entry["operation"]
            assert all(entry[api] for api in ("yaml", "builder", "low_level"))
        elif status == "not yet":
            # Checks cover only the APIs named in checked, each of which exists
            checked = entry.get("checked", [])
            assert bool(entry["checks"]) == bool(checked), entry["operation"]
            assert set(checked) <= set(entry.get("exists", [])), entry["operation"]
            assert entry["release"], entry["operation"]
        else:
            assert not entry["checks"] and entry["differences"], entry["operation"]


def test_the_simpy_version_is_read_from_the_lock():
    """The page names the SimPy version in uv.lock, the one the tests run on."""
    import tomllib

    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    version = next(p["version"] for p in lock["package"] if p["name"] == "simpy")
    assert gen_pages.simpy_version() == version
    assert f"a plain SimPy {version} model" in gen_pages.compatibility_page()


def test_a_not_yet_row_shows_its_checks():
    """A feature not yet native in all three APIs, with a check of one API, shows
    that API as checked and names the check."""
    page = gen_pages.compatibility_page()
    row = next(line for line in page.splitlines() if line.startswith("| Run in "))
    assert "exists, checked" in row
    assert "test_c6_stopped_and_resumed_run" in row
