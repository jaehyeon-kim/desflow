"""The generated Reference pages follow the code: every family and every statistic.

scripts/gen_pages.py builds the Distributions and Statistic Names pages during the
docs build. These tests check that each page lists everything the code has, so a new
family or statistic cannot be left off, or published without a meaning.
"""

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
