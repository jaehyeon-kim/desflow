"""Every set of code tabs has YAML, Builder and Low-level, in that order.

`content.tabs.link` switches every set on the site when a reader picks a tab, so a set
with another name, another order or a missing tab breaks that, and a reader who follows
one way of writing finds a gap.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXPECTED = ["YAML", "Builder", "Low-level"]
# Tab sets that are not about the way a simulation is written.
OTHER_SETS = [["uv", "pip"]]


def _tab_sets(text):
    """Each set of tabs as its list of titles; a set nested in a tab is its own set."""
    sets, open_sets = [], {}  # indent -> titles of the set being read at that indent
    for line in text.split("\n"):
        m = re.match(r'^( *)=== "([^"]+)"', line)
        indent = len(line) - len(line.lstrip(" "))
        if m:
            for deeper in [i for i in open_sets if i > indent]:
                sets.append(open_sets.pop(deeper))
            open_sets.setdefault(indent, []).append(m.group(2))
            continue
        if line.strip():
            for open_indent in [i for i in open_sets if indent <= i]:
                sets.append(open_sets.pop(open_indent))
    sets.extend(open_sets.values())
    return sets


def test_every_tab_set_is_complete_and_in_order():
    wrong = []
    for page in sorted((ROOT / "docs").rglob("*.md")):
        for tabs in _tab_sets(page.read_text(encoding="utf-8")):
            if tabs != EXPECTED and tabs not in OTHER_SETS:
                wrong.append(f"{page.relative_to(ROOT)}: {tabs}")
    assert wrong == [], (
        "Tab sets must be " + ", ".join(EXPECTED) + ": " + "; ".join(wrong)
    )
