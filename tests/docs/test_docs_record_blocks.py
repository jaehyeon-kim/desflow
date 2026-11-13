"""Every block of records on a page is what a run of its code prints today.

scripts/record_blocks.py regenerates the blocks; this fails when one is out of date,
so a change to the records, such as a new field, cannot leave a page showing the old
shape.
"""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "record_blocks.py"
_spec = importlib.util.spec_from_file_location("record_blocks", _SCRIPT)
assert _spec is not None and _spec.loader is not None
record_blocks = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(record_blocks)


def test_every_marked_block_has_a_generator():
    assert set(record_blocks.used()) <= set(record_blocks.BLOCKS)


def test_every_generator_is_used_on_a_page():
    assert set(record_blocks.BLOCKS) <= set(record_blocks.used())


# One test per block, so the docs jobs and their workers share the runs out
@pytest.mark.parametrize("name", sorted(record_blocks.used()))
def test_block_is_up_to_date(name):
    lines = record_blocks.BLOCKS[name]()
    stale = [
        str(page.relative_to(record_blocks.ROOT))
        for page in record_blocks.used()[name]
        for match in record_blocks._MARKER.finditer(page.read_text(encoding="utf-8"))
        if match["name"] == name
        and match["body"] != "\n".join(match["indent"] + line for line in lines)
    ]
    assert stale == [], (
        f"Run python scripts/record_blocks.py to regenerate {name} on: "
        + ", ".join(stale)
    )


def test_a_block_does_not_read_the_callers_environment(monkeypatch):
    """The default run of a block shows the blueprint's own default, whatever the
    shell that runs the generator sets."""
    before = record_blocks.BLOCKS["stock-yaml"]()
    monkeypatch.setenv("DELIVERY", "40")
    assert record_blocks.BLOCKS["stock-yaml"]() == before


@pytest.mark.parametrize(
    "text, lines",
    [
        ("<!-- records: a -->\n```text\nx\n```\n", []),
        ("intro\n<!-- records: a -->\n```txt\nx\n```\n", [2]),
    ],
    ids=["matched", "another fence word"],
)
def test_a_marker_that_matches_no_block_is_found(text, lines):
    assert record_blocks.unmatched_markers(text) == lines
