"""Every block of records on a page is what a run of its code prints today.

scripts/record_blocks.py regenerates the blocks; this fails when one is out of date,
so a change to the records, such as a new field, cannot leave a page showing the old
shape.
"""

import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "record_blocks.py"
_spec = importlib.util.spec_from_file_location("record_blocks", _SCRIPT)
assert _spec is not None and _spec.loader is not None
record_blocks = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(record_blocks)


def test_every_marked_block_has_a_generator():
    assert set(record_blocks.used()) <= set(record_blocks.BLOCKS)


def test_every_generator_is_used_on_a_page():
    assert set(record_blocks.BLOCKS) <= set(record_blocks.used())


def test_every_block_is_up_to_date():
    stale = record_blocks.stale()
    assert stale == [], (
        "Run python scripts/record_blocks.py to regenerate: " + "; ".join(stale)
    )
