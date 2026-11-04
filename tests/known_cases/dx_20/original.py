"""DX-20: A file sink that writes each event under its day.

The original is the snippet's own function: `daily_path` of
docs/snippets/yaml_build/routing_logic.py. `check` compares the `__path__` that the YAML form
puts in each finished order with what `daily_path` returns for the record.
"""

import sys
from pathlib import Path

HERE = Path(__file__).parent
SNIPPETS = HERE.parents[2] / "docs" / "snippets" / "yaml_build"


def check(records):
    """Each finished order's `__path__` is the path `daily_path` gives its record."""
    sys.path.insert(0, str(SNIPPETS))
    try:
        from routing_logic import daily_path
    finally:
        sys.path.remove(str(SNIPPETS))
    finished = [
        r
        for r in records
        if r["stream_type"] == "event"
        and r["value"].get("event_type") == "order_packed"
    ]
    assert finished
    for record in finished:
        assert record["value"]["__path__"] == daily_path(record)
